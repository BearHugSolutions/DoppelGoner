// src/reinforcement/service_orchestrator.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::service_confidence_tuner::ServiceConfidenceTuner;
use super::service_feature_cache_service::SharedServiceFeatureCache;
use super::service_feature_extraction;
use crate::db::PgPool;
use crate::models::{MatchMethodType, ServiceId};
use crate::reinforcement::service::service_feedback_processor::process_service_feedback_for_tuner;
use crate::EntityId;

pub struct ServiceMatchingOrchestrator {
    pub confidence_tuner: ServiceConfidenceTuner,
    // Feature cache reference
    feature_cache: Option<SharedServiceFeatureCache>,
}

impl ServiceMatchingOrchestrator {
    pub async fn new(pool: &PgPool) -> Result<Self> {
        let confidence_tuner = match ServiceConfidenceTuner::load_from_db(pool).await {
            Ok(tuner) => {
                info!(
                    "Loaded service confidence tuner from database version {}",
                    tuner.version
                );
                tuner
            }
            Err(e) => {
                warn!(
                    "Could not load service confidence tuner: {}. Creating new one.",
                    e
                );
                ServiceConfidenceTuner::new()
            }
        };

        Ok(Self {
            confidence_tuner,
            feature_cache: None,
        })
    }

    /// Set the feature cache to be used by this orchestrator
    pub fn set_feature_cache(&mut self, cache: SharedServiceFeatureCache) {
        info!("Setting feature cache for ServiceMatchingOrchestrator");
        self.feature_cache = Some(cache);
    }

    /// Extracts the feature vector for a given service pair.
    pub async fn extract_pair_context_features(
        pool: &PgPool,
        service1_id: &ServiceId,
        service2_id: &ServiceId,
    ) -> Result<Vec<f64>> {
        debug!(
            "ServiceOrchestrator: Extracting context features for pair ({}, {})",
            service1_id.0, service2_id.0
        );

        service_feature_extraction::extract_context_for_service_pair(pool, service1_id, service2_id)
            .await
            .context(format!(
                "Failed to extract context for service pair ({},{})",
                service1_id.0, service2_id.0
            ))
    }

    /// Gets features for a service pair, using cache if available
    pub async fn get_pair_features(
        &self,
        pool: &PgPool,
        service1_id: &ServiceId,
        service2_id: &ServiceId,
    ) -> Result<Vec<f64>> {
        if let Some(cache) = &self.feature_cache {
            let mut cache_guard = cache.lock().await;
            cache_guard
                .get_pair_features(pool, &service1_id, &service2_id)
                .await
        } else {
            // Fall back to direct extraction
            Self::extract_pair_context_features(pool, service1_id, service2_id).await
        }
    }

    /// Gets a tuned confidence score from the ServiceConfidenceTuner.
    pub fn get_tuned_confidence(
        &self,
        method_type: &MatchMethodType,
        pre_rl_confidence_score: f64,
        context_features: &[f64],
    ) -> Result<f64> {
        info!(
            "ServiceOrchestrator (ServiceConfidenceTuner v{}): Getting tuned confidence for method '{}', pre-RL score: {:.3}",
            self.confidence_tuner.version, method_type.as_str(), pre_rl_confidence_score
        );

        // For service matching, map standard MatchMethodTypes to service-specific method names
        let service_method_name = match method_type {
            MatchMethodType::ServiceNameSimilarity => "service_name",
            MatchMethodType::ServiceUrlMatch => "service_url",
            MatchMethodType::ServiceEmailMatch => "service_email",
            MatchMethodType::ServiceEmbeddingSimilarity => "service_embedding",
            _ => {
                warn!(
                    "Using default service method type for unknown method: {}",
                    method_type.as_str()
                );
                "default"
            }
        };

        let tuned_confidence = self.confidence_tuner.select_confidence(
            service_method_name,
            context_features,
            pre_rl_confidence_score,
        );

        info!(
            "ServiceOrchestrator: Method '{}', Pre-RL confidence: {:.3}, Tuned confidence: {:.3}",
            method_type.as_str(),
            pre_rl_confidence_score,
            tuned_confidence
        );
        Ok(tuned_confidence)
    }

    /// Logs the details of a match decision (after a service_group is created).
    pub async fn log_decision_snapshot(
        &self,
        pool: &PgPool,
        service_group_id: &str,
        pipeline_run_id: &str,
        snapshotted_features: &Vec<f64>,
        method_type_at_decision: &MatchMethodType,
        pre_rl_confidence_at_decision: f64,
        tuned_confidence_at_decision: f64,
    ) -> Result<()> {
        let snapshot_features_json =
            serde_json::to_value(snapshotted_features).unwrap_or(serde_json::Value::Null);

        let confidence_tuner_ver = self.confidence_tuner.version;

        match insert_service_match_decision_detail(
            pool,
            service_group_id,
            pipeline_run_id,
            snapshot_features_json,
            method_type_at_decision.as_str(),
            pre_rl_confidence_at_decision,
            tuned_confidence_at_decision,
            confidence_tuner_ver,
        )
        .await
        {
            Ok(_) => {
                debug!(
                "ServiceOrchestrator: Logged decision snapshot for service_group_id: {}, method: {}, pre-RL conf: {:.3}, tuned conf: {:.3}, tuner_v: {}",
                service_group_id, method_type_at_decision.as_str(), pre_rl_confidence_at_decision, tuned_confidence_at_decision, confidence_tuner_ver
            );
            }
            Err(e) => {
                warn!("Failed to insert service match decision detail: {}. This is non-critical and the pipeline will continue.", e);
            }
        }

        Ok(())
    }

    /// Saves the ServiceConfidenceTuner model to the database.
    pub async fn save_models(&mut self, pool: &PgPool) -> Result<()> {
        info!(
            "ServiceOrchestrator: Saving ServiceConfidenceTuner (v{}) to database.",
            self.confidence_tuner.version
        );
        let confidence_tuner_id = self.confidence_tuner.save_to_db(pool).await?;
        info!(
            "ServiceOrchestrator: Saved ServiceConfidenceTuner with new version {}. DB Record ID base: {}",
            self.confidence_tuner.version, confidence_tuner_id
        );
        Ok(())
    }

    // Method for manually triggering ServiceConfidenceTuner training/update cycle.
    pub async fn process_feedback_and_update_tuner(&mut self, pool: &PgPool) -> Result<()> {
        info!(
            "ServiceOrchestrator: Starting process_feedback_and_update_tuner for ServiceConfidenceTuner (v{}).",
            self.confidence_tuner.version
        );
        process_service_feedback_for_tuner(pool, &mut self.confidence_tuner)
            .await
            .context("Failed during service feedback processing and tuner update cycle")?;
        info!("ServiceOrchestrator: Completed feedback processing and tuner update. ServiceConfidenceTuner is now at v{}.", self.confidence_tuner.version);
        self.save_models(pool)
            .await
            .context("Failed to save ServiceConfidenceTuner after feedback processing")?;
        Ok(())
    }

    // Utility to get tuner stats - useful for reporting
    pub fn get_confidence_tuner_stats(&self) -> String {
        let stats = self.confidence_tuner.get_stats();
        let mut output = format!(
            "Service Confidence Tuner (v{}) Statistics:\n",
            self.confidence_tuner.version
        );
        for (method, values) in stats {
            output.push_str(&format!("\nMethod: {}\n", method));
            output.push_str("  Target Confidence | Avg Reward | Trials\n");
            output.push_str("  -----------------|------------|-------\n");
            for (confidence, reward, trials) in values {
                output.push_str(&format!(
                    "        {:.3}       |   {:.4}   |  {}\n",
                    confidence, reward, trials
                ));
            }
        }
        output
    }

    /// Get statistics from the feature cache if available
    pub async fn get_feature_cache_stats(&self) -> Option<(usize, usize, usize, usize)> {
        if let Some(cache) = &self.feature_cache {
            let cache_guard = cache.lock().await;
            Some(cache_guard.get_stats())
        } else {
            None
        }
    }
}

/// Insert a snapshot of a service match decision.
async fn insert_service_match_decision_detail(
    pool: &PgPool,
    service_group_id: &str,
    pipeline_run_id: &str,
    snapshotted_features: serde_json::Value,
    method_type_at_decision: &str,
    pre_rl_confidence_at_decision: f64,
    tuned_confidence_at_decision: f64,
    confidence_tuner_version_at_decision: u32,
) -> Result<Uuid> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for insert_service_match_decision_detail")?;

    // Verify that the service_group exists
    let service_group_exists = conn
        .query_one(
            "SELECT COUNT(*) > 0 as exists FROM public.service_group WHERE id = $1",
            &[&service_group_id],
        )
        .await
        .context("Failed to verify service_group_id exists")?
        .get::<_, bool>("exists");

    if !service_group_exists {
        return Err(anyhow::anyhow!(
            "Service group with id {} does not exist",
            service_group_id
        ));
    }

    let id = Uuid::new_v4();
    let id_str = id.to_string();

    conn.execute(
        "INSERT INTO clustering_metadata.service_match_decision_details
         (id, service_group_id, pipeline_run_id, snapshotted_features,
          method_type_at_decision, pre_rl_confidence_at_decision,
          tuned_confidence_at_decision, confidence_tuner_version_at_decision)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &id_str,
            &service_group_id,
            &pipeline_run_id,
            &snapshotted_features,
            &method_type_at_decision,
            &pre_rl_confidence_at_decision,
            &tuned_confidence_at_decision,
            &(confidence_tuner_version_at_decision as i32), // Cast u32 to i32 for DB
        ],
    )
    .await
    .context("Failed to insert service_match_decision_detail")?;

    Ok(id)
}
