// src/reinforcement/service_orchestrator.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::sync::Arc;
use tokio::sync::Mutex; // Ensure Mutex is from tokio::sync
// use uuid::Uuid; // Uuid was only used in the removed log_decision_snapshot

use super::service_confidence_tuner::ServiceConfidenceTuner;
use super::service_feature_cache_service::SharedServiceFeatureCache;
use super::service_feature_extraction;
use crate::db::PgPool;
use crate::models::{MatchMethodType, ServiceId};
use crate::reinforcement::service::service_feedback_processor::process_service_feedback_for_tuner;

pub struct ServiceMatchingOrchestrator {
    pub confidence_tuner: ServiceConfidenceTuner,
    // Feature cache reference for *feature vectors*
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

    /// Set the feature cache (for feature vectors) to be used by this orchestrator.
    pub fn set_feature_cache(&mut self, cache: SharedServiceFeatureCache) {
        info!("Setting feature cache for ServiceMatchingOrchestrator");
        self.feature_cache = Some(cache);
    }

    /// Extracts the feature vector for a given service pair.
    /// This is a static method that directly calls the feature extraction logic.
    pub async fn extract_pair_context_features(
        pool: &PgPool,
        service1_id: &ServiceId,
        service2_id: &ServiceId,
    ) -> Result<Vec<f64>> {
        debug!(
            "ServiceOrchestrator: Extracting context features for pair ({}, {})",
            service1_id.0, service2_id.0
        );

        // Calls the updated feature extraction function
        service_feature_extraction::extract_context_for_service_pair(pool, service1_id, service2_id)
            .await
            .context(format!(
                "Failed to extract context for service pair ({},{})",
                service1_id.0, service2_id.0
            ))
    }

    /// Gets features for a service pair, using the *feature vector cache* if available.
    pub async fn get_pair_features(
        &self,
        pool: &PgPool,
        service1_id: &ServiceId,
        service2_id: &ServiceId,
    ) -> Result<Vec<f64>> {
        if let Some(cache) = &self.feature_cache {
            let mut cache_guard = cache.lock().await;
            // The SharedServiceFeatureCache's get_pair_features will internally call
            // Self::extract_pair_context_features if there's a cache miss.
            cache_guard
                .get_pair_features(pool, service1_id, service2_id)
                .await
        } else {
            // Fall back to direct extraction if no feature cache is set on the orchestrator
            warn!("ServiceMatchingOrchestrator: Feature cache not set. Extracting pair features directly.");
            Self::extract_pair_context_features(pool, service1_id, service2_id).await
        }
    }

    /// Gets a tuned confidence score from the ServiceConfidenceTuner.
    pub fn get_tuned_confidence(
        &self,
        method_type: &MatchMethodType,
        pre_rl_confidence_score: f64,
        context_features: &[f64], // These are the features obtained via get_pair_features
    ) -> Result<f64> {
        info!(
            "ServiceOrchestrator (ServiceConfidenceTuner v{}): Getting tuned confidence for method '{}', pre-RL score: {:.3}",
            self.confidence_tuner.version, method_type.as_str(), pre_rl_confidence_score
        );

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
                "default" // Ensure 'default' is defined in ServiceConfidenceTuner
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

    /// Method for manually triggering ServiceConfidenceTuner training/update cycle.
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

    /// Utility to get tuner stats - useful for reporting.
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

    /// Get statistics from the feature cache (for feature vectors) if available.
    pub async fn get_feature_cache_stats(&self) -> Option<(usize, usize, usize, usize)> {
        if let Some(cache) = &self.feature_cache {
            let cache_guard = cache.lock().await;
            Some(cache_guard.get_stats())
        } else {
            None
        }
    }
}
