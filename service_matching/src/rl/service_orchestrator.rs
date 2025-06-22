// src/rl/service_orchestrator.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use tokio_postgres::Client as PgConnection;

use crate::models::matching::MatchMethodType;
use crate::rl::{ServiceConfidenceTuner, SharedServiceFeatureCache};
use crate::utils::db_connect::PgPool;

pub struct ServiceRLOrchestrator {
    pub confidence_tuner: ServiceConfidenceTuner,
    // Add reference to the service feature cache
    feature_cache: Option<SharedServiceFeatureCache>,
}

impl ServiceRLOrchestrator {
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
                warn!("Could not load service confidence tuner: {}. Creating new one.", e);
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
        info!("Setting service feature cache for ServiceRLOrchestrator");
        self.feature_cache = Some(cache);
    }

    /// Extracts the service feature vector for a given service pair using a connection.
    /// This is the preferred method when a connection is available.
    pub async fn extract_service_pair_context_features_with_conn(
        conn: &PgConnection,
        service1_id: &str,
        service2_id: &str,
    ) -> Result<Vec<f64>> {
        debug!(
            "ServiceOrchestrator: Extracting context features for service pair ({}, {}) with connection",
            service1_id, service2_id
        );

        super::service_feature_extraction::extract_service_context_for_pair(conn, service1_id, service2_id)
            .await
            .context(format!(
                "Failed to extract context for service pair ({},{})",
                service1_id, service2_id
            ))
    }

    /// Gets features for a service pair using a connection, using cache if available.
    /// This is the preferred method when a connection is available.
    /// FIXED: Avoids holding lock during DB operations
    pub async fn get_service_pair_features_with_conn(
        &self,
        conn: &PgConnection,
        service1_id: &str,
        service2_id: &str,
    ) -> Result<Vec<f64>> {
        // 1. Check cache without holding lock during DB access
        let cached_features = if let Some(cache) = &self.feature_cache {
            let mut cache_guard = cache.lock().await;
            let features = cache_guard.get_pair_features_cached(service1_id, service2_id);
            drop(cache_guard); // Explicitly drop the lock
            features
        } else {
            None
        };
        
        if let Some(features) = cached_features {
            debug!("Cache hit for service pair ({}, {})", service1_id, service2_id);
            return Ok(features);
        }

        // 2. Fetch from DB if not in cache
        debug!("Cache miss for service pair ({}, {}), extracting features", service1_id, service2_id);
        let features = Self::extract_service_pair_context_features_with_conn(
            conn, service1_id, service2_id
        )
        .await
        .context(format!(
            "Failed to extract context for service pair ({},{})",
            service1_id, service2_id
        ))?;

        // 3. Update cache if available
        if let Some(cache) = &self.feature_cache {
            let mut cache_guard = cache.lock().await;
            cache_guard.put_pair_features(service1_id, service2_id, features.clone());
            drop(cache_guard); // Explicitly drop the lock
        }

        Ok(features)
    }

    /// Gets a tuned confidence score from the ServiceConfidenceTuner.
    pub fn get_tuned_confidence(
        &self,
        method_type: &MatchMethodType,
        pre_rl_confidence_score: f64,
        context_features: &[f64],
    ) -> Result<f64> {
        debug!(
            "ServiceOrchestrator (ServiceConfidenceTuner v{}): Getting tuned confidence for method '{}', pre-RL score: {:.3}",
            self.confidence_tuner.version, method_type.as_str(), pre_rl_confidence_score
        );

        if context_features.len() != 31 {
            warn!(
                "ServiceOrchestrator: Expected 31 context features for ServiceConfidenceTuner, got {}. This might lead to suboptimal tuning.",
                context_features.len()
            );
        }

        let tuned_confidence = self.confidence_tuner.select_confidence(
            method_type.as_str(),
            context_features,
            pre_rl_confidence_score,
        );

        debug!(
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

    // Method for manually triggering ServiceConfidenceTuner training/update cycle.
    pub async fn process_feedback_and_update_tuner(&mut self, pool: &PgPool) -> Result<()> {
        info!("Processing service human feedback for tuner update...");
        super::service_feedback_processor::process_service_human_feedback_for_tuner(pool, &mut self.confidence_tuner).await
    }

    // Utility to get tuner stats - useful for reporting
    pub fn get_confidence_tuner_stats(&self) -> String {
        self.confidence_tuner.get_stats_display()
    }

    /// Get statistics from the service feature cache if available
    pub async fn get_feature_cache_stats(&self) -> Option<(usize, usize, usize, usize)> {
        if let Some(cache) = &self.feature_cache {
            let cache_guard = cache.lock().await;
            let stats = cache_guard.get_stats();
            drop(cache_guard); // Explicitly drop the lock
            Some(stats)
        } else {
            None
        }
    }

    /// Update the tuner with feedback (for integration with matching methods)
    pub fn update_tuner(
        &mut self,
        method_name: &str,
        tuned_confidence: f64,
        reward: f64,
        context_features: &[f64],
    ) -> Result<()> {
        self.confidence_tuner.update(method_name, tuned_confidence, reward, context_features)
    }
}