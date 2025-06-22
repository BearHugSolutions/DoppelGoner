// src/reinforcement/entity/orchestrator.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};

use crate::models::stats_models::MatchMethodType;
use crate::rl::{confidence_tuner::ConfidenceTuner, SharedFeatureCache};
use crate::utils::db_connect::PgPool;

pub struct RLOrchestrator {
    pub confidence_tuner: ConfidenceTuner,
    // Add reference to the feature cache
    feature_cache: Option<SharedFeatureCache>,
}

impl RLOrchestrator {
    pub async fn new(pool: &PgPool) -> Result<Self> {
        let confidence_tuner = match ConfidenceTuner::load_from_db(pool).await {
            Ok(tuner) => {
                info!(
                    "Loaded confidence tuner from database version {}",
                    tuner.version
                );
                tuner
            }
            Err(e) => {
                warn!("Could not load confidence tuner: {}. Creating new one.", e);
                ConfidenceTuner::new()
            }
        };

        Ok(Self {
            confidence_tuner,
            feature_cache: None,
        })
    }

    /// Set the feature cache to be used by this orchestrator
    pub fn set_feature_cache(&mut self, cache: SharedFeatureCache) {
        info!("Setting feature cache for RLOrchestrator");
        self.feature_cache = Some(cache);
    }

    /// Extracts the 31-element feature vector for a given entity pair.
    /// Now checks the cache first if available.
    pub async fn extract_pair_context_features(
        pool: &PgPool,
        entity1_id: &str,
        entity2_id: &str,
    ) -> Result<Vec<f64>> {
        debug!(
            "Orchestrator: Extracting context features for pair ({}, {})",
            entity1_id, entity2_id
        );

        // This static method cannot use the instance's cache
        // Callers should use get_pair_features instead when possible
        super::feature_extraction::extract_context_for_pair(pool, entity1_id, entity2_id)
            .await
            .context(format!(
                "Failed to extract context for pair ({},{})",
                entity1_id, entity2_id
            ))
    }

    /// Gets features for an entity pair, using cache if available
    pub async fn get_pair_features(
        &self,
        pool: &PgPool,
        entity1_id: &str,
        entity2_id: &str,
    ) -> Result<Vec<f64>> {
        if let Some(cache) = &self.feature_cache {
            // Use cache if available
            let mut cache_guard = cache.lock().await;
            cache_guard
                .get_pair_features(pool, entity1_id, entity2_id)
                .await
        } else {
            // Fall back to direct extraction
            Self::extract_pair_context_features(pool, entity1_id, entity2_id).await
        }
    }

    /// Gets a tuned confidence score from the ConfidenceTuner.
    pub fn get_tuned_confidence(
        &self,
        method_type: &MatchMethodType,
        pre_rl_confidence_score: f64,
        context_features: &[f64],
    ) -> Result<f64> {
        info!(
            "Orchestrator (ConfidenceTuner v{}): Getting tuned confidence for method '{}', pre-RL score: {:.3}",
            self.confidence_tuner.version, method_type.as_str(), pre_rl_confidence_score
        );

        if context_features.len() != 31 {
            warn!(
                "Orchestrator: Expected 31 context features for ConfidenceTuner, got {}. This might lead to suboptimal tuning.",
                context_features.len()
            );
        }

        let tuned_confidence = self.confidence_tuner.select_confidence(
            method_type.as_str(),
            context_features,
            pre_rl_confidence_score,
        );

        info!(
            "Orchestrator: Method '{}', Pre-RL confidence: {:.3}, Tuned confidence: {:.3}",
            method_type.as_str(),
            pre_rl_confidence_score,
            tuned_confidence
        );
        Ok(tuned_confidence)
    }

    /// Saves the ConfidenceTuner model to the database.
    pub async fn save_models(&mut self, pool: &PgPool) -> Result<()> {
        info!(
            "Orchestrator: Saving ConfidenceTuner (v{}) to database.",
            self.confidence_tuner.version
        );
        let confidence_tuner_id = self.confidence_tuner.save_to_db(pool).await?;
        info!(
            "Orchestrator: Saved ConfidenceTuner with new version {}. DB Record ID base: {}",
            self.confidence_tuner.version, confidence_tuner_id
        );
        Ok(())
    }

    // Method for manually triggering ConfidenceTuner training/update cycle.
    pub async fn process_feedback_and_update_tuner(&mut self, pool: &PgPool) -> Result<()> {
        // TODO: Implement this
        unimplemented!()
    }

    // Utility to get tuner stats - useful for reporting
    pub fn get_confidence_tuner_stats(&self) -> String {
        self.confidence_tuner.get_stats_display()
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
