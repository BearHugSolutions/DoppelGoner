// src/reinforcement/entity/confidence_tuner.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue}; // Renamed Value to JsonValue
use std::collections::HashMap;
use uuid::Uuid;

use crate::db::PgPool; // Assuming this is the main pool type
use crate::models::MatchMethodType; // Assuming this is the correct path

// Represents one "arm" in the multi-armed bandit for a specific method and confidence level.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ConfidenceArm {
    target_confidence: f64, // The confidence level this arm represents/targets
    reward_sum: f64,
    trials: usize,
    // Optional: could store context features that led to this arm being chosen if needed for more complex updates
}

impl ConfidenceArm {
    pub fn new(confidence: f64) -> Self {
        Self {
            target_confidence: confidence,
            reward_sum: 0.0,
            trials: 0,
        }
    }

    pub fn update(&mut self, reward: f64) {
        self.trials += 1;
        self.reward_sum += reward;
    }

    fn average_reward(&self) -> f64 {
        if self.trials == 0 {
            0.0 // Avoid division by zero; could also be a small default positive value
        } else {
            self.reward_sum / self.trials as f64
        }
    }

    // UCB1 score calculation
    fn ucb_score(&self, total_parent_trials: usize) -> f64 {
        if self.trials == 0 {
            return f64::INFINITY; // Prioritize unexplored arms
        }
        let exploitation_term = self.average_reward();
        // Ensure total_parent_trials is at least 1 to avoid ln(0)
        let exploration_term =
            (2.0 * (total_parent_trials.max(1) as f64).ln() / self.trials as f64).sqrt();
        exploitation_term + exploration_term
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConfidenceTuner {
    // Key: method_type (e.g., "email", "name")
    // Value: Vector of arms, each representing a discrete confidence output level for that method.
    method_arms: HashMap<String, Vec<ConfidenceArm>>,
    epsilon: f64, // Probability of choosing a random arm (exploration)
    pub version: u32,
    // For V1, context_thresholds are removed as ContextModel is removed.
    // The selection logic will directly use context_features if needed.
}

impl ConfidenceTuner {
    pub fn new() -> Self {
        let mut method_arms = HashMap::new();
        let default_epsilon = 0.1; // 10% exploration rate

        // Define discrete confidence levels (arms) for each matching method.
        // These are the potential confidence scores the tuner can output.
        let arm_levels: HashMap<&str, Vec<f64>> = [
            ("email", vec![0.85, 0.90, 0.95, 0.98, 1.0]),
            ("phone", vec![0.80, 0.85, 0.90, 0.95, 0.98]),
            ("url", vec![0.80, 0.85, 0.90, 0.95]),
            ("address", vec![0.75, 0.80, 0.85, 0.90, 0.95]),
            ("name", vec![0.70, 0.75, 0.80, 0.85, 0.90, 0.92, 0.95]),
            ("geospatial", vec![0.70, 0.75, 0.80, 0.85, 0.90]),
            // A default for any other methods that might be introduced
            ("default", vec![0.70, 0.80, 0.85, 0.90]),
        ]
        .iter()
        .cloned()
        .collect();

        for (method_name, conf_levels) in arm_levels {
            let arms = conf_levels.into_iter().map(ConfidenceArm::new).collect();
            method_arms.insert(method_name.to_string(), arms);
        }

        Self {
            method_arms,
            epsilon: default_epsilon,
            version: 1,
        }
    }

    /// Selects a tuned confidence score for a given method and its context features.
    /// For V1, this uses UCB1 based on the method type. Context features are passed
    /// for future enhancements but not directly used in arm selection in this simplified V1.
    /// The `pre_rl_confidence` is also logged but not directly used by this UCB selection.
    pub fn select_confidence(
        &self,
        method_name: &str,
        _context_features: &[f64], // Available for future, more sophisticated arm selection
        _pre_rl_confidence: f64,   // Available for logging and future use
    ) -> f64 {
        // Create a fallback vector that lives for the duration of this function
        let fallback_arms = vec![ConfidenceArm::new(0.85)];

        let arms_for_method = match self.method_arms.get(method_name) {
            Some(arms) if !arms.is_empty() => arms,
            _ => {
                warn!(
                    "No arms defined for method '{}' or arms list is empty. Using default arms.",
                    method_name
                );
                self.method_arms.get("default").unwrap_or_else(|| {
                // This should ideally not happen if "default" is always in method_arms
                warn!("Critical: Default arms not found in ConfidenceTuner. Returning fixed default.");
                // Reference the locally-created vector instead of creating a temporary one
                &fallback_arms
            })
            }
        };

        let mut rng = rand::thread_rng();
        if rng.gen_bool(self.epsilon) {
            // Exploration: Pick a random arm for this method
            let random_arm_index = rng.gen_range(0..arms_for_method.len());
            let selected_confidence = arms_for_method[random_arm_index].target_confidence;
            debug!(
                "ConfidenceTuner (v{}): EXPLORE for method '{}'. Selected arm with confidence {:.3}",
                self.version, method_name, selected_confidence
            );
            selected_confidence
        } else {
            // Exploitation: Pick the best arm using UCB1
            let total_trials_for_method: usize = arms_for_method.iter().map(|arm| arm.trials).sum();

            let best_arm = arms_for_method
                .iter()
                .max_by(|a, b| {
                    a.ucb_score(total_trials_for_method)
                        .partial_cmp(&b.ucb_score(total_trials_for_method))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap_or_else(|| {
                    // Should not happen if arms_for_method is not empty
                    warn!(
                        "Could not determine best arm for method '{}'. Using first arm.",
                        method_name
                    );
                    &arms_for_method[0]
                });

            let selected_confidence = best_arm.target_confidence;
            debug!(
                "ConfidenceTuner (v{}): EXPLOIT for method '{}'. Selected arm with confidence {:.3} (UCB score calculation based on {} total trials for method)",
                self.version, method_name, selected_confidence, total_trials_for_method
            );
            selected_confidence
        }
    }

    /// Updates the tuner based on feedback for a specific decision.
    /// The `tuned_confidence_output` is the confidence score that was actually output by `select_confidence`
    /// for this specific instance.
    pub fn update(
        &mut self,
        method_name: &str,
        tuned_confidence_output: f64, // The confidence score that was selected/used
        reward: f64,                  // e.g., 1.0 for correct, 0.0 for incorrect
    ) -> Result<()> {
        let arms_for_method = match self.method_arms.get_mut(method_name) {
            Some(arms) => arms,
            None => {
                warn!("ConfidenceTuner (v{}): Method '{}' not found during update. Cannot update arms.", self.version, method_name);
                // Optionally, create arms for this new method on the fly, or use "default"
                // For now, we'll just return if the method is unknown to prevent altering "default" unintentionally.
                return Ok(());
            }
        };

        // Find the arm that corresponds to the `tuned_confidence_output`.
        // This assumes `tuned_confidence_output` is one of the `target_confidence` values.
        if let Some(arm_to_update) = arms_for_method
            .iter_mut()
            .find(|arm| (arm.target_confidence - tuned_confidence_output).abs() < 1e-9)
        // Compare f64
        {
            arm_to_update.update(reward);
            debug!(
                "ConfidenceTuner (v{}): Updated arm for method '{}' (target_confidence: {:.3}) with reward {:.1}. New avg reward: {:.3}, trials: {}",
                self.version, method_name, arm_to_update.target_confidence, reward, arm_to_update.average_reward(), arm_to_update.trials
            );
        } else {
            warn!(
                "ConfidenceTuner (v{}): Could not find matching arm for method '{}' with output confidence {:.3} during update. No arm updated.",
                self.version, method_name, tuned_confidence_output
            );
        }
        Ok(())
    }

    pub fn get_stats(&self) -> HashMap<String, Vec<(f64, f64, usize)>> {
        let mut stats = HashMap::new();
        for (method, arms) in &self.method_arms {
            let method_stats: Vec<(f64, f64, usize)> = arms
                .iter()
                .map(|arm| (arm.target_confidence, arm.average_reward(), arm.trials))
                .collect();
            stats.insert(method.clone(), method_stats);
        }
        stats
    }

    pub async fn save_to_db(&mut self, pool: &PgPool) -> Result<String> {
        let conn = pool
            .get()
            .await
            .context("Failed to get DB connection for ConfidenceTuner save")?;
        self.version += 1; // Increment version on save
        let model_json =
            serde_json::to_value(&*self).context("Failed to serialize ConfidenceTuner to JSON")?;

        let id_prefix = "confidence_tuner";
        // Try to find the latest existing ID for this model_type to maintain a consistent ID if possible
        let latest_model_row = conn.query_opt(
            "SELECT id FROM clustering_metadata.ml_models WHERE model_type = $1 ORDER BY version DESC LIMIT 1",
            &[&id_prefix]
        ).await.context("Failed to query for latest ConfidenceTuner model ID")?;

        let model_id = if let Some(row) = latest_model_row {
            row.get::<_, String>(0) // Use existing base ID
        } else {
            format!("{}_{}", id_prefix, Uuid::new_v4().to_string()) // Create new base ID
        };

        let parameters = json!({
            "epsilon": self.epsilon,
            "arm_definitions": self.method_arms.iter().map(|(k, v_arms)| (k.clone(), v_arms.iter().map(|a| a.target_confidence).collect::<Vec<_>>())).collect::<HashMap<_,_>>(),
        });

        let metrics_map: HashMap<String, JsonValue> = self
            .method_arms
            .iter()
            .map(|(method, arms)| {
                let arm_stats: Vec<JsonValue> = arms
                    .iter()
                    .map(|arm| {
                        json!({
                            "target_confidence": arm.target_confidence,
                            "average_reward": arm.average_reward(),
                            "trials": arm.trials
                        })
                    })
                    .collect();
                (method.clone(), JsonValue::Array(arm_stats))
            })
            .collect();
        let metrics = json!(metrics_map);

        conn.execute(
            "INSERT INTO clustering_metadata.ml_models
             (id, model_type, parameters, metrics, version, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (id) DO UPDATE
             SET parameters = EXCLUDED.parameters, metrics = EXCLUDED.metrics, version = EXCLUDED.version, updated_at = CURRENT_TIMESTAMP",
            &[&model_id, &id_prefix, &parameters, &metrics, &(self.version as i32)],
        ).await.context(format!("Failed to insert/update ConfidenceTuner metadata for ID {}", model_id))?;

        // Store the full serialized model separately for easy loading
        let binary_model_id = format!("{}_binary", model_id);
        conn.execute(
            "INSERT INTO clustering_metadata.ml_models
             (id, model_type, parameters, version, created_at, updated_at)
             VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (id) DO UPDATE
             SET parameters = EXCLUDED.parameters, version = EXCLUDED.version, updated_at = CURRENT_TIMESTAMP",
            &[&binary_model_id, &format!("{}_binary", id_prefix), &model_json, &(self.version as i32)],
        ).await.context(format!("Failed to insert/update ConfidenceTuner binary for ID {}", binary_model_id))?;

        info!(
            "Saved ConfidenceTuner (v{}) to database. Metadata ID: {}, Binary ID: {}",
            self.version, model_id, binary_model_id
        );
        Ok(model_id)
    }

    pub async fn load_from_db(pool: &PgPool) -> Result<Self> {
        let conn = pool
            .get()
            .await
            .context("Failed to get DB connection for ConfidenceTuner load")?;
        let id_prefix = "confidence_tuner";
        let binary_model_type = format!("{}_binary", id_prefix);

        let binary_row_opt = conn.query_opt(
            "SELECT parameters FROM clustering_metadata.ml_models WHERE model_type = $1 ORDER BY version DESC LIMIT 1",
            &[&binary_model_type]
        ).await.context("Failed to query for latest ConfidenceTuner binary")?;

        if let Some(binary_row) = binary_row_opt {
            let model_json: JsonValue = binary_row.get(0);
            let loaded_tuner: ConfidenceTuner = serde_json::from_value(model_json)
                .context("Failed to deserialize ConfidenceTuner from DB JSON")?;
            info!(
                "Loaded ConfidenceTuner (v{}) from database.",
                loaded_tuner.version
            );
            Ok(loaded_tuner)
        } else {
            info!("No existing ConfidenceTuner found in DB (type: {}). Creating new default instance.", binary_model_type);
            Ok(Self::new())
        }
    }
}
