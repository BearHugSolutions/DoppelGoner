// src/rl/service_confidence_tuner.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use uuid::Uuid;

use crate::utils::db_connect::PgPool;

// Service feature vector size: 12 individual + 12 individual + 7+ pairwise = 31+ features
const SERVICE_FEATURE_VECTOR_SIZE: usize = 31;

// A lightweight logistic regression model trained via online gradient descent for services.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServiceOnlineLogisticRegression {
    // Service features + 1 bias term.
    weights: Vec<f64>,
    learning_rate: f64,
    trials: usize,
}

impl ServiceOnlineLogisticRegression {
    // Initializes the model with zero-weights.
    fn new(feature_count: usize) -> Self {
        Self {
            weights: vec![0.0; feature_count + 1], // +1 for the bias term
            learning_rate: 0.01,                   // A small, fixed learning rate
            trials: 0,
        }
    }

    // Predicts the probability of a positive reward (a correct service match).
    fn predict(&self, features: &[f64]) -> f64 {
        if features.len() != SERVICE_FEATURE_VECTOR_SIZE {
            warn!(
                "Expected {} service features, but got {}. Prediction will be unreliable.",
                SERVICE_FEATURE_VECTOR_SIZE,
                features.len()
            );
            return 0.5; // Return a neutral probability on error
        }
        // Add the bias term (1.0) to the features
        let features_with_bias = features.iter().chain(std::iter::once(&1.0));

        // Calculate the dot product of weights and features
        let logit: f64 = self
            .weights
            .iter()
            .zip(features_with_bias)
            .map(|(w, f)| w * f)
            .sum();

        // Apply the sigmoid function to get a probability between 0 and 1
        1.0 / (1.0 + (-logit).exp())
    }

    // Updates the model weights based on a single training example.
    fn update(&mut self, features: &[f64], reward: f64) {
        if features.len() != SERVICE_FEATURE_VECTOR_SIZE {
            warn!("Skipping service model update due to feature vector length mismatch.");
            return;
        }

        let prediction = self.predict(features);
        let error = reward - prediction;

        // Update each weight based on the error and the corresponding feature value
        for (i, feature_val) in features.iter().enumerate() {
            self.weights[i] += self.learning_rate * error * feature_val;
        }
        // Update the bias weight
        let bias_index = self.weights.len() - 1;
        self.weights[bias_index] += self.learning_rate * error; // Bias feature is always 1.0

        self.trials += 1;
    }
}

// Represents one "arm" in the service bandit. Contains its own predictive model.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServiceConfidenceArm {
    target_confidence: f64,
    // The simple reward tracking is replaced by a predictive model.
    model: ServiceOnlineLogisticRegression,
}

impl ServiceConfidenceArm {
    pub fn new(confidence: f64) -> Self {
        Self {
            target_confidence: confidence,
            model: ServiceOnlineLogisticRegression::new(SERVICE_FEATURE_VECTOR_SIZE),
        }
    }

    // The update now delegates to the internal model.
    pub fn update(&mut self, reward: f64, context_features: &[f64]) {
        self.model.update(context_features, reward);
    }

    // The UCB score now uses the model's prediction as the exploitation term.
    fn ucb_score(&self, total_parent_trials: usize, context_features: &[f64]) -> f64 {
        if self.model.trials == 0 {
            return f64::INFINITY; // Prioritize unexplored arms
        }
        // Exploitation term is now the predicted reward for the given context.
        let exploitation_term = self.model.predict(context_features);

        // Exploration term remains the same.
        let exploration_term =
            (2.0 * (total_parent_trials.max(1) as f64).ln() / self.model.trials as f64).sqrt();

        exploitation_term + exploration_term
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ServiceConfidenceTuner {
    method_arms: HashMap<String, Vec<ServiceConfidenceArm>>,
    epsilon: f64,
    pub version: u32,
}

impl ServiceConfidenceTuner {
    pub fn new() -> Self {
        let mut method_arms = HashMap::new();
        // Service-specific confidence levels for each matching method
        let arm_levels: HashMap<&str, Vec<f64>> = [
            // Service-specific matching methods with appropriate confidence ranges
            ("service_name", vec![0.75, 0.80, 0.85, 0.90, 0.95]),
            ("service_email", vec![0.85, 0.90, 0.95, 0.98, 1.0]),
            ("service_embedding", vec![0.85, 0.90, 0.95, 0.98]),
            ("service_location", vec![0.70, 0.75, 0.80, 0.85]),
            ("service_taxonomy", vec![0.80, 0.85, 0.90, 0.95]),
            ("default", vec![0.70, 0.80, 0.85, 0.90]),
        ]
        .iter()
        .cloned()
        .collect();

        for (method_name, conf_levels) in arm_levels {
            let arms = conf_levels.into_iter().map(ServiceConfidenceArm::new).collect();
            method_arms.insert(method_name.to_string(), arms);
        }

        Self {
            method_arms,
            epsilon: 0.1, // 10% exploration rate
            version: 1,   // Initial version for the service contextual bandit model
        }
    }

    // Selects a confidence score, now using the context features.
    pub fn select_confidence(
        &self,
        method_name: &str,
        context_features: &[f64],
        _pre_rl_confidence: f64,
    ) -> f64 {
        let fallback_arms = vec![ServiceConfidenceArm::new(0.85)];
        let arms_for_method = self
            .method_arms
            .get(method_name)
            .unwrap_or_else(|| self.method_arms.get("default").unwrap_or(&fallback_arms));

        if rand::thread_rng().gen_bool(self.epsilon) {
            // Exploration: Pick a random arm.
            let random_arm_index = rand::thread_rng().gen_range(0..arms_for_method.len());
            arms_for_method[random_arm_index].target_confidence
        } else {
            // Exploitation: Pick the best arm using the contextual UCB1 score.
            let total_trials_for_method: usize =
                arms_for_method.iter().map(|arm| arm.model.trials).sum();

            let best_arm = arms_for_method
                .iter()
                .max_by(|a, b| {
                    a.ucb_score(total_trials_for_method, context_features)
                        .partial_cmp(&b.ucb_score(total_trials_for_method, context_features))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap_or(&arms_for_method[0]);

            best_arm.target_confidence
        }
    }

    // Updates the tuner, now requiring the context_features to train the internal model.
    pub fn update(
        &mut self,
        method_name: &str,
        tuned_confidence_output: f64,
        reward: f64,
        context_features: &[f64], // This is now required
    ) -> Result<()> {
        let arms_for_method = self
            .method_arms
            .get_mut(method_name)
            .ok_or_else(|| anyhow::anyhow!("Service method '{}' not found during update.", method_name))?;

        if let Some(arm_to_update) = arms_for_method
            .iter_mut()
            .find(|arm| (arm.target_confidence - tuned_confidence_output).abs() < 1e-9)
        {
            // The update call now includes the features.
            arm_to_update.update(reward, context_features);
            debug!(
                "ServiceConfidenceTuner (v{}): Updated arm for method '{}' (target_confidence: {:.3}) with reward {:.1}",
                self.version, method_name, arm_to_update.target_confidence, reward
            );
        } else {
            warn!(
                "Could not find matching arm for service method '{}' with output confidence {:.3} during update.",
                method_name, tuned_confidence_output
            );
        }
        Ok(())
    }

    pub async fn save_to_db(&mut self, pool: &PgPool) -> Result<String> {
        let conn = pool.get().await.context("Failed to get DB connection")?;
        self.version += 1;
        let model_json =
            serde_json::to_value(&*self).context("Failed to serialize ServiceConfidenceTuner")?;

        let id_prefix = "service_confidence_tuner";
        let latest_model_row = conn.query_opt(
            "SELECT id FROM clustering_metadata.ml_models WHERE model_type = $1 ORDER BY version DESC LIMIT 1",
            &[&id_prefix]
        ).await.context("Failed to query for latest service model ID")?;

        let model_id = latest_model_row.map_or_else(
            || format!("{}_{}", id_prefix, Uuid::new_v4()),
            |row| row.get(0),
        );

        // For the contextual bandit, the weights ARE the model, so we store them.
        let parameters = json!({
            "model_type": "ServiceContextualBandit",
            "epsilon": self.epsilon,
            "learning_rate": self.method_arms.values().next().and_then(|v| v.first()).map(|a| a.model.learning_rate),
            "feature_count": SERVICE_FEATURE_VECTOR_SIZE,
            "service_methods": self.method_arms.keys().collect::<Vec<_>>(),
        });

        // Metrics will now store a sample of the weights for inspection.
        let metrics_map: HashMap<String, JsonValue> = self
            .method_arms
            .iter()
            .map(|(method, arms)| {
                let arm_stats: Vec<JsonValue> = arms
                    .iter()
                    .map(|arm| {
                        json!({
                            "target_confidence": arm.target_confidence,
                            "trials": arm.model.trials,
                            "model_weights_sample": arm.model.weights.iter().take(5).collect::<Vec<_>>(), // Sample of weights
                        })
                    })
                    .collect();
                (method.clone(), JsonValue::Array(arm_stats))
            })
            .collect();
        let metrics = json!(metrics_map);

        let binary_model_id = format!("{}_binary", model_id);

        // Save metadata
        conn.execute(
            "INSERT INTO clustering_metadata.ml_models (id, model_type, parameters, metrics, version, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
             ON CONFLICT (id) DO UPDATE SET parameters = EXCLUDED.parameters, metrics = EXCLUDED.metrics, version = EXCLUDED.version, updated_at = CURRENT_TIMESTAMP",
            &[&model_id, &id_prefix, &parameters, &metrics, &(self.version as i32)],
        ).await?;

        // Save full model
        conn.execute(
            "INSERT INTO clustering_metadata.ml_models (id, model_type, parameters, version)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE SET parameters = EXCLUDED.parameters, version = EXCLUDED.version",
            &[&binary_model_id, &format!("{}_binary", id_prefix), &model_json, &(self.version as i32)],
        ).await?;

        Ok(model_id)
    }

    pub async fn load_from_db(pool: &PgPool) -> Result<Self> {
        let conn = pool.get().await.context("Failed to get DB connection")?;
        let id_prefix = "service_confidence_tuner";
        let binary_model_type = format!("{}_binary", id_prefix);

        let binary_row_opt = conn.query_opt(
            "SELECT parameters FROM clustering_metadata.ml_models WHERE model_type = $1 ORDER BY version DESC LIMIT 1",
            &[&binary_model_type]
        ).await?;

        if let Some(binary_row) = binary_row_opt {
            let model_json: JsonValue = binary_row.get(0);
            let loaded_tuner: ServiceConfidenceTuner = serde_json::from_value(model_json)?;

            info!(
                "Loaded Service Contextual Bandit (v{}) from database.",
                loaded_tuner.version
            );
            Ok(loaded_tuner)
        } else {
            info!("No existing ServiceConfidenceTuner model found. Creating new Service Contextual Bandit (v1) model.");
            Ok(Self::new())
        }
    }

    // The stats display is simplified to show trials, as reward is now context-dependent.
    pub fn get_stats_display(&self) -> String {
        let mut output = format!(
            "Service Contextual Confidence Tuner (v{}) Statistics:\n",
            self.version
        );
        for (method, arms) in &self.method_arms {
            output.push_str(&format!("\nService Method: {}\n", method));
            output.push_str("  Target Confidence | Trials\n");
            output.push_str("  -----------------|-------\n");
            for arm in arms {
                output.push_str(&format!(
                    "        {:.3}       |  {}\n",
                    arm.target_confidence, arm.model.trials
                ));
            }
        }
        output
    }
}