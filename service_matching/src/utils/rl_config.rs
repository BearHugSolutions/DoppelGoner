// src/utils/rl_config.rs
use std::env;
use log::{info, warn};

#[derive(Debug, Clone)]
pub struct RLConfig {
    pub enabled: bool,
    pub model_path: Option<String>,
}

impl RLConfig {
    pub fn from_env() -> Self {
        let enabled = env::var("RL_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap_or(false);

        let model_path = if enabled {
            env::var("RL_MODEL_PATH").ok()
        } else {
            None
        };

        Self { enabled, model_path }
    }

    pub fn log_config(&self) {
        if self.enabled {
            info!("🤖 RL (Reinforcement Learning) ENABLED");
            if let Some(ref path) = self.model_path {
                info!("   Model path: {}", path);
            } else {
                warn!("   No model path specified, using default");
            }
        } else {
            info!("🤖 RL (Reinforcement Learning) DISABLED - using pre-RL confidence scores");
        }
    }
}