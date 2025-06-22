// src/rl/mod.rs
//! Reinforcement Learning module for service matching
//! 
//! This module provides contextual bandit functionality for service matching,
//! including feature extraction, caching, and confidence tuning.

pub mod service_confidence_tuner;
pub mod service_feature_cache;
pub mod service_feature_extraction;
pub mod service_feedback_processor;
pub mod service_orchestrator;

// Re-exports for convenience
pub use service_confidence_tuner::ServiceConfidenceTuner;
pub use service_feature_cache::{ServiceFeatureCacheService, SharedServiceFeatureCache, create_shared_service_cache};
pub use service_feature_extraction::{
    extract_and_store_all_service_contextual_features, 
    extract_service_context_for_pair,
    get_service_feature_metadata
};
pub use service_feedback_processor::process_service_human_feedback_for_tuner;
pub use service_orchestrator::ServiceRLOrchestrator;

// Feature metadata for service RL
pub use service_feature_extraction::ServiceFeatureMetadata;