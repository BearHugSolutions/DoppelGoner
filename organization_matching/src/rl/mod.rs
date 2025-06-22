// src/rl/mod.rs
pub mod confidence_tuner;
pub mod feature_cache;
pub mod feature_extraction;
pub mod feedback_processor;
pub mod orchestrator;

// Re-export main RL functions for clean API
pub use feature_extraction::{
    extract_and_store_all_contextual_features, extract_context_for_pair, get_feature_metadata,
    get_stored_entity_features,
};

pub use feature_cache::{create_shared_cache, FeatureCacheService, SharedFeatureCache};
