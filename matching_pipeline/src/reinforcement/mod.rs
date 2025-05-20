// src/reinforcement/mod.rs (updated to include feature_cache_service)

// Module declarations
mod confidence_tuner;
mod feature_extraction;
pub mod feedback_processor; // Public for main pipeline to trigger feedback processing
mod orchestrator;
mod types;
mod feature_cache_service; // New module

// Public re-exports - V1 API
pub use confidence_tuner::ConfidenceTuner;

pub use feature_extraction::{
    extract_context_for_pair, // Extracts the 31 features for a pair
    get_feature_metadata,
    get_stored_entity_features, // Gets 12 individual features, ensures extraction if not stored
                                // extract_entity_features is now a private helper within feature_extraction.rs
};

pub use orchestrator::MatchingOrchestrator;

// Export the feature cache service
pub use feature_cache_service::{create_shared_cache, FeatureCacheService, SharedFeatureCache};

// Core types used across the reinforcement learning system
pub use types::{
    ConfidenceClass,
    ExperimentResult,
    FeatureMetadata,
    HumanFeedbackDataForTuner, // Renamed from FeedbackItem for clarity in its new role
    ModelMetrics,
};