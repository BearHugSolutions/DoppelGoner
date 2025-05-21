// src/config.rs

// Thresholds for REVIEW_ENTITY_IN_GROUP suggestions
pub const MODERATE_LOW_SUGGESTION_THRESHOLD: f64 = 0.85;
pub const CRITICALLY_LOW_SUGGESTION_THRESHOLD: f64 = 0.30;

// Threshold for REVIEW_INTER_GROUP_LINK suggestions in consolidate_clusters
pub const WEAK_LINK_THRESHOLD: f64 = 0.1; // Example value

// Threshold for SUGGEST_SPLIT_CLUSTER suggestions in verify_clusters
pub const VERIFICATION_THRESHOLD: f64 = 0.7; // Example value

// Resolution parameter for Leiden algorithm in consolidate_clusters
pub const LEIDEN_RESOLUTION_PARAMETER: f64 = 1.0;

// Minimum final confidence for a geospatial pair to be formed
pub const MIN_GEO_PAIR_CONFIDENCE_THRESHOLD: f64 = 0.2; // Added constant

pub const SERVICE_WEAK_LINK_THRESHOLD: f64 = 0.15; // Slightly higher than entity threshold
pub const SERVICE_VERIFICATION_THRESHOLD: f64 = 0.65; // Slightly lower than entity threshold
pub const SERVICE_LEIDEN_RESOLUTION_PARAMETER: f64 = 1.2; // Higher resolution for more granular clusters