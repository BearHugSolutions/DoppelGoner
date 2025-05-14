// src/lib.rs
pub mod config;
pub mod consolidate_clusters;
pub mod db;
pub mod entity_organizations;
pub mod matching;
pub mod models;
pub mod reinforcement;
pub mod results;
pub mod service_matching;
pub mod utils;
pub mod cluster_visualization;

// Re-export common types for easier access
pub use models::{
    Entity, EntityGroup, EntityGroupId, EntityId, GroupCluster, GroupClusterId, MatchMethodType,
    ServiceMatchStatus,
};

// Re-export important functionality
pub use db::PgPool;
pub use reinforcement::MatchingOrchestrator;
