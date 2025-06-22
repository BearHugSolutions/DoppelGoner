// src/entity_identification/mod.rs
pub mod entity_id;

// Re-export main functions for clean API
pub use entity_id::{identify_entities, link_and_update_entity_features};
