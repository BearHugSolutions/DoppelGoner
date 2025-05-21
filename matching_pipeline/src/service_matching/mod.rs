// src/service_matching/mod.rs

pub mod name;
pub mod url;
pub mod email;
pub mod embedding;

// Re-export find_matches functions for cleaner imports
pub use name::find_matches as find_matches_by_name;
pub use url::find_matches as find_matches_by_url;
pub use email::find_matches as find_matches_by_email;
pub use embedding::find_matches as find_matches_by_embedding;