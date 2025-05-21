// src/service_matching/mod.rs

pub mod email;
pub mod embedding;
pub mod name;
pub mod url;

// Re-export find_matches functions for cleaner imports
pub use email::find_matches as find_matches_by_email;
pub use embedding::find_matches as find_matches_by_embedding;
pub use name::find_matches as find_matches_by_name;
pub use url::find_matches as find_matches_by_url;
