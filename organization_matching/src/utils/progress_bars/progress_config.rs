// src/utils/progress_config.rs

use indicatif::MultiProgress;
use std::env;

/// Configuration for progress tracking throughout the pipeline
#[derive(Debug, Clone)]
pub struct ProgressConfig {
    /// Whether to show progress bars at all
    pub enabled: bool,
    /// Whether to show detailed sub-progress bars
    pub detailed: bool,
    /// Refresh rate for progress bars in milliseconds
    pub refresh_rate_ms: u64,
    /// Whether to show memory usage in progress messages
    pub show_memory: bool,
    /// Whether to show cache statistics in progress messages
    pub show_cache_stats: bool,
    /// Whether to show database connection pool statistics
    pub show_db_connection_stats: bool, // NEW FIELD
    /// Maximum number of concurrent progress bars
    pub max_concurrent_bars: usize,
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            detailed: true,
            refresh_rate_ms: 100,
            show_memory: true,
            show_cache_stats: true,
            show_db_connection_stats: true, // Default to true
            max_concurrent_bars: 10,
        }
    }
}

impl ProgressConfig {
    /// Create progress configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("PROGRESS_ENABLED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            detailed: env::var("PROGRESS_DETAILED")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            refresh_rate_ms: env::var("PROGRESS_REFRESH_RATE_MS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            show_memory: env::var("PROGRESS_SHOW_MEMORY")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            show_cache_stats: env::var("PROGRESS_SHOW_CACHE_STATS")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            show_db_connection_stats: env::var("PROGRESS_SHOW_DB_CONNECTIONS") // NEW ENV VAR
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
            max_concurrent_bars: env::var("PROGRESS_MAX_CONCURRENT_BARS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),
        }
    }

    /// Create a MultiProgress instance if progress is enabled, None otherwise
    pub fn create_multi_progress(&self) -> Option<MultiProgress> {
        if self.enabled {
            Some(MultiProgress::new())
        } else {
            None
        }
    }

    /// Check if detailed progress should be shown
    pub fn should_show_detailed(&self) -> bool {
        self.enabled && self.detailed
    }

    /// Check if memory usage should be shown
    pub fn should_show_memory(&self) -> bool {
        self.enabled && self.show_memory
    }

    /// Check if cache statistics should be shown
    pub fn should_show_cache_stats(&self) -> bool {
        self.enabled && self.show_cache_stats
    }

    /// Check if DB connection statistics should be shown
    pub fn should_show_db_connection_stats(&self) -> bool { // NEW METHOD
        self.enabled && self.show_db_connection_stats
    }
}

/// Environment variable configuration example
pub fn print_env_config_example() {
    println!("# Progress Tracking Configuration");
    println!("# Set these environment variables to configure progress behavior:");
    println!();
    println!("# Enable/disable all progress bars (default: true)");
    println!("export PROGRESS_ENABLED=true");
    println!();
    println!("# Show detailed sub-progress bars (default: true)");
    println!("export PROGRESS_DETAILED=true");
    println!();
    println!("# Progress bar refresh rate in milliseconds (default: 100)");
    println!("export PROGRESS_REFRESH_RATE_MS=100");
    println!();
    println!("# Show memory usage in progress messages (default: true)");
    println!("export PROGRESS_SHOW_MEMORY=true");
    println!();
    println!("# Show cache hit/miss statistics (default: true)");
    println!("export PROGRESS_SHOW_CACHE_STATS=true");
    println!();
    println!("# Show database connection pool statistics (default: true)"); // NEW LINE
    println!("export PROGRESS_SHOW_DB_CONNECTIONS=true"); // NEW LINE
    println!();
    println!("# Maximum concurrent progress bars (default: 10)");
    println!("export PROGRESS_MAX_CONCURRENT_BARS=10");
    println!();
    println!("# For minimal output (CI/automated environments):");
    println!("export PROGRESS_ENABLED=false");
    println!("export PROGRESS_DETAILED=false");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_default_config() {
        let config = ProgressConfig::default();
        assert!(config.enabled);
        assert!(config.detailed);
        assert_eq!(config.refresh_rate_ms, 100);
        assert!(config.show_memory);
        assert!(config.show_cache_stats);
        assert!(config.show_db_connection_stats); // NEW ASSERT
        assert_eq!(config.max_concurrent_bars, 10);
    }

    #[test]
    fn test_env_config() {
        // Set test environment variables
        env::set_var("PROGRESS_ENABLED", "false");
        env::set_var("PROGRESS_DETAILED", "false");
        env::set_var("PROGRESS_REFRESH_RATE_MS", "50");
        env::set_var("PROGRESS_SHOW_MEMORY", "false");
        env::set_var("PROGRESS_SHOW_CACHE_STATS", "false");
        env::set_var("PROGRESS_SHOW_DB_CONNECTIONS", "false"); // NEW ENV VAR TEST
        env::set_var("PROGRESS_MAX_CONCURRENT_BARS", "5");

        let config = ProgressConfig::from_env();
        assert!(!config.enabled);
        assert!(!config.detailed);
        assert_eq!(config.refresh_rate_ms, 50);
        assert!(!config.show_memory);
        assert!(!config.show_cache_stats);
        assert!(!config.show_db_connection_stats); // NEW ASSERT
        assert_eq!(config.max_concurrent_bars, 5);

        // Clean up
        env::remove_var("PROGRESS_ENABLED");
        env::remove_var("PROGRESS_DETAILED");
        env::remove_var("PROGRESS_REFRESH_RATE_MS");
        env::remove_var("PROGRESS_SHOW_MEMORY");
        env::remove_var("PROGRESS_SHOW_CACHE_STATS");
        env::remove_var("PROGRESS_SHOW_DB_CONNECTIONS"); // CLEAN UP NEW ENV VAR
        env::remove_var("PROGRESS_MAX_CONCURRENT_BARS");
    }

    #[test]
    fn test_multi_progress_creation() {
        let mut config = ProgressConfig::default();

        // Should create MultiProgress when enabled
        config.enabled = true;
        assert!(config.create_multi_progress().is_some());

        // Should not create MultiProgress when disabled
        config.enabled = false;
        assert!(config.create_multi_progress().is_none());
    }

    #[test]
    fn test_should_show_methods() {
        let mut config = ProgressConfig::default();

        // All should be true when enabled and flags are true
        config.enabled = true;
        config.detailed = true;
        config.show_memory = true;
        config.show_cache_stats = true;
        config.show_db_connection_stats = true; // NEW ASSERT

        assert!(config.should_show_detailed());
        assert!(config.should_show_memory());
        assert!(config.should_show_cache_stats());
        assert!(config.should_show_db_connection_stats()); // NEW ASSERT

        // Should be false when disabled
        config.enabled = false;
        assert!(!config.should_show_detailed());
        assert!(!config.should_show_memory());
        assert!(!config.should_show_cache_stats());
        assert!(!config.should_show_db_connection_stats()); // NEW ASSERT

        // Should respect individual flags when enabled
        config.enabled = true;
        config.detailed = false;
        config.show_memory = false;
        config.show_cache_stats = false;
        config.show_db_connection_stats = false; // NEW ASSERT

        assert!(!config.should_show_detailed());
        assert!(!config.should_show_memory());
        assert!(!config.should_show_cache_stats());
        assert!(!config.should_show_db_connection_stats()); // NEW ASSERT
    }
}
