//! Contributor-based filtering for entity matching pipeline
//! Allows filtering entities to only match those from specified contributor organizations

use anyhow::{Context, Result};
use log::{info, warn, debug};
use std::env;
use std::collections::HashSet;
use tokio_postgres::types::ToSql;

#[derive(Debug, Clone)]
pub struct ContributorFilterConfig {
    pub enabled: bool,
    pub allowed_contributors: Vec<String>,
}

impl ContributorFilterConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = env::var("CONTRIBUTOR_FILTER_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap_or(false);

        let allowed_contributors = if enabled {
            env::var("ALLOWED_CONTRIBUTORS")
                .unwrap_or_else(|_| String::new())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            Vec::new()
        };

        debug!("Contributor filter config: enabled={}, contributors={:?}", enabled, allowed_contributors);

        Self { enabled, allowed_contributors }
    }

    /// Build SQL WHERE clause for contributor filtering (without table alias)
    /// Use this for simple queries where no table alias is needed
    pub fn build_simple_sql_filter(&self) -> Option<(String, Vec<String>)> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return None;
        }

        let placeholders: Vec<String> = (1..=self.allowed_contributors.len())
            .map(|i| format!("${}", i))
            .collect();
        
        let where_clause = format!(
            "contributor = ANY(ARRAY[{}]::text[])",
            placeholders.join(", ")
        );

        Some((where_clause, self.allowed_contributors.clone()))
    }

    /// Build SQL WHERE clause for contributor filtering (with table alias "o")
    /// Use this for complex queries with joins where table alias is needed
    pub fn build_sql_filter(&self) -> Option<(String, Vec<String>)> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return None;
        }

        let placeholders: Vec<String> = (1..=self.allowed_contributors.len())
            .map(|i| format!("${}", i))
            .collect();
        
        let where_clause = format!(
            "o.contributor = ANY(ARRAY[{}]::text[])",
            placeholders.join(", ")
        );

        Some((where_clause, self.allowed_contributors.clone()))
    }

    /// Build SQL WHERE clause with parameter offset for complex queries
    pub fn build_sql_filter_with_offset(&self, param_offset: usize) -> Option<(String, Vec<String>)> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return None;
        }

        let placeholders: Vec<String> = (1..=self.allowed_contributors.len())
            .map(|i| format!("${}", i + param_offset))
            .collect();
        
        let where_clause = format!(
            "o.contributor = ANY(ARRAY[{}]::text[])",
            placeholders.join(", ")
        );

        Some((where_clause, self.allowed_contributors.clone()))
    }

    /// Build WHERE clause for entity-level filtering (when organization table is already joined)
    pub fn build_entity_filter_clause(&self, table_alias: &str, param_offset: usize) -> Option<(String, Vec<String>)> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return None;
        }

        let placeholders: Vec<String> = (1..=self.allowed_contributors.len())
            .map(|i| format!("${}", i + param_offset))
            .collect();
        
        let where_clause = format!(
            "{}.contributor = ANY(ARRAY[{}]::text[])",
            table_alias,
            placeholders.join(", ")
        );

        Some((where_clause, self.allowed_contributors.clone()))
    }

    /// Log the current configuration
    pub fn log_config(&self) {
        if self.enabled {
            info!("🔍 Contributor filtering ENABLED");
            info!("   Allowed contributors: {:?}", self.allowed_contributors);
            info!("   This will significantly reduce the entity set for matching");
        } else {
            info!("🔍 Contributor filtering DISABLED - matching all entities");
        }
    }

    /// Validate that the specified contributors exist in the database
    pub async fn validate_contributors(&self, pool: &crate::utils::db_connect::PgPool) -> Result<()> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return Ok(());
        }

        let conn = pool.get().await.context("Failed to get DB connection for contributor validation")?;

        let query = "
            SELECT contributor, COUNT(*) as count 
            FROM public.organization 
            WHERE contributor = ANY($1) 
            GROUP BY contributor 
            ORDER BY count DESC";

        let rows = conn.query(query, &[&self.allowed_contributors]).await
            .context("Failed to validate contributors")?;

        if rows.is_empty() {
            warn!("⚠️ No organizations found with specified contributors: {:?}", self.allowed_contributors);
            warn!("   Pipeline will process zero entities. Check contributor names and database content.");
            return Ok(());
        }

        info!("✅ Contributor validation results:");
        let mut total_orgs = 0;
        let mut found_contributors = HashSet::new();
        
        for row in rows {
            let contributor: String = row.get("contributor");
            let count: i64 = row.get("count");
            total_orgs += count;
            found_contributors.insert(contributor.clone());
            info!("   {}: {} organizations", contributor, count);
        }

        // Check for missing contributors
        let missing_contributors: Vec<String> = self.allowed_contributors
            .iter()
            .filter(|c| !found_contributors.contains(*c))
            .cloned()
            .collect();

        if !missing_contributors.is_empty() {
            warn!("⚠️ Specified contributors not found in database: {:?}", missing_contributors);
        }

        info!("   Total organizations to be processed: {}", total_orgs);

        // Also check how many entities this represents
        let entity_query = "
            SELECT COUNT(DISTINCT e.id) as entity_count
            FROM public.organization o
            JOIN public.entity e ON o.id = e.organization_id
            WHERE o.contributor = ANY($1)";

        let entity_rows = conn.query(entity_query, &[&self.allowed_contributors]).await
            .context("Failed to count filtered entities")?;

        if let Some(row) = entity_rows.first() {
            let entity_count: i64 = row.get("entity_count");
            info!("   Total entities to be processed: {}", entity_count);
        }

        Ok(())
    }

    /// Check if filtering is effectively enabled (both flag and contributors present)
    pub fn is_active(&self) -> bool {
        self.enabled && !self.allowed_contributors.is_empty()
    }

    /// Get contributors as a vector of ToSql trait objects for query parameters
    pub fn get_contributors_as_params(&self) -> Vec<Box<dyn ToSql + Sync + Send>> {
        self.allowed_contributors
            .iter()
            .map(|c| Box::new(c.clone()) as Box<dyn ToSql + Sync + Send>)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_config_from_env_disabled() {
        env::remove_var("CONTRIBUTOR_FILTER_ENABLED");
        env::remove_var("ALLOWED_CONTRIBUTORS");
        
        let config = ContributorFilterConfig::from_env();
        assert!(!config.enabled);
        assert!(config.allowed_contributors.is_empty());
    }

    #[test]
    fn test_config_from_env_enabled() {
        env::set_var("CONTRIBUTOR_FILTER_ENABLED", "true");
        env::set_var("ALLOWED_CONTRIBUTORS", "NE211,PATHCRI211");
        
        let config = ContributorFilterConfig::from_env();
        assert!(config.enabled);
        assert_eq!(config.allowed_contributors, vec!["NE211", "PATHCRI211"]);
        
        // Cleanup
        env::remove_var("CONTRIBUTOR_FILTER_ENABLED");
        env::remove_var("ALLOWED_CONTRIBUTORS");
    }

    #[test]
    fn test_simple_sql_filter_generation() {
        let config = ContributorFilterConfig {
            enabled: true,
            allowed_contributors: vec!["NE211".to_string(), "PATHCRI211".to_string()],
        };

        let (where_clause, params) = config.build_simple_sql_filter().unwrap();
        assert_eq!(where_clause, "contributor = ANY(ARRAY[$1, $2]::text[])");
        assert_eq!(params, vec!["NE211", "PATHCRI211"]);
    }

    #[test]
    fn test_sql_filter_generation() {
        let config = ContributorFilterConfig {
            enabled: true,
            allowed_contributors: vec!["NE211".to_string(), "PATHCRI211".to_string()],
        };

        let (where_clause, params) = config.build_sql_filter().unwrap();
        assert_eq!(where_clause, "o.contributor = ANY(ARRAY[$1, $2]::text[])");
        assert_eq!(params, vec!["NE211", "PATHCRI211"]);
    }

    #[test]
    fn test_sql_filter_with_offset() {
        let config = ContributorFilterConfig {
            enabled: true,
            allowed_contributors: vec!["NE211".to_string()],
        };

        let (where_clause, params) = config.build_sql_filter_with_offset(2).unwrap();
        assert_eq!(where_clause, "o.contributor = ANY(ARRAY[$3]::text[])");
        assert_eq!(params, vec!["NE211"]);
    }

    #[test]
    fn test_disabled_filter_returns_none() {
        let config = ContributorFilterConfig {
            enabled: false,
            allowed_contributors: vec!["NE211".to_string()],
        };

        assert!(config.build_simple_sql_filter().is_none());
        assert!(config.build_sql_filter().is_none());
    }
}