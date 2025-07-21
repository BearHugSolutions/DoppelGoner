use anyhow::{Context, Result};
use log::{info, warn};
use std::env;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ServiceContributorFilterConfig {
    pub enabled: bool,
    pub allowed_contributors: Vec<String>,
}

impl ServiceContributorFilterConfig {
    pub fn from_env() -> Self {
        let enabled = env::var("SERVICE_CONTRIBUTOR_FILTER_ENABLED")
            .unwrap_or_else(|_| "false".to_string())
            .parse::<bool>()
            .unwrap_or(false);

        let allowed_contributors = if enabled {
            env::var("SERVICE_ALLOWED_CONTRIBUTORS")
                .unwrap_or_else(|_| String::new())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            Vec::new()
        };

        Self { enabled, allowed_contributors }
    }

    pub fn build_service_sql_filter(&self) -> Option<(String, Vec<String>)> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return None;
        }

        let placeholders: Vec<String> = (1..=self.allowed_contributors.len())
            .map(|i| format!("${}", i))
            .collect();
        
        let where_clause = format!(
            "s.source_system = ANY(ARRAY[{}]::text[])",
            placeholders.join(", ")
        );

        Some((where_clause, self.allowed_contributors.clone()))
    }

    pub fn build_service_sql_filter_with_offset(&self, param_offset: usize) -> Option<(String, Vec<String>)> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return None;
        }

        let placeholders: Vec<String> = (1..=self.allowed_contributors.len())
            .map(|i| format!("${}", i + param_offset))
            .collect();
        
        let where_clause = format!(
            "s.source_system = ANY(ARRAY[{}]::text[])",
            placeholders.join(", ")
        );

        Some((where_clause, self.allowed_contributors.clone()))
    }

    pub fn log_config(&self) {
        if self.enabled {
            info!("🔍 Service contributor filtering ENABLED");
            info!("   Allowed contributors: {:?}", self.allowed_contributors);
        } else {
            info!("🔍 Service contributor filtering DISABLED - matching all services");
        }
    }

    pub async fn validate_contributors(&self, pool: &crate::utils::db_connect::PgPool) -> Result<()> {
        if !self.enabled || self.allowed_contributors.is_empty() {
            return Ok(());
        }

        let conn = pool.get().await.context("Failed to get DB connection")?;

        let query = "
            SELECT source_system, COUNT(*) as count 
            FROM public.service 
            WHERE source_system = ANY($1) 
            GROUP BY source_system 
            ORDER BY count DESC";

        let rows = conn.query(query, &[&self.allowed_contributors]).await
            .context("Failed to validate service contributors")?;

        if rows.is_empty() {
            warn!("⚠️ No services found with specified contributors: {:?}", self.allowed_contributors);
            return Ok(());
        }

        info!("✅ Service contributor validation results:");
        let mut total_services = 0;
        for row in rows {
            let contributor: String = row.get("source_system");
            let count: i64 = row.get("count");
            total_services += count;
            info!("   {}: {} services", contributor, count);
        }

        info!("   Total services to be processed: {}", total_services);
        Ok(())
    }
}