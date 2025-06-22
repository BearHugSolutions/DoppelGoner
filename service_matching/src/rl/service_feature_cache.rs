// src/rl/service_feature_cache.rs
use anyhow::{Context, Result};
use log::info;
use lru::LruCache;
use std::num::NonZero;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::rl::service_feature_extraction::{extract_service_context_for_pair, get_stored_service_features};
use crate::utils::db_connect::PgPool;

// Default cache size - can be configured via environment variable
const DEFAULT_SERVICE_CACHE_SIZE: usize = 10000;

/// A service for caching service pair features to avoid redundant extraction.
/// This cache is specifically designed for RL contextual features for services.
pub struct ServiceFeatureCacheService {
    // Cache for individual service contextual features (service_id -> Vec<f64>)
    pub individual_cache: LruCache<String, Vec<f64>>,

    // Cache for pair contextual features (concat of sorted service_ids -> Vec<f64>)
    pub pair_cache: LruCache<String, Vec<f64>>,

    // Stats
    pub hits: usize,
    pub misses: usize,
    pub individual_hits: usize,
    pub individual_misses: usize,
}

impl ServiceFeatureCacheService {
    /// Create a new service feature cache service with default cache size
    pub fn new() -> Self {
        let cache_size = std::env::var("SERVICE_FEATURE_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_SERVICE_CACHE_SIZE);

        info!(
            "Initializing ServiceFeatureCacheService with cache size: {}",
            cache_size
        );

        Self {
            individual_cache: LruCache::new(NonZero::new(cache_size).unwrap()),
            pair_cache: LruCache::new(NonZero::new(cache_size).unwrap()),
            hits: 0,
            misses: 0,
            individual_hits: 0,
            individual_misses: 0,
        }
    }

    /// Get the cache key for a service pair
    pub fn get_pair_key(service1_id: &str, service2_id: &str) -> String {
        if service1_id < service2_id {
            format!("{}:{}", service1_id, service2_id)
        } else {
            format!("{}:{}", service2_id, service1_id)
        }
    }

    /// Get features without triggering extraction
    /// ADDED: New method to check cache without DB access
    pub fn get_pair_features_cached(&mut self, service1_id: &str, service2_id: &str) -> Option<Vec<f64>> {
        let key = Self::get_pair_key(service1_id, service2_id);
        self.pair_cache.get(&key).cloned()
    }
    
    /// Put features without DB access
    /// ADDED: New method to update cache without DB access
    pub fn put_pair_features(&mut self, service1_id: &str, service2_id: &str, features: Vec<f64>) {
        let key = Self::get_pair_key(service1_id, service2_id);
        self.pair_cache.put(key, features);
    }

    /// Get contextual features for a service pair, using cache if available.
    /// Returns the full feature vector for RL service matching decisions.
    pub async fn get_pair_features(
        &mut self,
        conn: &tokio_postgres::Client,
        service1_id: &str,
        service2_id: &str,
    ) -> Result<Vec<f64>> {
        let key = Self::get_pair_key(service1_id, service2_id);

        // Check if features are already in cache
        if let Some(features) = self.pair_cache.get(&key) {
            self.hits += 1;
            if self.hits % 100 == 0 {
                info!(
                    "ServiceFeatureCacheService stats - Pair cache hits: {}, misses: {}, hit rate: {:.2}%",
                    self.hits,
                    self.misses,
                    (self.hits as f64 / (self.hits + self.misses) as f64) * 100.0
                );
            }
            return Ok(features.clone());
        }

        // Features not in cache, need to extract using the provided connection
        self.misses += 1;
        let features = extract_service_context_for_pair(
            conn, service1_id, service2_id
        )
        .await
        .context(format!(
            "Failed to extract contextual features for service pair ({}, {})",
            service1_id, service2_id
        ))?;

        // Cache the features
        self.pair_cache.put(key, features.clone());

        Ok(features)
    }
    
    /// Get contextual features for an individual service, using cache if available.
    /// Returns the individual service feature vector.
    pub async fn get_individual_features(
        &mut self,
        conn: &tokio_postgres::Client,
        service_id: &str,
    ) -> Result<Vec<f64>> {
        // Check if features are already in cache
        if let Some(features) = self.individual_cache.get(service_id) {
            self.individual_hits += 1;
            if self.individual_hits % 100 == 0 {
                info!(
                    "ServiceFeatureCacheService stats - Individual cache hits: {}, misses: {}, hit rate: {:.2}%",
                    self.individual_hits,
                    self.individual_misses,
                    (self.individual_hits as f64 / (self.individual_hits + self.individual_misses) as f64) * 100.0
                );
            }
            return Ok(features.clone());
        }

        // Features not in cache, need to extract
        self.individual_misses += 1;
        let features = get_stored_service_features(conn, service_id)
            .await
            .context(format!(
                "Failed to extract individual contextual features for service {}",
                service_id
            ))?;

        // Cache the features
        self.individual_cache
            .put(service_id.to_string(), features.clone());

        Ok(features)
    }

    /// Get cache statistics
    pub fn get_stats(&self) -> (usize, usize, usize, usize) {
        (
            self.hits,
            self.misses,
            self.individual_hits,
            self.individual_misses,
        )
    }

    /// Clear the cache
    pub fn clear(&mut self) {
        self.pair_cache.clear();
        self.individual_cache.clear();
        self.hits = 0;
        self.misses = 0;
        self.individual_hits = 0;
        self.individual_misses = 0;
        info!("RL service feature cache cleared");
    }

    /// Get cache size and utilization information
    pub fn get_cache_info(&self) -> (usize, usize, usize, usize) {
        (
            self.individual_cache.len(),
            self.individual_cache.cap().get(),
            self.pair_cache.len(),
            self.pair_cache.cap().get(),
        )
    }
}

/// A thread-safe wrapper for the ServiceFeatureCacheService
pub type SharedServiceFeatureCache = Arc<Mutex<ServiceFeatureCacheService>>;

/// Create a new shared service feature cache for RL contextual features
pub fn create_shared_service_cache() -> SharedServiceFeatureCache {
    Arc::new(Mutex::new(ServiceFeatureCacheService::new()))
}
