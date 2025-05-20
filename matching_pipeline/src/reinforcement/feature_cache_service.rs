// src/reinforcement/feature_cache_service.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use lru::LruCache;
use std::num::NonZero;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::db::PgPool;
use crate::models::EntityId;

use super::feature_extraction;

// Default cache size - can be configured via environment variable
const DEFAULT_CACHE_SIZE: usize = 20000;

/// A service for caching entity pair features to avoid redundant extraction
pub struct FeatureCacheService {
    // Cache for individual entity features (entity_id -> Vec<f64>)
    individual_cache: LruCache<String, Vec<f64>>,
    
    // Cache for pair features (concat of sorted entity_ids -> Vec<f64>)
    pair_cache: LruCache<String, Vec<f64>>,
    
    // Stats
    hits: usize,
    misses: usize,
    individual_hits: usize,
    individual_misses: usize,
}

impl FeatureCacheService {
    /// Create a new feature cache service with default cache size
    pub fn new() -> Self {
        let cache_size = std::env::var("FEATURE_CACHE_SIZE")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_CACHE_SIZE);
            
        info!("Initializing FeatureCacheService with cache size: {}", cache_size);
        
        Self {
            individual_cache: LruCache::new(NonZero::new(cache_size).unwrap()),
            pair_cache: LruCache::new(NonZero::new(cache_size).unwrap()),
            hits: 0,
            misses: 0,
            individual_hits: 0,
            individual_misses: 0,
        }
    }
    
    /// Get the cache key for an entity pair
    fn get_pair_key(entity1_id: &EntityId, entity2_id: &EntityId) -> String {
        if entity1_id.0 < entity2_id.0 {
            format!("{}:{}", entity1_id.0, entity2_id.0)
        } else {
            format!("{}:{}", entity2_id.0, entity1_id.0)
        }
    }
    
    /// Get features for an entity pair, using cache if available
    pub async fn get_pair_features(
        &mut self,
        pool: &PgPool,
        entity1_id: &EntityId,
        entity2_id: &EntityId,
    ) -> Result<Vec<f64>> {
        let key = Self::get_pair_key(entity1_id, entity2_id);
        
        // Check if features are already in cache
        if let Some(features) = self.pair_cache.get(&key) {
            self.hits += 1;
            if self.hits % 100 == 0 {
                info!(
                    "FeatureCacheService stats - Pair cache hits: {}, misses: {}, hit rate: {:.2}%",
                    self.hits,
                    self.misses,
                    (self.hits as f64 / (self.hits + self.misses) as f64) * 100.0
                );
            }
            return Ok(features.clone());
        }
        
        // Features not in cache, need to extract
        self.misses += 1;
        let features = feature_extraction::extract_context_for_pair(pool, entity1_id, entity2_id)
            .await
            .context(format!(
                "Failed to extract features for pair ({}, {})",
                entity1_id.0, entity2_id.0
            ))?;
            
        // Cache the features
        self.pair_cache.put(key, features.clone());
        
        Ok(features)
    }
    
    /// Get features for an individual entity, using cache if available
    pub async fn get_individual_features(
        &mut self, 
        conn: &tokio_postgres::Client,
        entity_id: &EntityId
    ) -> Result<Vec<f64>> {
        // Check if features are already in cache
        if let Some(features) = self.individual_cache.get(&entity_id.0) {
            self.individual_hits += 1;
            if self.individual_hits % 100 == 0 {
                info!(
                    "FeatureCacheService stats - Individual cache hits: {}, misses: {}, hit rate: {:.2}%",
                    self.individual_hits,
                    self.individual_misses,
                    (self.individual_hits as f64 / (self.individual_hits + self.individual_misses) as f64) * 100.0
                );
            }
            return Ok(features.clone());
        }
        
        // Features not in cache, need to extract
        self.individual_misses += 1;
        let features = feature_extraction::get_stored_entity_features(conn, entity_id)
            .await
            .context(format!(
                "Failed to extract individual features for entity {}",
                entity_id.0
            ))?;
            
        // Cache the features
        self.individual_cache.put(entity_id.0.clone(), features.clone());
        
        Ok(features)
    }
    
    /// Get cache statistics
    pub fn get_stats(&self) -> (usize, usize, usize, usize) {
        (self.hits, self.misses, self.individual_hits, self.individual_misses)
    }
    
    /// Clear the cache
    pub fn clear(&mut self) {
        self.pair_cache.clear();
        self.individual_cache.clear();
        self.hits = 0;
        self.misses = 0;
        self.individual_hits = 0;
        self.individual_misses = 0;
        info!("Feature cache cleared");
    }
}

/// A thread-safe wrapper for the FeatureCacheService
pub type SharedFeatureCache = Arc<Mutex<FeatureCacheService>>;

/// Create a new shared feature cache
pub fn create_shared_cache() -> SharedFeatureCache {
    Arc::new(Mutex::new(FeatureCacheService::new()))
}