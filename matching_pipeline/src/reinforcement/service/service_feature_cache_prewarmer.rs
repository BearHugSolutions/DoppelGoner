// src/reinforcement/service/service_feature_cache_prewarmer.rs

use anyhow::{Context, Result};
use futures::future;
use log::{debug, info, warn};
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_postgres::types::Json;

use crate::db::PgPool;
use crate::models::ServiceId;
use crate::reinforcement::service::service_feature_cache_service::{
    ServiceFeatureCacheService, SharedServiceFeatureCache,
};
use crate::reinforcement::service::service_feature_extraction::{
    self, get_stored_service_features,
};

// Maximum batch size to process at once
const MAX_BATCH_SIZE: usize = 20;

// Calculate number of batches based on CPU cores
fn calculate_batch_size(pair_count: usize) -> usize {
    let cpu_count = num_cpus::get();
    let optimal_batch_size = (pair_count / cpu_count).max(1).min(MAX_BATCH_SIZE);
    optimal_batch_size
}

/// Struct to hold various batch priorities for prewarming
pub struct ServicePriorityBatches {
    high_confidence_pairs: Vec<(ServiceId, ServiceId, f64)>,
    taxonomy_similar_pairs: Vec<(ServiceId, ServiceId, f64)>,
    same_organization_pairs: Vec<(ServiceId, ServiceId, f64)>,
    name_similar_pairs: Vec<(ServiceId, ServiceId, f64)>,
    cached_feature_pairs: Vec<(ServiceId, ServiceId, Vec<f64>)>,
}

impl ServicePriorityBatches {
    pub fn new() -> Self {
        Self {
            high_confidence_pairs: Vec::new(),
            taxonomy_similar_pairs: Vec::new(),
            same_organization_pairs: Vec::new(),
            name_similar_pairs: Vec::new(),
            cached_feature_pairs: Vec::new(),
        }
    }

    pub fn total_pairs(&self) -> usize {
        self.high_confidence_pairs.len()
            + self.taxonomy_similar_pairs.len()
            + self.same_organization_pairs.len()
            + self.name_similar_pairs.len()
            + self.cached_feature_pairs.len()
    }
}

pub async fn extract_and_store_all_service_context_features(
    pool: &PgPool,
    feature_cache: &SharedServiceFeatureCache,
) -> Result<usize> {
    let conn = pool.get().await?;

    // Get all service IDs
    let rows = conn.query("SELECT id FROM public.service", &[]).await?;
    let service_ids: Vec<ServiceId> = rows.iter().map(|row| ServiceId(row.get(0))).collect();

    info!(
        "Found {} services for feature extraction",
        service_ids.len()
    );

    // Process in batches
    let batch_size = 100;
    let mut processed_count = 0;

    for chunk in service_ids.chunks(batch_size) {
        // Create a new connection for this batch
        let batch_conn = pool.get().await?;

        // Process each service in this batch
        for service_id in chunk {
            // Get or extract features - this will store them in the DB if needed
            match get_stored_service_features(&*batch_conn, service_id).await {
                Ok(features) => {
                    // Also populate the in-memory cache with these features
                    let mut cache_guard = feature_cache.lock().await;
                    let cache_key = service_id.0.clone();
                    cache_guard.individual_cache.put(cache_key, features);
                    processed_count += 1;
                }
                Err(e) => {
                    warn!(
                        "Failed to extract features for service {}: {}",
                        service_id.0, e
                    );
                }
            }
        }

        info!(
            "Processed {}/{} services",
            processed_count,
            service_ids.len()
        );
    }

    // Cache statistics for observability
    {
        let cache_guard = feature_cache.lock().await;
        let (_, _, ind_hits, ind_misses) = cache_guard.get_stats();
        info!(
            "Individual service features cache stats after pre-extraction: hits={}, misses={}",
            ind_hits, ind_misses
        );
    }

    Ok(processed_count)
}

pub async fn extract_and_store_all_service_features_and_prewarm_cache(
    pool: &PgPool,
    feature_cache: &SharedServiceFeatureCache, // The service-specific shared cache
) -> Result<usize> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for pre-warming service cache")?;

    // Get all service IDs
    let service_rows = conn
        .query("SELECT id FROM public.service", &[])
        .await
        .context("Failed to query service IDs for cache pre-warming")?;

    let service_ids: Vec<ServiceId> = service_rows
        .iter()
        .map(|row| ServiceId(row.get("id")))
        .collect();

    info!(
        "Found {} services for context feature extraction and cache pre-warming",
        service_ids.len()
    );

    let batch_size = 100; // Configurable batch size
    let mut processed_count = 0;
    let total_services = service_ids.len();

    for chunk in service_ids.chunks(batch_size) {
        let mut batch_futures: Vec<tokio::task::JoinHandle<Result<usize, anyhow::Error>>> =
            Vec::new();

        for service_id_in_chunk in chunk {
            let pool_clone = pool.clone();
            let feature_cache_clone = feature_cache.clone();
            let service_id_clone = service_id_in_chunk.clone(); // Clone each service_id before moving into async block

            batch_futures.push(tokio::spawn(async move {
                // Each task gets its own connection from the pool
                let conn_task = pool_clone.get().await.context(format!(
                    "Failed to get DB connection for service {}",
                    service_id_clone.0
                ))?;
                match get_stored_service_features(&*conn_task, &service_id_clone).await {
                    // get_stored_service_features handles DB storage
                    Ok(features) => {
                        let mut cache_guard = feature_cache_clone.lock().await;
                        cache_guard
                            .individual_cache
                            .put(service_id_clone.0.clone(), features);
                        Ok(1) // Successfully processed one service
                    }
                    Err(e) => {
                        warn!(
                            "Failed to extract/store features for service {}: {}",
                            service_id_clone.0, e
                        );
                        Ok(0) // Failed to process this service
                    }
                }
            }));
        }

        for handle in batch_futures {
            match handle.await {
                Ok(Ok(count_processed_in_task)) => {
                    processed_count += count_processed_in_task;
                }
                Ok(Err(e)) => {
                    // Error from within the task's logic
                    warn!(
                        "A task in service feature pre-warming batch failed: {:?}",
                        e
                    );
                }
                Err(e) => {
                    // JoinError, task panicked
                    warn!(
                        "A task in service feature pre-warming batch panicked: {:?}",
                        e
                    );
                }
            }
        }
        info!(
            "Pre-warmed service features cache: Processed {}/{} services",
            processed_count, total_services
        );
    }

    // Cache statistics for observability
    {
        let cache_guard = feature_cache.lock().await;
        let (_, _, ind_hits, ind_misses) = cache_guard.get_stats();
        info!(
            "Service individual features cache stats after pre-extraction: hits={}, misses={}",
            ind_hits, ind_misses
        );
    }

    Ok(processed_count)
}

/// Extract service pairs for cache prewarming from various sources
pub async fn extract_priority_service_pairs(
    pool: &PgPool,
    max_pairs: usize,
) -> Result<ServicePriorityBatches> {
    let mut batches = ServicePriorityBatches::new();
    let pairs_per_category = (max_pairs / 5).max(100); // Distribute among categories, minimum 100 per category

    // 1. High-confidence existing pairs
    let high_conf_pairs = get_high_confidence_pairs(pool, pairs_per_category).await?;
    batches.high_confidence_pairs = high_conf_pairs;
    info!(
        "Found {} high confidence service pairs for prewarming",
        batches.high_confidence_pairs.len()
    );

    // 2. Taxonomy similar pairs
    let taxonomy_similar = get_taxonomy_similar_pairs(pool, pairs_per_category).await?;
    batches.taxonomy_similar_pairs = taxonomy_similar;
    info!(
        "Found {} taxonomy similar pairs for prewarming",
        batches.taxonomy_similar_pairs.len()
    );

    // 3. Same organization pairs
    let same_org = get_same_organization_pairs(pool, pairs_per_category).await?;
    batches.same_organization_pairs = same_org;
    info!(
        "Found {} same organization pairs for prewarming",
        batches.same_organization_pairs.len()
    );

    // 4. Name similar pairs
    let name_similar = get_name_similar_pairs(pool, pairs_per_category).await?;
    batches.name_similar_pairs = name_similar;
    info!(
        "Found {} name similar pairs for prewarming",
        batches.name_similar_pairs.len()
    );

    // 5. Cached feature pairs
    let cached = get_cached_feature_pairs(pool, pairs_per_category).await?;
    batches.cached_feature_pairs = cached;
    info!(
        "Found {} service pairs with cached features for prewarming",
        batches.cached_feature_pairs.len()
    );

    info!(
        "Total of {} priority service pairs collected for prewarming",
        batches.total_pairs()
    );

    Ok(batches)
}

/// Get high confidence pairs from existing service_group records
async fn get_high_confidence_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(ServiceId, ServiceId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "SELECT service_id_1, service_id_2, confidence_score
             FROM public.service_group
             WHERE confidence_score >= 0.7
             ORDER BY confidence_score DESC
             LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query high confidence service pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let service_id_1: String = row.get(0);
            let service_id_2: String = row.get(1);
            let confidence: f64 = row.get(2);
            (ServiceId(service_id_1), ServiceId(service_id_2), confidence)
        })
        .collect();

    Ok(pairs)
}

/// Get service pairs with similar taxonomies
async fn get_taxonomy_similar_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(ServiceId, ServiceId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "WITH service_taxonomies AS (
                SELECT service_id, array_agg(taxonomy_term_id) AS term_ids
                FROM public.service_taxonomy
                GROUP BY service_id
            ),
            taxonomy_pairs AS (
                SELECT 
                    st1.service_id AS service_id_1,
                    st2.service_id AS service_id_2,
                    -- Jaccard similarity: intersection / union
                    (array_length(ARRAY(SELECT UNNEST(st1.term_ids) INTERSECT SELECT UNNEST(st2.term_ids)), 1)::float / 
                     array_length(ARRAY(SELECT UNNEST(st1.term_ids) UNION SELECT UNNEST(st2.term_ids)), 1)::float) AS similarity
                FROM service_taxonomies st1
                CROSS JOIN service_taxonomies st2
                WHERE st1.service_id < st2.service_id
                AND array_length(st1.term_ids, 1) >= 3
                AND array_length(st2.term_ids, 1) >= 3
                AND array_length(ARRAY(SELECT UNNEST(st1.term_ids) INTERSECT SELECT UNNEST(st2.term_ids)), 1) >= 2
            )
            SELECT service_id_1, service_id_2, similarity
            FROM taxonomy_pairs
            LEFT JOIN public.service_group sg ON 
                (taxonomy_pairs.service_id_1 = sg.service_id_1 AND taxonomy_pairs.service_id_2 = sg.service_id_2) OR
                (taxonomy_pairs.service_id_1 = sg.service_id_2 AND taxonomy_pairs.service_id_2 = sg.service_id_1)
            WHERE sg.id IS NULL  -- Only get pairs that don't already have a service_group
            ORDER BY similarity DESC
            LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query taxonomy similar pairs")?;

    let pairs = rows
        .into_iter()
        .filter_map(|row| {
            let service_id_1: String = row.get(0);
            let service_id_2: String = row.get(1);
            let similarity: Option<f64> = row.try_get(2).ok();

            similarity.map(|sim| (ServiceId(service_id_1), ServiceId(service_id_2), sim))
        })
        .collect();

    Ok(pairs)
}

/// Get service pairs from the same organization
async fn get_same_organization_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(ServiceId, ServiceId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "WITH org_services AS (
                SELECT 
                    organization_id,
                    array_agg(id) AS service_ids,
                    count(*) AS service_count
                FROM public.service
                GROUP BY organization_id
                HAVING count(*) >= 2 AND count(*) <= 20  -- Reasonable number of services
            ),
            service_pairs AS (
                SELECT 
                    LEAST(s1, s2) AS service_id_1,
                    GREATEST(s1, s2) AS service_id_2,
                    0.8 AS base_confidence  -- Initial confidence for same org
                FROM org_services os,
                LATERAL (
                    SELECT DISTINCT s1, s2
                    FROM unnest(os.service_ids) s1,
                    unnest(os.service_ids) s2
                    WHERE s1 < s2
                ) pairs
            )
            SELECT service_id_1, service_id_2, base_confidence
            FROM service_pairs
            LEFT JOIN public.service_group sg ON 
                (service_pairs.service_id_1 = sg.service_id_1 AND service_pairs.service_id_2 = sg.service_id_2) OR
                (service_pairs.service_id_1 = sg.service_id_2 AND service_pairs.service_id_2 = sg.service_id_1)
            WHERE sg.id IS NULL  -- Only get pairs that don't already have a service_group
            LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query same organization pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let service_id_1: String = row.get(0);
            let service_id_2: String = row.get(1);
            let confidence: f64 = row.get(2);
            (ServiceId(service_id_1), ServiceId(service_id_2), confidence)
        })
        .collect();

    Ok(pairs)
}

/// Get service pairs with similar names
async fn get_name_similar_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(ServiceId, ServiceId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "WITH name_pairs AS (
                SELECT 
                    s1.id AS service_id_1,
                    s2.id AS service_id_2,
                    similarity(s1.name, s2.name) AS name_similarity
                FROM public.service s1
                CROSS JOIN public.service s2
                WHERE s1.id < s2.id
                  AND length(s1.name) > 5
                  AND length(s2.name) > 5
                  AND s1.organization_id != s2.organization_id  -- Different organizations
                  AND similarity(s1.name, s2.name) > 0.6  -- Reasonably similar names
            )
            SELECT service_id_1, service_id_2, name_similarity
            FROM name_pairs
            LEFT JOIN public.service_group sg ON 
                (name_pairs.service_id_1 = sg.service_id_1 AND name_pairs.service_id_2 = sg.service_id_2) OR
                (name_pairs.service_id_1 = sg.service_id_2 AND name_pairs.service_id_2 = sg.service_id_1)
            WHERE sg.id IS NULL  -- Only get pairs that don't already have a service_group
            ORDER BY name_similarity DESC
            LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query name similar pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let service_id_1: String = row.get(0);
            let service_id_2: String = row.get(1);
            let similarity: f64 = row.get(2);
            (ServiceId(service_id_1), ServiceId(service_id_2), similarity)
        })
        .collect();

    Ok(pairs)
}

/// Get pairs with already cached features from service_match_decision_details
async fn get_cached_feature_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(ServiceId, ServiceId, Vec<f64>)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "SELECT 
                sg.service_id_1,
                sg.service_id_2,
                smdd.snapshotted_features
             FROM clustering_metadata.service_match_decision_details smdd
             JOIN public.service_group sg ON smdd.service_group_id = sg.id
             WHERE smdd.snapshotted_features IS NOT NULL
               AND smdd.created_at > (CURRENT_TIMESTAMP - INTERVAL '30 days')
             ORDER BY smdd.created_at DESC
             LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query cached feature service pairs")?;

    let mut pairs = Vec::with_capacity(rows.len());

    for row in rows {
        let service_id_1: String = row.get(0);
        let service_id_2: String = row.get(1);
        let features_json: serde_json::Value = row.get(2);

        // Parse the feature vector from JSON
        if let Ok(features) = serde_json::from_value::<Vec<f64>>(features_json) {
            pairs.push((ServiceId(service_id_1), ServiceId(service_id_2), features));
        }
    }

    Ok(pairs)
}

/// Main function to prewarm service pair features cache
pub async fn prewarm_service_pair_features_cache(
    pool: &PgPool,
    feature_cache: &SharedServiceFeatureCache,
    max_pairs: usize,
) -> Result<usize> {
    info!(
        "Starting service pair features cache prewarming (max pairs: {})",
        max_pairs
    );

    // 1. Extract priority pairs for prewarming
    let priority_batches = extract_priority_service_pairs(pool, max_pairs).await?;

    // 2. Process already cached feature pairs directly (no need for DB extraction)
    let mut processed_count = 0;
    {
        let mut cache_guard = feature_cache.lock().await;
        for (service_id_1, service_id_2, features) in &priority_batches.cached_feature_pairs {
            let key = ServiceFeatureCacheService::get_pair_key(service_id_1, service_id_2);
            cache_guard.pair_cache.put(key, features.clone());
            processed_count += 1;
        }
        info!(
            "Added {} service pairs with pre-extracted features to cache",
            processed_count
        );
    }

    // 3. Create a deduplicated list of pairs to extract features for
    let mut pairs_to_process = HashSet::new();

    // Add high confidence pairs
    for (s1, s2, _) in &priority_batches.high_confidence_pairs {
        let key = if s1.0 < s2.0 {
            (s1.clone(), s2.clone())
        } else {
            (s2.clone(), s1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Add taxonomy similar pairs
    for (s1, s2, _) in &priority_batches.taxonomy_similar_pairs {
        let key = if s1.0 < s2.0 {
            (s1.clone(), s2.clone())
        } else {
            (s2.clone(), s1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Add same organization pairs
    for (s1, s2, _) in &priority_batches.same_organization_pairs {
        let key = if s1.0 < s2.0 {
            (s1.clone(), s2.clone())
        } else {
            (s2.clone(), s1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Add name similar pairs
    for (s1, s2, _) in &priority_batches.name_similar_pairs {
        let key = if s1.0 < s2.0 {
            (s1.clone(), s2.clone())
        } else {
            (s2.clone(), s1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Limit to max_pairs if needed
    let mut deduplicated_pairs: Vec<(ServiceId, ServiceId)> =
        pairs_to_process.into_iter().collect();
    if deduplicated_pairs.len() > max_pairs {
        // Shuffle and take max_pairs
        let mut rng = rand::thread_rng();
        deduplicated_pairs.shuffle(&mut rng);
        deduplicated_pairs.truncate(max_pairs);
    }

    info!(
        "Processing {} unique service pairs for feature extraction and caching",
        deduplicated_pairs.len()
    );

    // 4. Calculate optimal batch size
    let batch_size = calculate_batch_size(deduplicated_pairs.len());
    info!("Using batch size of {} for parallel processing", batch_size);

    // 5. Process in batches
    let total_batches = (deduplicated_pairs.len() + batch_size - 1) / batch_size;
    let processed_counter = Arc::new(AtomicUsize::new(0));

    for batch_index in 0..total_batches {
        let start_idx = batch_index * batch_size;
        let end_idx = (start_idx + batch_size).min(deduplicated_pairs.len());
        let batch = &deduplicated_pairs[start_idx..end_idx];

        // Get the total length before entering the async block
        let total_pairs = deduplicated_pairs.len();

        info!(
            "Processing batch {}/{} ({} pairs)",
            batch_index + 1,
            total_batches,
            batch.len()
        );

        let mut batch_futures: Vec<tokio::task::JoinHandle<Result<usize, anyhow::Error>>> =
            Vec::with_capacity(batch.len());

        for (service_id_1, service_id_2) in batch {
            let pool_clone = pool.clone();
            let feature_cache_clone = feature_cache.clone();
            let service_id_1_clone = service_id_1.clone();
            let service_id_2_clone = service_id_2.clone();
            let counter_clone = processed_counter.clone();
            let total_pairs = total_pairs; // Capture the pre-computed length

            let future = tokio::spawn(async move {
                let pair_context =
                    format!("Pair ({}, {})", service_id_1_clone.0, service_id_2_clone.0);

                match service_feature_extraction::extract_context_for_service_pair(
                    &pool_clone,
                    &service_id_1_clone,
                    &service_id_2_clone,
                )
                .await
                {
                    Ok(features) => {
                        // Cache the features
                        let mut cache_guard = feature_cache_clone.lock().await;
                        let key = ServiceFeatureCacheService::get_pair_key(
                            &service_id_1_clone,
                            &service_id_2_clone,
                        );
                        cache_guard.pair_cache.put(key, features);

                        // Update counter
                        let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;
                        if count % 100 == 0 {
                            debug!(
                                "Service prewarming progress: {}/{} pairs processed",
                                count, total_pairs
                            );
                        }
                        Ok(1)
                    }
                    Err(e) => {
                        warn!(
                            "{}: Failed to extract service context features: {}",
                            pair_context, e
                        );
                        Ok(0)
                    }
                }
            });

            batch_futures.push(future);
        }

        // Wait for all futures in this batch
        for result in future::join_all(batch_futures).await {
            match result {
                Ok(Ok(count)) => {
                    processed_count += count;
                }
                Ok(Err(e)) => {
                    warn!("Error in batch processing: {}", e);
                }
                Err(e) => {
                    warn!("Task join error: {}", e);
                }
            }
        }
    }

    // Final cache stats
    {
        let cache_guard = feature_cache.lock().await;
        let (hits, misses, ind_hits, ind_misses) = cache_guard.get_stats();
        info!(
            "Service pair feature cache after prewarming: {} entries, hits={}, misses={}",
            cache_guard.pair_cache.len(),
            hits,
            misses
        );
        info!(
            "Service individual feature cache: {} entries, hits={}, misses={}",
            cache_guard.individual_cache.len(),
            ind_hits,
            ind_misses
        );
    }

    info!(
        "Completed service pair features cache prewarming. Added {} pairs to cache.",
        processed_count
    );
    Ok(processed_count)
}
