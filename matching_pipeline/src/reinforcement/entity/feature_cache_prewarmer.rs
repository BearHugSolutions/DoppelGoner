// src/reinforcement/entity/feature_cache_prewarmer.rs

use anyhow::{Context, Error as AnyhowError, Result};
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
use crate::models::EntityId;
use crate::reinforcement::entity::feature_cache_service::{
    FeatureCacheService, SharedFeatureCache,
};
use crate::reinforcement::entity::feature_extraction::{self, get_stored_entity_features};

// Maximum batch size to process at once
const MAX_BATCH_SIZE: usize = 50;

// Calculate number of batches based on CPU cores
fn calculate_batch_size(pair_count: usize) -> usize {
    let cpu_count = num_cpus::get();
    let optimal_batch_size = (pair_count / cpu_count).max(1).min(MAX_BATCH_SIZE);
    optimal_batch_size
}

/// Struct to hold various batch priorities for prewarming
pub struct PriorityBatches {
    high_confidence_pairs: Vec<(EntityId, EntityId, f64)>,
    transitive_pairs: Vec<(EntityId, EntityId, f64)>,
    method_crossover_pairs: Vec<(EntityId, EntityId, f64)>,
    high_degree_pairs: Vec<(EntityId, EntityId, f64)>,
    cached_feature_pairs: Vec<(EntityId, EntityId, Vec<f64>)>,
}

impl PriorityBatches {
    pub fn new() -> Self {
        Self {
            high_confidence_pairs: Vec::new(),
            transitive_pairs: Vec::new(),
            method_crossover_pairs: Vec::new(),
            high_degree_pairs: Vec::new(),
            cached_feature_pairs: Vec::new(),
        }
    }

    pub fn total_pairs(&self) -> usize {
        self.high_confidence_pairs.len()
            + self.transitive_pairs.len()
            + self.method_crossover_pairs.len()
            + self.high_degree_pairs.len()
            + self.cached_feature_pairs.len()
    }
}

pub async fn extract_and_store_all_entity_features_and_prewarm_cache(
    pool: &PgPool,
    feature_cache: &SharedFeatureCache, // The entity-specific shared cache
) -> Result<usize> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for pre-warming entity cache")?;

    // Get all entity IDs
    let entity_rows = conn
        .query("SELECT id FROM public.entity", &[])
        .await
        .context("Failed to query entity IDs for cache pre-warming")?;

    let entity_ids: Vec<EntityId> = entity_rows
        .iter()
        .map(|row| EntityId(row.get("id")))
        .collect();

    info!(
        "Found {} entities for context feature extraction and cache pre-warming",
        entity_ids.len()
    );

    let batch_size = 100; // Configurable batch size
    let mut processed_count = 0;
    let total_entities = entity_ids.len();

    for chunk in entity_ids.chunks(batch_size) {
        let mut batch_futures: Vec<tokio::task::JoinHandle<Result<usize, anyhow::Error>>> =
            Vec::new();

        for entity_id_in_chunk in chunk {
            let pool_clone = pool.clone();
            let feature_cache_clone = feature_cache.clone();
            let entity_id_clone = entity_id_in_chunk.clone(); // Clone each entity_id before moving into async block

            batch_futures.push(tokio::spawn(async move {
                // Each task gets its own connection from the pool
                let conn_task = pool_clone.get().await.context(format!(
                    "Failed to get DB connection for entity {}",
                    entity_id_clone.0
                ))?;
                match get_stored_entity_features(&*conn_task, &entity_id_clone).await {
                    // get_stored_entity_features handles DB storage
                    Ok(features) => {
                        let mut cache_guard = feature_cache_clone.lock().await;
                        cache_guard
                            .individual_cache
                            .put(entity_id_clone.0.clone(), features);
                        Ok(1) // Successfully processed one entity
                    }
                    Err(e) => {
                        warn!(
                            "Failed to extract/store features for entity {}: {}",
                            entity_id_clone.0, e
                        );
                        Ok(0) // Failed to process this entity
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
                    warn!("A task in entity feature pre-warming batch failed: {:?}", e);
                }
                Err(e) => {
                    // JoinError, task panicked
                    warn!(
                        "A task in entity feature pre-warming batch panicked: {:?}",
                        e
                    );
                }
            }
        }
        info!(
            "Pre-warmed entity features cache: Processed {}/{} entities",
            processed_count, total_entities
        );
    }

    // Cache statistics for observability
    {
        let cache_guard = feature_cache.lock().await;
        let (_, _, ind_hits, ind_misses) = cache_guard.get_stats();
        info!(
            "Entity individual features cache stats after pre-extraction: hits={}, misses={}",
            ind_hits, ind_misses
        );
    }

    Ok(processed_count)
}

/// Extract entity pairs for cache prewarming from various sources
pub async fn extract_priority_entity_pairs(
    pool: &PgPool,
    max_pairs: usize,
) -> Result<PriorityBatches> {
    let mut batches = PriorityBatches::new();
    let pairs_per_category = (max_pairs / 5).max(100); // Distribute among categories, minimum 100 per category

    // 1. High-confidence existing pairs
    let high_conf_pairs = get_high_confidence_pairs(pool, pairs_per_category).await?;
    batches.high_confidence_pairs = high_conf_pairs;
    info!(
        "Found {} high confidence pairs for prewarming",
        batches.high_confidence_pairs.len()
    );

    // 2. Transitive relationship pairs
    let transitive = get_transitive_pairs(pool, pairs_per_category).await?;
    batches.transitive_pairs = transitive;
    info!(
        "Found {} transitive pairs for prewarming",
        batches.transitive_pairs.len()
    );

    // 3. Method crossover pairs
    let crossover = get_method_crossover_pairs(pool, pairs_per_category).await?;
    batches.method_crossover_pairs = crossover;
    info!(
        "Found {} method crossover pairs for prewarming",
        batches.method_crossover_pairs.len()
    );

    // 4. High degree entity pairs
    let high_degree = get_high_degree_pairs(pool, pairs_per_category).await?;
    batches.high_degree_pairs = high_degree;
    info!(
        "Found {} high degree entity pairs for prewarming",
        batches.high_degree_pairs.len()
    );

    // 5. Cached feature pairs
    let cached = get_cached_feature_pairs(pool, pairs_per_category).await?;
    batches.cached_feature_pairs = cached;
    info!(
        "Found {} pairs with cached features for prewarming",
        batches.cached_feature_pairs.len()
    );

    info!(
        "Total of {} priority pairs collected for prewarming",
        batches.total_pairs()
    );

    Ok(batches)
}

/// Get high confidence pairs from existing entity_group records
async fn get_high_confidence_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "SELECT entity_id_1, entity_id_2, confidence_score
         FROM public.entity_group
         WHERE confidence_score >= 0.7
         ORDER BY confidence_score DESC
         LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query high confidence pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let entity_id_1: String = row.get(0);
            let entity_id_2: String = row.get(1);
            let confidence: f64 = row.get(2);
            (EntityId(entity_id_1), EntityId(entity_id_2), confidence)
        })
        .collect();

    Ok(pairs)
}

/// Get transitive pairs (entities matched to common third entities)
async fn get_transitive_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "WITH transitive_pairs AS (
          SELECT DISTINCT 
            CASE WHEN eg1.entity_id_1 < eg2.entity_id_1 
                 THEN eg1.entity_id_1 ELSE eg2.entity_id_1 END AS entity_id_1,
            CASE WHEN eg1.entity_id_1 < eg2.entity_id_1 
                 THEN eg2.entity_id_1 ELSE eg1.entity_id_1 END AS entity_id_2,
            eg1.confidence_score + eg2.confidence_score AS combined_score
          FROM public.entity_group eg1
          JOIN public.entity_group eg2 ON eg1.entity_id_2 = eg2.entity_id_2
          WHERE eg1.entity_id_1 != eg2.entity_id_1
            AND eg1.confidence_score >= 0.6
            AND eg2.confidence_score >= 0.6
          
          UNION
          
          SELECT DISTINCT
            CASE WHEN eg1.entity_id_2 < eg2.entity_id_2 
                 THEN eg1.entity_id_2 ELSE eg2.entity_id_2 END AS entity_id_1,
            CASE WHEN eg1.entity_id_2 < eg2.entity_id_2 
                 THEN eg2.entity_id_2 ELSE eg1.entity_id_2 END AS entity_id_2,
            eg1.confidence_score + eg2.confidence_score AS combined_score
          FROM public.entity_group eg1
          JOIN public.entity_group eg2 ON eg1.entity_id_1 = eg2.entity_id_1
          WHERE eg1.entity_id_2 != eg2.entity_id_2
            AND eg1.confidence_score >= 0.6
            AND eg2.confidence_score >= 0.6
        )
        SELECT tp.entity_id_1, tp.entity_id_2, tp.combined_score
        FROM transitive_pairs tp
        LEFT JOIN public.entity_group eg ON 
          (tp.entity_id_1 = eg.entity_id_1 AND tp.entity_id_2 = eg.entity_id_2) OR
          (tp.entity_id_1 = eg.entity_id_2 AND tp.entity_id_2 = eg.entity_id_1)
        WHERE eg.id IS NULL
        ORDER BY tp.combined_score DESC
        LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query transitive pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let entity_id_1: String = row.get(0);
            let entity_id_2: String = row.get(1);
            let combined_score: f64 = row.get(2);
            // Normalize to 0-1 range
            let normalized_score = (combined_score / 2.0).min(1.0);
            (
                EntityId(entity_id_1),
                EntityId(entity_id_2),
                normalized_score,
            )
        })
        .collect();

    Ok(pairs)
}

/// Get method crossover pairs (entities matched on one method but not others)
async fn get_method_crossover_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // This query is complex - we need to find pairs that matched on one strategy
    // but might match on others
    let rows = conn
        .query(
            "WITH matched_pairs AS (
          SELECT 
            LEAST(entity_id_1, entity_id_2) AS smaller_id,
            GREATEST(entity_id_1, entity_id_2) AS larger_id,
            method_type,
            confidence_score
          FROM public.entity_group
          WHERE confidence_score >= 0.7
        ),
        distinct_pairs AS (
          SELECT 
            smaller_id,
            larger_id,
            ARRAY_AGG(method_type) AS matched_methods,
            AVG(confidence_score) AS avg_confidence
          FROM matched_pairs
          GROUP BY smaller_id, larger_id
        ),
        all_methods AS (
          SELECT DISTINCT method_type FROM public.entity_group
        ),
        crossover_candidates AS (
          SELECT 
            dp.smaller_id AS entity_id_1,
            dp.larger_id AS entity_id_2,
            dp.avg_confidence
          FROM distinct_pairs dp
          WHERE EXISTS (
            SELECT 1 FROM all_methods am
            WHERE am.method_type != ALL(dp.matched_methods)
          )
          AND dp.avg_confidence >= 0.75
        )
        SELECT entity_id_1, entity_id_2, avg_confidence 
        FROM crossover_candidates
        ORDER BY avg_confidence DESC
        LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query method crossover pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let entity_id_1: String = row.get(0);
            let entity_id_2: String = row.get(1);
            let avg_confidence: f64 = row.get(2);
            (EntityId(entity_id_1), EntityId(entity_id_2), avg_confidence)
        })
        .collect();

    Ok(pairs)
}

/// Get pairs involving high-degree entities (entities that appear in many matches)
async fn get_high_degree_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "WITH entity_degree AS (
          SELECT entity_id, COUNT(*) AS match_count
          FROM (
            SELECT entity_id_1 AS entity_id FROM public.entity_group
            UNION ALL
            SELECT entity_id_2 AS entity_id FROM public.entity_group
          ) all_entities
          GROUP BY entity_id
          ORDER BY match_count DESC
          LIMIT 50
        ),
        candidate_pairs AS (
          SELECT DISTINCT
            LEAST(ed1.entity_id, ed2.entity_id) AS entity_id_1,
            GREATEST(ed1.entity_id, ed2.entity_id) AS entity_id_2,
            (ed1.match_count + ed2.match_count) / 100.0 AS popularity_score
          FROM entity_degree ed1
          CROSS JOIN entity_degree ed2
          WHERE ed1.entity_id < ed2.entity_id
        )
        SELECT cp.entity_id_1, cp.entity_id_2, LEAST(cp.popularity_score, 1.0) AS score
        FROM candidate_pairs cp
        LEFT JOIN public.entity_group eg ON 
          (cp.entity_id_1 = eg.entity_id_1 AND cp.entity_id_2 = eg.entity_id_2) OR
          (cp.entity_id_1 = eg.entity_id_2 AND cp.entity_id_2 = eg.entity_id_1)
        WHERE eg.id IS NULL
        ORDER BY score DESC
        LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query high degree pairs")?;

    let pairs = rows
        .into_iter()
        .map(|row| {
            let entity_id_1: String = row.get(0);
            let entity_id_2: String = row.get(1);
            let score: f64 = row.get(2);
            (EntityId(entity_id_1), EntityId(entity_id_2), score)
        })
        .collect();

    Ok(pairs)
}

/// Get pairs with already cached features from match_decision_details
async fn get_cached_feature_pairs(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<(EntityId, EntityId, Vec<f64>)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "SELECT 
          eg.entity_id_1,
          eg.entity_id_2,
          mdd.snapshotted_features
         FROM clustering_metadata.match_decision_details mdd
         JOIN public.entity_group eg ON mdd.entity_group_id = eg.id
         WHERE mdd.snapshotted_features IS NOT NULL
           AND mdd.created_at > (CURRENT_TIMESTAMP - INTERVAL '30 days')
         ORDER BY mdd.created_at DESC
         LIMIT $1",
            &[&(limit as i64)],
        )
        .await
        .context("Failed to query cached feature pairs")?;

    let mut pairs = Vec::with_capacity(rows.len());

    for row in rows {
        let entity_id_1: String = row.get(0);
        let entity_id_2: String = row.get(1);
        let features_json: serde_json::Value = row.get(2);

        // Parse the feature vector from JSON
        if let Ok(features) = serde_json::from_value::<Vec<f64>>(features_json) {
            pairs.push((EntityId(entity_id_1), EntityId(entity_id_2), features));
        }
    }

    Ok(pairs)
}

/// Main function to prewarm pair features cache
pub async fn prewarm_pair_features_cache(
    pool: &PgPool,
    feature_cache: &SharedFeatureCache,
    max_pairs: usize,
) -> Result<usize> {
    info!(
        "Starting pair features cache prewarming (max pairs: {})",
        max_pairs
    );

    // 1. Extract priority pairs for prewarming
    let priority_batches = extract_priority_entity_pairs(pool, max_pairs).await?;

    // 2. Process already cached feature pairs directly (no need for DB extraction)
    let mut processed_count = 0;
    {
        let mut cache_guard = feature_cache.lock().await;
        for (entity_id_1, entity_id_2, features) in &priority_batches.cached_feature_pairs {
            let key = FeatureCacheService::get_pair_key(entity_id_1, entity_id_2);
            cache_guard.pair_cache.put(key, features.clone());
            processed_count += 1;
        }
        info!(
            "Added {} pairs with pre-extracted features to cache",
            processed_count
        );
    }

    // 3. Create a deduplicated list of pairs to extract features for
    let mut pairs_to_process = HashSet::new();

    // Add high confidence pairs
    for (e1, e2, _) in &priority_batches.high_confidence_pairs {
        let key = if e1.0 < e2.0 {
            (e1.clone(), e2.clone())
        } else {
            (e2.clone(), e1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Add transitive pairs
    for (e1, e2, _) in &priority_batches.transitive_pairs {
        let key = if e1.0 < e2.0 {
            (e1.clone(), e2.clone())
        } else {
            (e2.clone(), e1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Add method crossover pairs
    for (e1, e2, _) in &priority_batches.method_crossover_pairs {
        let key = if e1.0 < e2.0 {
            (e1.clone(), e2.clone())
        } else {
            (e2.clone(), e1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Add high degree pairs
    for (e1, e2, _) in &priority_batches.high_degree_pairs {
        let key = if e1.0 < e2.0 {
            (e1.clone(), e2.clone())
        } else {
            (e2.clone(), e1.clone())
        };
        pairs_to_process.insert(key);
    }

    // Limit to max_pairs if needed
    let mut deduplicated_pairs: Vec<(EntityId, EntityId)> = pairs_to_process.into_iter().collect();
    if deduplicated_pairs.len() > max_pairs {
        // Shuffle and take max_pairs
        let mut rng = rand::thread_rng();
        deduplicated_pairs.shuffle(&mut rng);
        deduplicated_pairs.truncate(max_pairs);
    }

    info!(
        "Processing {} unique entity pairs for feature extraction and caching",
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

        for (entity_id_1, entity_id_2) in batch {
            let pool_clone = pool.clone();
            let feature_cache_clone = feature_cache.clone();
            let entity_id_1_clone = entity_id_1.clone();
            let entity_id_2_clone = entity_id_2.clone();
            let counter_clone = processed_counter.clone();
            let total_pairs = total_pairs; // Capture the pre-computed length

            let future = tokio::spawn(async move {
                let pair_context =
                    format!("Pair ({}, {})", entity_id_1_clone.0, entity_id_2_clone.0);

                match feature_extraction::extract_context_for_pair(
                    &pool_clone,
                    &entity_id_1_clone,
                    &entity_id_2_clone,
                )
                .await
                {
                    Ok(features) => {
                        // Cache the features
                        let mut cache_guard = feature_cache_clone.lock().await;
                        let key = FeatureCacheService::get_pair_key(
                            &entity_id_1_clone,
                            &entity_id_2_clone,
                        );
                        cache_guard.pair_cache.put(key, features);

                        // Update counter
                        let count = counter_clone.fetch_add(1, Ordering::Relaxed) + 1;
                        if count % 100 == 0 {
                            debug!(
                                "Prewarming progress: {}/{} pairs processed",
                                count, total_pairs
                            );
                        }
                        Ok(1)
                    }
                    Err(e) => {
                        warn!(
                            "{}: Failed to extract context features: {}",
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
            "Pair feature cache after prewarming: {} entries, hits={}, misses={}",
            cache_guard.pair_cache.len(),
            hits,
            misses
        );
        info!(
            "Individual feature cache: {} entries, hits={}, misses={}",
            cache_guard.individual_cache.len(),
            ind_hits,
            ind_misses
        );
    }

    info!(
        "Completed pair features cache prewarming. Added {} pairs to cache.",
        processed_count
    );
    Ok(processed_count)
}
