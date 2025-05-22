// src/matching/geospatial.rs - Fixed version with better duplicate detection
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use futures::future::{join_all, try_join_all};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config;
use crate::db::PgPool;
use crate::models::{
    ActionType, EntityGroupId, EntityId, GeospatialMatchValue, MatchMethodType, MatchValues,
    NewSuggestedAction, SuggestionStatus,
};
use crate::reinforcement::entity::feature_cache_service::{
    FeatureCacheService, SharedFeatureCache,
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, GeospatialMatchResult, MatchMethodStats};
use serde_json;

const METERS_TO_CHECK: f64 = 100.0; // 0.062 miles

// Increased batch size for better efficiency
const BATCH_SIZE: usize = 250;

// Number of batches to process in parallel
// This controls the maximum number of concurrent database connections used
const MAX_PARALLEL_BATCHES: usize = 4;

/// Main function to find geospatial-based matches
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting pairwise geospatial matching (run ID: {}){}...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    // Initial database operations
    let init_conn = pool
        .get()
        .await
        .context("Geospatial: Failed to get DB connection for initial queries")?;

    // Verify that entities exist in the database
    info!("Geospatial: First verifying entity existence in database...");
    let valid_entities = fetch_valid_entity_ids(&*init_conn).await?;
    info!(
        "Geospatial: Found {} valid entities in database.",
        valid_entities.len()
    );

    // Fetch all candidate pairs in a single query with ANTI-JOIN to exclude existing pairs
    let candidate_pairs = fetch_candidate_pairs_excluding_existing(&*init_conn).await?;

    // Done with initial connection
    drop(init_conn);

    let total_candidate_count = candidate_pairs.len();
    info!(
        "Geospatial: Found {} new geospatial pairs from DB query (excluding already processed).",
        total_candidate_count
    );

    // Filter candidates to only include valid entities
    let filtered_candidates: Vec<_> = candidate_pairs
        .into_iter()
        .filter(|(e1, e2, ..)| valid_entities.contains(&e1.0) && valid_entities.contains(&e2.0))
        .collect();

    info!(
        "Geospatial: Filtered out {} pairs with invalid entity IDs, {} valid pairs remain.",
        total_candidate_count - filtered_candidates.len(),
        filtered_candidates.len()
    );

    // If no pairs to process, return early
    if filtered_candidates.is_empty() {
        info!("Geospatial: No new pairs to process.");
        let method_stats = MatchMethodStats {
            method_type: MatchMethodType::Geospatial,
            groups_created: 0,
            entities_matched: 0,
            avg_confidence: 0.0,
            avg_group_size: 0.0,
        };

        let geospatial_specific_result = GeospatialMatchResult {
            groups_created: 0,
            stats: method_stats,
        };

        return Ok(AnyMatchResult::Geospatial(geospatial_specific_result));
    }

    // Track stats
    let stats_mutex = Arc::new(Mutex::new((
        0,              // new_pairs_created_count
        HashSet::new(), // entities_in_new_pairs
        Vec::new(),     // confidence_scores_for_stats
        0,              // individual_operation_errors
        0,              // pairs_processed_count
        0,              // feature_extraction_count
        0,              // feature_extraction_failures
        0,              // skipped_existing_pairs (should be 0 now due to pre-filtering)
    )));

    // Process batches in parallel with controlled concurrency
    let total_batches = (filtered_candidates.len() + BATCH_SIZE - 1) / BATCH_SIZE;

    info!(
        "Geospatial: Processing {} batches of size {} with max {} parallel batches",
        total_batches, BATCH_SIZE, MAX_PARALLEL_BATCHES
    );

    // Process all batches with controlled parallelism
    let mut global_batch_num = 0;

    for chunk_of_batches in filtered_candidates.chunks(BATCH_SIZE * MAX_PARALLEL_BATCHES) {
        let mut batch_futures = Vec::new();

        for batch in chunk_of_batches.chunks(BATCH_SIZE) {
            global_batch_num += 1;
            let batch_to_process = batch.to_vec();
            let pool_clone = pool.clone();
            let ro_option_clone = reinforcement_orchestrator_option.clone();
            let run_id = pipeline_run_id.to_string();
            let feature_cache_clone = feature_cache.clone();
            let stats_arc = stats_mutex.clone();
            let current_batch_num = global_batch_num;

            let batch_future = tokio::spawn(async move {
                process_batch(
                    batch_to_process,
                    &pool_clone,
                    ro_option_clone.as_ref(),
                    &run_id,
                    feature_cache_clone,
                    stats_arc,
                    current_batch_num,
                    total_batches,
                )
                .await
            });

            batch_futures.push(batch_future);
        }

        let results = join_all(batch_futures).await;

        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                warn!("Geospatial: Batch processing task {} error: {}", i, e);
            }
        }
    }

    // Extract final stats
    let (
        new_pairs_created_count,
        entities_in_new_pairs,
        confidence_scores_for_stats,
        individual_operation_errors,
        pairs_processed_count,
        feature_extraction_count,
        feature_extraction_failures,
        skipped_existing_pairs,
    ) = {
        let stats = stats_mutex.lock().await;
        (
            stats.0,
            stats.1.clone(),
            stats.2.clone(),
            stats.3,
            stats.4,
            stats.5,
            stats.6,
            stats.7,
        )
    };

    // Report statistics
    let avg_confidence: f64 = if !confidence_scores_for_stats.is_empty() {
        confidence_scores_for_stats.iter().sum::<f64>() / confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Geospatial,
        groups_created: new_pairs_created_count,
        entities_matched: entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if new_pairs_created_count > 0 {
            2.0
        } else {
            0.0
        },
    };

    let elapsed = start_time.elapsed();
    info!(
        "Geospatial matching complete in {:.2?}: processed {} pairs, skipped {} existing pairs, created {} new pairs ({} errors), involving {} unique entities.",
        elapsed,
        pairs_processed_count,
        skipped_existing_pairs,
        method_stats.groups_created,
        individual_operation_errors,
        method_stats.entities_matched
    );

    info!(
        "Geospatial feature extraction stats: {} successful, {} failed",
        feature_extraction_count, feature_extraction_failures
    );

    let geospatial_specific_result = GeospatialMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    };

    Ok(AnyMatchResult::Geospatial(geospatial_specific_result))
}

/// Process a batch of candidate pairs with batch database operations
/// Note: existing pairs are now pre-filtered, so no need to check again
async fn process_batch(
    batch: Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    stats_mutex: Arc<
        Mutex<(
            usize,
            HashSet<EntityId>,
            Vec<f64>,
            usize,
            usize,
            usize,
            usize,
            usize,
        )>,
    >,
    batch_num: usize,
    total_batches: usize,
) -> Result<()> {
    info!(
        "Geospatial: Processing batch {}/{} ({} pairs)...",
        batch_num,
        total_batches,
        batch.len()
    );

    // Prepare data structures for batch operations
    let mut pairs_to_create = Vec::new();
    let mut suggestions_to_create = Vec::new();
    let mut decision_snapshots_to_create = Vec::new();

    // Local stats for this batch
    let mut local_new_pairs = 0;
    let mut local_entities = HashSet::new();
    let mut local_confidence_scores = Vec::new();
    let mut local_errors = 0;
    let mut local_pairs_processed = 0;
    let mut local_feature_extractions = 0;
    let mut local_feature_failures = 0;
    let mut local_skipped_pairs = 0; // Should be 0 since pairs are pre-filtered

    // Collect pairs that need feature extraction due to cache misses
    let mut cache_miss_pairs: Vec<(
        EntityId,
        EntityId,
        f64,
        f64,
        f64,
        f64,
        f64,
        f64,
        EntityGroupId,
        EntityId,
        EntityId,
        MatchValues,
    )> = Vec::new();

    // First pass: Process pairs with cache hits immediately, collect cache misses for batch processing
    for (e1_id, e2_id, lat1, lon1, lat2, lon2, distance_meters) in batch {
        // Since pairs are pre-filtered, we don't need to check existing pairs again
        // But we still need to ensure proper ordering for consistency
        let (ordered_id_1, ordered_id_2, ordered_match_values) = 
            normalize_pair_for_storage(e1_id, e2_id, lat1, lon1, lat2, lon2, distance_meters);

        local_pairs_processed += 1;

        // Calculate pre-RL confidence score
        let pre_rl_confidence_score = 0.85; // Default confidence

        // Generate entity group ID
        let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());

        // Check if we need to do feature extraction and if so, is it in cache?
        let should_check_cache =
            reinforcement_orchestrator_option.is_some() && feature_cache.is_some();
        let mut cache_hit = false;

        if should_check_cache {
            if let Some(cache) = &feature_cache {
                let key = FeatureCacheService::get_pair_key(&ordered_id_1, &ordered_id_2);
                let cache_guard = cache.lock().await;
                if cache_guard.pair_cache.contains(&key) {
                    cache_hit = true;
                }
                drop(cache_guard);
            }
        }

        if should_check_cache && !cache_hit {
            // This pair has a cache miss - add it to our collection for batch processing
            cache_miss_pairs.push((
                ordered_id_1.clone(),
                ordered_id_2.clone(),
                lat1,
                lon1,
                lat2,
                lon2,
                distance_meters,
                pre_rl_confidence_score,
                new_entity_group_id,
                ordered_id_1,
                ordered_id_2,
                ordered_match_values,
            ));
            continue;
        }

        // Process pairs with cache hit or those not needing RL scoring immediately
        let mut final_confidence_score = pre_rl_confidence_score;
        let mut features_for_snapshot: Option<Vec<f64>> = None;

        // Apply RL tuning if available (and we have a cache hit)
        if let Some(ro_arc) = reinforcement_orchestrator_option {
            match if let Some(cache) = feature_cache.as_ref() {
                let orchestrator_guard = ro_arc.lock().await;
                orchestrator_guard
                    .get_pair_features(pool, &ordered_id_1, &ordered_id_2)
                    .await
            } else {
                MatchingOrchestrator::extract_pair_context_features(
                    pool,
                    &ordered_id_1,
                    &ordered_id_2,
                )
                .await
            } {
                Ok(features_vec) => {
                    local_feature_extractions += 1;
                    if !features_vec.is_empty() {
                        features_for_snapshot = Some(features_vec.clone());
                        let orchestrator_guard = ro_arc.lock().await;
                        match orchestrator_guard.get_tuned_confidence(
                            &MatchMethodType::Geospatial,
                            pre_rl_confidence_score,
                            &features_vec,
                        ) {
                            Ok(tuned_score) => final_confidence_score = tuned_score,
                            Err(e) => warn!("Geospatial: Failed to get tuned confidence for ({}, {}): {}. Using pre-RL score.", ordered_id_1.0, ordered_id_2.0, e),
                        }
                    }
                }
                Err(e) => {
                    local_feature_failures += 1;
                    warn!("Geospatial: Failed to extract features for ({}, {}): {}. Using pre-RL score.", ordered_id_1.0, ordered_id_2.0, e);
                }
            }
        }

        // Add this pair to the ones we'll create
        process_single_pair(
            &mut pairs_to_create,
            &mut suggestions_to_create,
            &mut decision_snapshots_to_create,
            &new_entity_group_id,
            &ordered_id_1,
            &ordered_id_2,
            &ordered_match_values,
            final_confidence_score,
            pre_rl_confidence_score,
            features_for_snapshot,
            reinforcement_orchestrator_option,
            pipeline_run_id,
            distance_meters,
            lat1,
            lon1,
            lat2,
            lon2,
        );

        // Update local stats
        local_new_pairs += 1;
        local_entities.insert(ordered_id_1);
        local_entities.insert(ordered_id_2);
        local_confidence_scores.push(final_confidence_score);
    }

    // Process all cache misses in a batch
    if !cache_miss_pairs.is_empty() {
        info!(
            "Geospatial: Processing {} cache misses in batch",
            cache_miss_pairs.len()
        );

        let feature_cache_ref = feature_cache.as_ref();

        let results = batch_extract_features(
            pool,
            &cache_miss_pairs
                .iter()
                .map(|(e1, e2, ..)| (e1.clone(), e2.clone()))
                .collect::<Vec<_>>(),
            reinforcement_orchestrator_option,
        )
        .await;

        // Process each pair with its extracted features
        for (
            i,
            (
                e1_id,
                e2_id,
                lat1,
                lon1,
                lat2,
                lon2,
                distance_meters,
                pre_rl_confidence_score,
                new_entity_group_id,
                ordered_id_1,
                ordered_id_2,
                ordered_match_values,
            ),
        ) in cache_miss_pairs.iter().enumerate()
        {
            if i >= results.len() {
                warn!("Geospatial: Index mismatch in results array for cache miss processing");
                continue;
            }

            let features_result = &results[i];
            let mut final_confidence_score = *pre_rl_confidence_score;
            let mut features_for_snapshot: Option<Vec<f64>> = None;

            match features_result {
                Ok(features_vec) => {
                    local_feature_extractions += 1;

                    // Update the cache
                    if let Some(cache) = feature_cache_ref {
                        let mut cache_guard = cache.lock().await;
                        let key = FeatureCacheService::get_pair_key(&ordered_id_1, &ordered_id_2);
                        cache_guard.pair_cache.put(key, features_vec.clone());
                    }

                    if !features_vec.is_empty() {
                        features_for_snapshot = Some(features_vec.clone());

                        // Get tuned confidence score
                        if let Some(ro_arc) = reinforcement_orchestrator_option {
                            let orchestrator_guard = ro_arc.lock().await;
                            match orchestrator_guard.get_tuned_confidence(
                                &MatchMethodType::Geospatial,
                                *pre_rl_confidence_score,
                                &features_vec
                            ) {
                                Ok(tuned_score) => final_confidence_score = tuned_score,
                                Err(e) => warn!("Geospatial: Failed to get tuned confidence for ({}, {}): {}. Using pre-RL score.", 
                                              ordered_id_1.0, ordered_id_2.0, e),
                            }
                        }
                    }
                }
                Err(e) => {
                    local_feature_failures += 1;
                    warn!("Geospatial: Batch feature extraction failed for ({}, {}): {}. Using pre-RL score.", 
                        ordered_id_1.0, ordered_id_2.0, e);
                }
            }

            // Process this pair
            process_single_pair(
                &mut pairs_to_create,
                &mut suggestions_to_create,
                &mut decision_snapshots_to_create,
                new_entity_group_id,
                ordered_id_1,
                ordered_id_2,
                ordered_match_values,
                final_confidence_score,
                *pre_rl_confidence_score,
                features_for_snapshot,
                reinforcement_orchestrator_option,
                pipeline_run_id,
                *distance_meters,
                *lat1,
                *lon1,
                *lat2,
                *lon2,
            );

            // Update local stats
            local_new_pairs += 1;
            local_entities.insert(ordered_id_1.clone());
            local_entities.insert(ordered_id_2.clone());
            local_confidence_scores.push(final_confidence_score);
        }
    }

    // Execute batch operations if there are pairs to create
    if !pairs_to_create.is_empty() {
        match batch_create_entity_groups(
            pool,
            pairs_to_create,
            suggestions_to_create,
            decision_snapshots_to_create,
        )
        .await
        {
            Ok(created_count) => {
                debug!(
                    "Geospatial: Successfully created {} pairs in batch",
                    created_count
                );
            }
            Err(e) => {
                warn!("Geospatial: Batch creation failed: {}", e);
                local_errors += 1;
            }
        }
    }

    // Update global stats
    {
        let mut stats = stats_mutex.lock().await;
        stats.0 += local_new_pairs;
        stats.1.extend(local_entities);
        stats.2.extend(local_confidence_scores);
        stats.3 += local_errors;
        stats.4 += local_pairs_processed;
        stats.5 += local_feature_extractions;
        stats.6 += local_feature_failures;
        stats.7 += local_skipped_pairs;

        if local_new_pairs > 0 {
            info!(
                "Geospatial: Batch {}/{} complete - created {} new pairs (total: {})",
                batch_num, total_batches, local_new_pairs, stats.0
            );
        } else {
            debug!(
                "Geospatial: Batch {}/{} complete - all pairs skipped (total created so far: {})",
                batch_num, total_batches, stats.0
            );
        }
    }

    Ok(())
}

/// Helper function to normalize pair order and create match values
fn normalize_pair_for_storage(
    e1_id: EntityId,
    e2_id: EntityId,
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
    distance_meters: f64,
) -> (EntityId, EntityId, MatchValues) {
    // Ensure entity_id_1 < entity_id_2 to satisfy database ordering constraints
    if e1_id.0 <= e2_id.0 {
        let match_values = MatchValues::Geospatial(GeospatialMatchValue {
            latitude1: lat1,
            longitude1: lon1,
            latitude2: lat2,
            longitude2: lon2,
            distance: distance_meters,
        });
        (e1_id, e2_id, match_values)
    } else {
        // Swap entity IDs and corresponding coordinates
        let match_values = MatchValues::Geospatial(GeospatialMatchValue {
            latitude1: lat2,
            longitude1: lon2,
            latitude2: lat1,
            longitude2: lon1,
            distance: distance_meters,
        });
        (e2_id, e1_id, match_values)
    }
}

/// Modified candidate pairs query that excludes already existing pairs using ANTI-JOIN
async fn fetch_candidate_pairs_excluding_existing(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>> {
    let geo_candidates_query = "
        WITH EntityLocations AS (
            SELECT
                e.id AS entity_id,
                l.geom,
                l.latitude,
                l.longitude
            FROM
                public.entity e
            JOIN
                public.location l ON e.organization_id = l.organization_id
            WHERE
                l.geom IS NOT NULL AND e.id IS NOT NULL
        ),
        CandidatePairs AS (
            SELECT
                el1.entity_id AS entity_id_1_str,
                el2.entity_id AS entity_id_2_str,
                el1.latitude AS lat1,
                el1.longitude AS lon1,
                el2.latitude AS lat2,
                el2.longitude AS lon2,
                ST_Distance(el1.geom, el2.geom) AS distance_meters
            FROM
                EntityLocations el1
            JOIN
                EntityLocations el2 ON el1.entity_id < el2.entity_id -- Ensures order & avoids self-match
                AND ST_DWithin(el1.geom, el2.geom, $1)
        )
        SELECT
            cp.entity_id_1_str,
            cp.entity_id_2_str,
            cp.lat1,
            cp.lon1,
            cp.lat2,
            cp.lon2,
            cp.distance_meters
        FROM
            CandidatePairs cp
        LEFT JOIN
            public.entity_group eg ON 
                cp.entity_id_1_str = eg.entity_id_1 
                AND cp.entity_id_2_str = eg.entity_id_2 
                AND eg.method_type = 'geospatial'
        WHERE
            eg.id IS NULL  -- Only include pairs that don't already exist
    ";

    debug!("Geospatial: Executing geospatial candidate query with existing pair exclusion...");
    let candidate_rows = conn
        .query(geo_candidates_query, &[&METERS_TO_CHECK])
        .await
        .context("Geospatial: Candidate query with exclusion failed")?;

    let mut result = Vec::with_capacity(candidate_rows.len());
    for row in candidate_rows {
        let entity_id1_str: String = row.get("entity_id_1_str");
        let entity_id2_str: String = row.get("entity_id_2_str");
        let lat1: f64 = row.get("lat1");
        let lon1: f64 = row.get("lon1");
        let lat2: f64 = row.get("lat2");
        let lon2: f64 = row.get("lon2");
        let distance_meters: f64 = row.get("distance_meters");

        result.push((
            EntityId(entity_id1_str),
            EntityId(entity_id2_str),
            lat1,
            lon1,
            lat2,
            lon2,
            distance_meters,
        ));
    }

    Ok(result)
}

// New function to batch extract features for multiple pairs
async fn batch_extract_features(
    pool: &PgPool,
    pairs: &[(EntityId, EntityId)],
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
) -> Vec<Result<Vec<f64>, anyhow::Error>> {
    let mut results = Vec::with_capacity(pairs.len());

    // Calculate optimal batch size based on available CPUs
    let num_cpus = num_cpus::get();
    let batch_size = (pairs.len() / num_cpus.max(1)).max(1).min(50); // Max 50 pairs per batch, min 1

    info!(
        "Geospatial: Batch extracting features for {} pairs (batch size: {})",
        pairs.len(),
        batch_size
    );

    // Process in smaller batches for better parallelism
    for chunk in pairs.chunks(batch_size) {
        let mut futures = Vec::with_capacity(chunk.len());

        for (e1_id, e2_id) in chunk {
            // Clone for the async block
            let pool_clone = pool.clone();
            let e1_clone = e1_id.clone();
            let e2_clone = e2_id.clone();

            // Spawn a task for feature extraction
            let future = tokio::spawn(async move {
                MatchingOrchestrator::extract_pair_context_features(
                    &pool_clone,
                    &e1_clone,
                    &e2_clone,
                )
                .await
            });

            futures.push(future);
        }

        // Wait for all futures in this chunk
        let chunk_results = futures::future::join_all(futures).await;

        // Process results
        for result in chunk_results {
            match result {
                Ok(inner_result) => results.push(inner_result),
                Err(e) => results.push(Err(anyhow::anyhow!("Task join error: {}", e))),
            }
        }
    }

    results
}

// Helper function to process a single pair - refactored from duplicated code
async fn process_single_pair(
    pairs_to_create: &mut Vec<(EntityGroupId, EntityId, EntityId, MatchValues, f64, f64)>,
    suggestions_to_create: &mut Vec<(
        String,
        String,
        String,
        f64,
        serde_json::Value,
        String,
        String,
        i32,
        String,
    )>,
    decision_snapshots_to_create: &mut Vec<(String, String, Vec<f64>, f64, f64, i32)>,
    new_entity_group_id: &EntityGroupId,
    ordered_id_1: &EntityId,
    ordered_id_2: &EntityId,
    ordered_match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    distance_meters: f64,
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
) {
    // Add to pairs to create
    pairs_to_create.push((
        new_entity_group_id.clone(),
        ordered_id_1.clone(),
        ordered_id_2.clone(),
        ordered_match_values.clone(),
        final_confidence_score,
        pre_rl_confidence_score,
    ));

    // Create suggestion for low confidence matches if needed
    if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
        let priority = if final_confidence_score < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
            2
        } else {
            1
        };

        let details_json = serde_json::json!({
            "method_type": MatchMethodType::Geospatial.as_str(),
            "distance_meters": distance_meters,
            "latitude1": lat1, "longitude1": lon1,
            "latitude2": lat2, "longitude2": lon2,
            "entity_group_id": &new_entity_group_id.0,
            "pre_rl_confidence": pre_rl_confidence_score,
        });

        let reason_message = format!(
            "Pair ({}, {}) matched by Geospatial (distance: {:.2}m) with tuned confidence ({:.4}).",
            ordered_id_1.0, ordered_id_2.0, distance_meters, final_confidence_score
        );

        suggestions_to_create.push((
            pipeline_run_id.to_string(),
            ActionType::ReviewEntityInGroup.as_str().to_string(),
            new_entity_group_id.0.clone(),
            final_confidence_score,
            details_json,
            "LOW_TUNED_CONFIDENCE_PAIR".to_string(),
            reason_message,
            priority as i32,
            SuggestionStatus::PendingReview.as_str().to_string(),
        ));
    }

    // Log decision snapshot if features are available
    if let (Some(orch), Some(features_vec)) =
        (reinforcement_orchestrator_option, features_for_snapshot)
    {
        let orchestrator_guard = orch.lock().await;
        let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;

        decision_snapshots_to_create.push((
            new_entity_group_id.0.clone(),
            pipeline_run_id.to_string(),
            features_vec,
            pre_rl_confidence_score,
            final_confidence_score,
            confidence_tuner_ver as i32,
        ));
    }
}

/// Create multiple entity groups in a single batch transaction
/// Create multiple entity groups in a single batch transaction
async fn batch_create_entity_groups(
    pool: &PgPool,
    pairs: Vec<(EntityGroupId, EntityId, EntityId, MatchValues, f64, f64)>,
    suggestions: Vec<(
        String,
        String,
        String,
        f64,
        serde_json::Value,
        String,
        String,
        i32,
        String,
    )>,
    decision_snapshots: Vec<(String, String, Vec<f64>, f64, f64, i32)>,
) -> Result<usize> {
    if pairs.is_empty() {
        return Ok(0);
    }

    // Get a connection for all operations
    let mut conn = pool
        .get()
        .await
        .context("Geospatial: Failed to get DB connection for batch entity group creation")?;

    // Start a transaction that will include all operations
    let tx = conn
        .transaction()
        .await
        .context("Geospatial: Failed to start transaction for batch entity group creation")?;

    // Prepare statement for entity_group insertion
    let stmt = tx
        .prepare(
            "INSERT INTO public.entity_group
        (id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
        pre_rl_confidence_score)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING
        RETURNING id", // Add RETURNING to know which inserts succeeded
        )
        .await
        .context("Failed to prepare entity_group insert statement")?;

    // Insert all entity groups and track which ones were successfully inserted
    let mut created_count = 0;
    let mut successful_group_ids = HashSet::new();

    // Create a map from group_id to its index in the suggestions and decision_snapshots arrays
    let mut suggestion_map: HashMap<String, Vec<usize>> = HashMap::new();
    let mut decision_map: HashMap<String, Vec<usize>> = HashMap::new();

    // Populate the maps
    for (i, (_, _, group_id, _, _, _, _, _, _)) in suggestions.iter().enumerate() {
        suggestion_map.entry(group_id.clone()).or_default().push(i);
    }

    for (i, (group_id, _, _, _, _, _)) in decision_snapshots.iter().enumerate() {
        decision_map.entry(group_id.clone()).or_default().push(i);
    }

    for (group_id, entity_id_1, entity_id_2, match_values, confidence_score, pre_rl_score) in pairs
    {
        // Serialize match values
        let match_values_json = match serde_json::to_value(&match_values) {
            Ok(json) => json,
            Err(e) => {
                warn!("Geospatial: Failed to serialize match values: {}", e);
                continue;
            }
        };

        // Execute insert
        match tx
            .query(
                // Changed from execute to query to get results
                &stmt,
                &[
                    &group_id.0,
                    &entity_id_1.0,
                    &entity_id_2.0,
                    &MatchMethodType::Geospatial.as_str(),
                    &match_values_json,
                    &confidence_score,
                    &pre_rl_score,
                ],
            )
            .await
        {
            Ok(rows) => {
                if !rows.is_empty() {
                    // This insert succeeded
                    created_count += 1;
                    successful_group_ids.insert(group_id.0.clone());
                }
            }
            Err(e) => {
                warn!("Geospatial: Failed to insert entity group: {}", e);
            }
        }
    }

    // Only insert suggestions for entity groups that were successfully created
    if !suggestions.is_empty() {
        let suggestion_stmt = tx
            .prepare(
                "INSERT INTO clustering_metadata.suggested_actions (
                pipeline_run_id, action_type, group_id_1, 
                triggering_confidence, details, reason_code, reason_message, priority, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            )
            .await
            .context("Failed to prepare suggestion insert statement")?;

        for (
            i,
            (
                pipeline_run_id,
                action_type,
                group_id,
                confidence,
                details,
                reason_code,
                reason_message,
                priority,
                status,
            ),
        ) in suggestions.iter().enumerate()
        {
            // Only insert if the referenced entity group was successfully created
            if successful_group_ids.contains(group_id) {
                if let Err(e) = tx
                    .execute(
                        &suggestion_stmt,
                        &[
                            pipeline_run_id,
                            action_type,
                            group_id,
                            confidence,
                            details,
                            reason_code,
                            reason_message,
                            priority,
                            status,
                        ],
                    )
                    .await
                {
                    warn!(
                        "Geospatial: Failed to insert suggestion for existing group {}: {}",
                        group_id, e
                    );
                }
            }
        }
    }

    // Only insert decision snapshots for entity groups that were successfully created
    if !decision_snapshots.is_empty() {
        let decision_stmt = tx
            .prepare(
                "INSERT INTO clustering_metadata.match_decision_details (
                entity_group_id, pipeline_run_id, snapshotted_features,
                method_type_at_decision, pre_rl_confidence_at_decision,
                tuned_confidence_at_decision, confidence_tuner_version_at_decision
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .await
            .context("Failed to prepare decision snapshot insert statement")?;

        for (group_id, run_id, features, pre_rl_score, tuned_score, tuner_version) in
            decision_snapshots.iter()
        {
            // Only insert if the referenced entity group was successfully created
            if successful_group_ids.contains(group_id) {
                // Serialize features
                let features_json = match serde_json::to_value(features) {
                    Ok(json) => json,
                    Err(e) => {
                        warn!("Geospatial: Failed to serialize features: {}", e);
                        continue;
                    }
                };

                if let Err(e) = tx
                    .execute(
                        &decision_stmt,
                        &[
                            group_id,
                            run_id,
                            &features_json,
                            &MatchMethodType::Geospatial.as_str(),
                            pre_rl_score,
                            tuned_score,
                            tuner_version,
                        ],
                    )
                    .await
                {
                    warn!(
                        "Geospatial: Failed to insert decision snapshot for existing group {}: {}",
                        group_id, e
                    );
                }
            }
        }
    }

    // Commit the transaction with all operations
    tx.commit().await?;

    Ok(created_count)
}

/// Helper function to fetch a set of valid entity IDs
async fn fetch_valid_entity_ids(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<HashSet<String>> {
    let rows = conn
        .query("SELECT id FROM public.entity", &[])
        .await
        .context("Geospatial: Failed to query valid entity IDs")?;

    let mut valid_ids = HashSet::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        valid_ids.insert(id);
    }

    Ok(valid_ids)
}

/// Helper function to fetch existing pairs with method type parameter
async fn fetch_existing_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(EntityId, EntityId)>> {
    debug!(
        "Geospatial: Fetching existing {}-matched pairs...",
        method_type.as_str()
    );
    let existing_pairs_query = "
        SELECT entity_id_1, entity_id_2
        FROM public.entity_group
        WHERE method_type = $1";

    let existing_pair_rows = conn
        .query(existing_pairs_query, &[&method_type.as_str()])
        .await
        .context(format!(
            "Geospatial: Failed to query existing {}-matched pairs",
            method_type.as_str()
        ))?;

    let mut existing_processed_pairs: HashSet<(EntityId, EntityId)> =
        HashSet::with_capacity(existing_pair_rows.len());
    for row in existing_pair_rows {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        // Always ensure consistent ordering for lookup - smaller ID first
        if id1_str < id2_str {
            existing_processed_pairs.insert((EntityId(id1_str), EntityId(id2_str)));
        } else {
            existing_processed_pairs.insert((EntityId(id2_str), EntityId(id1_str)));
        }
    }

    Ok(existing_processed_pairs)
}

/// Helper function to fetch all candidate pairs
async fn fetch_candidate_pairs(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>> {
    let geo_candidates_query = "
        WITH EntityLocations AS (
            SELECT
                e.id AS entity_id,
                l.geom,
                l.latitude,
                l.longitude
            FROM
                public.entity e
            JOIN
                public.location l ON e.organization_id = l.organization_id
            WHERE
                l.geom IS NOT NULL AND e.id IS NOT NULL
        )
        SELECT
            el1.entity_id AS entity_id_1_str,
            el2.entity_id AS entity_id_2_str,
            el1.latitude AS lat1,
            el1.longitude AS lon1,
            el2.latitude AS lat2,
            el2.longitude AS lon2,
            ST_Distance(el1.geom, el2.geom) AS distance_meters
        FROM
            EntityLocations el1
        JOIN
            EntityLocations el2 ON el1.entity_id < el2.entity_id -- Ensures order & avoids self-match
            AND ST_DWithin(el1.geom, el2.geom, $1)
    ";

    debug!("Geospatial: Executing geospatial candidate query...");
    let candidate_rows = conn
        .query(geo_candidates_query, &[&METERS_TO_CHECK])
        .await
        .context("Geospatial: Candidate query failed")?;

    let mut result = Vec::with_capacity(candidate_rows.len());
    for row in candidate_rows {
        let entity_id1_str: String = row.get("entity_id_1_str");
        let entity_id2_str: String = row.get("entity_id_2_str");
        let lat1: f64 = row.get("lat1");
        let lon1: f64 = row.get("lon1");
        let lat2: f64 = row.get("lat2");
        let lon2: f64 = row.get("lon2");
        let distance_meters: f64 = row.get("distance_meters");

        result.push((
            EntityId(entity_id1_str),
            EntityId(entity_id2_str),
            lat1,
            lon1,
            lat2,
            lon2,
            distance_meters,
        ));
    }

    Ok(result)
}
