// src/matching/geospatial.rs
use anyhow::{Context, Result};
// Removed NaiveDateTime as it wasn't used
use futures::future::join_all; // Removed try_join_all as it wasn't strictly necessary with individual error handling
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
    FeatureCacheService, SharedFeatureCache, // Assuming FeatureCacheService has get_pair_key
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, GeospatialMatchResult, MatchMethodStats};
use serde_json;

// Import pipeline_state_utils
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
    // EntitySignatureData and CachedComparisonResult are implicitly used via the utils
};

pub const METERS_TO_CHECK: f64 = 100.0; // 0.062 miles
const BATCH_SIZE: usize = 250;
const MAX_PARALLEL_BATCHES: usize = 4;

// SQL query for inserting into entity_group (assuming it might be needed directly or is good to have defined)
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score,
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING
RETURNING id";


/// Main function to find geospatial-based matches
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting pairwise geospatial matching (run ID: {}){} with INCREMENTAL CHECKS...", // Log update
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    let init_conn = pool
        .get()
        .await
        .context("Geospatial: Failed to get DB connection for initial queries")?;

    info!("Geospatial: First verifying entity existence in database...");
    let valid_entities = fetch_valid_entity_ids(&*init_conn).await?;
    info!(
        "Geospatial: Found {} valid entities in database.",
        valid_entities.len()
    );

    // Fetch candidate pairs, already excluding those in entity_group for 'geospatial'
    let candidate_pairs = fetch_candidate_pairs_excluding_existing(&*init_conn).await?;
    drop(init_conn);

    let total_candidate_count = candidate_pairs.len();
    info!(
        "Geospatial: Found {} new geospatial pairs from DB query (excluding already in entity_group).",
        total_candidate_count
    );

    let filtered_candidates: Vec<_> = candidate_pairs
        .into_iter()
        .filter(|(e1, e2, ..)| valid_entities.contains(&e1.0) && valid_entities.contains(&e2.0))
        .collect();

    info!(
        "Geospatial: Filtered out {} pairs with invalid entity IDs, {} valid pairs remain.",
        total_candidate_count - filtered_candidates.len(),
        filtered_candidates.len()
    );

    if filtered_candidates.is_empty() {
        info!("Geospatial: No new pairs to process.");
        // Return empty stats
        let method_stats = MatchMethodStats {
            method_type: MatchMethodType::Geospatial,
            groups_created: 0,
            entities_matched: 0,
            avg_confidence: 0.0,
            avg_group_size: 0.0,
        };
        return Ok(AnyMatchResult::Geospatial(GeospatialMatchResult {
            groups_created: 0,
            stats: method_stats,
        }));
    }

    // Shared set to track pairs processed in this run to avoid redundant work if a pair appears multiple times
    // (e.g. due to complex upstream logic, though fetch_candidate_pairs should make them unique)
    // More importantly, it's used to skip after a cache hit or after processing a cache miss.
    let processed_pairs_this_run_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));


    // Stats: (new_pairs, entities_set, conf_scores, errors, pairs_processed_db_query,
    //         feature_extractions, feature_failures, cache_hits)
    let stats_mutex = Arc::new(Mutex::new((
        0,              // new_pairs_created_count
        HashSet::new(), // entities_in_new_pairs
        Vec::new(),     // confidence_scores_for_stats
        0,              // individual_operation_errors
        0,              // pairs_processed_from_db_candidates (renamed for clarity)
        0,              // feature_extraction_count
        0,              // feature_extraction_failures
        0,              // cache_hits_count (new)
    )));

    let total_batches = (filtered_candidates.len() + BATCH_SIZE - 1) / BATCH_SIZE;
    info!(
        "Geospatial: Processing {} batches of size {} with max {} parallel batches",
        total_batches, BATCH_SIZE, MAX_PARALLEL_BATCHES
    );

    let mut global_batch_num = 0;
    for chunk_of_batches in filtered_candidates.chunks(BATCH_SIZE * MAX_PARALLEL_BATCHES) {
        let mut batch_futures = Vec::new();
        for batch_data in chunk_of_batches.chunks(BATCH_SIZE) {
            global_batch_num += 1;
            let batch_to_process = batch_data.to_vec(); // Clone batch data for the async task
            let pool_clone = pool.clone();
            let ro_option_clone = reinforcement_orchestrator_option.clone();
            let run_id_clone = pipeline_run_id.to_string();
            let feature_cache_clone = feature_cache.clone();
            let stats_arc_clone = stats_mutex.clone();
            let processed_pairs_arc_clone = processed_pairs_this_run_arc.clone();
            let current_batch_num = global_batch_num;

            batch_futures.push(tokio::spawn(async move {
                process_batch(
                    batch_to_process,
                    &pool_clone,
                    ro_option_clone.as_ref(),
                    &run_id_clone,
                    feature_cache_clone,
                    stats_arc_clone,
                    processed_pairs_arc_clone, // Pass the Arc<Mutex<HashSet>>
                    current_batch_num,
                    total_batches,
                )
                .await
            }));
        }

        let results = join_all(batch_futures).await;
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                warn!(
                    "Geospatial: Batch processing task (approx. batch {}) error: {}",
                    global_batch_num - chunk_of_batches.chunks(BATCH_SIZE).len() + i + 1, e
                );
                // Potentially increment an error counter in stats_mutex if task-level errors are critical
            }
        }
    }

    let (
        new_pairs_created_count,
        entities_in_new_pairs,
        confidence_scores_for_stats,
        individual_operation_errors,
        pairs_processed_from_db_candidates,
        feature_extraction_count,
        feature_extraction_failures,
        cache_hits_count,
    ) = {
        let stats_guard = stats_mutex.lock().await;
        (
            stats_guard.0,
            stats_guard.1.clone(),
            stats_guard.2.clone(),
            stats_guard.3,
            stats_guard.4,
            stats_guard.5,
            stats_guard.6,
            stats_guard.7,
        )
    };

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
        avg_group_size: if new_pairs_created_count > 0 { 2.0 } else { 0.0 },
    };

    let elapsed = start_time.elapsed();
    info!(
        "Geospatial matching complete in {:.2?}: processed {} candidate pairs from DB, cache hits: {}, created {} new pairs ({} errors), involving {} unique entities.",
        elapsed,
        pairs_processed_from_db_candidates, // This counts pairs attempted from the initial DB query
        cache_hits_count,
        method_stats.groups_created,
        individual_operation_errors,
        method_stats.entities_matched
    );
    info!(
        "Geospatial feature extraction stats: {} successful, {} failed",
        feature_extraction_count, feature_extraction_failures
    );

    Ok(AnyMatchResult::Geospatial(GeospatialMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Process a batch of candidate pairs.
#[allow(clippy::too_many_arguments)] // Existing function signature
async fn process_batch(
    batch: Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>, // (e1, e2, lat1, lon1, lat2, lon2, dist)
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Used for RL feature caching, not the main comparison cache
    stats_mutex: Arc<
        Mutex<(
            usize,                // new_pairs_created_count
            HashSet<EntityId>,    // entities_in_new_pairs
            Vec<f64>,             // confidence_scores_for_stats
            usize,                // individual_operation_errors
            usize,                // pairs_processed_from_db_candidates
            usize,                // feature_extraction_count
            usize,                // feature_extraction_failures
            usize,                // cache_hits_count
        )>,
    >,
    processed_pairs_this_run_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    batch_num: usize,
    total_batches: usize,
) -> Result<()> {
    info!(
        "Geospatial: Processing batch {}/{} ({} pairs)...",
        batch_num,
        total_batches,
        batch.len()
    );

    let mut pairs_to_create_in_db = Vec::new();
    let mut suggestions_to_create_in_db = Vec::new();
    let mut decision_snapshots_to_create_in_db = Vec::new();

    // Local batch stats to minimize lock contention on global stats_mutex
    let mut local_new_pairs_count = 0;
    let mut local_entities_in_new_pairs = HashSet::new();
    let mut local_confidence_scores = Vec::new();
    let mut local_individual_operation_errors = 0;
    let mut local_pairs_processed_from_db = 0;
    let mut local_feature_extraction_count = 0;
    let mut local_feature_extraction_failures = 0;
    let mut local_cache_hits_count = 0;


    for (e1_id_orig, e2_id_orig, lat1, lon1, lat2, lon2, distance_meters) in batch {
        local_pairs_processed_from_db += 1; // Count each pair from the input batch

        // Ensure consistent ordering for cache keys, processed_pairs_this_run, and entity_group
        let (ordered_e1_id, ordered_e2_id, _ordered_lat1, _ordered_lon1, _ordered_lat2, _ordered_lon2) =
            if e1_id_orig.0 < e2_id_orig.0 {
                (e1_id_orig.clone(), e2_id_orig.clone(), lat1, lon1, lat2, lon2)
            } else {
                (e2_id_orig.clone(), e1_id_orig.clone(), lat2, lon2, lat1, lon1)
            };
        
        let current_pair_ordered = (ordered_e1_id.clone(), ordered_e2_id.clone());

        // Check if this ordered pair has already been processed in this run (e.g., by a previous batch or earlier in this one)
        // This check is crucial if the input `filtered_candidates` could somehow contain duplicates despite earlier filtering.
        // It also handles the case where a pair is processed after a cache miss.
        {
            let mut processed_set = processed_pairs_this_run_arc.lock().await;
            if processed_set.contains(&current_pair_ordered) {
                debug!("Geospatial: Pair ({}, {}) already processed in this run. Skipping.", ordered_e1_id.0, ordered_e2_id.0);
                continue; // Skip to the next pair in the batch
            }
            // Will add to processed_set later, after cache check/comparison
        }


        // --- INCREMENTAL PROCESSING LOGIC ---
        let current_signatures_opt = match get_current_signatures_for_pair(pool, &ordered_e1_id, &ordered_e2_id).await {
            Ok(sigs) => sigs,
            Err(e) => {
                warn!("Geospatial: Failed to get signatures for pair ({}, {}): {}. Proceeding without cache.", ordered_e1_id.0, ordered_e2_id.0, e);
                None
            }
        };

        if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
            match check_comparison_cache(pool, &ordered_e1_id, &ordered_e2_id, &sig1_data.signature, &sig2_data.signature, &MatchMethodType::Geospatial).await {
                Ok(Some(cached_eval)) => {
                    local_cache_hits_count += 1;
                    debug!("Geospatial: Cache HIT for pair ({}, {}). Result: {}, Score: {:?}", ordered_e1_id.0, ordered_e2_id.0, cached_eval.comparison_result, cached_eval.similarity_score);
                    
                    // Mark as processed to avoid re-processing if it somehow appears again
                    let mut processed_set = processed_pairs_this_run_arc.lock().await;
                    processed_set.insert(current_pair_ordered.clone());
                    drop(processed_set);

                    // If the cached result was a "MATCH", it should have been caught by fetch_candidate_pairs_excluding_existing
                    // if it led to an entity_group. If it was a "NON_MATCH", we correctly skip.
                    // No further action needed for a cache hit here, as we're skipping the comparison.
                    continue; // Skip to the next pair
                }
                Ok(None) => {
                    debug!("Geospatial: Cache MISS for pair ({}, {}). Signatures: ({}..., {}...). Proceeding with comparison.", ordered_e1_id.0, ordered_e2_id.0, &sig1_data.signature[..std::cmp::min(8, sig1_data.signature.len())], &sig2_data.signature[..std::cmp::min(8, sig2_data.signature.len())]);
                }
                Err(e) => {
                    warn!("Geospatial: Error checking comparison cache for pair ({}, {}): {}. Proceeding with comparison.", ordered_e1_id.0, ordered_e2_id.0, e);
                }
            }
        }
        // --- END INCREMENTAL PROCESSING LOGIC ---

        // If we've reached here, it's either a cache miss or an error occurred trying to use the cache.
        // Proceed with the actual comparison.
        
        let pre_rl_confidence_score = 0.85; // Default base confidence for geospatial match
        let mut final_confidence_score = pre_rl_confidence_score;
        let mut features_for_rl_snapshot: Option<Vec<f64>> = None; // For RL tuning and snapshotting
        let mut features_json_for_cache: Option<serde_json::Value> = None; // For comparison_cache

        if let Some(ro_arc) = reinforcement_orchestrator_option {
            // Use the existing feature_cache (for RL features) if available
            match if let Some(rl_feat_cache) = feature_cache.as_ref() {
                let orchestrator_guard = ro_arc.lock().await;
                // get_pair_features likely uses the rl_feat_cache internally
                orchestrator_guard.get_pair_features(pool, &ordered_e1_id, &ordered_e2_id).await
            } else {
                MatchingOrchestrator::extract_pair_context_features(pool, &ordered_e1_id, &ordered_e2_id).await
            } {
                Ok(features_vec) => {
                    local_feature_extraction_count += 1;
                    if !features_vec.is_empty() {
                        features_for_rl_snapshot = Some(features_vec.clone());
                        features_json_for_cache = serde_json::to_value(features_vec.clone()).ok(); // For comparison_cache

                        let orchestrator_guard = ro_arc.lock().await;
                        match orchestrator_guard.get_tuned_confidence(
                            &MatchMethodType::Geospatial,
                            pre_rl_confidence_score,
                            &features_vec, // Pass the Vec<f64>
                        ) {
                            Ok(tuned_score) => final_confidence_score = tuned_score,
                            Err(e) => warn!("Geospatial: RL tuning failed for ({}, {}): {}. Using pre-RL score.", ordered_e1_id.0, ordered_e2_id.0, e),
                        }
                    } else {
                         warn!("Geospatial: Extracted features vector is empty for pair ({}, {}). Using pre-RL score.", ordered_e1_id.0, ordered_e2_id.0);
                    }
                }
                Err(e) => {
                    local_feature_extraction_failures += 1;
                    warn!("Geospatial: Feature extraction failed for ({}, {}): {}. Using pre-RL score.", ordered_e1_id.0, ordered_e2_id.0, e);
                }
            }
        }

        // Determine outcome for caching and entity_group creation
        // For geospatial, a "MATCH" is simply being within METERS_TO_CHECK.
        // The final_confidence_score (possibly tuned) is what's stored and used for suggestions.
        let comparison_outcome_for_cache = if distance_meters <= METERS_TO_CHECK { "MATCH" } else { "NON_MATCH" };

        // Store result in comparison_cache if signatures were available
        if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
            if let Err(e) = store_in_comparison_cache(
                pool, &ordered_e1_id, &ordered_e2_id,
                &sig1_data.signature, &sig2_data.signature,
                &MatchMethodType::Geospatial, pipeline_run_id,
                comparison_outcome_for_cache, Some(final_confidence_score), // Store the final (possibly tuned) score
                features_json_for_cache.as_ref(), // Pass Option<&JsonValue>
            ).await {
                warn!("Geospatial: Failed to store in comparison_cache for ({}, {}): {}", ordered_e1_id.0, ordered_e2_id.0, e);
            }
        }
        
        // Mark as processed *after* comparison and cache storage attempt
        {
            let mut processed_set = processed_pairs_this_run_arc.lock().await;
            processed_set.insert(current_pair_ordered);
        }


        // Only create entity_group if it's a "MATCH" based on distance
        if comparison_outcome_for_cache == "MATCH" {
            let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());
            let (_, _, match_values_for_storage) = normalize_pair_for_storage(
                e1_id_orig.clone(), // Use original IDs for correct lat/lon in match_values
                e2_id_orig.clone(),
                lat1, lon1, lat2, lon2, distance_meters
            );

            // Prepare data for batch insertion (this was the role of process_single_pair)
            pairs_to_create_in_db.push((
                new_entity_group_id.clone(),
                ordered_e1_id.clone(), // Use ordered IDs for entity_group table
                ordered_e2_id.clone(),
                match_values_for_storage, // Contains original lat/lon relative to e1_id_orig, e2_id_orig
                final_confidence_score,
                pre_rl_confidence_score,
            ));

            local_new_pairs_count += 1;
            local_entities_in_new_pairs.insert(ordered_e1_id.clone());
            local_entities_in_new_pairs.insert(ordered_e2_id.clone());
            local_confidence_scores.push(final_confidence_score);

            // Create suggestion if confidence is low
            if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
                let priority = if final_confidence_score < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };
                let details_json = serde_json::json!({
                    "method_type": MatchMethodType::Geospatial.as_str(),
                    "distance_meters": distance_meters,
                    "latitude1": lat1, "longitude1": lon1, // original orientation for context
                    "latitude2": lat2, "longitude2": lon2,
                    "entity_group_id": &new_entity_group_id.0,
                    "pre_rl_confidence": pre_rl_confidence_score,
                    "final_confidence": final_confidence_score,
                });
                let reason_message = format!(
                    "Pair ({}, {}) matched by Geospatial (distance: {:.2}m) with tuned confidence ({:.4}). Pre-RL: {:.2}",
                    ordered_e1_id.0, ordered_e2_id.0, distance_meters, final_confidence_score, pre_rl_confidence_score
                );
                suggestions_to_create_in_db.push((
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

            // Log decision snapshot if features were generated
            if let (Some(ro_arc_inner), Some(features_vec)) = (reinforcement_orchestrator_option, features_for_rl_snapshot) {
                let orchestrator_guard = ro_arc_inner.lock().await;
                let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;
                decision_snapshots_to_create_in_db.push((
                    new_entity_group_id.0.clone(),
                    pipeline_run_id.to_string(),
                    features_vec, // This is Vec<f64>
                    pre_rl_confidence_score,
                    final_confidence_score,
                    confidence_tuner_ver as i32,
                ));
            }
        }
    } // End of loop over batch pairs

    // Execute batch DB operations if there's anything to create
    if !pairs_to_create_in_db.is_empty() {
        match batch_create_entity_groups(
            pool,
            pairs_to_create_in_db,
            suggestions_to_create_in_db,
            decision_snapshots_to_create_in_db,
        )
        .await
        {
            Ok(created_count_from_db) => {
                debug!(
                    "Geospatial: Batch {}/{} - Successfully created {} entity groups in DB.",
                    batch_num, total_batches, created_count_from_db
                );
                 // Note: local_new_pairs_count is already incremented when preparing data.
                 // created_count_from_db is the actual number inserted, could be less due to ON CONFLICT.
                 // For accurate stats, we might need to reconcile, but local_new_pairs_count reflects attempts.
            }
            Err(e) => {
                warn!("Geospatial: Batch {}/{} - DB creation failed: {}", batch_num, total_batches, e);
                local_individual_operation_errors += 1; // Count as one batch error
            }
        }
    }

    // Update global stats from local batch stats
    {
        let mut stats_guard = stats_mutex.lock().await;
        stats_guard.0 += local_new_pairs_count;
        stats_guard.1.extend(local_entities_in_new_pairs);
        stats_guard.2.extend(local_confidence_scores);
        stats_guard.3 += local_individual_operation_errors;
        stats_guard.4 += local_pairs_processed_from_db;
        stats_guard.5 += local_feature_extraction_count;
        stats_guard.6 += local_feature_extraction_failures;
        stats_guard.7 += local_cache_hits_count;

        if local_new_pairs_count > 0 || local_cache_hits_count > 0 {
            info!(
                "Geospatial: Batch {}/{} complete - created {} new pairs, {} cache hits (total created: {}, total hits: {}). Processed {} candidates from DB.",
                batch_num, total_batches, local_new_pairs_count, local_cache_hits_count, stats_guard.0, stats_guard.7, local_pairs_processed_from_db
            );
        } else {
            debug!(
                "Geospatial: Batch {}/{} complete - no new pairs or cache hits. Processed {} candidates from DB.",
                batch_num, total_batches, local_pairs_processed_from_db
            );
        }
    }

    Ok(())
}


/// Helper function to normalize pair order and create match values.
/// Ensures entity_id_1 < entity_id_2 for the returned EntityId tuple.
/// The MatchValues will store coordinates relative to the *original* e1_id, e2_id.
fn normalize_pair_for_storage(
    e1_id: EntityId,
    e2_id: EntityId,
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
    distance_meters: f64,
) -> (EntityId, EntityId, MatchValues) {
    // Determine ordered IDs for storage in entity_group or cache keys
    let (ordered_e1_id, ordered_e2_id) = if e1_id.0 <= e2_id.0 {
        (e1_id.clone(), e2_id.clone())
    } else {
        (e2_id.clone(), e1_id.clone())
    };

    // MatchValues should reflect the actual entities being compared,
    // so use original lat/lon associated with e1_id and e2_id.
    // If e1_id (original) is now ordered_e1_id, its coords are (lat1, lon1).
    // If e1_id (original) is now ordered_e2_id, its coords are (lat1, lon1), but they'll be stored as lat2/lon2 in MatchValues.
    // This ensures MatchValues always has coords for (item1, item2) where item1.id < item2.id.
    
    let (mv_lat1, mv_lon1, mv_lat2, mv_lon2) = if e1_id.0 <= e2_id.0 {
        (lat1, lon1, lat2, lon2) // e1_id is item1, e2_id is item2
    } else {
        (lat2, lon2, lat1, lon1) // e2_id is item1, e1_id is item2
    };

    let match_values = MatchValues::Geospatial(GeospatialMatchValue {
        latitude1: mv_lat1,
        longitude1: mv_lon1,
        latitude2: mv_lat2,
        longitude2: mv_lon2,
        distance: distance_meters,
    });
    (ordered_e1_id, ordered_e2_id, match_values)
}


/// Fetches candidate pairs excluding those already in `public.entity_group` for 'geospatial' method.
async fn fetch_candidate_pairs_excluding_existing(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>> {
    let geo_candidates_query = "
        WITH EntityLocations AS (
            SELECT
                e.id AS entity_id,
                l.geom, /* Assuming PostGIS geometry column */
                l.latitude,
                l.longitude
            FROM
                public.entity e
            /* This join implies one location per organization, which might be a simplification.
               If an entity can be linked to multiple locations, or if locations are directly
               linked to entities via entity_feature, this query would need adjustment.
               For now, assuming public.location.organization_id links to public.entity.organization_id
               and that this is the basis for an entity's location.
            */
            JOIN
                public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
            JOIN
                public.location l ON ef.table_id = l.id
            WHERE
                l.geom IS NOT NULL AND e.id IS NOT NULL AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL
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
                AND ST_DWithin(el1.geom, el2.geom, $1) -- $1 is METERS_TO_CHECK
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
            eg.id IS NULL  -- Only include pairs that don't already exist in entity_group for this method
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

// Batch feature extraction remains largely the same, called by process_batch if needed.
// No direct changes needed for caching strategy here, as it's about RL features.
async fn batch_extract_features(
    pool: &PgPool,
    pairs: &[(EntityId, EntityId)], // Expects already ordered pairs if relevant for cache
    _reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>, // Not directly used here, but implies RL context
) -> Vec<Result<Vec<f64>>> { // Simpler return type based on usage
    let mut results = Vec::with_capacity(pairs.len());
    if pairs.is_empty() {
        return results;
    }

    let num_cpus = num_cpus::get_physical(); // Use physical cores
    let batch_size = (pairs.len() / num_cpus.max(1)).max(1).min(50);

    info!(
        "Geospatial (RL Features): Batch extracting features for {} pairs (batch size: {})",
        pairs.len(),
        batch_size
    );

    for chunk in pairs.chunks(batch_size) {
        let mut futures = Vec::with_capacity(chunk.len());
        for (e1_id, e2_id) in chunk {
            let pool_clone = pool.clone();
            let e1_clone = e1_id.clone();
            let e2_clone = e2_id.clone();
            futures.push(tokio::spawn(async move {
                MatchingOrchestrator::extract_pair_context_features(&pool_clone, &e1_clone, &e2_clone).await
            }));
        }
        let chunk_results = join_all(futures).await;
        for result in chunk_results {
            match result {
                Ok(inner_result) => results.push(inner_result),
                Err(e) => results.push(Err(anyhow::anyhow!("Task join error for feature extraction: {}", e))),
            }
        }
    }
    results
}


// process_single_pair is effectively integrated into process_batch's main loop now.
// The logic for preparing data for batch_create_entity_groups is inside process_batch.

/// Batch creates entity groups, suggestions, and decision snapshots in a transaction.
#[allow(clippy::too_many_arguments)] // Existing function signature
async fn batch_create_entity_groups(
    pool: &PgPool,
    pairs_data: Vec<(EntityGroupId, EntityId, EntityId, MatchValues, f64, f64)>, // (group_id, e1, e2, match_values, final_conf, pre_rl_conf)
    suggestions_data: Vec<(
        String,         // pipeline_run_id
        String,         // action_type
        String,         // group_id_1 (entity_group_id)
        f64,            // triggering_confidence
        serde_json::Value, // details
        String,         // reason_code
        String,         // reason_message
        i32,            // priority
        String,         // status
    )>,
    decision_snapshots_data: Vec<(
        String,         // entity_group_id
        String,         // pipeline_run_id
        Vec<f64>,       // snapshotted_features (Vec<f64>)
        f64,            // pre_rl_confidence_at_decision
        f64,            // tuned_confidence_at_decision
        i32,            // confidence_tuner_version_at_decision
    )>,
) -> Result<usize> { // Returns number of successfully inserted entity groups
    if pairs_data.is_empty() {
        return Ok(0);
    }

    let mut conn = pool.get().await.context("Geospatial: DB conn for batch create")?;
    let tx = conn.transaction().await.context("Geospatial: Start TX for batch create")?;

    // 1. Insert Entity Groups
    let entity_group_stmt = tx.prepare(INSERT_ENTITY_GROUP_SQL).await
        .context("Geospatial: Prepare entity_group insert")?;
    
    let mut successfully_inserted_group_ids = HashSet::new();
    for (group_id, e1_id, e2_id, match_values, final_conf, pre_rl_conf) in &pairs_data {
        let match_values_json = serde_json::to_value(match_values)
            .with_context(|| format!("Geospatial: Serialize match_values for group {}", group_id.0))?;
        
        let rows = tx.query(&entity_group_stmt, &[
            &group_id.0, &e1_id.0, &e2_id.0, &MatchMethodType::Geospatial.as_str(),
            &match_values_json, &final_conf, &pre_rl_conf,
        ]).await.with_context(|| format!("Geospatial: Insert entity_group {}", group_id.0))?;

        if !rows.is_empty() { // Check if RETURNING id produced a row
            successfully_inserted_group_ids.insert(group_id.0.clone());
        }
    }

    // 2. Insert Suggestions (only for successfully created groups)
    if !suggestions_data.is_empty() {
        let suggestion_stmt = tx.prepare(
            "INSERT INTO clustering_metadata.suggested_actions (
                pipeline_run_id, action_type, group_id_1, triggering_confidence, details, 
                reason_code, reason_message, priority, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ).await.context("Geospatial: Prepare suggestion insert")?;

        for (pr_id, act_type, grp_id, trig_conf, details, r_code, r_msg, prio, status) in &suggestions_data {
            if successfully_inserted_group_ids.contains(grp_id) {
                tx.execute(&suggestion_stmt, &[
                    pr_id, act_type, grp_id, trig_conf, details, r_code, r_msg, prio, status,
                ]).await.with_context(|| format!("Geospatial: Insert suggestion for group {}", grp_id))?;
            }
        }
    }

    // 3. Insert Decision Snapshots (only for successfully created groups)
    if !decision_snapshots_data.is_empty() {
        let decision_stmt = tx.prepare(
            "INSERT INTO clustering_metadata.match_decision_details (
                entity_group_id, pipeline_run_id, snapshotted_features, method_type_at_decision, 
                pre_rl_confidence_at_decision, tuned_confidence_at_decision, 
                confidence_tuner_version_at_decision
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)"
        ).await.context("Geospatial: Prepare decision_details insert")?;

        for (grp_id, pr_id, features_vec, pre_rl, tuned_conf, tuner_ver) in &decision_snapshots_data {
            if successfully_inserted_group_ids.contains(grp_id) {
                let features_json = serde_json::to_value(features_vec)
                    .with_context(|| format!("Geospatial: Serialize features for decision snapshot of group {}", grp_id))?;
                tx.execute(&decision_stmt, &[
                    grp_id, pr_id, &features_json, &MatchMethodType::Geospatial.as_str(),
                    pre_rl, tuned_conf, tuner_ver,
                ]).await.with_context(|| format!("Geospatial: Insert decision_details for group {}", grp_id))?;
            }
        }
    }

    tx.commit().await.context("Geospatial: Commit batch create transaction")?;
    Ok(successfully_inserted_group_ids.len())
}


/// Helper function to fetch a set of valid entity IDs
async fn fetch_valid_entity_ids(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<HashSet<String>> {
    let rows = conn
        .query("SELECT id FROM public.entity", &[])
        .await
        .context("Geospatial: Failed to query valid entity IDs")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

// fetch_existing_pairs is no longer needed as its logic is incorporated into fetch_candidate_pairs_excluding_existing
// fetch_candidate_pairs is also superseded by fetch_candidate_pairs_excluding_existing
