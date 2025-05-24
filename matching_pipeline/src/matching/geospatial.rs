// src/matching/geospatial.rs
use anyhow::{Context, Result};
use futures::future::join_all;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config;
use crate::db::{
    self, PgPool, // Using db::upsert_entity_group, db::insert_match_decision_detail_direct, db::insert_suggestion
};
use crate::models::{
    ActionType, EntityId, GeospatialMatchValue, MatchMethodType, MatchValues, NewSuggestedAction,
    SuggestionStatus,
};
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, GeospatialMatchResult, MatchMethodStats};
use serde_json;

pub const METERS_TO_CHECK: f64 = 100.0;
const BATCH_SIZE: usize = 250; // Number of pairs to process in one DB transaction/batch
const MAX_PARALLEL_BATCHES: usize = 4; // Number of DB transactions to run in parallel

/// Data structure to hold all necessary info for creating a group within a batch.
struct PairToProcessGeo {
    entity_id_1: EntityId,
    entity_id_2: EntityId,
    lat1: f64, // Retain original lat/lon for suggestions if needed
    lon1: f64,
    lat2: f64,
    lon2: f64,
    distance_meters: f64,
    base_confidence: f64,
    final_confidence: f64,
    match_values: MatchValues, // Already ordered
    features: Option<Vec<f64>>,
    sig1: Option<String>, // Signature of entity_id_1
    sig2: Option<String>, // Signature of entity_id_2
}

/// Statistics tracking for geospatial matching.
#[derive(Default)]
struct GeospatialStats {
    new_pairs_created_db: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores: Vec<f64>,
    task_errors: usize,
    pairs_processed_from_db: usize, // Candidates fetched from initial DB query
    feature_extractions: usize,
    feature_failures: usize,
    cache_hits: usize,
}

/// Main function to find geospatial-based matches.
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting V2 (Direct DB) pairwise geospatial matching (run ID: {}){}",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL tuning"
        } else {
            ""
        }
    );
    let start_time = Instant::now();

    let init_conn = pool.get().await.context("Geospatial: DB conn")?;
    let valid_entities = fetch_valid_entity_ids(&*init_conn).await?;
    // Fetch pairs that are NOT already in entity_group for 'geospatial' method type
    let candidate_pairs = fetch_candidate_pairs_excluding_existing(&*init_conn).await?;
    drop(init_conn);

    info!(
        "Geospatial: Found {} candidate pairs from DB, {} valid entities.",
        candidate_pairs.len(),
        valid_entities.len()
    );

    // Filter candidate_pairs to only include pairs where both entities are in valid_entities
    let filtered_candidates: Vec<_> = candidate_pairs
        .into_iter()
        .filter(|(e1, e2, ..)| valid_entities.contains(&e1.0) && valid_entities.contains(&e2.0))
        .collect();

    let total_to_process = filtered_candidates.len();
    info!(
        "Geospatial: {} valid pairs remain after filtering by valid_entities set.",
        total_to_process
    );

    if filtered_candidates.is_empty() {
        return Ok(AnyMatchResult::Geospatial(GeospatialMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(MatchMethodType::Geospatial),
        }));
    }

    let stats_mutex = Arc::new(Mutex::new(GeospatialStats::default()));
    // This set tracks pairs processed *within this run* to avoid redundant work if a pair appears multiple times from source query
    let processed_pairs_this_run_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));

    let total_batches = (total_to_process + BATCH_SIZE - 1) / BATCH_SIZE;

    // Process chunks of batches in parallel
    for chunk_of_batches_data in
        filtered_candidates.chunks(BATCH_SIZE * MAX_PARALLEL_BATCHES)
    {
        let mut batch_processing_futures = Vec::new();
        for (batch_idx_in_chunk, actual_batch_data) in
            chunk_of_batches_data.chunks(BATCH_SIZE).enumerate()
        {
            // It's okay if actual_batch_data.len() < BATCH_SIZE for the last batch in a chunk/overall
            let batch_to_process_async = actual_batch_data.to_vec(); // Clone data for the async task

            batch_processing_futures.push(tokio::spawn({
                // Clone Arcs and other necessary data for the spawned task
                let pool_clone = pool.clone();
                let ro_opt_clone = reinforcement_orchestrator_option.clone();
                let run_id_clone = pipeline_run_id.to_string();
                let feat_cache_clone = feature_cache.clone();
                let stats_clone = Arc::clone(&stats_mutex);
                let processed_clone = Arc::clone(&processed_pairs_this_run_arc);
                // batch_num is just for logging, can be simplified if not strictly needed for logic
                let current_batch_num_for_log = batch_idx_in_chunk + 1;

                async move {
                    process_single_batch_geo( // Renamed from process_batch_geo to avoid confusion
                        batch_to_process_async,
                        &pool_clone,
                        ro_opt_clone.as_ref(), // Pass as Option<&Arc<...>>
                        &run_id_clone,
                        feat_cache_clone.as_ref(), // Pass as Option<&SharedFeatureCache>
                        stats_clone,
                        processed_clone,
                        current_batch_num_for_log, // For logging
                        total_batches,             // For logging
                    )
                    .await
                }
            }));
        }
        // Wait for all futures in the current chunk of batches to complete
        let results = join_all(batch_processing_futures).await;
        for result in results {
            if let Err(e) = result {
                // This outer error is from tokio::spawn (e.g. panic in task)
                warn!("Geospatial: A batch processing task failed: {:?}", e);
                let mut stats = stats_mutex.lock().await;
                stats.task_errors += 1; // Increment general task error
            }
            // Inner Result<()> from process_single_batch_geo is handled within that function by updating stats.task_errors
        }
    }

    let final_stats = stats_mutex.lock().await;
    let avg_confidence = if !final_stats.confidence_scores.is_empty() {
        final_stats.confidence_scores.iter().sum::<f64>()
            / final_stats.confidence_scores.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Geospatial,
        groups_created: final_stats.new_pairs_created_db,
        entities_matched: final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats.new_pairs_created_db > 0 {
            2.0 // Always 2 for pairwise
        } else {
            0.0
        },
    };

    info!(
        "Geospatial matching V2 (Direct DB) complete in {:.2?}: {} candidate pairs from DB, {} cache hits, {} new groups ({} processing errors).",
        start_time.elapsed(),
        final_stats.pairs_processed_from_db, // Total pairs fetched from DB initially
        final_stats.cache_hits,
        final_stats.new_pairs_created_db,
        final_stats.task_errors
    );
    Ok(AnyMatchResult::Geospatial(GeospatialMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Processes a single batch of candidate pairs.
#[allow(clippy::too_many_arguments)]
async fn process_single_batch_geo(
    batch_data: Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>, // Contains (e1, e2, lat1, lon1, lat2, lon2, dist)
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>,
    stats_mutex: Arc<Mutex<GeospatialStats>>,
    processed_pairs_this_run_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    batch_num_for_log: usize,
    total_batches_for_log: usize,
) -> Result<()> { // Returns Result to indicate if the batch processing itself had an issue
    info!(
        "Geospatial: Processing batch {}/{} ({} pairs)...",
        batch_num_for_log,
        total_batches_for_log,
        batch_data.len()
    );
    let mut pairs_to_insert_into_db: Vec<PairToProcessGeo> = Vec::new();

    for (e1_orig, e2_orig, lat1, lon1, lat2, lon2, distance_meters) in batch_data {
        // Ensure canonical order for processing and cache keys
        let (e1_id, e2_id, o_lat1, o_lon1, o_lat2, o_lon2) = if e1_orig.0 < e2_orig.0 {
            (e1_orig, e2_orig, lat1, lon1, lat2, lon2)
        } else {
            (e2_orig, e1_orig, lat2, lon2, lat1, lon1)
        };
        let current_pair_ordered = (e1_id.clone(), e2_id.clone());

        { // Scope for processed_pairs_this_run_arc lock
            let mut processed_this_run = processed_pairs_this_run_arc.lock().await;
            if processed_this_run.contains(&current_pair_ordered) {
                continue; // Already processed in this run (e.g. by another concurrent batch if not careful with input splitting)
            }
            // Mark as being processed now to prevent other concurrent tasks from picking it up if input isn't perfectly disjoint.
            // This is more of a safeguard. Ideally, input to parallel tasks should be disjoint.
            processed_this_run.insert(current_pair_ordered.clone());
        }


        let mut sig1_opt: Option<String> = None;
        let mut sig2_opt: Option<String> = None;

        // Check comparison cache
        if let Ok(Some((s1_data, s2_data))) =
            get_current_signatures_for_pair(pool, &e1_id, &e2_id).await
        {
            sig1_opt = Some(s1_data.signature.clone());
            sig2_opt = Some(s2_data.signature.clone());
            if let Ok(Some(_cached_eval)) = check_comparison_cache(
                pool,
                &e1_id,
                &e2_id,
                &s1_data.signature,
                &s2_data.signature,
                &MatchMethodType::Geospatial,
            )
            .await
            {
                let mut stats = stats_mutex.lock().await;
                stats.cache_hits += 1;
                continue; // Skip if cache hit
            }
        }

        // If cache miss, proceed with confidence calculation and feature extraction
        let pre_rl_confidence = 0.85; // Base confidence for geospatial proximity
        let mut final_confidence = pre_rl_confidence;
        let mut features_vec: Option<Vec<f64>> = None;
        let mut features_json_for_cache: Option<serde_json::Value> = None;

        if let Some(ro_arc) = reinforcement_orchestrator_option {
            let mut stats_guard = stats_mutex.lock().await; // Lock stats for update
            stats_guard.feature_extractions += 1;
            drop(stats_guard); // Release lock before await

            match if let Some(fc_arc) = feature_cache {
                let mut fc_guard = fc_arc.lock().await;
                fc_guard.get_pair_features(pool, &e1_id, &e2_id).await
            } else {
                MatchingOrchestrator::extract_pair_context_features(pool, &e1_id, &e2_id).await
            } {
                Ok(features) => {
                    if !features.is_empty() {
                        features_vec = Some(features.clone());
                        features_json_for_cache = serde_json::to_value(features.clone()).ok();
                        let guard = ro_arc.lock().await;
                        match guard.get_tuned_confidence(
                            &MatchMethodType::Geospatial,
                            pre_rl_confidence,
                            &features,
                        ) {
                            Ok(tuned) => final_confidence = tuned,
                            Err(e) => {
                                warn!("Geo: RL tuning failed for ({},{}): {}", e1_id.0, e2_id.0, e)
                            }
                        }
                    }
                }
                Err(e) => {
                    let mut stats_guard = stats_mutex.lock().await;
                    stats_guard.feature_failures += 1;
                    drop(stats_guard);
                    warn!(
                        "Geo: Feature extraction failed for ({},{}): {}",
                        e1_id.0, e2_id.0, e
                    );
                }
            }
        }

        let (_, _, match_values) = normalize_pair_for_storage(
            e1_id.clone(), e2_id.clone(), // Pass cloned values
            o_lat1, o_lon1, o_lat2, o_lon2, // Use ordered lat/lon for match_values consistency
            distance_meters
        );

        pairs_to_insert_into_db.push(PairToProcessGeo {
            entity_id_1: e1_id.clone(), // Store ordered IDs
            entity_id_2: e2_id.clone(),
            lat1: o_lat1, // Store original orientation for suggestions if needed, though match_values has ordered
            lon1: o_lon1,
            lat2: o_lat2,
            lon2: o_lon2,
            distance_meters,
            base_confidence: pre_rl_confidence,
            final_confidence,
            match_values,
            features: features_vec,
            sig1: sig1_opt,
            sig2: sig2_opt,
        });
    }

    if pairs_to_insert_into_db.is_empty() {
        return Ok(());
    }

    // Perform DB operations for the collected pairs
    let mut local_batch_errors = 0;
    let mut created_this_batch_count = 0;

    for pair_to_db in pairs_to_insert_into_db {
        let new_entity_group_id_str = Uuid::new_v4().to_string();
        match db::upsert_entity_group(
            pool,
            &new_entity_group_id_str,
            &pair_to_db.entity_id_1,
            &pair_to_db.entity_id_2,
            pair_to_db.final_confidence,
            pair_to_db.base_confidence,
            MatchMethodType::Geospatial,
            pair_to_db.match_values.clone(),
        ).await {
            Ok((group_id, was_newly_inserted)) => {
                if was_newly_inserted {
                    created_this_batch_count +=1;
                    let mut stats_guard = stats_mutex.lock().await;
                    stats_guard.entities_in_new_pairs.insert(pair_to_db.entity_id_1.clone());
                    stats_guard.entities_in_new_pairs.insert(pair_to_db.entity_id_2.clone());
                    stats_guard.confidence_scores.push(pair_to_db.final_confidence);
                    drop(stats_guard);

                    if let (Some(ro_arc), Some(feats)) = (reinforcement_orchestrator_option, &pair_to_db.features) {
                        let version = ro_arc.lock().await.confidence_tuner.version;
                        let feats_json = serde_json::to_value(feats).unwrap_or_default();
                        if let Err(e) = db::insert_match_decision_detail_direct(
                            pool, &group_id, pipeline_run_id, feats_json,
                            MatchMethodType::Geospatial.as_str(), pair_to_db.base_confidence,
                            pair_to_db.final_confidence, Some(version as i32)
                        ).await {
                            warn!("Geo: Decision detail insert failed for group {}: {}. Group created, RL context lost.", group_id, e);
                        }
                    }

                    if pair_to_db.final_confidence < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
                        let sugg = create_suggestion_geo(&group_id, &pair_to_db, pipeline_run_id);
                        if let Err(e) = db::insert_suggestion(pool, &sugg).await {
                             warn!("Geo: Suggestion insert failed for group {}: {}. Group and detail (if any) created.", group_id, e);
                        }
                    }
                }
                // Cache the result (MATCH, as we attempted to upsert)
                if let (Some(s1), Some(s2)) = (pair_to_db.sig1, pair_to_db.sig2) {
                    let _ = store_in_comparison_cache(
                        pool, &pair_to_db.entity_id_1, &pair_to_db.entity_id_2,
                        &s1, &s2, &MatchMethodType::Geospatial, pipeline_run_id,
                        "MATCH", Some(pair_to_db.final_confidence),
                        pair_to_db.features.as_ref().map(|f| serde_json::to_value(f).unwrap_or_default()).as_ref()
                    ).await;
                }
            }
            Err(e) => {
                warn!("Geo: upsert_entity_group failed for pair ({}, {}): {}",
                    pair_to_db.entity_id_1.0, pair_to_db.entity_id_2.0, e);
                local_batch_errors += 1;
            }
        }
    }

    let mut stats_guard = stats_mutex.lock().await;
    stats_guard.new_pairs_created_db += created_this_batch_count;
    stats_guard.task_errors += local_batch_errors; // Add errors from this batch to global
    drop(stats_guard);

    if local_batch_errors > 0 {
        // If there were errors, we might want to signal this up, but the function returns Result<()>
        // The errors are logged, and stats are updated.
        // For now, Ok(()) means the batch function itself completed without panicking.
        // Individual errors within the batch are tracked in stats.
    }

    Ok(())
}


/// Creates a suggestion object for geospatial matches.
fn create_suggestion_geo(
    group_id: &str,
    pair: &PairToProcessGeo, // Use the struct that has all info
    pipeline_run_id: &str,
) -> NewSuggestedAction {
    let priority = if pair.final_confidence < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
        2 // High
    } else {
        1 // Medium
    };
    let details_json = serde_json::json!({
        "method_type": MatchMethodType::Geospatial.as_str(),
        "distance_meters": pair.distance_meters,
        "latitude1": pair.lat1, // Use the lat/lon from PairToProcessGeo
        "longitude1": pair.lon1,
        "latitude2": pair.lat2,
        "longitude2": pair.lon2,
        "entity_group_id": group_id,
        "pre_rl_confidence": pair.base_confidence,
    });
    let reason_message = format!(
        "Pair ({}, {}) Geospatial ({:.2}m) low confidence ({:.4}).",
        pair.entity_id_1.0, pair.entity_id_2.0, pair.distance_meters, pair.final_confidence
    );
    NewSuggestedAction {
        pipeline_run_id: Some(pipeline_run_id.to_string()),
        action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
        entity_id: None, // Not relevant for pair review
        group_id_1: Some(group_id.to_string()),
        group_id_2: None,
        cluster_id: None, // Not relevant at pair creation stage
        triggering_confidence: Some(pair.final_confidence),
        details: Some(details_json),
        reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
        reason_message: Some(reason_message),
        priority,
        status: SuggestionStatus::PendingReview.as_str().to_string(),
        reviewer_id: None,
        reviewed_at: None,
        review_notes: None,
    }
}

async fn fetch_valid_entity_ids(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<HashSet<String>> {
    let rows = conn
        .query("SELECT id FROM public.entity", &[])
        .await
        .context("Geospatial: Failed to query valid entity IDs")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn fetch_candidate_pairs_excluding_existing(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>> {
    // This query fetches pairs of entities within METERS_TO_CHECK distance
    // that DO NOT already exist in entity_group with method_type 'geospatial'.
    // This is crucial to avoid re-processing already matched pairs.
    let geo_candidates_query = "
        WITH EntityLocations AS (
            SELECT e.id AS entity_id, l.geom, l.latitude, l.longitude FROM public.entity e
            JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
            JOIN public.location l ON ef.table_id = l.id
            WHERE l.geom IS NOT NULL AND e.id IS NOT NULL AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL
        ), CandidatePairs AS (
            SELECT el1.entity_id AS entity_id_1_str, el2.entity_id AS entity_id_2_str,
                   el1.latitude AS lat1, el1.longitude AS lon1, el2.latitude AS lat2, el2.longitude AS lon2,
                   ST_Distance(el1.geom, el2.geom) AS distance_meters
            FROM EntityLocations el1 JOIN EntityLocations el2 ON el1.entity_id < el2.entity_id -- Ensures e1 < e2
            AND ST_DWithin(el1.geom, el2.geom, $1) -- $1 is METERS_TO_CHECK
        )
        SELECT cp.entity_id_1_str, cp.entity_id_2_str, cp.lat1, cp.lon1, cp.lat2, cp.lon2, cp.distance_meters
        FROM CandidatePairs cp
        LEFT JOIN public.entity_group eg
        ON cp.entity_id_1_str = eg.entity_id_1 AND cp.entity_id_2_str = eg.entity_id_2 AND eg.method_type = 'geospatial'
        WHERE eg.id IS NULL"; // Only pairs NOT in entity_group for 'geospatial'

    let candidate_rows = conn
        .query(geo_candidates_query, &[&METERS_TO_CHECK])
        .await
        .context("Geospatial: Candidate query failed")?;

    Ok(candidate_rows
        .into_iter()
        .map(|row| {
            (
                EntityId(row.get("entity_id_1_str")),
                EntityId(row.get("entity_id_2_str")),
                row.get("lat1"),
                row.get("lon1"),
                row.get("lat2"),
                row.get("lon2"),
                row.get("distance_meters"),
            )
        })
        .collect())
}

/// Normalizes pair order and creates MatchValues.
/// Ensures entity_id_1 < entity_id_2 for the returned tuple used in MatchValues.
fn normalize_pair_for_storage(
    e1_id: EntityId,
    e2_id: EntityId,
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
    distance_meters: f64,
) -> (EntityId, EntityId, MatchValues) { // Returns ordered EntityIds and MatchValues
    let (ordered_e1_id, ordered_e2_id, mv_lat1, mv_lon1, mv_lat2, mv_lon2) = if e1_id.0 <= e2_id.0 {
        (e1_id.clone(), e2_id.clone(), lat1, lon1, lat2, lon2)
    } else {
        (e2_id.clone(), e1_id.clone(), lat2, lon2, lat1, lon1) // Swap coordinates along with IDs
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
