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
    self, insert_entity_group_tx, insert_match_decision_detail_tx, insert_suggestion_tx, PgPool,
};
use crate::models::{
    ActionType, EntityGroupId, EntityId, GeospatialMatchValue, MatchMethodType, MatchValues,
    NewSuggestedAction, SuggestionStatus,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, GeospatialMatchResult, MatchMethodStats};
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};
use serde_json;

pub const METERS_TO_CHECK: f64 = 100.0;
const BATCH_SIZE: usize = 250;
const MAX_PARALLEL_BATCHES: usize = 4;

/// Data structure to hold all necessary info for creating a group within a batch.
struct PairToProcessGeo {
    entity_id_1: EntityId,
    entity_id_2: EntityId,
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
    distance_meters: f64,
    base_confidence: f64,
    final_confidence: f64,
    match_values: MatchValues,
    features: Option<Vec<f64>>,
    sig1: Option<String>,
    sig2: Option<String>,
}

/// Statistics tracking for geospatial matching.
#[derive(Default)]
struct GeospatialStats {
    new_pairs_created_db: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores: Vec<f64>,
    task_errors: usize,
    pairs_processed_from_db: usize,
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
        "Starting V2 (TX) pairwise geospatial matching (run ID: {}){}",
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
    let candidate_pairs = fetch_candidate_pairs_excluding_existing(&*init_conn).await?;
    drop(init_conn);

    info!(
        "Geospatial: Found {} candidates, {} valid entities.",
        candidate_pairs.len(),
        valid_entities.len()
    );

    let filtered_candidates: Vec<_> = candidate_pairs
        .into_iter()
        .filter(|(e1, e2, ..)| valid_entities.contains(&e1.0) && valid_entities.contains(&e2.0))
        .collect();

    let total_to_process = filtered_candidates.len();
    info!(
        "Geospatial: {} valid pairs remain after filtering.",
        total_to_process
    );

    if filtered_candidates.is_empty() {
        return Ok(AnyMatchResult::Geospatial(GeospatialMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(MatchMethodType::Geospatial),
        }));
    }

    let stats_mutex = Arc::new(Mutex::new(GeospatialStats::default()));
    let processed_pairs_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));
    let total_batches = (total_to_process + BATCH_SIZE - 1) / BATCH_SIZE;

    for chunk in filtered_candidates.chunks(BATCH_SIZE * MAX_PARALLEL_BATCHES) {
        let mut batch_futures = Vec::new();
        for (batch_idx, batch_data) in chunk.chunks(BATCH_SIZE).enumerate() {
            batch_futures.push(tokio::spawn({
                let batch_to_process = batch_data.to_vec();
                let pool_clone = pool.clone();
                let ro_opt_clone = reinforcement_orchestrator_option.clone();
                let run_id_clone = pipeline_run_id.to_string();
                let feat_cache_clone = feature_cache.clone();
                let stats_clone = stats_mutex.clone();
                let processed_clone = processed_pairs_arc.clone();
                let current_batch_num = batch_idx + 1; // Simplified batch num

                async move {
                    process_batch_geo(
                        batch_to_process,
                        &pool_clone,
                        ro_opt_clone, // Pass Option<Arc<...>>
                        &run_id_clone,
                        feat_cache_clone,
                        stats_clone,
                        processed_clone,
                        current_batch_num,
                        total_batches,
                    )
                    .await
                }
            }));
        }
        join_all(batch_futures).await; // Wait for chunk
    }

    let final_stats = stats_mutex.lock().await;
    let avg_confidence = if !final_stats.confidence_scores.is_empty() {
        final_stats.confidence_scores.iter().sum::<f64>() / final_stats.confidence_scores.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Geospatial,
        groups_created: final_stats.new_pairs_created_db,
        entities_matched: final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats.new_pairs_created_db > 0 { 2.0 } else { 0.0 },
    };

    info!(
        "Geospatial matching V2 complete in {:.2?}: {} pairs processed, {} hits, {} new groups ({} errors).",
        start_time.elapsed(),
        final_stats.pairs_processed_from_db,
        final_stats.cache_hits,
        final_stats.new_pairs_created_db,
        final_stats.task_errors
    );
    Ok(AnyMatchResult::Geospatial(GeospatialMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Processes a batch: checks cache, calculates scores, prepares data for DB.
#[allow(clippy::too_many_arguments)]
async fn process_batch_geo(
    batch: Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    stats_mutex: Arc<Mutex<GeospatialStats>>,
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
    let mut pairs_to_db = Vec::new();

    for (e1_orig, e2_orig, lat1, lon1, lat2, lon2, distance_meters) in batch {
        let (e1_id, e2_id, _o_lat1, _o_lon1, _o_lat2, _o_lon2) =
            if e1_orig.0 < e2_orig.0 {
                (e1_orig.clone(), e2_orig.clone(), lat1, lon1, lat2, lon2)
            } else {
                (e2_orig.clone(), e1_orig.clone(), lat2, lon2, lat1, lon1)
            };
        let current_pair = (e1_id.clone(), e2_id.clone());

        {
            let mut stats = stats_mutex.lock().await;
            stats.pairs_processed_from_db += 1;
        }

        {
            let processed = processed_pairs_this_run_arc.lock().await;
            if processed.contains(&current_pair) {
                continue;
            }
        }

        let mut sig1_opt: Option<String> = None;
        let mut sig2_opt: Option<String> = None;

        if let Ok(Some((s1_data, s2_data))) =
            get_current_signatures_for_pair(pool, &e1_id, &e2_id).await
        {
            sig1_opt = Some(s1_data.signature.clone());
            sig2_opt = Some(s2_data.signature.clone());
            if let Ok(Some(_)) = check_comparison_cache(
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
                let mut processed = processed_pairs_this_run_arc.lock().await;
                processed.insert(current_pair);
                continue;
            }
        }

        let pre_rl_confidence = 0.85; // Base confidence
        let mut final_confidence = pre_rl_confidence;
        let mut features_vec: Option<Vec<f64>> = None;

        if let Some(ro_arc) = &reinforcement_orchestrator_option {
            let mut stats = stats_mutex.lock().await;
            match MatchingOrchestrator::extract_pair_context_features(pool, &e1_id, &e2_id).await {
                Ok(features) => {
                    stats.feature_extractions += 1;
                    if !features.is_empty() {
                        features_vec = Some(features.clone());
                        let guard = ro_arc.lock().await;
                        match guard.get_tuned_confidence(
                            &MatchMethodType::Geospatial,
                            pre_rl_confidence,
                            &features,
                        ) {
                            Ok(tuned) => final_confidence = tuned,
                            Err(e) => warn!("RL tuning failed for ({},{}): {}", e1_id.0, e2_id.0, e),
                        }
                    }
                }
                Err(e) => {
                    stats.feature_failures += 1;
                    warn!("Feature extraction failed for ({},{}): {}", e1_id.0, e2_id.0, e);
                }
            }
        }

        let (_, _, match_values) = normalize_pair_for_storage(
            e1_orig, e2_orig, lat1, lon1, lat2, lon2, distance_meters,
        );

        pairs_to_db.push(PairToProcessGeo {
            entity_id_1: e1_id.clone(),
            entity_id_2: e2_id.clone(),
            lat1, lon1, lat2, lon2, // Keep original orientation for suggestions if needed
            distance_meters,
            base_confidence: pre_rl_confidence,
            final_confidence,
            match_values,
            features: features_vec,
            sig1: sig1_opt,
            sig2: sig2_opt,
        });

        {
            let mut processed = processed_pairs_this_run_arc.lock().await;
            processed.insert(current_pair);
        }
    }

    if !pairs_to_db.is_empty() {
        match run_batch_db_transaction_geo(
            pool,
            pairs_to_db,
            pipeline_run_id,
            reinforcement_orchestrator_option,
        )
        .await
        {
            Ok(created_count) => {
                let mut stats = stats_mutex.lock().await;
                stats.new_pairs_created_db += created_count;
                // Note: entities and confidence scores are updated *inside* run_batch_db_transaction_geo
            }
            Err(e) => {
                warn!("Geospatial: Batch DB TX failed: {}", e);
                let mut stats = stats_mutex.lock().await;
                stats.task_errors += 1;
            }
        }
    }

    Ok(())
}

/// Runs a single transaction for a batch, using savepoints.
async fn run_batch_db_transaction_geo(
    pool: &PgPool,
    pairs_to_process: Vec<PairToProcessGeo>,
    pipeline_run_id: &str,
    ro_opt: Option<Arc<Mutex<MatchingOrchestrator>>>,
) -> Result<usize> {
    let mut conn = pool.get().await.context("Geospatial: DB conn for TX")?;
    let mut tx = conn.transaction().await.context("Geospatial: Start TX")?;
    let mut new_groups_count = 0;

    for pair in pairs_to_process {
        let mut sp = tx.savepoint("geo_pair").await?;
        let new_gid_str = Uuid::new_v4().to_string();

        match insert_entity_group_tx(
            &mut sp,
            &new_gid_str,
            &pair.entity_id_1,
            &pair.entity_id_2,
            pair.final_confidence,
            pair.base_confidence,
            MatchMethodType::Geospatial,
            pair.match_values.clone(),
        )
        .await
        {
            Ok((group_id, was_new)) => {
                if was_new {
                    new_groups_count += 1;
                    let mut version = None;
                    if let (Some(ro), Some(feats)) = (&ro_opt, &pair.features) {
                        let guard = ro.lock().await;
                        version = Some(guard.confidence_tuner.version as i32);
                        let feats_json = serde_json::to_value(feats).unwrap_or_default();
                        if let Err(e) = insert_match_decision_detail_tx(
                            &mut sp, &group_id, pipeline_run_id, feats_json,
                            MatchMethodType::Geospatial.as_str(), pair.base_confidence,
                            pair.final_confidence, version,
                        )
                        .await
                        {
                            warn!("Geo: Decision detail insert failed ({}): {}", group_id, e);
                            sp.rollback().await?; continue;
                        }
                    }

                    if pair.final_confidence < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
                        let sugg = create_suggestion_geo(&group_id, &pair, pipeline_run_id);
                        if let Err(e) = insert_suggestion_tx(&mut sp, &sugg).await {
                            warn!("Geo: Suggestion insert failed ({}): {}", group_id, e);
                            sp.rollback().await?; continue;
                        }
                    }
                }

                // Store in cache *after* successful processing attempt
                if let (Some(s1), Some(s2)) = (pair.sig1, pair.sig2) {
                     let _ = store_in_comparison_cache( // Use the main pool, not the transaction
                        pool, &pair.entity_id_1, &pair.entity_id_2, &s1, &s2,
                        &MatchMethodType::Geospatial, pipeline_run_id, "MATCH",
                        Some(pair.final_confidence), pair.features.as_ref().map(|f| serde_json::to_value(f).unwrap_or_default()).as_ref()
                    ).await;
                }

                sp.commit().await?; // Commit successful pair
            }
            Err(e) => {
                warn!(
                    "Geo: Group insert failed ({}, {}): {}",
                    pair.entity_id_1.0, pair.entity_id_2.0, e
                );
                sp.rollback().await?; // Rollback failed pair
            }
        }
    }

    tx.commit().await.context("Geospatial: Commit main TX")?;
    Ok(new_groups_count)
}

/// Creates a suggestion object for geospatial matches.
fn create_suggestion_geo(
    group_id: &str,
    pair: &PairToProcessGeo,
    pipeline_run_id: &str,
) -> NewSuggestedAction {
    let priority = if pair.final_confidence < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };
    let details_json = serde_json::json!({
        "method_type": MatchMethodType::Geospatial.as_str(),
        "distance_meters": pair.distance_meters,
        "latitude1": pair.lat1, "longitude1": pair.lon1,
        "latitude2": pair.lat2, "longitude2": pair.lon2,
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
        entity_id: None,
        group_id_1: Some(group_id.to_string()),
        group_id_2: None, cluster_id: None,
        triggering_confidence: Some(pair.final_confidence),
        details: Some(details_json),
        reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
        reason_message: Some(reason_message),
        priority, status: SuggestionStatus::PendingReview.as_str().to_string(),
        reviewer_id: None, reviewed_at: None, review_notes: None,
    }
}

// --- Keep existing helper functions ---
async fn fetch_valid_entity_ids(conn: &impl tokio_postgres::GenericClient) -> Result<HashSet<String>> {
    let rows = conn.query("SELECT id FROM public.entity", &[]).await.context("Geospatial: Failed to query valid entity IDs")?;
    Ok(rows.into_iter().map(|row| row.get(0)).collect())
}

async fn fetch_candidate_pairs_excluding_existing(conn: &impl tokio_postgres::GenericClient) -> Result<Vec<(EntityId, EntityId, f64, f64, f64, f64, f64)>> {
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
            FROM EntityLocations el1 JOIN EntityLocations el2 ON el1.entity_id < el2.entity_id
            AND ST_DWithin(el1.geom, el2.geom, $1)
        )
        SELECT cp.entity_id_1_str, cp.entity_id_2_str, cp.lat1, cp.lon1, cp.lat2, cp.lon2, cp.distance_meters
        FROM CandidatePairs cp LEFT JOIN public.entity_group eg
        ON cp.entity_id_1_str = eg.entity_id_1 AND cp.entity_id_2_str = eg.entity_id_2 AND eg.method_type = 'geospatial'
        WHERE eg.id IS NULL";
    let candidate_rows = conn.query(geo_candidates_query, &[&METERS_TO_CHECK]).await.context("Geospatial: Candidate query failed")?;
    Ok(candidate_rows.into_iter().map(|row| (
        EntityId(row.get("entity_id_1_str")), EntityId(row.get("entity_id_2_str")),
        row.get("lat1"), row.get("lon1"), row.get("lat2"), row.get("lon2"), row.get("distance_meters"),
    )).collect())
}

fn normalize_pair_for_storage(
    e1_id: EntityId, e2_id: EntityId, lat1: f64, lon1: f64, lat2: f64, lon2: f64, distance_meters: f64,
) -> (EntityId, EntityId, MatchValues) {
    let (ordered_e1_id, ordered_e2_id, mv_lat1, mv_lon1, mv_lat2, mv_lon2) = if e1_id.0 <= e2_id.0 {
        (e1_id.clone(), e2_id.clone(), lat1, lon1, lat2, lon2)
    } else {
        (e2_id.clone(), e1_id.clone(), lat2, lon2, lat1, lon1)
    };
    let match_values = MatchValues::Geospatial(GeospatialMatchValue {
        latitude1: mv_lat1, longitude1: mv_lon1, latitude2: mv_lat2, longitude2: mv_lon2, distance: distance_meters,
    });
    (ordered_e1_id, ordered_e2_id, match_values)
}