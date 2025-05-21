// src/matching/geospatial.rs - Updated to use feature cache
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
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
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, GeospatialMatchResult, MatchMethodStats};
use serde_json;

const METERS_IN_0_15_MILES: f64 = 241.4016; // 0.15 miles

// Reduced batch size to avoid overwhelming the system
const BATCH_SIZE: usize = 25;

/// Main function to find geospatial-based matches
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Add feature_cache parameter
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

    // Cache existing pairs for faster lookup - IMPROVED: Now using same approach as name.rs
    let existing_processed_pairs_set =
        fetch_existing_pairs(&*init_conn, MatchMethodType::Geospatial).await?;
    info!(
        "Geospatial: Found {} existing geospatial-matched pairs to skip.",
        existing_processed_pairs_set.len()
    );

    // Fetch all candidate pairs in a single query
    let candidate_pairs = fetch_candidate_pairs(&*init_conn).await?;

    // Done with initial connection
    drop(init_conn);

    let total_candidate_count = candidate_pairs.len();
    info!(
        "Geospatial: Found {} potential geospatial pairs from DB query.",
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

    // Track stats
    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;
    let mut pairs_processed_count = 0;
    let mut feature_extraction_count = 0;
    let mut feature_extraction_failures = 0;
    let mut skipped_existing_pairs = 0;

    // Process pairs in batches to avoid memory issues
    for batch in filtered_candidates.chunks(BATCH_SIZE) {
        info!(
            "Geospatial: Processing batch {}/{} ({} pairs)...",
            (pairs_processed_count / BATCH_SIZE) + 1,
            (filtered_candidates.len() + BATCH_SIZE - 1) / BATCH_SIZE,
            batch.len()
        );

        for (e1_id, e2_id, lat1, lon1, lat2, lon2, distance_meters) in batch {
            // Normalize order of entity IDs for consistent lookup
            let (id_to_check_1, id_to_check_2) = if e1_id.0 < e2_id.0 {
                (e1_id.clone(), e2_id.clone())
            } else {
                (e2_id.clone(), e1_id.clone())
            };

            // Skip if already processed (using the normalized pair)
            if existing_processed_pairs_set.contains(&(id_to_check_1, id_to_check_2)) {
                skipped_existing_pairs += 1;
                if skipped_existing_pairs % 100 == 0 {
                    debug!(
                        "Geospatial: Skipped {} existing pairs so far",
                        skipped_existing_pairs
                    );
                }
                continue;
            }

            pairs_processed_count += 1;

            // Calculate pre-RL confidence score
            let pre_rl_confidence_score = 0.85; // Default confidence
            let mut final_confidence_score = pre_rl_confidence_score;
            let mut features_for_snapshot: Option<Vec<f64>> = None;

            // Apply RL tuning if available
            if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                // Use the feature cache if available
                match if let Some(cache) = feature_cache.as_ref() {
                    // Use the cache through the orchestrator
                    let orchestrator_guard = ro_arc.lock().await;
                    orchestrator_guard
                        .get_pair_features(pool, e1_id, e2_id)
                        .await
                } else {
                    // Fall back to direct extraction if no cache
                    MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
                } {
                    Ok(features_vec) => {
                        feature_extraction_count += 1;
                        if !features_vec.is_empty() {
                            features_for_snapshot = Some(features_vec.clone());
                            let orchestrator_guard = ro_arc.lock().await;
                            match orchestrator_guard.get_tuned_confidence(
                                &MatchMethodType::Geospatial,
                                pre_rl_confidence_score,
                                &features_vec,
                            ) {
                                Ok(tuned_score) => final_confidence_score = tuned_score,
                                Err(e) => warn!("Geospatial: Failed to get tuned confidence for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                            }
                        }
                    }
                    Err(e) => {
                        feature_extraction_failures += 1;
                        warn!("Geospatial: Failed to extract features for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e);
                    }
                }
            }

            // Create match values
            let match_values = MatchValues::Geospatial(GeospatialMatchValue {
                latitude1: *lat1,
                longitude1: *lon1,
                latitude2: *lat2,
                longitude2: *lon2,
                distance: *distance_meters,
            });

            // Process entity group creation
            match create_entity_group(
                pool,
                e1_id,
                e2_id,
                &match_values,
                final_confidence_score,
                pre_rl_confidence_score,
                reinforcement_orchestrator_option.as_ref(),
                pipeline_run_id,
                *distance_meters,
                features_for_snapshot.as_ref(),
                feature_cache.clone(), // Pass the feature cache
            )
            .await
            {
                Ok(created) => {
                    if created {
                        new_pairs_created_count += 1;
                        entities_in_new_pairs.insert(e1_id.clone());
                        entities_in_new_pairs.insert(e2_id.clone());
                        confidence_scores_for_stats.push(final_confidence_score);

                        if new_pairs_created_count % 25 == 0 {
                            info!(
                                "Geospatial: Created {} pairs so far (current: {}-{}, distance: {:.2}m)",
                                new_pairs_created_count, e1_id.0, e2_id.0, distance_meters
                            );
                        }
                    }
                }
                Err(e) => {
                    individual_operation_errors += 1;
                    warn!(
                        "Geospatial: Failed to process pair ({}, {}): {}",
                        e1_id.0, e2_id.0, e
                    );
                }
            }
        }
    }

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

/// Create entity group record using a single transaction approach with ID ordering
async fn create_entity_group(
    pool: &PgPool,
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    distance_meters: f64,
    features: Option<&Vec<f64>>,
    feature_cache: Option<SharedFeatureCache>, // Add feature_cache parameter
) -> Result<bool> {
    // Get a single connection for all operations
    let mut conn = pool
        .get()
        .await
        .context("Geospatial: Failed to get DB connection for entity group creation")?;

    // Start a transaction that will include all operations
    let tx = conn
        .transaction()
        .await
        .context("Geospatial: Failed to start transaction for entity group creation")?;

    // Generate entity group ID
    let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());

    // Ensure entity_id_1 < entity_id_2 to satisfy any database ordering constraints
    let (ordered_id_1, ordered_id_2);
    let ordered_match_values;

    if entity_id_1.0 <= entity_id_2.0 {
        ordered_id_1 = entity_id_1;
        ordered_id_2 = entity_id_2;
        ordered_match_values = match_values.clone(); // Clone the original match values
    } else {
        // Need to swap the entity IDs to maintain consistent ordering
        ordered_id_1 = entity_id_2;
        ordered_id_2 = entity_id_1;

        // Create new match values with swapped fields for GeospatialMatchValue
        if let MatchValues::Geospatial(geo_match) = match_values {
            ordered_match_values = MatchValues::Geospatial(GeospatialMatchValue {
                latitude1: geo_match.latitude2,
                longitude1: geo_match.longitude2,
                latitude2: geo_match.latitude1,
                longitude2: geo_match.longitude1,
                distance: geo_match.distance,
            });
        } else {
            // This shouldn't happen for geospatial matching
            ordered_match_values = match_values.clone();
        }
    }

    // Serialize match values
    let match_values_json = serde_json::to_value(&ordered_match_values)
        .context("Geospatial: Failed to serialize match values")?;

    // Extract geospatial data for suggestion if needed
    let (lat1, lon1, lat2, lon2) = match &ordered_match_values {
        MatchValues::Geospatial(g) => (g.latitude1, g.longitude1, g.latitude2, g.longitude2),
        _ => (0.0, 0.0, 0.0, 0.0),
    };

    // Use ON CONFLICT DO NOTHING for idempotent insertion
    const INSERT_ENTITY_GROUP_SQL: &str = "
        INSERT INTO public.entity_group
        (id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
        pre_rl_confidence_score)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING
        RETURNING id";

    // Insert entity group using ordered IDs with safer query that returns a value
    let result = tx
        .query_opt(
            INSERT_ENTITY_GROUP_SQL,
            &[
                &new_entity_group_id.0,
                &ordered_id_1.0, // Using the ordered entity ID
                &ordered_id_2.0, // Using the ordered entity ID
                &MatchMethodType::Geospatial.as_str(),
                &match_values_json,
                &final_confidence_score,
                &pre_rl_confidence_score,
            ],
        )
        .await?;

    if result.is_some() {
        // Row was inserted - proceed with suggestion and logging
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

            // Insert suggestion directly in this transaction
            const INSERT_SUGGESTION_SQL: &str = "
                INSERT INTO clustering_metadata.suggested_actions (
                    pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
                    triggering_confidence, details, reason_code, reason_message, priority, status,
                    reviewer_id, reviewed_at, review_notes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                RETURNING id";

            if let Err(e) = tx
                .query_one(
                    INSERT_SUGGESTION_SQL,
                    &[
                        &Some(pipeline_run_id.to_string()),
                        &ActionType::ReviewEntityInGroup.as_str(),
                        &None::<String>,
                        &Some(new_entity_group_id.0.clone()),
                        &None::<String>,
                        &None::<String>,
                        &Some(final_confidence_score),
                        &Some(details_json),
                        &Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                        &Some(reason_message),
                        &(priority as i32),
                        &SuggestionStatus::PendingReview.as_str(),
                        &None::<String>,
                        &None::<NaiveDateTime>,
                        &None::<String>,
                    ],
                )
                .await
            {
                warn!("Geospatial: Failed to create suggestion: {}", e);
            }
        }

        // Log decision snapshot if features are available
        if let (Some(orch), Some(features_vec)) = (reinforcement_orchestrator, features.as_ref()) {
            let orchestrator_guard = orch.lock().await;
            let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;

            let snapshot_features_json =
                serde_json::to_value(features_vec).unwrap_or(serde_json::Value::Null);

            // Insert decision directly with SQL to avoid verification
            const INSERT_DECISION_SQL: &str = "
                INSERT INTO clustering_metadata.match_decision_details (
                    entity_group_id, pipeline_run_id, snapshotted_features,
                    method_type_at_decision, pre_rl_confidence_at_decision,
                    tuned_confidence_at_decision, confidence_tuner_version_at_decision
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id";

            if let Err(e) = tx
                .query_one(
                    INSERT_DECISION_SQL,
                    &[
                        &new_entity_group_id.0,
                        &pipeline_run_id,
                        &snapshot_features_json,
                        &MatchMethodType::Geospatial.as_str(),
                        &pre_rl_confidence_score,
                        &final_confidence_score,
                        &(confidence_tuner_ver as i32),
                    ],
                )
                .await
            {
                warn!("Geospatial: Failed to log decision snapshot: {}", e);
            }
        }

        // Commit the transaction with all operations
        tx.commit().await?;
        Ok(true)
    } else {
        // No rows affected, the pair already exists (caught by the ON CONFLICT DO NOTHING)
        tx.commit().await?;
        debug!(
            "Geospatial: Pair ({}, {}) already exists, no insertion performed",
            ordered_id_1.0, ordered_id_2.0
        );
        Ok(false)
    }
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
        .query(geo_candidates_query, &[&METERS_IN_0_15_MILES])
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
