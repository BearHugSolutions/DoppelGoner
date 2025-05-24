// src/matching/phone.rs
use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{self, CRITICALLY_LOW_SUGGESTION_THRESHOLD, MODERATE_LOW_SUGGESTION_THRESHOLD};
use crate::db::{insert_entity_group_tx, insert_match_decision_detail_tx, insert_suggestion_tx, PgPool};
use crate::models::{
    ActionType, EntityGroupId, EntityId, MatchMethodType, MatchValues, NewSuggestedAction,
    PhoneMatchValue, SuggestionStatus,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, MatchMethodStats, PhoneMatchResult};
use serde_json;

// Import pipeline_state_utils
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};

// SQL query for inserting into entity_group
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING RETURNING id"; // Added ON CONFLICT and RETURNING

pub async fn find_matches(
    pool: &PgPool,
    // Changed to Option<Arc<Mutex<MatchingOrchestrator>>> for consistency
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting pairwise phone matching (run ID: {}){} with INCREMENTAL CHECKS...", // Log update
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    let initial_conn = pool
        .get()
        .await
        .context("Phone: Failed to get DB connection for initial queries")?;

    debug!("Phone: Fetching existing phone-matched pairs from entity_group...");
    let existing_entity_groups = fetch_existing_entity_groups(&*initial_conn).await?;
    info!(
        "Phone: Found {} existing phone-matched pairs in entity_group.",
        existing_entity_groups.len()
    );

    let phone_query = "
        SELECT e.id as entity_id, p.number, p.extension
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'phone'
        JOIN public.phone p ON ef.table_id = p.id 
        WHERE p.number IS NOT NULL AND p.number != ''
    "; // Ensured ef.table_name = 'phone'
    debug!("Phone: Executing phone query for all entities...");
    let phone_rows = initial_conn
        .query(phone_query, &[])
        .await
        .context("Phone: Failed to query entities with phone numbers")?;
    info!(
        "Phone: Found {} phone records across all entities.",
        phone_rows.len()
    );
    drop(initial_conn);

    let mut phone_map: HashMap<String, HashMap<EntityId, (String, Option<String>)>> =
        HashMap::new();
    for row in &phone_rows {
        let entity_id = EntityId(row.get("entity_id"));
        let number: String = row.get("number");
        let extension: Option<String> = row
            .try_get("extension")
            .ok()
            .flatten()
            .filter(|s: &String| !s.is_empty());
        let normalized_phone = normalize_phone(&number);

        if !normalized_phone.is_empty() {
            phone_map
                .entry(normalized_phone)
                .or_default()
                .insert(entity_id, (number, extension));
        }
    }
    info!(
        "Phone: Processed {} unique normalized phone numbers.",
        phone_map.len()
    );

    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;
    let mut processed_pairs_this_run: HashSet<(EntityId, EntityId)> = HashSet::new();
    let mut cache_hits_count = 0;

    for (normalized_shared_phone, current_entity_map) in phone_map {
        if current_entity_map.len() < 2 {
            continue;
        }

        let entities_sharing_phone: Vec<_> = current_entity_map.iter().collect();

        for i in 0..entities_sharing_phone.len() {
            for j in (i + 1)..entities_sharing_phone.len() {
                let (entity_id1_obj, (original_phone1, original_ext1)) = entities_sharing_phone[i];
                let (entity_id2_obj, (original_phone2, original_ext2)) = entities_sharing_phone[j];

                let (e1_id, e1_orig_phone, e1_orig_ext, e2_id, e2_orig_phone, e2_orig_ext) =
                    if entity_id1_obj.0 < entity_id2_obj.0 {
                        (
                            entity_id1_obj,
                            original_phone1,
                            original_ext1,
                            entity_id2_obj,
                            original_phone2,
                            original_ext2,
                        )
                    } else {
                        (
                            entity_id2_obj,
                            original_phone2,
                            original_ext2,
                            entity_id1_obj,
                            original_phone1,
                            original_ext1,
                        )
                    };

                let current_pair_ordered = (e1_id.clone(), e2_id.clone());

                if existing_entity_groups.contains(&current_pair_ordered)
                    || processed_pairs_this_run.contains(&current_pair_ordered)
                {
                    debug!("Phone: Pair ({}, {}) already in entity_group or processed this run. Skipping.", e1_id.0, e2_id.0);
                    continue;
                }

                let current_signatures_opt = match get_current_signatures_for_pair(
                    pool, e1_id, e2_id,
                )
                .await
                {
                    Ok(sigs) => sigs,
                    Err(e) => {
                        warn!("Phone: Failed to get signatures for pair ({}, {}): {}. Proceeding without cache.", e1_id.0, e2_id.0, e);
                        None
                    }
                };

                if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                    match check_comparison_cache(
                        pool,
                        e1_id,
                        e2_id,
                        &sig1_data.signature,
                        &sig2_data.signature,
                        &MatchMethodType::Phone,
                    )
                    .await
                    {
                        Ok(Some(cached_eval)) => {
                            cache_hits_count += 1;
                            debug!(
                                "Phone: Cache HIT for pair ({}, {}). Result: {}, Score: {:?}",
                                e1_id.0,
                                e2_id.0,
                                cached_eval.comparison_result,
                                cached_eval.similarity_score
                            );
                            processed_pairs_this_run.insert(current_pair_ordered.clone());
                            continue;
                        }
                        Ok(None) => {
                            debug!("Phone: Cache MISS for pair ({}, {}). Signatures: ({}..., {}...). Proceeding.", e1_id.0, e2_id.0, &sig1_data.signature[..std::cmp::min(8, sig1_data.signature.len())], &sig2_data.signature[..std::cmp::min(8, sig2_data.signature.len())]);
                        }
                        Err(e) => {
                            warn!("Phone: Error checking comparison cache for pair ({}, {}): {}. Proceeding.", e1_id.0, e2_id.0, e);
                        }
                    }
                }

                let base_confidence = if e1_orig_ext == e2_orig_ext {
                    0.95
                } else {
                    0.85
                };
                let pre_rl_confidence = base_confidence;
                let mut final_confidence_score = base_confidence;
                let mut features_for_rl_snapshot: Option<Vec<f64>> = None;
                let mut features_json_for_cache: Option<serde_json::Value> = None;

                if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                    match if let Some(_cache_service) = feature_cache.as_ref() { // Renamed parameter to _cache_service to mark it as intentionally unused for now
                        // Assuming get_pair_features uses the cache_service internally
                        let orchestrator_guard = ro_arc.lock().await;
                        orchestrator_guard.get_pair_features(pool, e1_id, e2_id).await
                    } else {
                        MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
                    } {
                        Ok(features_vec) => {
                            if !features_vec.is_empty() {
                                features_for_rl_snapshot = Some(features_vec.clone());
                                features_json_for_cache = serde_json::to_value(features_vec.clone()).ok();

                                let orchestrator_guard = ro_arc.lock().await;
                                match orchestrator_guard.get_tuned_confidence(
                                    &MatchMethodType::Phone,
                                    pre_rl_confidence,
                                    &features_vec,
                                ) {
                                    Ok(tuned_score) => final_confidence_score = tuned_score,
                                    Err(e) => warn!("Phone: RL tuning failed for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                                }
                            } else {
                                warn!("Phone: Empty features vector for ({}, {}). Using pre-RL score.", e1_id.0, e2_id.0);
                            }
                        }
                        Err(e) => warn!("Phone: Feature extraction failed for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                    }
                }

                // For phone, a "MATCH" is if normalized numbers are the same. Confidence varies.
                let comparison_outcome_for_cache = "MATCH"; // Since they share a normalized_shared_phone

                if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                    if let Err(e) = store_in_comparison_cache(
                        pool,
                        e1_id,
                        e2_id,
                        &sig1_data.signature,
                        &sig2_data.signature,
                        &MatchMethodType::Phone,
                        pipeline_run_id,
                        comparison_outcome_for_cache,
                        Some(final_confidence_score),
                        features_json_for_cache.as_ref(),
                    )
                    .await
                    {
                        warn!(
                            "Phone: Failed to store in comparison_cache for ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        );
                    }
                }
                processed_pairs_this_run.insert(current_pair_ordered.clone());

                // Only create entity_group if confidence is high enough (or whatever criteria for phone)
                // The original code created an entity_group if they shared a normalized phone.
                // We'll keep that, but the confidence might be low.
                // The comparison_outcome_for_cache being "MATCH" means we proceed.

                let match_values_obj = MatchValues::Phone(PhoneMatchValue {
                    original_phone1: e1_orig_phone.clone(),
                    original_phone2: e2_orig_phone.clone(),
                    normalized_shared_phone: normalized_shared_phone.clone(),
                    extension1: e1_orig_ext.clone(),
                    extension2: e2_orig_ext.clone(),
                });

                match create_entity_group_entry( // Renamed to avoid confusion with old create_entity_group
                    pool,
                    e1_id, // Pass ordered IDs
                    e2_id,
                    &match_values_obj, // This will be ordered internally by create_entity_group_entry
                    final_confidence_score,
                    pre_rl_confidence,
                    reinforcement_orchestrator_option.as_ref(), // Pass as Option<&Arc<Mutex<...>>>
                    pipeline_run_id,
                    &normalized_shared_phone, // For suggestion details
                    features_for_rl_snapshot, // Pass Option<Vec<f64>>
                ).await // Removed feature_cache.clone() from here as it's not used
                {
                    Ok(created) => {
                        if created {
                            // Log was here, it's fine.
                            new_pairs_created_count += 1;
                            entities_in_new_pairs.insert(e1_id.clone());
                            entities_in_new_pairs.insert(e2_id.clone());
                            confidence_scores_for_stats.push(final_confidence_score);
                        }
                    }
                    Err(e) => {
                        individual_operation_errors += 1;
                        warn!("Phone: Failed to process pair ({}, {}): {}", e1_id.0, e2_id.0, e);
                    }
                }
            }
        }
    }

    if individual_operation_errors > 0 {
        warn!(
            "Phone: {} errors during individual pair operations.",
            individual_operation_errors
        );
    }
    info!("Phone: Cache hits during this run: {}", cache_hits_count);

    let avg_confidence: f64 = if !confidence_scores_for_stats.is_empty() {
        confidence_scores_for_stats.iter().sum::<f64>() / confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Phone,
        groups_created: new_pairs_created_count,
        entities_matched: entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if new_pairs_created_count > 0 {
            2.0
        } else {
            0.0
        },
    };

    info!(
        "Pairwise phone matching complete in {:.2?}: created {} new pairs, involving {} unique entities. Cache hits: {}.",
        start_time.elapsed(),
        method_stats.groups_created,
        method_stats.entities_matched,
        cache_hits_count
    );

    Ok(AnyMatchResult::Phone(PhoneMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Creates an entity group entry, logs decision, and adds suggestions if needed.
/// Ensures entity_id_1 < entity_id_2 in the database.
#[allow(clippy::too_many_arguments)]
async fn create_entity_group_entry(
    pool: &PgPool,
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    normalized_shared_phone_for_suggestion: &str,
    features_for_rl_snapshot: Option<Vec<f64>>,
) -> Result<bool> {

    let new_entity_group_id_str = Uuid::new_v4().to_string();

    let mut conn = pool.get().await.context("Phone: Failed to get DB connection")?;
    let mut tx = conn.transaction().await.context("Phone: Failed to start transaction")?;

    let (group_id, was_newly_inserted) = match insert_entity_group_tx(
        &mut tx,
        &new_entity_group_id_str,
        entity_id_1,
        entity_id_2,
        final_confidence_score,
        pre_rl_confidence_score,
        MatchMethodType::Phone,
        match_values.clone(),
    ).await {
        Ok(result) => result,
        Err(e) => {
            tx.rollback().await.context("Phone: Failed to rollback after insert_entity_group_tx error")?;
            return Err(e.context("Phone: insert_entity_group_tx failed"));
        }
    };

    if was_newly_inserted {
        let mut version: Option<i32> = None;

        if let (Some(ro_arc), Some(features_vec)) = (reinforcement_orchestrator_option, features_for_rl_snapshot) {
            let orchestrator_guard = ro_arc.lock().await;
            version = Some(orchestrator_guard.confidence_tuner.version as i32);
            let snapshot_features_json = serde_json::to_value(features_vec).unwrap_or(serde_json::Value::Null);

            if let Err(e) = insert_match_decision_detail_tx(
                &mut tx,
                &group_id,
                pipeline_run_id,
                snapshot_features_json,
                MatchMethodType::Phone.as_str(),
                pre_rl_confidence_score,
                final_confidence_score,
                version,
            ).await {
                warn!("Phone: Failed to log decision snapshot for new group {}: {}. Rolling back.", group_id, e);
                tx.rollback().await.context("Phone: Failed to rollback after decision detail error")?;
                return Err(e.context("Phone: insert_match_decision_detail_tx failed"));
            }
        }

        if final_confidence_score < MODERATE_LOW_SUGGESTION_THRESHOLD {
            let priority = if final_confidence_score < CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };
            let (p1, p2, e1, e2) = if let MatchValues::Phone(p) = match_values {
                (p.original_phone1.clone(), p.original_phone2.clone(), p.extension1.clone(), p.extension2.clone())
            } else { ("".into(), "".into(), None, None) };

            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Phone.as_str(),
                "matched_value": normalized_shared_phone_for_suggestion,
                "original_phone1": p1, "original_phone2": p2,
                "extension1": e1, "extension2": e2,
                "entity_group_id": &group_id,
                "pre_rl_confidence": pre_rl_confidence_score,
            });
            let reason_message = format!(
                "Pair ({}, {}) matched by Phone with low tuned confidence ({:.4}). Pre-RL: {:.2}.",
                entity_id_1.0, entity_id_2.0, final_confidence_score, pre_rl_confidence_score
            );

            let suggestion = NewSuggestedAction {
                pipeline_run_id: Some(pipeline_run_id.to_string()),
                action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
                entity_id: None, group_id_1: Some(group_id.clone()), group_id_2: None, cluster_id: None,
                triggering_confidence: Some(final_confidence_score),
                details: Some(details_json), reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                reason_message: Some(reason_message), priority,
                status: SuggestionStatus::PendingReview.as_str().to_string(),
                reviewer_id: None, reviewed_at: None, review_notes: None,
            };

            if let Err(e) = insert_suggestion_tx(&mut tx, &suggestion).await {
                 warn!("Phone: Failed to insert suggestion for new group {}: {}. Rolling back.", group_id, e);
                 tx.rollback().await.context("Phone: Failed to rollback after suggestion error")?;
                 return Err(e.context("Phone: insert_suggestion_tx failed"));
            }
        }

        tx.commit().await.context("Phone: Failed to commit transaction (new group)")?;
        Ok(true)
    } else {
        debug!("Phone: Group for pair ({}, {}) already exists or updated (ID: {}). Skipping.", entity_id_1.0, entity_id_2.0, group_id);
        tx.commit().await.context("Phone: Failed to commit transaction (existing group)")?;
        Ok(false)
    }
}

/// Fetch existing phone-matched pairs from public.entity_group
async fn fetch_existing_entity_groups(
    // Renamed for clarity
    conn: &impl tokio_postgres::GenericClient,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&MatchMethodType::Phone.as_str()])
        .await
        .context("Phone: Failed to query existing entity_groups")?;

    let mut existing_pairs = HashSet::new();
    for row in rows {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        // Order is guaranteed by DB constraint or application logic before this,
        // but ensuring order here for the HashSet is robust.
        if id1_str < id2_str {
            existing_pairs.insert((EntityId(id1_str), EntityId(id2_str)));
        } else {
            existing_pairs.insert((EntityId(id2_str), EntityId(id1_str)));
        }
    }
    Ok(existing_pairs)
}

pub fn normalize_phone(phone: &str) -> String {
    let digits_only: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits_only.len() == 11 && digits_only.starts_with('1') {
        return digits_only[1..].to_string();
    }
    if (7..=15).contains(&digits_only.len()) {
        // More idiomatic range check
        return digits_only;
    }
    debug!(
        "Phone number '{}' normalized to '{}', considered invalid for matching.",
        phone, digits_only
    );
    String::new()
}
