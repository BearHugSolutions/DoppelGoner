// src/matching/address.rs
use anyhow::{Context, Result};
use chrono::Utc; // Required for NaiveDateTime if used in NewSuggestedAction, though not directly in this file's logic for it
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

// Ensure these are correctly defined in your actual config.rs
use crate::config::{CRITICALLY_LOW_SUGGESTION_THRESHOLD, MODERATE_LOW_SUGGESTION_THRESHOLD};
use crate::db::{self, PgPool}; // db for insert_suggestion
use crate::models::{
    ActionType, AddressMatchValue, EntityGroupId, EntityId, MatchMethodType, MatchValues,
    NewSuggestedAction, SuggestionStatus,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AddressMatchResult, AnyMatchResult, MatchMethodStats};
use serde_json;

use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};

const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score,
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING"; // Important for return value logic

// Modified process_pair to return Result<bool> indicating if a new group was created
async fn process_pair(
    pool: &PgPool, // Changed to take pool directly
    e1_id: &EntityId,
    e2_id: &EntityId,
    match_values: MatchValues,
    pre_rl_confidence_score: f64,
    final_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    _feature_cache: Option<SharedFeatureCache>, // Keep if orchestrator might use it internally, else mark unused
) -> Result<bool> {
    // Returns true if a new group was created
    let mut conn = pool
        .get()
        .await
        .context("Address: Failed to get DB connection for process_pair")?;
    let tx = conn
        .transaction()
        .await
        .context("Address: Failed to start transaction for process_pair")?;

    let new_entity_group_id_val = EntityGroupId(Uuid::new_v4().to_string());
    let match_values_json = serde_json::to_value(&match_values)?;

    let rows_affected = tx
        .execute(
            INSERT_ENTITY_GROUP_SQL,
            &[
                &new_entity_group_id_val.0,
                &e1_id.0,
                &e2_id.0,
                &MatchMethodType::Address.as_str(),
                &match_values_json,
                &final_confidence_score,
                &pre_rl_confidence_score,
            ],
        )
        .await
        .context("Address: Failed to execute insert entity group")?;

    if rows_affected > 0 {
        // Only log decision details and suggestions if a new group was actually inserted
        if let (Some(ro_arc), Some(features)) =
            (reinforcement_orchestrator_option, &features_for_snapshot)
        {
            // Using `db::insert_match_decision_detail` which manages its own connection/transaction
            if let Err(e) = db::insert_match_decision_detail(
                pool, // Pass pool here
                &new_entity_group_id_val.0,
                pipeline_run_id,
                serde_json::to_value(features).unwrap_or(serde_json::Value::Null), // Convert features to Value
                MatchMethodType::Address.as_str(),
                pre_rl_confidence_score,
                final_confidence_score,
                ro_arc.lock().await.confidence_tuner.version, // Access tuner version
            )
            .await
            {
                warn!(
                    "Address: Failed to log decision snapshot for new group {}: {}",
                    new_entity_group_id_val.0, e
                );
                // Decide if this error should cause the whole transaction to rollback
            }
        }

        if final_confidence_score < MODERATE_LOW_SUGGESTION_THRESHOLD {
            if let MatchValues::Address(addr_values) = &match_values {
                let details_json = serde_json::json!({
                    "method_type": MatchMethodType::Address.as_str(),
                    "matched_value": &addr_values.normalized_shared_address,
                    "original_address1": &addr_values.original_address1,
                    "original_address2": &addr_values.original_address2,
                    "entity_group_id": &new_entity_group_id_val.0,
                    "pre_rl_confidence": pre_rl_confidence_score,
                });
                let reason_message = format!(
                    "Pair ({}, {}) matched by Address with low tuned confidence ({:.4}). Pre-RL: {:.2}.",
                    e1_id.0, e2_id.0, final_confidence_score, pre_rl_confidence_score
                );
                let suggestion = NewSuggestedAction {
                    pipeline_run_id: Some(pipeline_run_id.to_string()),
                    action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
                    entity_id: None,
                    group_id_1: Some(new_entity_group_id_val.0.clone()),
                    group_id_2: None,
                    cluster_id: None,
                    triggering_confidence: Some(final_confidence_score),
                    details: Some(details_json),
                    reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                    reason_message: Some(reason_message),
                    priority: if final_confidence_score < CRITICALLY_LOW_SUGGESTION_THRESHOLD {
                        2
                    } else {
                        1
                    },
                    status: SuggestionStatus::PendingReview.as_str().to_string(),
                    reviewer_id: None,
                    reviewed_at: None,
                    review_notes: None,
                };
                // Call db::insert_suggestion with the pool, as it manages its own transaction.
                if let Err(e) = db::insert_suggestion(pool, &suggestion).await {
                    // Pass pool here
                    warn!(
                        "Address: Failed to insert suggestion for new group {}: {}",
                        new_entity_group_id_val.0, e
                    );
                    // Decide if this error should cause rollback
                }
            }
        }
        tx.commit()
            .await
            .context("Address: Failed to commit transaction (group created)")?;
        Ok(true)
    } else {
        // No new group created (e.g., due to ON CONFLICT DO NOTHING), commit the (potentially empty) transaction.
        tx.commit()
            .await
            .context("Address: Failed to commit transaction (no new group created)")?;
        Ok(false)
    }
}

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 pairwise address matching (run ID: {}){} with INCREMENTAL CHECKS...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    let mut conn_init = pool
        .get()
        .await
        .context("Address: Failed to get DB connection for initial reads")?;
    let existing_entity_groups: HashSet<(EntityId, EntityId)> =
        fetch_existing_pairs(&*conn_init, MatchMethodType::Address).await?;
    info!(
        "Address: Found {} existing address-matched pairs in entity_group.",
        existing_entity_groups.len()
    );

    let address_query = "
        SELECT e.id AS entity_id, l.id as location_id,
               a.address_1, a.address_2, a.city, a.state_province, a.postal_code, a.country
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        JOIN public.location l ON ef.table_id = l.id
        JOIN public.address a ON a.location_id = l.id
        WHERE a.address_1 IS NOT NULL AND a.address_1 != '' AND a.city IS NOT NULL AND a.city != ''";
    let address_rows = conn_init
        .query(address_query, &[])
        .await
        .context("Address: Failed to query addresses")?;
    info!("Address: Found {} address records.", address_rows.len());

    let mut address_map: HashMap<String, HashMap<EntityId, String>> = HashMap::new();
    for row in &address_rows {
        let entity_id = EntityId(row.get("entity_id"));
        let full_address = format_full_address(row)?;
        let normalized_address_str = normalize_address(&full_address);
        if !normalized_address_str.is_empty() {
            address_map
                .entry(normalized_address_str)
                .or_default()
                .insert(entity_id, full_address);
        }
    }
    info!(
        "Address: Processed {} unique normalized addresses.",
        address_map.len()
    );
    drop(conn_init);

    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;
    let mut processed_pairs_this_run: HashSet<(EntityId, EntityId)> = HashSet::new();
    let mut cache_hits_count = 0;

    for (normalized_shared_address, current_entity_map) in address_map {
        if current_entity_map.len() < 2 {
            continue;
        }
        let entities_sharing_address: Vec<_> = current_entity_map.iter().collect();

        for i in 0..entities_sharing_address.len() {
            for j in (i + 1)..entities_sharing_address.len() {
                let (entity_id1_obj_orig, original_address1) = entities_sharing_address[i];
                let (entity_id2_obj_orig, original_address2) = entities_sharing_address[j];
                let (e1_id, e1_orig_addr, e2_id, e2_orig_addr) =
                    if entity_id1_obj_orig.0 < entity_id2_obj_orig.0 {
                        (
                            entity_id1_obj_orig,
                            original_address1,
                            entity_id2_obj_orig,
                            original_address2,
                        )
                    } else {
                        (
                            entity_id2_obj_orig,
                            original_address2,
                            entity_id1_obj_orig,
                            original_address1,
                        )
                    };
                let current_pair_ordered = (e1_id.clone(), e2_id.clone());

                if existing_entity_groups.contains(&current_pair_ordered)
                    || processed_pairs_this_run.contains(&current_pair_ordered)
                {
                    debug!("Address: Pair ({}, {}) already in entity_group or processed this run. Skipping.", e1_id.0, e2_id.0);
                    continue;
                }

                let current_signatures_opt = match get_current_signatures_for_pair(
                    pool, e1_id, e2_id,
                )
                .await
                {
                    Ok(sigs) => sigs,
                    Err(e) => {
                        warn!("Address: Failed to get signatures for pair ({}, {}): {}. Proceeding without cache.", e1_id.0, e2_id.0, e);
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
                        &MatchMethodType::Address,
                    )
                    .await
                    {
                        Ok(Some(cached_eval)) => {
                            cache_hits_count += 1;
                            debug!(
                                "Address: Cache HIT for pair ({}, {}). Result: {}, Score: {:?}",
                                e1_id.0,
                                e2_id.0,
                                cached_eval.comparison_result,
                                cached_eval.similarity_score
                            );
                            processed_pairs_this_run.insert(current_pair_ordered.clone());
                            continue;
                        }
                        Ok(None) => {
                            debug!(
                                "Address: Cache MISS for pair ({}, {}). Proceeding.",
                                e1_id.0, e2_id.0
                            );
                        }
                        Err(e) => {
                            warn!("Address: Error checking comparison cache for pair ({}, {}): {}. Proceeding.", e1_id.0, e2_id.0, e);
                        }
                    }
                }

                let mut pre_rl_confidence_score = 0.95;
                let unit1 = extract_unit(e1_orig_addr);
                let unit2 = extract_unit(e2_orig_addr);
                if !unit1.is_empty() && !unit2.is_empty() && unit1 != unit2 {
                    pre_rl_confidence_score *= 0.85;
                }

                let mut final_confidence_score = pre_rl_confidence_score;
                let mut features_vec_for_rl: Option<Vec<f64>> = None;
                let mut features_json_for_cache: Option<serde_json::Value> = None;

                if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                    match if let Some(_cache_service) = feature_cache.as_ref() {
                        // Renamed param to _cache_service to mark as unused
                        ro_arc
                            .lock()
                            .await
                            .get_pair_features(pool, e1_id, e2_id)
                            .await
                    } else {
                        MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id)
                            .await
                    } {
                        Ok(features) => {
                            if !features.is_empty() {
                                features_vec_for_rl = Some(features.clone());
                                features_json_for_cache =
                                    serde_json::to_value(features.clone()).ok();
                                match ro_arc.lock().await.get_tuned_confidence(
                                    &MatchMethodType::Address,
                                    pre_rl_confidence_score,
                                    features_vec_for_rl.as_ref().unwrap(),
                                ) {
                                    Ok(tuned_score) => final_confidence_score = tuned_score,
                                    Err(e) => warn!(
                                        "Address: RL tuning failed for ({}, {}): {}.",
                                        e1_id.0, e2_id.0, e
                                    ),
                                }
                            } else {
                                warn!("Address: Extracted features vector is empty for pair ({}, {}).", e1_id.0, e2_id.0);
                            }
                        }
                        Err(e) => warn!(
                            "Address: Feature extraction failed for ({}, {}): {}.",
                            e1_id.0, e2_id.0, e
                        ),
                    }
                }

                let match_values_obj = MatchValues::Address(AddressMatchValue {
                    original_address1: e1_orig_addr.clone(),
                    original_address2: e2_orig_addr.clone(),
                    normalized_shared_address: normalized_shared_address.clone(),
                    pairwise_match_score: Some(pre_rl_confidence_score as f32),
                });

                let mut group_created_successfully = false;
                // Pass pool directly to process_pair
                match process_pair(
                    pool,
                    e1_id,
                    e2_id,
                    match_values_obj, // Pass pool
                    pre_rl_confidence_score,
                    final_confidence_score,
                    features_vec_for_rl,
                    reinforcement_orchestrator_option.as_ref(),
                    pipeline_run_id,
                    feature_cache.clone(),
                )
                .await
                {
                    Ok(created) => {
                        group_created_successfully = created;
                        if created {
                            new_pairs_created_count += 1;
                            entities_in_new_pairs.insert(e1_id.clone());
                            entities_in_new_pairs.insert(e2_id.clone());
                            confidence_scores_for_stats.push(final_confidence_score);
                        }
                    }
                    Err(e) => {
                        individual_operation_errors += 1;
                        warn!(
                            "Address: process_pair failed for pair ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        );
                        // group_created_successfully remains false
                    }
                }

                let comparison_outcome_for_cache = if group_created_successfully {
                    "MATCH"
                } else {
                    "NON_MATCH"
                };
                if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                    if let Err(e) = store_in_comparison_cache(
                        pool,
                        e1_id,
                        e2_id,
                        &sig1_data.signature,
                        &sig2_data.signature,
                        &MatchMethodType::Address,
                        pipeline_run_id,
                        comparison_outcome_for_cache,
                        Some(final_confidence_score),
                        features_json_for_cache.as_ref(),
                    )
                    .await
                    {
                        warn!(
                            "Address: Failed to store in comparison_cache for ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        );
                    }
                }
                processed_pairs_this_run.insert(current_pair_ordered);
            }
        }
    }

    if individual_operation_errors > 0 {
        warn!(
            "Address: {} errors during individual pair operations.",
            individual_operation_errors
        );
    }
    info!("Address: Cache hits during this run: {}", cache_hits_count);
    let avg_confidence = if !confidence_scores_for_stats.is_empty() {
        confidence_scores_for_stats.iter().sum::<f64>() / confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };
    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Address,
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
        "Address matching complete in {:.2?}: {} new pairs, {} unique entities.",
        start_time.elapsed(),
        method_stats.groups_created,
        method_stats.entities_matched
    );
    Ok(AnyMatchResult::Address(AddressMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

pub fn normalize_address(address: &str) -> String {
    let lower = address.to_lowercase();
    let mut normalized = lower
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace() || *c == '#')
        .collect::<String>();
    normalized = normalized
        .replace(" st ", " street ")
        .replace(" str ", " street ")
        .replace(" rd ", " road ")
        .replace(" ave ", " avenue ")
        .replace(" av ", " avenue ")
        .replace(" blvd ", " boulevard ")
        .replace(" blv ", " boulevard ")
        .replace(" dr ", " drive ")
        .replace(" ln ", " lane ")
        .replace(" ct ", " court ")
        .replace(" pl ", " place ")
        .replace(" sq ", " square ")
        .replace(" pkwy ", " parkway ")
        .replace(" cir ", " circle ");
    let patterns_to_remove = [
        "apt ",
        "apartment ",
        "suite ",
        "ste ",
        "unit ",
        "#",
        "bldg ",
        "building ",
        "fl ",
        "floor ",
        "dept ",
        "department ",
        "room ",
        "rm ",
        "po box ",
        "p o box ",
        "p.o. box ",
    ];
    if let Some(idx) = normalized.find('#') {
        if normalized
            .chars()
            .nth(idx + 1)
            .map_or(false, |c| c.is_whitespace())
            && normalized
                .chars()
                .nth(idx + 2)
                .map_or(false, |c| c.is_alphanumeric())
        {
            let (before, after_pattern) = normalized.split_at(idx);
            let mut rest = after_pattern
                .trim_start_matches('#')
                .trim_start()
                .to_string();
            if let Some(space_idx) = rest.find(|c: char| c.is_whitespace() || c == ',') {
                rest = rest[space_idx..].to_string();
            } else {
                rest.clear();
            }
            normalized = format!("{}{}", before.trim_end(), rest.trim_start());
        }
    }
    for pattern_base in patterns_to_remove {
        while let Some(idx) = normalized.find(pattern_base) {
            let (before, after_pattern_full) = normalized.split_at(idx);
            let mut rest_of_string = after_pattern_full
                .trim_start_matches(pattern_base)
                .to_string();
            if let Some(end_of_unit_idx) =
                rest_of_string.find(|c: char| c.is_whitespace() || c == ',')
            {
                rest_of_string = rest_of_string[end_of_unit_idx..].to_string();
            } else {
                rest_of_string.clear();
            }
            normalized = format!("{}{}", before.trim_end(), rest_of_string.trim_start());
            normalized = normalized.trim().to_string();
        }
    }
    normalized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn extract_unit(address: &str) -> String {
    let lower = address.to_lowercase();
    let unit_patterns = [
        "apt",
        "apartment",
        "suite",
        "ste",
        "unit",
        "#",
        "bldg",
        "building",
        "fl",
        "floor",
        "room",
        "rm",
    ];
    for pattern in unit_patterns {
        if let Some(idx) = lower.find(pattern) {
            let after_pattern = &lower[idx..];
            if let Some(end_idx) = after_pattern.find(|c: char| c == ',' || c == ';') {
                return after_pattern[0..end_idx].trim().to_string();
            } else {
                // If no comma/semicolon, take up to the next space or end of string
                let mut end_idx_space = after_pattern.len();
                let mut found_non_space_after_pattern = false;
                // Start search after the pattern itself
                let pattern_len = pattern.len();
                if pattern_len < after_pattern.len() {
                    for (i, c) in after_pattern[pattern_len..].char_indices() {
                        if c.is_whitespace() {
                            if found_non_space_after_pattern {
                                // Found a space after some non-space unit identifier
                                end_idx_space = pattern_len + i;
                                break;
                            }
                        } else {
                            found_non_space_after_pattern = true;
                        }
                    }
                }
                // If pattern is '#', it might be like "# 123" or "#123"
                if pattern == "#" {
                    let mut unit_val = String::new();
                    let mut num_started = false;
                    for c in after_pattern.chars().skip(1) {
                        // Skip '#'
                        if c.is_ascii_digit() {
                            unit_val.push(c);
                            num_started = true;
                        } else if c.is_whitespace() {
                            if num_started {
                                break;
                            } // Number ended
                        } else if num_started {
                            // Non-digit, non-space after number started
                            break;
                        } else {
                            // Non-digit before number started (e.g. "# abc") - not a unit number
                            unit_val.clear();
                            break;
                        }
                    }
                    if !unit_val.is_empty() {
                        return format!("#{}", unit_val);
                    }
                }

                return after_pattern[0..end_idx_space].trim().to_string();
            }
        }
    }
    String::new()
}

pub fn format_full_address(row: &tokio_postgres::Row) -> Result<String> {
    let address_1: String = row.try_get("address_1").context("Missing address_1")?;
    let address_2: Option<String> = row
        .try_get("address_2")
        .ok()
        .flatten()
        .filter(|s: &String| !s.trim().is_empty());
    let city: String = row.try_get("city").context("Missing city")?;
    let state_province: String = row
        .try_get("state_province")
        .context("Missing state_province")?;
    let postal_code: String = row.try_get("postal_code").context("Missing postal_code")?;
    let country: String = row.try_get("country").context("Missing country")?;
    Ok(format!(
        "{}{}, {}, {} {}, {}",
        address_1.trim(),
        address_2.map_or("".to_string(), |a| format!(", {}", a.trim())),
        city.trim(),
        state_province.trim(),
        postal_code.trim(),
        country.trim()
    ))
}

async fn fetch_existing_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&method_type.as_str()])
        .await
        .with_context(|| format!("Failed to query existing {:?}-matched pairs", method_type))?;
    let mut existing_pairs = HashSet::new();
    for row in rows {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        if id1_str < id2_str {
            existing_pairs.insert((EntityId(id1_str), EntityId(id2_str)));
        } else {
            existing_pairs.insert((EntityId(id2_str), EntityId(id1_str)));
        }
    }
    Ok(existing_pairs)
}
