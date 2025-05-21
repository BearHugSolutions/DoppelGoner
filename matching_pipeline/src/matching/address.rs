// src/matching/address.rs - Updated to use feature cache
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType,
    AddressMatchValue, // Specific MatchValue for Address
    EntityGroupId,
    EntityId,
    MatchMethodType,
    MatchValues, // Enum holding different MatchValue types
    NewSuggestedAction,
    SuggestionStatus,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AddressMatchResult, AnyMatchResult, MatchMethodStats}; // PairMlResult might not be needed if not used
use serde_json;

// SQL query for inserting into entity_group
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

// src/matching/address.rs - Refactored find_matches function

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Add feature_cache parameter
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 pairwise address matching (run ID: {}){}...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    // Get initial connection for data loading
    let mut conn = pool
        .get()
        .await
        .context("Address: Failed to get DB connection for initial reads")?;

    // 1. Fetch existing pairs to avoid duplicates
    let existing_processed_pairs: HashSet<(EntityId, EntityId)> =
        fetch_existing_pairs(&*conn, MatchMethodType::Address).await?;
    info!(
        "Address: Found {} existing address-matched pairs.",
        existing_processed_pairs.len()
    );

    // 2. Fetch address data
    let address_query = "
        SELECT e.id AS entity_id, l.id as location_id,
               a.address_1, a.address_2, a.city, a.state_province, a.postal_code, a.country
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        JOIN public.location l ON ef.table_id = l.id
        JOIN public.address a ON a.location_id = l.id
        WHERE a.address_1 IS NOT NULL AND a.address_1 != '' AND a.city IS NOT NULL AND a.city != ''";

    let address_rows = conn
        .query(address_query, &[])
        .await
        .context("Address: Failed to query addresses")?;
    info!("Address: Found {} address records.", address_rows.len());

    // 3. Process and normalize addresses
    let mut address_map: HashMap<String, HashMap<EntityId, String>> = HashMap::new();
    for row in &address_rows {
        let entity_id = EntityId(row.get("entity_id"));
        let full_address = format_full_address(row)?;
        let normalized_address = normalize_address(&full_address);
        if !normalized_address.is_empty() {
            address_map
                .entry(normalized_address)
                .or_default()
                .insert(entity_id, full_address);
        }
    }
    info!(
        "Address: Processed {} unique normalized addresses.",
        address_map.len()
    );

    // Release initial connection before loop
    drop(conn);

    // 4. Stats tracking
    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;

    // Add this at the beginning of the find_matches function, near other declarations:
    let mut processed_in_this_run: HashSet<(EntityId, EntityId)> = HashSet::new();

    // 5. Process potential matches
    for (normalized_shared_address, current_entity_map) in address_map {
        if current_entity_map.len() < 2 {
            continue;
        }

        let entities_sharing_address: Vec<_> = current_entity_map.iter().collect();

        for i in 0..entities_sharing_address.len() {
            for j in (i + 1)..entities_sharing_address.len() {
                let (entity_id1_obj, original_address1) = entities_sharing_address[i];
                let (entity_id2_obj, original_address2) = entities_sharing_address[j];

                // Ensure consistent ordering of entity IDs
                let (e1_id, e1_orig_addr, e2_id, e2_orig_addr) =
                    if entity_id1_obj.0 < entity_id2_obj.0 {
                        (
                            entity_id1_obj,
                            original_address1,
                            entity_id2_obj,
                            original_address2,
                        )
                    } else {
                        (
                            entity_id2_obj,
                            original_address2,
                            entity_id1_obj,
                            original_address1,
                        )
                    };

                // Create a tuple for the pair to use in our checks
                let entity_pair = (e1_id.clone(), e2_id.clone());

                // Skip if pair already exists in DB OR has been processed in this run
                if existing_processed_pairs.contains(&entity_pair)
                    || processed_in_this_run.contains(&entity_pair)
                {
                    debug!(
                        "Address: Skipping already processed pair ({}, {})",
                        e1_id.0, e2_id.0
                    );
                    continue;
                }

                // Mark this pair as processed to avoid duplicate attempts in this run
                processed_in_this_run.insert(entity_pair);

                // 6. Calculate pre-RL confidence score
                let mut pre_rl_confidence_score = 0.95; // Base for address match
                let unit1 = extract_unit(e1_orig_addr);
                let unit2 = extract_unit(e2_orig_addr);
                if !unit1.is_empty() && !unit2.is_empty() && unit1 != unit2 {
                    pre_rl_confidence_score *= 0.85; // Penalty for different units
                }

                // 7. Extract features and get tuned confidence if applicable
                let mut final_confidence_score = pre_rl_confidence_score;
                let mut features_for_snapshot: Option<Vec<f64>> = None;

                if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                    // Use the feature cache if available
                    match if let Some(cache) = feature_cache.as_ref() {
                    // Use the cache through the orchestrator
                    let orchestrator_guard = ro_arc.lock().await;
                    orchestrator_guard.get_pair_features(pool, e1_id, e2_id).await
                } else {
                    // Fall back to direct extraction if no cache
                    MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
                } {
                    Ok(features_vec) => {
                        if !features_vec.is_empty() {
                            features_for_snapshot = Some(features_vec.clone());
                            let orchestrator_guard = ro_arc.lock().await;
                            match orchestrator_guard.get_tuned_confidence(
                                &MatchMethodType::Address,
                                pre_rl_confidence_score,
                                &features_vec,
                            ) {
                                Ok(tuned_score) => final_confidence_score = tuned_score,
                                Err(e) => warn!("Address: Failed to get tuned confidence for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                            }
                        } else {
                            warn!("Address: Extracted features vector is empty for pair ({}, {}). Using pre-RL score.", e1_id.0, e2_id.0);
                        }
                    }
                    Err(e) => warn!("Address: Failed to extract features for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                }
                }

                // Remaining code stays the same...
                // 8. Create the match values
                let match_values = MatchValues::Address(AddressMatchValue {
                    original_address1: e1_orig_addr.clone(),
                    original_address2: e2_orig_addr.clone(),
                    normalized_shared_address: normalized_shared_address.clone(),
                    pairwise_match_score: Some(pre_rl_confidence_score as f32),
                });

                // 9. Get a connection for the combined operations
                let conn_for_operations = match pool.get().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        individual_operation_errors += 1;
                        warn!(
                            "Address: Failed to get DB connection for pair ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        );
                        continue;
                    }
                };

                // 10. Process this pair within a single transaction
                let process_result = process_pair(
                    conn_for_operations,
                    e1_id,
                    e2_id,
                    match_values,
                    pre_rl_confidence_score,
                    final_confidence_score,
                    features_for_snapshot,
                    reinforcement_orchestrator_option.as_ref(),
                    pipeline_run_id,
                    feature_cache.clone(), // Pass the feature cache
                )
                .await;

                match process_result {
                    Ok(()) => {
                        new_pairs_created_count += 1;
                        entities_in_new_pairs.insert(e1_id.clone());
                        entities_in_new_pairs.insert(e2_id.clone());
                        confidence_scores_for_stats.push(final_confidence_score);
                    }
                    Err(e) => {
                        individual_operation_errors += 1;
                        warn!(
                            "Address: Failed to process pair ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        );
                    }
                }
            }
        }
    }

    // 11. Report errors if any
    if individual_operation_errors > 0 {
        warn!(
            "Address: {} errors during individual pair operations.",
            individual_operation_errors
        );
    }

    // 12. Calculate stats
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

// Helper function to process a single entity pair within a transaction
async fn process_pair(
    mut conn: bb8::PooledConnection<
        '_,
        bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>,
    >,
    e1_id: &EntityId,
    e2_id: &EntityId,
    match_values: MatchValues,
    pre_rl_confidence_score: f64,
    final_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Add feature_cache parameter
) -> Result<()> {
    // Start a transaction for all operations on this pair
    let tx = conn
        .transaction()
        .await
        .context("Address: Failed to start transaction")?;

    // Generate entity group ID and prepare values
    let new_entity_group_id_val = EntityGroupId(Uuid::new_v4().to_string());
    let match_values_json = serde_json::to_value(&match_values)?;

    // 1. Insert the entity group
    tx.execute(
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
    .context("Address: Failed to insert entity group")?;

    // 2. If we have features and an orchestrator, log the decision in the same transaction
    if let (Some(ro_arc), Some(features)) =
        (reinforcement_orchestrator_option, features_for_snapshot)
    {
        let orchestrator_guard = ro_arc.lock().await;

        // Use direct SQL to log the decision (no need to verify the entity group exists)
        const INSERT_DECISION_SQL: &str = "
            INSERT INTO clustering_metadata.match_decision_details (
                entity_group_id, pipeline_run_id, snapshotted_features,
                method_type_at_decision, pre_rl_confidence_at_decision,
                tuned_confidence_at_decision, confidence_tuner_version_at_decision
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id";

        let snapshot_features_json =
            serde_json::to_value(&features).unwrap_or(serde_json::Value::Null);

        let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;

        tx.query_one(
            INSERT_DECISION_SQL,
            &[
                &new_entity_group_id_val.0,
                &pipeline_run_id,
                &snapshot_features_json,
                &MatchMethodType::Address.as_str(),
                &pre_rl_confidence_score,
                &final_confidence_score,
                &(confidence_tuner_ver as i32),
            ],
        )
        .await
        .context("Address: Failed to log decision snapshot")?;
    }

    // 3. If confidence is low, create a suggestion for review
    if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
        match &match_values {
            MatchValues::Address(addr_values) => {
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
                    priority: if final_confidence_score
                        < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD
                    {
                        2
                    } else {
                        1
                    },
                    status: SuggestionStatus::PendingReview.as_str().to_string(),
                    reviewer_id: None,
                    reviewed_at: None,
                    review_notes: None,
                };

                db::insert_suggestion(&tx, &suggestion)
                    .await
                    .context("Address: Failed to insert suggestion")?;
            }
            _ => {
                // This should never happen as we're using MatchValues::Address
                warn!("Address: Unexpected match values type when creating suggestion");
            }
        }
    }

    // 4. Commit the transaction
    tx.commit()
        .await
        .context("Address: Failed to commit transaction")?;

    Ok(())
}

/// Normalize an address by:
/// - Converting to lowercase
/// - Removing most punctuation (keeps alphanumeric and whitespace)
/// - Standardizing common abbreviations (street, road, etc.)
/// - Removing common unit designators (apt, suite, unit)
/// - Trimming extra whitespace
fn normalize_address(address: &str) -> String {
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

/// Extract unit or suite information from an address
/// Returns the extracted unit/suite or an empty string if none is found
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
                let mut found_non_space = false;
                let mut end_idx = 0;
                for (i, c) in after_pattern.char_indices() {
                    if !c.is_whitespace() {
                        found_non_space = true;
                    } else if found_non_space {
                        end_idx = i;
                        break;
                    }
                }
                if end_idx > 0 {
                    return after_pattern[0..end_idx].trim().to_string();
                } else {
                    return after_pattern.trim().to_string();
                }
            }
        }
    }
    String::new()
}
fn format_full_address(row: &tokio_postgres::Row) -> Result<String> {
    let address_1: String = row.try_get("address_1").context("Missing address_1")?;
    let address_2: Option<String> = row.try_get("address_2").ok().flatten();
    let city: String = row.try_get("city").context("Missing city")?;
    let state_province: String = row
        .try_get("state_province")
        .context("Missing state_province")?;
    let postal_code: String = row.try_get("postal_code").context("Missing postal_code")?;
    let country: String = row.try_get("country").context("Missing country")?;
    Ok(format!(
        "{}{}, {}, {} {}, {}",
        address_1.trim(),
        address_2
            .as_deref()
            .map_or("".to_string(), |a| format!(", {}", a.trim())),
        city.trim(),
        state_province.trim(),
        postal_code.trim(),
        country.trim()
    ))
}

// Helper to fetch existing pairs (can be moved to a shared db utility if not already present in email.rs)
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
