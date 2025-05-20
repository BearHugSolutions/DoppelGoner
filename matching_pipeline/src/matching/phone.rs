// src/matching/phone.rs
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config;
use crate::db::PgPool;
use crate::models::{
    ActionType, EntityGroupId, EntityId, MatchMethodType, MatchValues, NewSuggestedAction,
    PhoneMatchValue, SuggestionStatus,
};
use crate::reinforcement::{MatchingOrchestrator, SharedFeatureCache};
use crate::results::{AnyMatchResult, MatchMethodStats, PhoneMatchResult};
use serde_json;

// SQL query for inserting into entity_group
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Mutex<MatchingOrchestrator>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Added feature_cache parameter
) -> Result<AnyMatchResult> {
    info!(
        "Starting pairwise phone matching (run ID: {}){}...",
        pipeline_run_id,
        if reinforcement_orchestrator.is_some() {
            " with ML guidance"
        } else {
            ""
        }
    );
    let start_time = Instant::now();

    let initial_conn = pool
        .get()
        .await
        .context("Phone: Failed to get DB connection for initial queries")?;

    // 1. Fetch existing phone-matched pairs to avoid duplicates
    debug!("Phone: Fetching existing phone-matched pairs...");
    let existing_processed_pairs = fetch_existing_pairs(&*initial_conn).await?;
    info!(
        "Phone: Found {} existing phone-matched pairs.",
        existing_processed_pairs.len()
    );

    // 2. Fetch Phone Data for all entities
    let phone_query = "
        SELECT e.id as entity_id, p.number, p.extension
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id
        JOIN public.phone p ON ef.table_id = p.id AND ef.table_name = 'phone'
        WHERE p.number IS NOT NULL AND p.number != ''
    ";
    debug!("Phone: Executing phone query for all entities...");
    let phone_rows = initial_conn
        .query(phone_query, &[])
        .await
        .context("Phone: Failed to query entities with phone numbers")?;
    info!(
        "Phone: Found {} phone records across all entities.",
        phone_rows.len()
    );

    // Release initial connection 
    drop(initial_conn);

    // 3. Process phone data into a map of normalized phones to entities
    let mut phone_map: HashMap<String, HashMap<EntityId, (String, Option<String>)>> = HashMap::new();
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

    // 4. Stats tracking
    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;

    // 5. Process each normalized phone and form pairs
    for (normalized_shared_phone, current_entity_map) in phone_map {
        if current_entity_map.len() < 2 {
            continue;
        }

        let entities_sharing_phone: Vec<_> = current_entity_map.iter().collect();

        for i in 0..entities_sharing_phone.len() {
            for j in (i + 1)..entities_sharing_phone.len() {
                let (entity_id1_obj, (original_phone1, original_ext1)) = entities_sharing_phone[i];
                let (entity_id2_obj, (original_phone2, original_ext2)) = entities_sharing_phone[j];

                // Ensure consistent ordering
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

                // Skip if already processed
                if existing_processed_pairs.contains(&(e1_id.clone(), e2_id.clone())) {
                    debug!(
                        "Phone: Pair ({}, {}) already processed. Skipping.",
                        e1_id.0, e2_id.0
                    );
                    continue;
                }

                // 6. Calculate confidence scores
                let base_confidence = if e1_orig_ext == e2_orig_ext {
                    0.95
                } else {
                    0.85
                };
                
                let pre_rl_confidence = base_confidence;
                let mut final_confidence_score = base_confidence;
                let mut features_for_snapshot: Option<Vec<f64>> = None;

                // 7. Extract features for RL tuning using cache if available
                if let Some(orch) = reinforcement_orchestrator {
                    // Use the feature cache if available
                    match if let Some(cache) = feature_cache.as_ref() {
                        let orchestrator_arc = Arc::new(orch.clone());
                        let orchestrator_guard = orchestrator_arc.lock().await;
                        orchestrator_guard.get_pair_features(pool, e1_id, e2_id).await
                    } else {
                        // Extract features directly
                        MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
                    } {
                        Ok(features_vec) => {
                            if !features_vec.is_empty() {
                                features_for_snapshot = Some(features_vec.clone());
                                let orchestrator_arc = Arc::new(orch.clone());
                                let orchestrator_guard = orchestrator_arc.lock().await;
                                match orchestrator_guard.get_tuned_confidence(
                                    &MatchMethodType::Phone,
                                    pre_rl_confidence,
                                    &features_vec,
                                ) {
                                    Ok(tuned_score) => {
                                        final_confidence_score = tuned_score;
                                    },
                                    Err(e) => {
                                        warn!("Phone: Failed to get tuned confidence for pair ({}, {}): {}. Using pre-RL score.", 
                                            e1_id.0, e2_id.0, e);
                                    }
                                }
                            } else {
                                warn!("Phone: Empty features vector for pair ({}, {}). Using pre-RL score.", 
                                    e1_id.0, e2_id.0);
                            }
                        },
                        Err(e) => {
                            warn!("Phone: Failed to extract features for pair ({}, {}): {}. Using pre-RL score.", 
                                e1_id.0, e2_id.0, e);
                        }
                    }
                }

                // 9. Create match values
                let match_values = MatchValues::Phone(PhoneMatchValue {
                    original_phone1: e1_orig_phone.clone(),
                    original_phone2: e2_orig_phone.clone(),
                    normalized_shared_phone: normalized_shared_phone.clone(),
                    extension1: e1_orig_ext.clone(),
                    extension2: e2_orig_ext.clone(),
                });

                // 10. Create entity group with single transaction approach
                match create_entity_group(
                    pool,
                    e1_id,
                    e2_id,
                    &match_values,
                    final_confidence_score,
                    pre_rl_confidence,
                    reinforcement_orchestrator,
                    pipeline_run_id,
                    &normalized_shared_phone,
                    features_for_snapshot,
                    feature_cache.clone(), // Pass the feature cache
                ).await {
                    Ok(created) => {
                        if created {
                            info!(
                                "Phone: Created pair ({}, {}) with shared phone '{}', confidence: {:.4}",
                                e1_id.0, e2_id.0, normalized_shared_phone, final_confidence_score
                            );
                            new_pairs_created_count += 1;
                            entities_in_new_pairs.insert(e1_id.clone());
                            entities_in_new_pairs.insert(e2_id.clone());
                            confidence_scores_for_stats.push(final_confidence_score);
                        }
                    },
                    Err(e) => {
                        individual_operation_errors += 1;
                        warn!("Phone: Failed to process pair ({}, {}): {}", e1_id.0, e2_id.0, e);
                    }
                }
            }
        }
    }

    // Report errors if any
    if individual_operation_errors > 0 {
        warn!(
            "Phone: {} errors during individual pair operations.",
            individual_operation_errors
        );
    }

    // Calculate stats
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
        avg_group_size: if new_pairs_created_count > 0 { 2.0 } else { 0.0 },
    };

    let elapsed = start_time.elapsed();
    info!(
        "Pairwise phone matching complete in {:.2?}: created {} new pairs, involving {} unique entities.",
        elapsed,
        method_stats.groups_created,
        method_stats.entities_matched
    );

    Ok(AnyMatchResult::Phone(PhoneMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Create entity group record in a single transaction, along with decision logging and suggestions
async fn create_entity_group(
    pool: &PgPool,
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator: Option<&Mutex<MatchingOrchestrator>>,
    pipeline_run_id: &str,
    normalized_shared_phone: &str,
    features_for_snapshot: Option<Vec<f64>>,
    feature_cache: Option<SharedFeatureCache>, // Added feature_cache parameter
) -> Result<bool> {
    // Get a single connection for all operations
    let mut conn = pool.get().await
        .context("Phone: Failed to get DB connection for entity group creation")?;
    
    // Start a transaction that will include all operations
    let tx = conn.transaction().await
        .context("Phone: Failed to start transaction for entity group creation")?;
    
    // Generate entity group ID
    let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());
    
    // Ensure entity_id_1 < entity_id_2 to satisfy the check_entity_order constraint
    let (ordered_id_1, ordered_id_2);
    let ordered_match_values;
    
    if entity_id_1.0 <= entity_id_2.0 {
        ordered_id_1 = entity_id_1;
        ordered_id_2 = entity_id_2;
        ordered_match_values = match_values.clone();
    } else {
        // Swap IDs for ordering
        ordered_id_1 = entity_id_2;
        ordered_id_2 = entity_id_1;
        
        // Create new match values with swapped fields for PhoneMatchValue
        if let MatchValues::Phone(phone_match) = match_values {
            ordered_match_values = MatchValues::Phone(PhoneMatchValue {
                original_phone1: phone_match.original_phone2.clone(),
                original_phone2: phone_match.original_phone1.clone(),
                normalized_shared_phone: phone_match.normalized_shared_phone.clone(),
                extension1: phone_match.extension2.clone(),
                extension2: phone_match.extension1.clone(),
            });
        } else {
            // This shouldn't happen for phone matching
            ordered_match_values = match_values.clone();
        }
    }
    
    // Serialize match values
    let match_values_json = serde_json::to_value(&ordered_match_values)
        .context("Phone: Failed to serialize match values")?;
    
    // Extract phone specific data
    let (original_phone1, original_phone2, extension1, extension2) = match &ordered_match_values {
        MatchValues::Phone(p) => (
            &p.original_phone1, 
            &p.original_phone2,
            &p.extension1,
            &p.extension2
        ),
        _ => (&String::new(), &String::new(), &None, &None),
    };
    
    // Insert entity group
    let rows_affected = tx.execute(
        INSERT_ENTITY_GROUP_SQL,
        &[
            &new_entity_group_id.0,
            &ordered_id_1.0,
            &ordered_id_2.0,
            &MatchMethodType::Phone.as_str(),
            &match_values_json,
            &final_confidence_score,
            &pre_rl_confidence_score,
        ],
    ).await?;
    
    if rows_affected > 0 {
        // Create suggestion for low confidence matches if needed
        if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
            let priority = if final_confidence_score < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
                2
            } else {
                1
            };
            
            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Phone.as_str(),
                "matched_value": normalized_shared_phone,
                "original_phone1": original_phone1,
                "original_phone2": original_phone2,
                "extension1": extension1,
                "extension2": extension2,
                "entity_group_id": &new_entity_group_id.0,
            });
            
            let reason_message = format!(
                "Pair ({}, {}) matched by Phone with low confidence ({:.4}). Pre-RL: {:.2}.",
                ordered_id_1.0, ordered_id_2.0, final_confidence_score, pre_rl_confidence_score
            );
            
            // Insert suggestion directly in this transaction
            const INSERT_SUGGESTION_SQL: &str = "
                INSERT INTO clustering_metadata.suggested_actions (
                    pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
                    triggering_confidence, details, reason_code, reason_message, priority, status,
                    reviewer_id, reviewed_at, review_notes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                RETURNING id";
                
            if let Err(e) = tx.query_one(
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
                    &Some("LOW_CONFIDENCE_PAIR".to_string()),
                    &Some(reason_message),
                    &(priority as i32),
                    &SuggestionStatus::PendingReview.as_str(),
                    &None::<String>,
                    &None::<NaiveDateTime>,
                    &None::<String>,
                ],
            ).await {
                warn!("Phone: Failed to create suggestion: {}", e);
            }
        }
        
        // Log decision snapshot if orchestrator available
        if let Some(orch) = reinforcement_orchestrator {
            // Use provided features if available, otherwise try to get from cache or extract
            let features_to_log = if let Some(features) = features_for_snapshot {
                Some(features)
            } else if let Some(cache) = feature_cache.as_ref() {
                // Try to get features from cache
                let orchestrator_arc = Arc::new(orch.clone());
                let orchestrator_guard = orchestrator_arc.lock().await;
                match orchestrator_guard.get_pair_features(pool, ordered_id_1, ordered_id_2).await {
                    Ok(f) => Some(f),
                    Err(_) => None,
                }
            } else {
                // Direct extraction as last resort
                match MatchingOrchestrator::extract_pair_context_features(pool, ordered_id_1, ordered_id_2).await {
                    Ok(f) => Some(f),
                    Err(_) => None,
                }
            };
            
            if let Some(features_vec) = features_to_log {
                let orchestrator_arc = Arc::new(orch.clone());
                let orchestrator_guard = orchestrator_arc.lock().await;
                let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;
                
                let snapshot_features_json = serde_json::to_value(&features_vec)
                    .unwrap_or(serde_json::Value::Null);
                    
                // Insert decision directly with SQL to avoid verification
                const INSERT_DECISION_SQL: &str = "
                    INSERT INTO clustering_metadata.match_decision_details (
                        entity_group_id, pipeline_run_id, snapshotted_features,
                        method_type_at_decision, pre_rl_confidence_at_decision,
                        tuned_confidence_at_decision, confidence_tuner_version_at_decision
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id";
                    
                if let Err(e) = tx.query_one(
                    INSERT_DECISION_SQL,
                    &[
                        &new_entity_group_id.0,
                        &pipeline_run_id,
                        &snapshot_features_json,
                        &MatchMethodType::Phone.as_str(),
                        &pre_rl_confidence_score,
                        &final_confidence_score,
                        &(confidence_tuner_ver as i32),
                    ],
                ).await {
                    warn!("Phone: Failed to log decision snapshot: {}", e);
                }
            }
        }
        
        // Commit the transaction with all operations
        tx.commit().await?;
        Ok(true)
    } else {
        // No rows affected, possibly already exists
        tx.commit().await?;
        debug!("Phone: No rows affected when inserting entity group, pair may already exist");
        Ok(false)
    }
}

/// Fetch existing phone-matched pairs
async fn fetch_existing_pairs(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&MatchMethodType::Phone.as_str()])
        .await
        .context("Phone: Failed to query existing pairs")?;

    let mut existing_pairs = HashSet::new();
    for row in rows {
        let id1: String = row.get("entity_id_1");
        let id2: String = row.get("entity_id_2");
        
        // Ensure consistent ordering
        if id1 < id2 {
            existing_pairs.insert((EntityId(id1), EntityId(id2)));
        } else {
            existing_pairs.insert((EntityId(id2), EntityId(id1)));
        }
    }
    
    Ok(existing_pairs)
}

/// Normalize a phone number by:
/// - Removing all non-numeric characters
/// - Basic handling for US country code (stripping leading '1' if 11 digits and length becomes 10)
/// - Returns empty string if normalization results in a number outside typical lengths.
fn normalize_phone(phone: &str) -> String {
    let digits_only: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();

    if digits_only.len() == 11 && digits_only.starts_with('1') {
        // Standard 10-digit US number after stripping country code
        return digits_only[1..].to_string();
    }

    // Basic validation for common phone number lengths (e.g., 7 to 15 digits)
    if digits_only.len() >= 7 && digits_only.len() <= 15 {
        return digits_only;
    }

    // If it doesn't match common patterns or is too short/long after stripping non-digits,
    // consider it invalid for matching purposes.
    debug!(
        "Phone number '{}' normalized to '{}', which is considered invalid for matching.",
        phone, digits_only
    );
    String::new()
}

// Add missing NaiveDateTime for compile
use chrono::NaiveDateTime;