// src/matching/email.rs - Updated for feature caching
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
    ActionType,
    EmailMatchValue,
    EntityGroupId,
    EntityId,
    MatchMethodType,
    MatchValues,
    NewSuggestedAction,
    SuggestionStatus,
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::results::{AnyMatchResult, EmailMatchResult, MatchMethodStats};
use serde_json;

// SQL query for inserting into entity_group
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Add feature_cache parameter
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 pairwise email matching (run ID: {}){}...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    let mut initial_read_conn = pool
        .get()
        .await
        .context("Email: Failed to get DB connection for initial reads")?;

    // 1. Fetch existing email-matched pairs
    let existing_processed_pairs: HashSet<(EntityId, EntityId)> =
        fetch_existing_pairs(&*initial_read_conn, MatchMethodType::Email).await?;
    info!(
        "Email: Found {} existing email-matched pairs.",
        existing_processed_pairs.len()
    );

    // 2. Fetch email data from both organizations and services
    let email_query = "
        SELECT 'organization' as source, e.id as entity_id, o.email, o.name as entity_name
        FROM entity e
        JOIN organization o ON e.organization_id = o.id
        WHERE o.email IS NOT NULL AND o.email != ''
        UNION ALL
        SELECT 'service' as source, e.id as entity_id, s.email, s.name as entity_name
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id
        JOIN public.service s ON ef.table_id = s.id AND ef.table_name = 'service'
        WHERE s.email IS NOT NULL AND s.email != ''
    ";
    let email_rows = initial_read_conn
        .query(email_query, &[])
        .await
        .context("Email: Failed to query entities with emails")?;
    info!(
        "Email: Found {} email records across all entities.",
        email_rows.len()
    );
    
    // Release initial connection before processing
    drop(initial_read_conn);

    // 3. Organize emails by normalized address
    let mut email_map: HashMap<String, HashMap<EntityId, String>> = HashMap::new();
    for row in &email_rows {
        let entity_id = EntityId(row.get("entity_id"));
        let email: String = row.get("email");
        let normalized = normalize_email(&email);
        if !normalized.is_empty() {
            email_map
                .entry(normalized)
                .or_default()
                .insert(entity_id, email);
        }
    }
    
    // Calculate email frequency for confidence scoring
    let email_frequency = calculate_email_frequency(&email_map);

    // 4. Stats tracking
    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;

    // 5. Process potential matches
    for (normalized_shared_email, current_entity_map) in email_map {
        if current_entity_map.len() < 2 {
            continue;
        }
        
        let entities_sharing_email: Vec<_> = current_entity_map.iter().collect();

        for i in 0..entities_sharing_email.len() {
            for j in (i + 1)..entities_sharing_email.len() {
                let (entity_id1_obj, original_email1) = entities_sharing_email[i];
                let (entity_id2_obj, original_email2) = entities_sharing_email[j];

                // Ensure consistent ordering
                let (e1_id, e1_orig_email, e2_id, e2_orig_email) =
                    if entity_id1_obj.0 < entity_id2_obj.0 {
                        (
                            entity_id1_obj,
                            original_email1,
                            entity_id2_obj,
                            original_email2,
                        )
                    } else {
                        (
                            entity_id2_obj,
                            original_email2,
                            entity_id1_obj,
                            original_email1,
                        )
                    };

                // Skip if already processed
                if existing_processed_pairs.contains(&(e1_id.clone(), e2_id.clone())) {
                    continue;
                }

                // 6. Calculate confidence scores
                let mut pre_rl_confidence_score = 1.0; // Base for email match
                
                // Apply adjustments for generic/high-volume emails
                if is_generic_organizational_email(&normalized_shared_email) {
                    pre_rl_confidence_score *= 0.9;
                }
                
                let email_count = email_frequency
                    .get(&normalized_shared_email)
                    .cloned()
                    .unwrap_or(1);
                    
                if email_count > 10 {
                    pre_rl_confidence_score *= 0.85;
                } else if email_count > 5 {
                    pre_rl_confidence_score *= 0.92;
                }

                // Default to pre-RL score
                let mut final_confidence_score = pre_rl_confidence_score;
                let mut features_for_snapshot: Option<Vec<f64>> = None;

                // 7. Apply RL tuning if available
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
                                    &MatchMethodType::Email,
                                    pre_rl_confidence_score,
                                    &features_vec,
                                ) {
                                    Ok(tuned_score) => final_confidence_score = tuned_score,
                                    Err(e) => warn!("Email: Failed to get tuned confidence for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                                }
                            } else {
                                warn!("Email: Extracted features vector is empty for pair ({}, {}). Using pre-RL score.", e1_id.0, e2_id.0);
                            }
                        }
                        Err(e) => warn!("Email: Failed to extract features for ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                    }
                }

                // 8. Create match values
                let match_values = MatchValues::Email(EmailMatchValue {
                    original_email1: e1_orig_email.clone(),
                    original_email2: e2_orig_email.clone(),
                    normalized_shared_email: normalized_shared_email.clone(),
                });

                // 9. Create entity group using the unified approach
                match create_entity_group(
                    pool,
                    e1_id,
                    e2_id,
                    &match_values,
                    final_confidence_score,
                    pre_rl_confidence_score,
                    reinforcement_orchestrator_option.as_ref(),
                    pipeline_run_id,
                    &normalized_shared_email,
                    feature_cache.clone(), // Pass the feature cache
                ).await {
                    Ok(created) => {
                        if created {
                            new_pairs_created_count += 1;
                            entities_in_new_pairs.insert(e1_id.clone());
                            entities_in_new_pairs.insert(e2_id.clone());
                            confidence_scores_for_stats.push(final_confidence_score);
                        }
                    }
                    Err(e) => {
                        individual_operation_errors += 1;
                        warn!("Email: Failed to process pair ({}, {}): {}", e1_id.0, e2_id.0, e);
                    }
                }
            }
        }
    }

    // Report errors if any
    if individual_operation_errors > 0 {
        warn!(
            "Email: {} errors during individual pair operations.",
            individual_operation_errors
        );
    }

    // Calculate stats
    let avg_confidence = if !confidence_scores_for_stats.is_empty() {
        confidence_scores_for_stats.iter().sum::<f64>() / confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Email,
        groups_created: new_pairs_created_count,
        entities_matched: entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if new_pairs_created_count > 0 { 2.0 } else { 0.0 },
    };

    info!(
        "Email matching complete in {:.2?}: {} new pairs, {} unique entities.",
        start_time.elapsed(),
        method_stats.groups_created,
        method_stats.entities_matched
    );
    
    Ok(AnyMatchResult::Email(EmailMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Create entity group record in a single transaction, along with decision logging
async fn create_entity_group(
    pool: &PgPool,
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    normalized_shared_email: &str,
    feature_cache: Option<SharedFeatureCache>, // Add feature_cache parameter
) -> Result<bool> {
    // Get a single connection for all operations
    let mut conn = pool.get().await
        .context("Email: Failed to get DB connection for entity group creation")?;
    
    // Start a transaction that will include both the entity group creation and decision logging
    let tx = conn.transaction().await
        .context("Email: Failed to start transaction for entity group creation")?;
    
    // Generate entity group ID
    let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());
    
    // Serialize match values
    let match_values_json = serde_json::to_value(match_values)
        .context("Email: Failed to serialize match values")?;
    
    // Insert entity group
    match tx.execute(
        INSERT_ENTITY_GROUP_SQL,
        &[
            &new_entity_group_id.0,
            &entity_id_1.0,
            &entity_id_2.0,
            &MatchMethodType::Email.as_str(),
            &match_values_json,
            &final_confidence_score,
            &pre_rl_confidence_score,
        ],
    ).await {
        Ok(rows_affected) => {
            if rows_affected > 0 {
                // Successfully created entity group
                info!(
                    "Email: Created pair ({}, {}) with shared email '{}', confidence: {:.4}",
                    entity_id_1.0, entity_id_2.0, normalized_shared_email, final_confidence_score
                );
                
                // Create suggestion for low confidence if needed
                if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
                    let details_json = serde_json::json!({
                        "method_type": MatchMethodType::Email.as_str(),
                        "matched_value": normalized_shared_email,
                        "original_email1": match match_values {
                            MatchValues::Email(e) => &e.original_email1,
                            _ => "",
                        },
                        "original_email2": match match_values {
                            MatchValues::Email(e) => &e.original_email2,
                            _ => "",
                        },
                        "entity_group_id": &new_entity_group_id.0,
                        "pre_rl_confidence": pre_rl_confidence_score,
                    });
                    
                    let reason_message = format!(
                        "Pair ({}, {}) matched by Email with low tuned confidence ({:.4}). Pre-RL: {:.2}.",
                        entity_id_1.0, entity_id_2.0, final_confidence_score, pre_rl_confidence_score
                    );
                    
                    let suggestion = NewSuggestedAction {
                        pipeline_run_id: Some(pipeline_run_id.to_string()),
                        action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
                        entity_id: None,
                        group_id_1: Some(new_entity_group_id.0.clone()),
                        group_id_2: None,
                        cluster_id: None,
                        triggering_confidence: Some(final_confidence_score),
                        details: Some(details_json),
                        reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                        reason_message: Some(reason_message),
                        priority: if final_confidence_score < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
                            2
                        } else {
                            1
                        },
                        status: SuggestionStatus::PendingReview.as_str().to_string(),
                        reviewer_id: None,
                        reviewed_at: None,
                        review_notes: None,
                    };
                    
                    // Insert suggestion in the same transaction
                    if let Err(e) = tx.query_one(
                        "INSERT INTO clustering_metadata.suggested_actions (
                            pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
                            triggering_confidence, details, reason_code, reason_message, priority, status,
                            reviewer_id, reviewed_at, review_notes
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        RETURNING id",
                        &[
                            &suggestion.pipeline_run_id,
                            &suggestion.action_type,
                            &suggestion.entity_id,
                            &suggestion.group_id_1,
                            &suggestion.group_id_2,
                            &suggestion.cluster_id,
                            &suggestion.triggering_confidence,
                            &suggestion.details,
                            &suggestion.reason_code,
                            &suggestion.reason_message,
                            &(suggestion.priority as i32),
                            &suggestion.status,
                            &suggestion.reviewer_id,
                            &suggestion.reviewed_at,
                            &suggestion.review_notes,
                        ]
                    ).await {
                        warn!("Email: Failed to create suggestion for entity group {}: {}", new_entity_group_id.0, e);
                    }
                }
                
                // Log decision to orchestrator if available
                if let Some(orch_arc) = reinforcement_orchestrator {
                    // Get features from cache if available or extract new ones
                    let features = if let Some(cache) = feature_cache.as_ref() {
                        let orchestrator_guard = orch_arc.lock().await;
                        orchestrator_guard.get_pair_features(pool, entity_id_1, entity_id_2).await
                    } else {
                        MatchingOrchestrator::extract_pair_context_features(pool, entity_id_1, entity_id_2).await
                    };
                    
                    if let Ok(features_vec) = features {
                        let orchestrator_guard = orch_arc.lock().await;
                        let snapshot_features_json = serde_json::to_value(&features_vec)
                            .unwrap_or(serde_json::Value::Null);
                            
                        let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;
                        
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
                                &MatchMethodType::Email.as_str(),
                                &pre_rl_confidence_score,
                                &final_confidence_score,
                                &(confidence_tuner_ver as i32),
                            ]
                        ).await {
                            warn!("Email: Failed to log decision snapshot for entity group {}: {}", new_entity_group_id.0, e);
                        }
                    }
                }
                
                // Commit all operations
                tx.commit().await.context("Email: Failed to commit transaction")?;
                return Ok(true);
            } else {
                // No rows affected, possibly due to constraint violation
                tx.commit().await.context("Email: Failed to commit empty transaction")?;
                return Ok(false);
            }
        },
        Err(e) => {
            let _ = tx.rollback().await;
            return Err(anyhow::anyhow!("Email: Failed to insert entity group: {}", e));
        }
    }
}

/// Helper to fetch existing pairs
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

/// Calculate email frequency for confidence scoring
fn calculate_email_frequency(
    email_map: &HashMap<String, HashMap<EntityId, String>>,
) -> HashMap<String, usize> {
    let mut freq = HashMap::new();
    for (normalized_email, entities) in email_map {
        freq.insert(normalized_email.clone(), entities.len());
    }
    freq
}

/// Normalize an email address for matching
fn normalize_email(email: &str) -> String {
    let email_trimmed = email.trim().to_lowercase();
    if !email_trimmed.contains('@') {
        return email_trimmed;
    }
    let parts: Vec<&str> = email_trimmed.splitn(2, '@').collect();
    if parts.len() != 2 {
        return email_trimmed;
    }
    let (local_part_full, domain_part) = (parts[0], parts[1]);
    let local_part_no_plus = local_part_full.split('+').next().unwrap_or("").to_string();
    let final_local_part = if domain_part == "gmail.com" || domain_part == "googlemail.com" {
        local_part_no_plus.replace('.', "")
    } else {
        local_part_no_plus
    };
    let final_domain_part = match domain_part {
        "googlemail.com" => "gmail.com",
        _ => domain_part,
    };
    if final_local_part.is_empty() {
        String::new()
    } else {
        format!("{}@{}", final_local_part, final_domain_part)
    }
}

/// Check if email is a generic organizational one
fn is_generic_organizational_email(email: &str) -> bool {
    ["info@", "contact@", "office@", "admin@"]
        .iter()
        .any(|prefix| email.starts_with(prefix))
}

// Add missing NaiveDateTime for compile
use chrono::NaiveDateTime;