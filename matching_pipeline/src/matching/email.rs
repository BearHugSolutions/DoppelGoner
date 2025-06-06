// src/matching/email.rs
use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc}; // NaiveDateTime for NewSuggestedAction
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{CRITICALLY_LOW_SUGGESTION_THRESHOLD, MODERATE_LOW_SUGGESTION_THRESHOLD};
use crate::db::{
    self, PgPool, // Using db::upsert_entity_group, db::insert_match_decision_detail_direct, db::insert_suggestion
};
use crate::models::{
    ActionType, EmailMatchValue, EntityId, MatchMethodType, MatchValues, NewSuggestedAction,
    SuggestionStatus,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, EmailMatchResult, MatchMethodStats};
use serde_json;

use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};

async fn create_entity_group(
    pool: &PgPool, // Pass pool directly
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    normalized_shared_email: &str,
) -> Result<bool> {
    let new_entity_group_id_str = Uuid::new_v4().to_string();

    // Step 1: Upsert Entity Group
    let (group_id, was_newly_inserted) = match db::upsert_entity_group(
        pool,
        &new_entity_group_id_str,
        entity_id_1,
        entity_id_2,
        final_confidence_score,
        pre_rl_confidence_score,
        MatchMethodType::Email,
        match_values.clone(),
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            return Err(e.context("Email: upsert_entity_group failed"));
        }
    };

    if was_newly_inserted {
        info!(
            "Email: Created group {} for pair ({}, {}) with email '{}', conf: {:.4}",
            group_id, entity_id_1.0, entity_id_2.0, normalized_shared_email, final_confidence_score
        );

        if let (Some(orch_arc), Some(features_vec)) =
            (reinforcement_orchestrator, &features_for_snapshot)
        {
            let version = orch_arc.lock().await.confidence_tuner.version;
            let snapshot_features_json =
                serde_json::to_value(features_vec).unwrap_or(serde_json::Value::Null);

            if let Err(e) = db::insert_match_decision_detail_direct(
                pool,
                &group_id,
                pipeline_run_id,
                snapshot_features_json,
                MatchMethodType::Email.as_str(),
                pre_rl_confidence_score,
                final_confidence_score,
                Some(version as i32),
            )
            .await
            {
                warn!(
                    "Email: Failed to log decision snapshot for new group {}: {}. Group created, RL context lost.",
                    group_id, e
                );
            }
        }

        if final_confidence_score < MODERATE_LOW_SUGGESTION_THRESHOLD {
            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Email.as_str(),
                "matched_value": normalized_shared_email,
                "original_email1": match match_values { MatchValues::Email(em_val) => &em_val.original_email1, _ => "" },
                "original_email2": match match_values { MatchValues::Email(em_val) => &em_val.original_email2, _ => "" },
                "entity_group_id": &group_id,
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
                group_id_1: Some(group_id.clone()),
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

            if let Err(e) = db::insert_suggestion(pool, &suggestion).await {
                warn!(
                    "Email: Failed to insert suggestion for new group {}: {}. Group and detail (if any) created.",
                    group_id, e
                );
            }
        }
        Ok(true)
    } else {
        debug!("Email: Group for pair ({}, {}) already exists or updated (ID: {}). Skipping details/suggestions.", entity_id_1.0, entity_id_2.0, group_id);
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
        "Starting V1 pairwise email matching (run ID: {}){} with INCREMENTAL CHECKS (direct DB calls)...",
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
    let existing_entity_groups: HashSet<(EntityId, EntityId)> =
        fetch_existing_pairs(&*initial_read_conn, MatchMethodType::Email).await?;
    info!(
        "Email: Found {} existing email-matched pairs in entity_group.",
        existing_entity_groups.len()
    );

    let email_query = "
        SELECT 'organization' as source, e.id as entity_id, o.email, o.name as entity_name
        FROM entity e JOIN organization o ON e.organization_id = o.id
        WHERE o.email IS NOT NULL AND o.email != ''
        UNION ALL
        SELECT 'service' as source, e.id as entity_id, s.email, s.name as entity_name
        FROM public.entity e JOIN public.entity_feature ef ON e.id = ef.entity_id
        JOIN public.service s ON ef.table_id = s.id AND ef.table_name = 'service'
        WHERE s.email IS NOT NULL AND s.email != ''";
    let email_rows = initial_read_conn
        .query(email_query, &[])
        .await
        .context("Email: Failed to query entities with emails")?;
    info!("Email: Found {} email records.", email_rows.len());
    drop(initial_read_conn);

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
    let email_frequency = calculate_email_frequency(&email_map);

    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;
    let mut processed_pairs_this_run: HashSet<(EntityId, EntityId)> = HashSet::new();
    let mut cache_hits_count = 0;

    for (normalized_shared_email, current_entity_map) in email_map {
        if current_entity_map.len() < 2 {
            continue;
        }
        let entities_sharing_email: Vec<_> = current_entity_map.iter().collect();

        for i in 0..entities_sharing_email.len() {
            for j in (i + 1)..entities_sharing_email.len() {
                let (entity_id1_obj_orig, original_email1) = entities_sharing_email[i];
                let (entity_id2_obj_orig, original_email2) = entities_sharing_email[j];
                let (e1_id, e1_orig_email, e2_id, e2_orig_email) =
                    if entity_id1_obj_orig.0 < entity_id2_obj_orig.0 {
                        (
                            entity_id1_obj_orig,
                            original_email1,
                            entity_id2_obj_orig,
                            original_email2,
                        )
                    } else {
                        (
                            entity_id2_obj_orig,
                            original_email2,
                            entity_id1_obj_orig,
                            original_email1,
                        )
                    };
                let current_pair_ordered = (e1_id.clone(), e2_id.clone());

                if existing_entity_groups.contains(&current_pair_ordered)
                    || processed_pairs_this_run.contains(&current_pair_ordered)
                {
                    debug!("Email: Pair ({}, {}) already in entity_group or processed this run. Skipping.", e1_id.0, e2_id.0);
                    continue;
                }

                let current_signatures_opt = match get_current_signatures_for_pair(
                    pool, e1_id, e2_id,
                )
                .await
                {
                    Ok(sigs) => sigs,
                    Err(e) => {
                        warn!("Email: Failed to get signatures for pair ({}, {}): {}. Proceeding without cache.", e1_id.0, e2_id.0, e);
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
                        &MatchMethodType::Email,
                    )
                    .await
                    {
                        Ok(Some(cached_eval)) => {
                            cache_hits_count += 1;
                            debug!(
                                "Email: Cache HIT for pair ({}, {}). Result: {}, Score: {:?}",
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
                                "Email: Cache MISS for pair ({}, {}). Proceeding.",
                                e1_id.0, e2_id.0
                            );
                        }
                        Err(e) => {
                            warn!("Email: Error checking comparison cache for pair ({}, {}): {}. Proceeding.", e1_id.0, e2_id.0, e);
                        }
                    }
                }

                let mut pre_rl_confidence_score = 1.0; // Default high for email
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

                let mut final_confidence_score = pre_rl_confidence_score;
                let mut features_vec_for_rl: Option<Vec<f64>> = None;
                let mut features_json_for_cache: Option<serde_json::Value> = None;

                if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                    match if let Some(cache_service_arc) = feature_cache.as_ref() {
                        let mut cache_service_guard = cache_service_arc.lock().await;
                        cache_service_guard.get_pair_features(pool, e1_id, e2_id).await
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
                                    &MatchMethodType::Email,
                                    pre_rl_confidence_score,
                                    features_vec_for_rl.as_ref().unwrap(),
                                ) {
                                    Ok(tuned_score) => final_confidence_score = tuned_score,
                                    Err(e) => warn!(
                                        "Email: RL tuning failed for ({}, {}): {}.",
                                        e1_id.0, e2_id.0, e
                                    ),
                                }
                            } else {
                                warn!(
                                    "Email: Extracted features vector is empty for pair ({}, {}).",
                                    e1_id.0, e2_id.0
                                );
                            }
                        }
                        Err(e) => warn!(
                            "Email: Feature extraction failed for ({}, {}): {}.",
                            e1_id.0, e2_id.0, e
                        ),
                    }
                }

                let match_values_obj = MatchValues::Email(EmailMatchValue {
                    original_email1: e1_orig_email.clone(),
                    original_email2: e2_orig_email.clone(),
                    normalized_shared_email: normalized_shared_email.clone(),
                });

                let mut group_created_successfully = false;
                match create_entity_group(
                    pool,
                    e1_id,
                    e2_id,
                    &match_values_obj,
                    final_confidence_score,
                    pre_rl_confidence_score,
                    features_vec_for_rl,
                    reinforcement_orchestrator_option.as_ref(),
                    pipeline_run_id,
                    &normalized_shared_email,
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
                            "Email: create_entity_group failed for pair ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        );
                    }
                }

                let comparison_outcome_for_cache = if group_created_successfully {
                    "MATCH"
                } else {
                    "NON_MATCH_OR_EXISTED"
                };
                if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                    if let Err(e) = store_in_comparison_cache(
                        pool,
                        e1_id,
                        e2_id,
                        &sig1_data.signature,
                        &sig2_data.signature,
                        &MatchMethodType::Email,
                        pipeline_run_id,
                        comparison_outcome_for_cache,
                        Some(final_confidence_score),
                        features_json_for_cache.as_ref(),
                    )
                    .await
                    {
                        warn!(
                            "Email: Failed to store in comparison_cache for ({}, {}): {}",
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
            "Email: {} errors during individual pair operations.",
            individual_operation_errors
        );
    }
    info!("Email: Cache hits during this run: {}", cache_hits_count);
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
        avg_group_size: if new_pairs_created_count > 0 {
            2.0
        } else {
            0.0
        },
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

fn calculate_email_frequency(
    email_map: &HashMap<String, HashMap<EntityId, String>>,
) -> HashMap<String, usize> {
    email_map
        .iter()
        .map(|(k, v)| (k.clone(), v.len()))
        .collect()
}

pub fn normalize_email(email: &str) -> String {
    let email_trimmed = email.trim().to_lowercase();
    if !email_trimmed.contains('@') {
        return email_trimmed; // Or handle as invalid
    }
    let parts: Vec<&str> = email_trimmed.splitn(2, '@').collect();
    if parts.len() != 2 {
        return email_trimmed; // Or handle as invalid
    }
    let (local_part_full, domain_part) = (parts[0], parts[1]);

    // Remove part after '+'
    let local_part_no_plus = local_part_full.split('+').next().unwrap_or("").to_string();

    // Normalize domain (e.g., googlemail.com -> gmail.com)
    let final_domain_part = match domain_part {
        "googlemail.com" => "gmail.com",
        _ => domain_part,
    };

    // Remove dots from local part for Gmail addresses
    let final_local_part = if final_domain_part == "gmail.com" {
        local_part_no_plus.replace('.', "")
    } else {
        local_part_no_plus
    };

    if final_local_part.is_empty() {
        String::new() // Invalid or empty local part after normalization
    } else {
        format!("{}@{}", final_local_part, final_domain_part)
    }
}

fn is_generic_organizational_email(email: &str) -> bool {
    ["info@", "contact@", "office@", "admin@", "support@", "sales@", "hello@", "help@"]
        .iter()
        .any(|prefix| email.starts_with(prefix))
}
