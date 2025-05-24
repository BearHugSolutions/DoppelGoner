// src/service_matching/email.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::{
    self, // Now using db::upsert_service_group and db::insert_service_match_decision_detail_direct
    NewServiceComparisonCacheEntry, PgPool,
};
use crate::models::{EmailMatchValue, MatchMethodType, MatchValues, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_feature_extraction;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};

const RL_CONFIDENCE_THRESHOLD: f64 = 0.85; // Standard threshold for email matches

pub async fn find_matches_in_cluster(
    pool: &PgPool, // Pass the pool directly
    service_orchestrator_option: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String)>,
    cluster_id: String,
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service email matching for cluster_id: {} with {} services (using direct DB calls)...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!(
            "No services for email matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceEmailMatch,
        ));
    }

    let mut groups_created_count = 0;
    let mut total_confidence_sum = 0.0;
    let mut processed_service_ids = HashSet::new();

    let mut service_data_map = HashMap::new();
    // Fetch signatures (connection will be acquired and released by the db function if needed)
    // For bulk operations like this, it might be more efficient to get one connection
    // and pass it to a helper, but for now, sticking to the refactor of individual calls.
    let conn_for_sigs = pool
        .get()
        .await
        .context("Failed to get DB conn for pre-fetching signatures for email matching")?;
    for (service_id, _email) in &services_in_cluster {
        processed_service_ids.insert(service_id.0.clone());
        if !service_data_map.contains_key(&service_id.0) {
            match service_feature_extraction::get_service_raw_data_and_signature(
                &conn_for_sigs, // Pass the acquired connection
                service_id,
            )
            .await
            {
                Ok(data) => {
                    service_data_map.insert(service_id.0.clone(), data.1);
                }
                Err(e) => {
                    warn!("Failed to get raw_data/signature for service {}: {}. Attempting calculation.", service_id.0, e);
                    // calculate_and_store_service_signature will manage its own connection
                    match crate::signature::calculate_and_store_service_signature(pool, service_id).await {
                        Ok(data) => {
                            info!("Successfully calculated signature for service {} after initial failure.", service_id.0);
                            service_data_map.insert(service_id.0.clone(), data.1);
                        }
                        Err(calc_err) => {
                            warn!("Failed to calculate signature for service {}: {}. It will be skipped in email comparisons.", service_id.0, calc_err);
                        }
                    }
                }
            }
        }
    }
    drop(conn_for_sigs); // Release the connection used for batch signature fetching

    let mut email_map: HashMap<String, Vec<&(ServiceId, String)>> = HashMap::new();
    for service_tuple in services_in_cluster.iter() {
        let (_service_id, email_addr) = service_tuple;
        let normalized_email = normalize_email(email_addr);
        if !normalized_email.is_empty() {
            email_map
                .entry(normalized_email)
                .or_default()
                .push(service_tuple);
        }
    }
    info!(
        "Built email map with {} unique normalized emails for cluster_id: {}",
        email_map.len(),
        cluster_id
    );

    for (_normalized_shared_email, services_with_same_email) in email_map {
        if services_with_same_email.len() < 2 {
            continue;
        }

        for i in 0..services_with_same_email.len() {
            for j in (i + 1)..services_with_same_email.len() {
                let (service1_id, original_email1) = services_with_same_email[i];
                let (service2_id, original_email2) = services_with_same_email[j];

                if service1_id.0 == service2_id.0 {
                    continue;
                }

                let s1_data = service_data_map.get(&service1_id.0);
                let s2_data = service_data_map.get(&service2_id.0);

                if s1_data.is_none() || s2_data.is_none() {
                    warn!(
                        "Missing signature data for {} or {} in email matching. Skipping pair.",
                        service1_id.0, service2_id.0
                    );
                    continue;
                }
                let s1_sig = s1_data.unwrap();
                let s2_sig = s2_data.unwrap();

                let method_type_str = MatchMethodType::ServiceEmailMatch.as_str();

                if let Some(cached_entry) = db::get_service_comparison_cache_entry(
                    pool, // Pass pool
                    &service1_id.0,
                    &service2_id.0,
                    s1_sig,
                    s2_sig,
                    method_type_str,
                )
                .await
                .context("Service comparison cache check failed for email matching")?
                {
                    info!(
                        "Cache HIT for EMAIL ({}, {}) method {}. Result: {}",
                        service1_id.0,
                        service2_id.0,
                        method_type_str,
                        cached_entry.comparison_result
                    );
                    continue;
                }
                info!(
                    "Cache MISS for EMAIL ({}, {}) method {}. Proceeding with full comparison.",
                    service1_id.0, service2_id.0, method_type_str
                );

                let pre_rl_confidence = 0.95; // Email matches are initially high confidence

                let features = if let Some(ref cache_arc) = feature_cache {
                    let mut cache_guard = cache_arc.lock().await;
                    cache_guard
                        .get_pair_features(pool, service1_id, service2_id)
                        .await?
                } else if let Some(ref orch_arc) = service_orchestrator_option {
                    let orch = orch_arc.lock().await;
                    orch.get_pair_features(pool, service1_id, service2_id)
                        .await?
                } else {
                    ServiceMatchingOrchestrator::extract_pair_context_features(
                        pool,
                        service1_id,
                        service2_id,
                    )
                    .await?
                };

                let (tuned_confidence, tuner_version) =
                    if let Some(ref orch_arc) = service_orchestrator_option {
                        let orchestrator = orch_arc.lock().await;
                        let conf = orchestrator.get_tuned_confidence(
                            &MatchMethodType::ServiceEmailMatch,
                            pre_rl_confidence,
                            &features,
                        )?;
                        (conf, Some(orchestrator.confidence_tuner.version))
                    } else {
                        (pre_rl_confidence, None)
                    };

                let mut comparison_cache_result_val = "NO_MATCH";

                if tuned_confidence >= RL_CONFIDENCE_THRESHOLD {
                    let match_values = EmailMatchValue {
                        original_email1: original_email1.clone(),
                        original_email2: original_email2.clone(),
                        normalized_shared_email: normalize_email(original_email1), // Use one of the original emails for normalization
                    };
                    let proposed_service_group_id_str = Uuid::new_v4().to_string();

                    // --- Refactored DB Insert Logic (No Transaction) ---
                    match db::upsert_service_group(
                        pool,
                        &proposed_service_group_id_str,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceEmailMatch,
                        MatchValues::ServiceEmail(match_values),
                    )
                    .await
                    {
                        Ok(actual_service_group_id) => {
                            // Service group upserted, now insert decision detail
                            match db::insert_service_match_decision_detail_direct(
                                pool,
                                &actual_service_group_id,
                                pipeline_run_id,
                                serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                                MatchMethodType::ServiceEmailMatch.as_str(),
                                pre_rl_confidence,
                                tuned_confidence,
                                tuner_version.map(|v| v as i32),
                            )
                            .await {
                                Ok(_) => {
                                    // Both operations successful
                                    debug!(
                                        "Successfully recorded email match and detail for group_id: {}",
                                        actual_service_group_id
                                    );
                                    groups_created_count += 1; // Consider if upsert should always increment this
                                    total_confidence_sum += tuned_confidence;
                                    comparison_cache_result_val = "MATCH";
                                }
                                Err(detail_err) => {
                                    warn!(
                                        "Upserted service_group {} for email match but FAILED to insert decision detail: {:?}. Match recorded, RL context lost for this run.",
                                        actual_service_group_id, detail_err
                                    );
                                    // Still count as a match for cache, but with a warning.
                                    comparison_cache_result_val = "MATCH_NO_DETAIL";
                                }
                            }
                        }
                        Err(group_err) => {
                            warn!(
                                "FAILED to upsert service_group for email match ({} vs {}): {:?}. No decision detail recorded.",
                                service1_id.0, service2_id.0, group_err
                            );
                            comparison_cache_result_val = "ERROR_IN_PROCESSING";
                        }
                    }
                    // --- End Refactored DB Insert Logic ---
                }

                // Insert into cache regardless of match success/failure (unless error in processing)
                if comparison_cache_result_val != "ERROR_IN_PROCESSING" {
                    let new_cache_entry = NewServiceComparisonCacheEntry {
                        service_id_1: &service1_id.0,
                        service_id_2: &service2_id.0,
                        signature_1: s1_sig,
                        signature_2: s2_sig,
                        method_type: method_type_str,
                        pipeline_run_id: Some(pipeline_run_id),
                        comparison_result: comparison_cache_result_val, // This will be "MATCH", "NO_MATCH", or "MATCH_NO_DETAIL"
                        confidence_score: Some(tuned_confidence),
                        snapshotted_features: Some(
                            serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                        ),
                    };
                    if let Err(e) =
                        db::insert_service_comparison_cache_entry(pool, new_cache_entry).await
                    {
                        warn!(
                            "Failed to insert into service comparison cache for EMAIL ({}, {}): {}",
                            service1_id.0, service2_id.0, e
                        );
                    }
                }
            }
        }
    }

    let avg_confidence = if groups_created_count > 0 {
        total_confidence_sum / groups_created_count as f64
    } else {
        0.0
    };

    info!(
        "Service email matching for cluster_id: {} complete. Created {} groups with avg_confidence: {:.3}",
        cluster_id, groups_created_count, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmailMatch,
        groups_created: groups_created_count,
        entities_matched: processed_service_ids.len(),
        avg_confidence,
        avg_group_size: if groups_created_count > 0 { 2.0 } else { 0.0 }, // Pairwise
    };

    Ok(ServiceMatchResult {
        groups_created: groups_created_count,
        stats: method_stats.clone(),
        method_stats: vec![method_stats],
    })
}

fn normalize_email(email: &str) -> String {
    let email_trimmed = email.trim().to_lowercase();
    if let Some(at_pos) = email_trimmed.find('@') {
        let (local_part, domain_part) = email_trimmed.split_at(at_pos);
        let mut normalized_local = local_part.to_string();
        if let Some(plus_pos) = normalized_local.find('+') {
            normalized_local.truncate(plus_pos);
        }
        let normalized_domain = if domain_part == "@googlemail.com" {
            "@gmail.com"
        } else {
            domain_part
        };

        if normalized_domain.contains("gmail.com") {
            normalized_local = normalized_local.replace('.', "");
        }
        normalized_local + normalized_domain
    } else {
        email_trimmed
    }
}

fn create_empty_result_for_cluster(method_type: MatchMethodType) -> ServiceMatchResult {
    let empty_stats = MatchMethodStats {
        method_type,
        groups_created: 0,
        entities_matched: 0,
        avg_confidence: 0.0,
        avg_group_size: 0.0,
    };
    ServiceMatchResult {
        groups_created: 0,
        stats: empty_stats.clone(),
        method_stats: vec![empty_stats],
    }
}
