// src/service_matching/name.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::{
    self, // Now using db::upsert_service_group and db::insert_service_match_decision_detail_direct
    NewServiceComparisonCacheEntry,
    PgPool,
};
use crate::models::{MatchMethodType, MatchValues, ServiceId, ServiceNameMatchValue};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_feature_extraction;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};

const RL_CONFIDENCE_THRESHOLD: f64 = 0.85;

pub async fn find_matches_in_cluster(
    pool: &PgPool, // Pass the pool directly
    service_orchestrator_option: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String)>,
    cluster_id: String,
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service name matching for cluster_id: {} with {} services (direct DB calls)...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!(
            "No services for name matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceNameSimilarity,
        ));
    }

    let mut groups_created_count = 0;
    let mut total_confidence_sum = 0.0;
    let mut processed_service_ids = HashSet::new();

    let mut service_data_map = HashMap::new();
    let conn_for_sigs = pool
        .get()
        .await
        .context("Failed to get DB conn for pre-fetching signatures for name matching")?;
    for (service_id, _name) in &services_in_cluster {
        processed_service_ids.insert(service_id.0.clone());
        if !service_data_map.contains_key(&service_id.0) {
            match service_feature_extraction::get_service_raw_data_and_signature(
                &conn_for_sigs,
                service_id,
            )
            .await
            {
                Ok(data) => {
                    service_data_map.insert(service_id.0.clone(), data.1);
                }
                Err(e) => {
                    warn!("Failed to get raw_data/signature for service {}: {}. Attempting calculation.", service_id.0, e);
                    match crate::signature::calculate_and_store_service_signature(pool, service_id).await {
                        Ok(data) => {
                            info!("Successfully calculated signature for service {} after initial failure.", service_id.0);
                            service_data_map.insert(service_id.0.clone(), data.1);
                        }
                        Err(calc_err) => {
                            warn!("Failed to calculate signature for service {}: {}. It will be skipped in name comparisons.", service_id.0, calc_err);
                        }
                    }
                }
            }
        }
    }
    drop(conn_for_sigs);

    for i in 0..services_in_cluster.len() {
        for j in (i + 1)..services_in_cluster.len() {
            let (service1_id, original_name1) = &services_in_cluster[i];
            let (service2_id, original_name2) = &services_in_cluster[j];

            if service1_id.0 == service2_id.0 {
                continue;
            }

            let s1_data = service_data_map.get(&service1_id.0);
            let s2_data = service_data_map.get(&service2_id.0);

            if s1_data.is_none() || s2_data.is_none() {
                warn!(
                    "Missing signature data for {} or {} in name matching. Skipping pair.",
                    service1_id.0, service2_id.0
                );
                continue;
            }
            let s1_sig = s1_data.unwrap();
            let s2_sig = s2_data.unwrap();

            let method_type_str = MatchMethodType::ServiceNameSimilarity.as_str();

            if let Some(cached_entry) = db::get_service_comparison_cache_entry(
                pool, // Pass pool
                &service1_id.0,
                &service2_id.0,
                s1_sig,
                s2_sig,
                method_type_str,
            )
            .await
            .context("Service comparison cache check failed for name matching")?
            {
                info!(
                    "Cache HIT for NAME ({}, {}) method {}. Result: {}",
                    service1_id.0, service2_id.0, method_type_str, cached_entry.comparison_result
                );
                continue;
            }
            info!(
                "Cache MISS for NAME ({}, {}) method {}. Proceeding with full comparison.",
                service1_id.0, service2_id.0, method_type_str
            );

            let normalized_name1 = normalize_service_name(original_name1);
            let normalized_name2 = normalize_service_name(original_name2);
            let pre_rl_confidence = strsim::jaro_winkler(&normalized_name1, &normalized_name2);

            // Initial check for very low pre-RL confidence to avoid unnecessary feature extraction
            if pre_rl_confidence < 0.6 { // Threshold for Jaro-Winkler
                let new_cache_entry = NewServiceComparisonCacheEntry {
                    service_id_1: &service1_id.0,
                    service_id_2: &service2_id.0,
                    signature_1: s1_sig,
                    signature_2: s2_sig,
                    method_type: method_type_str,
                    pipeline_run_id: Some(pipeline_run_id),
                    comparison_result: "NO_MATCH",
                    confidence_score: Some(pre_rl_confidence),
                    snapshotted_features: None, // No features extracted for this path
                };
                if let Err(e) =
                    db::insert_service_comparison_cache_entry(pool, new_cache_entry).await
                {
                    warn!("Failed to insert NO_MATCH (low pre-RL) into service comparison cache for NAME ({}, {}): {}", service1_id.0, service2_id.0, e);
                }
                continue; // Skip to next pair
            }


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
                        &MatchMethodType::ServiceNameSimilarity,
                        pre_rl_confidence,
                        &features,
                    )?;
                    (conf, Some(orchestrator.confidence_tuner.version))
                } else {
                    (pre_rl_confidence, None)
                };

            let mut comparison_cache_result_val = "NO_MATCH";

            if tuned_confidence >= RL_CONFIDENCE_THRESHOLD {
                let match_values = ServiceNameMatchValue {
                    original_name1: original_name1.clone(),
                    original_name2: original_name2.clone(),
                    normalized_name1: normalized_name1.clone(),
                    normalized_name2: normalized_name2.clone(),
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
                    MatchMethodType::ServiceNameSimilarity,
                    MatchValues::ServiceName(match_values),
                )
                .await
                {
                    Ok(actual_service_group_id) => {
                        match db::insert_service_match_decision_detail_direct(
                            pool,
                            &actual_service_group_id,
                            pipeline_run_id,
                            serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                            MatchMethodType::ServiceNameSimilarity.as_str(),
                            pre_rl_confidence,
                            tuned_confidence,
                            tuner_version.map(|v| v as i32),
                        )
                        .await {
                            Ok(_) => {
                                debug!(
                                    "Successfully recorded name match and detail for group_id: {}",
                                    actual_service_group_id
                                );
                                groups_created_count += 1;
                                total_confidence_sum += tuned_confidence;
                                comparison_cache_result_val = "MATCH";
                            }
                            Err(detail_err) => {
                                warn!(
                                    "Upserted service_group {} for name match but FAILED to insert decision detail: {:?}. Match recorded, RL context lost.",
                                    actual_service_group_id, detail_err
                                );
                                comparison_cache_result_val = "MATCH_NO_DETAIL";
                            }
                        }
                    }
                    Err(group_err) => {
                        warn!(
                            "FAILED to upsert service_group for name match ({} vs {}): {:?}. No decision detail recorded.",
                            service1_id.0, service2_id.0, group_err
                        );
                        comparison_cache_result_val = "ERROR_IN_PROCESSING";
                    }
                }
                // --- End Refactored DB Insert Logic ---
            }

            if comparison_cache_result_val != "ERROR_IN_PROCESSING" {
                let new_cache_entry = NewServiceComparisonCacheEntry {
                    service_id_1: &service1_id.0,
                    service_id_2: &service2_id.0,
                    signature_1: s1_sig,
                    signature_2: s2_sig,
                    method_type: method_type_str,
                    pipeline_run_id: Some(pipeline_run_id),
                    comparison_result: comparison_cache_result_val,
                    confidence_score: Some(tuned_confidence),
                    snapshotted_features: Some(
                        serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                    ),
                };
                if let Err(e) =
                    db::insert_service_comparison_cache_entry(pool, new_cache_entry).await
                {
                    warn!(
                        "Failed to insert into service comparison cache for NAME ({}, {}): {}",
                        service1_id.0, service2_id.0, e
                    );
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
        "Service name matching for cluster_id: {} complete. Created {} groups with avg_confidence: {:.3}",
        cluster_id, groups_created_count, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceNameSimilarity,
        groups_created: groups_created_count,
        entities_matched: processed_service_ids.len(),
        avg_confidence,
        avg_group_size: if groups_created_count > 0 { 2.0 } else { 0.0 },
    };

    Ok(ServiceMatchResult {
        groups_created: groups_created_count,
        stats: method_stats.clone(),
        method_stats: vec![method_stats],
    })
}

fn normalize_service_name(name: &str) -> String {
    let lowercased = name.to_lowercase();
    let stop_words = ["center", "clinic", "services", "inc", "llc", "co", "ltd", "health", "medical", "department", "agency", "program", "office"];
    let mut normalized = lowercased
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>();

    for sw in stop_words {
        // Using a simple string replacement. For more robust whole-word matching, regex is better.
        // However, for simplicity and to avoid regex dependency if not already present:
        let word_boundary_pattern_start = format!(" {} ", sw); // Matches " word "
        let word_boundary_pattern_prefix = format!("{} ", sw); // Matches "word " at start
        let word_boundary_pattern_suffix = format!(" {}", sw); // Matches " word" at end

        normalized = normalized.replace(&word_boundary_pattern_start, " ");
        if normalized.starts_with(&word_boundary_pattern_prefix) {
            normalized = normalized.replacen(&word_boundary_pattern_prefix, "", 1);
        }
        if normalized.ends_with(&word_boundary_pattern_suffix) {
            // This is a bit tricky with replace. A regex \b{}\b would be cleaner.
            // For now, this might leave a space if the word was at the very end.
            // The final split/join/trim will handle extra spaces.
            normalized = normalized.replace(&word_boundary_pattern_suffix, "");
        }
         // Handle exact match if the normalized string IS the stop word
        if normalized == *sw {
            normalized = String::new();
        }
    }

    normalized
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join(" ")
        .trim()
        .to_string()
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
