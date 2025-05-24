// src/service_matching/name.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::{
    self, // For db::get_service_comparison_cache_entry and db::insert_service_comparison_cache_entry
    insert_service_group_tx,
    insert_service_match_decision_detail,
    PgPool,
    NewServiceComparisonCacheEntry,
};
use crate::models::{MatchMethodType, MatchValues, ServiceId, ServiceNameMatchValue};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::reinforcement::service::service_feature_extraction; // For get_service_raw_data_and_signature
use crate::results::{MatchMethodStats, ServiceMatchResult};

const RL_CONFIDENCE_THRESHOLD: f64 = 0.85; // Standard threshold for making a match after RL

pub async fn find_matches_in_cluster(
    pool: &PgPool,
    service_orchestrator_option: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String)>, // Input: Vec of (ServiceId, name)
    cluster_id: String,
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service name matching for cluster_id: {} with {} services (with cache integration)...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!("No services for name matching in cluster_id: {}.", cluster_id);
        return Ok(create_empty_result_for_cluster(MatchMethodType::ServiceNameSimilarity));
    }

    let mut groups_created_count = 0;
    let mut total_confidence_sum = 0.0;
    let mut processed_service_ids = HashSet::new();

    // Pre-fetch all raw data and signatures for services in the cluster to minimize DB calls in the loop
    let mut service_data_map = HashMap::new();
    let conn_for_sigs = pool.get().await.context("Failed to get DB conn for pre-fetching signatures")?;
    for (service_id, _name) in &services_in_cluster {
        processed_service_ids.insert(service_id.0.clone());
        if !service_data_map.contains_key(&service_id.0) {
            match service_feature_extraction::get_service_raw_data_and_signature(&conn_for_sigs, service_id).await {
                Ok(data) => {
                    service_data_map.insert(service_id.0.clone(), data);
                }
                Err(e) => {
                    warn!("Failed to get raw_data/signature for service {}: {}. It will be skipped in comparisons requiring its signature.", service_id.0, e);
                }
            }
        }
    }
    // Release the connection early if it's from a limited pool
    drop(conn_for_sigs);


    for i in 0..services_in_cluster.len() {
        for j in (i + 1)..services_in_cluster.len() {
            let (service1_id, original_name1) = &services_in_cluster[i];
            let (service2_id, original_name2) = &services_in_cluster[j];

            if service1_id.0 == service2_id.0 {
                continue;
            }

            // Get pre-fetched signatures
            let s1_data = service_data_map.get(&service1_id.0);
            let s2_data = service_data_map.get(&service2_id.0);

            if s1_data.is_none() || s2_data.is_none() {
                warn!("Missing pre-fetched signature data for pair ({}, {}). Skipping cache check and direct comparison.", service1_id.0, service2_id.0);
                continue;
            }
            let (_s1_raw_data, s1_sig) = s1_data.unwrap();
            let (_s2_raw_data, s2_sig) = s2_data.unwrap();

            let method_type_str = MatchMethodType::ServiceNameSimilarity.as_str();

            // --- Service Comparison Cache Check ---
            if let Some(cached_entry) = db::get_service_comparison_cache_entry(
                pool,
                &service1_id.0,
                &service2_id.0,
                s1_sig,
                s2_sig,
                method_type_str,
            ).await.context("Service comparison cache check failed for name matching")? {
                info!("Cache HIT for NAME ({}, {}) method {}. Result: {}", service1_id.0, service2_id.0, method_type_str, cached_entry.comparison_result);
                if cached_entry.comparison_result == "MATCH" && cached_entry.confidence_score.is_some() {
                    // Potentially re-evaluate if group needs to be formed or updated based on cached confidence
                    // This part depends on how strictly "cached implies done" is interpreted.
                    // For now, if it's a MATCH, we assume it's handled or will be by insert_service_group_tx logic.
                }
                continue; // Skip to next pair if valid cache entry found
            }
            info!("Cache MISS for NAME ({}, {}) method {}. Proceeding with full comparison.", service1_id.0, service2_id.0, method_type_str);


            // --- Full Comparison Logic (Cache Miss) ---
            let normalized_name1 = normalize_service_name(original_name1);
            let normalized_name2 = normalize_service_name(original_name2);

            // Jaro-Winkler similarity is a common choice for name similarity.
            let pre_rl_confidence = strsim::jaro_winkler(&normalized_name1, &normalized_name2);

            // If pre-RL confidence is too low, don't even bother with feature extraction or RL.
            // This threshold can be tuned.
            if pre_rl_confidence < 0.6 { // Example: early exit for very dissimilar names
                // Still cache this "NO_MATCH" decision
                let new_cache_entry = NewServiceComparisonCacheEntry {
                    service_id_1: &service1_id.0,
                    service_id_2: &service2_id.0,
                    signature_1: s1_sig,
                    signature_2: s2_sig,
                    method_type: method_type_str,
                    pipeline_run_id: Some(pipeline_run_id),
                    comparison_result: "NO_MATCH",
                    confidence_score: Some(pre_rl_confidence),
                    snapshotted_features: None, // No features extracted
                };
                if let Err(e) = db::insert_service_comparison_cache_entry(pool, new_cache_entry).await {
                    warn!("Failed to insert NO_MATCH (low pre-RL) into service comparison cache for NAME ({}, {}): {}", service1_id.0, service2_id.0, e);
                }
                continue;
            }


            let features = if let Some(ref cache_arc) = feature_cache {
                let mut cache_guard = cache_arc.lock().await;
                cache_guard.get_pair_features(pool, service1_id, service2_id).await?
            } else if let Some(ref orch_arc) = service_orchestrator_option {
                let orch = orch_arc.lock().await;
                orch.get_pair_features(pool, service1_id, service2_id).await?
            } else {
                ServiceMatchingOrchestrator::extract_pair_context_features(pool, service1_id, service2_id).await?
            };


            let (tuned_confidence, tuner_version) = if let Some(ref orch_arc) = service_orchestrator_option {
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
                comparison_cache_result_val = "MATCH";
                let match_values = ServiceNameMatchValue {
                    original_name1: original_name1.clone(),
                    original_name2: original_name2.clone(),
                    normalized_name1: normalized_name1.clone(), // Store normalized for review
                    normalized_name2: normalized_name2.clone(), // Store normalized for review
                };
                let service_group_id_str = Uuid::new_v4().to_string();

                let mut tx_conn = pool.get().await.context("Failed to get DB connection for transaction")?;
                let mut transaction = tx_conn.transaction().await.context("Failed to start transaction")?;

                let tx_result: Result<()> = async {
                    insert_service_group_tx(
                        &mut transaction,
                        &service_group_id_str,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceNameSimilarity,
                        MatchValues::ServiceName(match_values),
                    ).await?;

                    insert_service_match_decision_detail(
                        &mut transaction,
                        &service_group_id_str,
                        pipeline_run_id,
                        serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                        MatchMethodType::ServiceNameSimilarity.as_str(),
                        pre_rl_confidence,
                        tuned_confidence,
                        tuner_version.map(|v| v as i32),
                    ).await?;
                    Ok(())
                }.await;

                match tx_result {
                    Ok(_) => {
                        transaction.commit().await.context("Failed to commit transaction")?;
                        groups_created_count += 1;
                        total_confidence_sum += tuned_confidence;
                    }
                    Err(e) => {
                        transaction.rollback().await.context("Failed to rollback transaction")?;
                        warn!("Transaction failed for name match ({} vs {}): {:?}. Rolled back.", service1_id.0, service2_id.0, e);
                        comparison_cache_result_val = "ERROR_IN_PROCESSING";
                    }
                }
            }

            // Store result in Service Comparison Cache
            let new_cache_entry = NewServiceComparisonCacheEntry {
                service_id_1: &service1_id.0,
                service_id_2: &service2_id.0,
                signature_1: s1_sig,
                signature_2: s2_sig,
                method_type: method_type_str,
                pipeline_run_id: Some(pipeline_run_id),
                comparison_result: comparison_cache_result_val,
                confidence_score: Some(tuned_confidence),
                snapshotted_features: Some(serde_json::to_value(&features).unwrap_or(serde_json::Value::Null)),
            };
            if let Err(e) = db::insert_service_comparison_cache_entry(pool, new_cache_entry).await {
                warn!("Failed to insert into service comparison cache for NAME ({}, {}): {}", service1_id.0, service2_id.0, e);
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

// Enhanced normalization based on plan (III.C.2)
fn normalize_service_name(name: &str) -> String {
    let lowercased = name.to_lowercase();
    // Example stop words - this list should be curated based on data
    let stop_words = ["center", "clinic", "services", "inc", "llc", "co", "ltd"];
    let mut normalized = lowercased
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect::<String>();

    for sw in stop_words {
        // Regex might be better for whole word replacement
        let pattern = format!(r"\b{}\b", sw); // \b for word boundaries
        if let Ok(re) = regex::Regex::new(&pattern) {
             normalized = re.replace_all(&normalized, "").to_string();
        }
    }

    normalized
        .split_whitespace()
        .filter(|s| !s.is_empty()) // Remove empty strings that might result from stop word removal
        .collect::<Vec<&str>>()
        .join(" ")
        .trim() // Trim leading/trailing whitespace
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
