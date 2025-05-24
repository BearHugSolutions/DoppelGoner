// src/service_matching/url.rs

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
use crate::models::{MatchMethodType, MatchValues, ServiceId, ServiceUrlMatchValue};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_feature_extraction;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};
use crate::utils::extract_domain;

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
        "Starting service URL matching for cluster_id: {} with {} services (direct DB calls)...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!(
            "No services for URL matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceUrlMatch,
        ));
    }

    let mut groups_created_count = 0;
    let mut total_confidence_sum = 0.0;
    let mut processed_service_ids = HashSet::new();

    let mut service_data_map = HashMap::new();
    let conn_for_sigs = pool
        .get()
        .await
        .context("Failed to get DB conn for pre-fetching signatures for URL matching")?;
    for (service_id, _url) in &services_in_cluster {
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
                            warn!("Failed to calculate signature for service {}: {}. It will be skipped in URL comparisons.", service_id.0, calc_err);
                        }
                    }
                }
            }
        }
    }
    drop(conn_for_sigs);

    for i in 0..services_in_cluster.len() {
        for j in (i + 1)..services_in_cluster.len() {
            let (service1_id, original_url1) = &services_in_cluster[i];
            let (service2_id, original_url2) = &services_in_cluster[j];

            if service1_id.0 == service2_id.0 {
                continue;
            }

            let s1_data = service_data_map.get(&service1_id.0);
            let s2_data = service_data_map.get(&service2_id.0);

            if s1_data.is_none() || s2_data.is_none() {
                warn!(
                    "Missing signature data for {} or {} in URL matching. Skipping pair.",
                    service1_id.0, service2_id.0
                );
                continue;
            }
            let s1_sig = s1_data.unwrap();
            let s2_sig = s2_data.unwrap();

            let method_type_str = MatchMethodType::ServiceUrlMatch.as_str();

            if let Some(cached_entry) = db::get_service_comparison_cache_entry(
                pool, // Pass pool
                &service1_id.0,
                &service2_id.0,
                s1_sig,
                s2_sig,
                method_type_str,
            )
            .await
            .context("Service comparison cache check failed for URL matching")?
            {
                info!(
                    "Cache HIT for URL ({}, {}) method {}. Result: {}",
                    service1_id.0, service2_id.0, method_type_str, cached_entry.comparison_result
                );
                continue;
            }
            info!(
                "Cache MISS for URL ({}, {}) method {}. Proceeding with full comparison.",
                service1_id.0, service2_id.0, method_type_str
            );

            let domain1 = extract_domain(original_url1);
            let domain2 = extract_domain(original_url2);

            let pre_rl_confidence: f64;
            let shared_domain: Option<String>;

            if domain1.is_some() && domain1 == domain2 {
                shared_domain = domain1; // domain1 is already an Option<String>
                let matching_slugs = calculate_matching_path_segments(original_url1, original_url2);
                pre_rl_confidence = calculate_url_confidence(
                    original_url1,
                    original_url2,
                    shared_domain.as_ref().unwrap(), // Safe unwrap due to is_some() check
                    matching_slugs,
                );
            } else {
                pre_rl_confidence = 0.0;
                shared_domain = None;
            }

            if pre_rl_confidence < 0.5 { // Threshold for URL pre-RL confidence
                let new_cache_entry = NewServiceComparisonCacheEntry {
                    service_id_1: &service1_id.0,
                    service_id_2: &service2_id.0,
                    signature_1: s1_sig,
                    signature_2: s2_sig,
                    method_type: method_type_str,
                    pipeline_run_id: Some(pipeline_run_id),
                    comparison_result: "NO_MATCH",
                    confidence_score: Some(pre_rl_confidence),
                    snapshotted_features: None,
                };
                if let Err(e) =
                    db::insert_service_comparison_cache_entry(pool, new_cache_entry).await
                {
                    warn!("Failed to insert NO_MATCH (low pre-RL) into service comparison cache for URL ({}, {}): {}", service1_id.0, service2_id.0, e);
                }
                continue;
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
                        &MatchMethodType::ServiceUrlMatch,
                        pre_rl_confidence,
                        &features,
                    )?;
                    (conf, Some(orchestrator.confidence_tuner.version))
                } else {
                    (pre_rl_confidence, None)
                };

            let mut comparison_cache_result_val = "NO_MATCH";

            if tuned_confidence >= RL_CONFIDENCE_THRESHOLD && shared_domain.is_some() {
                let match_values = ServiceUrlMatchValue {
                    original_url1: original_url1.clone(),
                    original_url2: original_url2.clone(),
                    normalized_shared_domain: shared_domain.clone().unwrap_or_default(), // Safe due to check
                    matching_slug_count: calculate_matching_path_segments(
                        original_url1,
                        original_url2,
                    ),
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
                    MatchMethodType::ServiceUrlMatch,
                    MatchValues::ServiceUrl(match_values),
                )
                .await
                {
                    Ok(actual_service_group_id) => {
                        match db::insert_service_match_decision_detail_direct(
                            pool,
                            &actual_service_group_id,
                            pipeline_run_id,
                            serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                            MatchMethodType::ServiceUrlMatch.as_str(),
                            pre_rl_confidence,
                            tuned_confidence,
                            tuner_version.map(|v| v as i32),
                        )
                        .await {
                            Ok(_) => {
                                debug!(
                                    "Successfully recorded URL match and detail for group_id: {}",
                                    actual_service_group_id
                                );
                                groups_created_count += 1;
                                total_confidence_sum += tuned_confidence;
                                comparison_cache_result_val = "MATCH";
                            }
                            Err(detail_err) => {
                                warn!(
                                    "Upserted service_group {} for URL match but FAILED to insert decision detail: {:?}. Match recorded, RL context lost.",
                                    actual_service_group_id, detail_err
                                );
                                comparison_cache_result_val = "MATCH_NO_DETAIL";
                            }
                        }
                    }
                    Err(group_err) => {
                        warn!(
                            "FAILED to upsert service_group for URL match ({} vs {}): {:?}. No decision detail recorded.",
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
                        "Failed to insert into service comparison cache for URL ({}, {}): {}",
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
        "Service URL matching for cluster_id: {} complete. Created {} groups with avg_confidence: {:.3}",
        cluster_id, groups_created_count, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceUrlMatch,
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

fn get_path_segments(url_str: &str) -> Vec<String> {
    match url::Url::parse(url_str) {
        Ok(parsed_url) => parsed_url.path_segments().map_or(Vec::new(), |segments| {
            segments
                .filter(|s| !s.is_empty())
                .map(|s| s.to_lowercase())
                .collect()
        }),
        Err(_) => Vec::new(),
    }
}

fn calculate_matching_path_segments(url1_str: &str, url2_str: &str) -> usize {
    let segments1 = get_path_segments(url1_str);
    let segments2 = get_path_segments(url2_str);

    if segments1.is_empty() || segments2.is_empty() {
        return 0;
    }

    let mut matching_count = 0;
    let len1 = segments1.len();
    let len2 = segments2.len();
    let min_len = std::cmp::min(len1, len2);

    for i in 0..min_len {
        if segments1[i] == segments2[i] {
            matching_count += 1;
        } else {
            break;
        }
    }
    matching_count
}

fn calculate_url_confidence(
    url1_str: &str,
    url2_str: &str,
    domain: &str, // Expecting a non-empty domain string
    matching_path_segments: usize,
) -> f64 {
    let mut confidence = 0.0;

    if domain.is_empty() { // Should ideally not happen if called after domain check
        return 0.0;
    }

    confidence = 0.75; // Base for shared domain

    let segments1 = get_path_segments(url1_str);
    let segments2 = get_path_segments(url2_str);

    if segments1.is_empty() && segments2.is_empty() {
        confidence = 0.95; // Domains match, no paths
    } else if segments1 == segments2 {
        confidence = 1.0; // Exact path match
    } else if matching_path_segments > 0 {
        let max_len = std::cmp::max(segments1.len(), segments2.len());
        if max_len > 0 {
            let proportion_matching = matching_path_segments as f64 / max_len as f64;
            confidence += proportion_matching * 0.20; // Bonus for path similarity
        }
    } else if !segments1.is_empty() && !segments2.is_empty() && segments1[0] != segments2[0] {
        confidence *= 0.8; // Penalize if first path segments differ
    }

    let generic_domains = [
        "wordpress.com", "blogspot.com", "sites.google.com", "github.io",
        "facebook.com", "twitter.com", "linkedin.com", "instagram.com", "youtube.com",
        // Add other common free/generic hosting or social media domains
    ];
    if generic_domains.contains(&domain) && segments1 != segments2 {
        confidence *= 0.9; // Reduce confidence for generic domains if paths aren't identical
    }

    confidence.min(1.0).max(0.0)
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
