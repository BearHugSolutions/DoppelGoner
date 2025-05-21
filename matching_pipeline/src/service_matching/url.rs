// src/service_matching/url.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::PgPool; //
use crate::models::{MatchMethodType, MatchValues, ServiceId, ServiceUrlMatchValue}; //
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache; //
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator; //
use crate::results::{MatchMethodStats, ServiceMatchResult}; //
use crate::service_matching::name::insert_service_group; // Assuming it's made pub(crate)
use crate::utils::extract_domain; //

pub async fn find_matches_in_cluster(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String)>, // Input: Vec of (ServiceId, url)
    cluster_id: String,                            // For logging context
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service URL matching for cluster_id: {} with {} services...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!(
            "No services provided for URL matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceUrlMatch,
        ));
    }

    let mut groups_created = 0;
    let mut confidence_sum = 0.0;
    let mut distinct_services_processed_count = 0;

    let mut domain_map: HashMap<String, Vec<(ServiceId, String)>> = HashMap::new(); //

    for (service_id, url) in services_in_cluster {
        if let Some(domain) = extract_domain(&url) {
            //
            if !domain.is_empty() {
                domain_map
                    .entry(domain.clone())
                    .or_default()
                    .push((service_id, url)); //
            }
        }
    }
    distinct_services_processed_count = domain_map
        .values()
        .flat_map(|v| v.iter().map(|(id, _)| id))
        .collect::<HashSet<_>>()
        .len();

    info!(
        //
        "Built domain map with {} unique domains for cluster_id: {}",
        domain_map.len(),
        cluster_id
    );

    for (domain, services_with_same_domain) in domain_map {
        //
        if services_with_same_domain.len() < 2 {
            //
            continue;
        }

        for i in 0..services_with_same_domain.len() {
            //
            for j in (i + 1)..services_with_same_domain.len() {
                //
                let (service1_id, original_url1) = &services_with_same_domain[i]; //
                let (service2_id, original_url2) = &services_with_same_domain[j]; //

                if service1_id.0 == service2_id.0 {
                    continue;
                } //

                let matching_slug_count = calculate_matching_slugs(original_url1, original_url2); //
                let pre_rl_confidence = calculate_url_confidence(
                    original_url1,
                    original_url2,
                    &domain,
                    matching_slug_count,
                ); //

                let tuned_confidence = if let Some(orchestrator_arc) = &service_orchestrator {
                    //
                    let mut orchestrator = orchestrator_arc.lock().await; //
                    let features = if let Some(cache) = &feature_cache {
                        //
                        let mut cache_guard = cache.lock().await; //
                        cache_guard
                            .get_pair_features(pool, service1_id, service2_id)
                            .await? //
                    } else {
                        ServiceMatchingOrchestrator::extract_pair_context_features(
                            pool,
                            service1_id,
                            service2_id,
                        )
                        .await? //
                    };
                    let tuned = orchestrator.get_tuned_confidence(
                        //
                        &MatchMethodType::ServiceUrlMatch,
                        pre_rl_confidence,
                        &features,
                    )?;
                    let service_group_id_for_log = Uuid::new_v4().to_string(); //
                    let _ = orchestrator
                        .log_decision_snapshot(
                            //
                            pool,
                            &service_group_id_for_log,
                            pipeline_run_id,
                            &features,
                            &MatchMethodType::ServiceUrlMatch,
                            pre_rl_confidence,
                            tuned,
                        )
                        .await;
                    tuned
                } else {
                    pre_rl_confidence //
                };

                if tuned_confidence >= 0.85 {
                    //
                    let match_values = ServiceUrlMatchValue {
                        //
                        original_url1: original_url1.clone(), //
                        original_url2: original_url2.clone(), //
                        normalized_shared_domain: domain.clone(), //
                        matching_slug_count,                  //
                    };
                    let service_group_id = Uuid::new_v4().to_string(); //
                    let result = insert_service_group(
                        //
                        pool,
                        &service_group_id,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceUrlMatch,
                        MatchValues::ServiceUrl(match_values), //
                    )
                    .await;

                    if result.is_ok() {
                        groups_created += 1; //
                        confidence_sum += tuned_confidence; //
                        debug!( //
                            "Cluster_id: {}. Created service group for matched URLs. ID: {}, s1: {}, s2: {}, confidence: {:.3}",
                            cluster_id, service_group_id, service1_id.0, service2_id.0, tuned_confidence
                        );
                    } else {
                        warn!(
                            "Failed to insert service group for URL match in cluster {}: {:?}",
                            cluster_id,
                            result.err()
                        );
                    }
                }
            }
        }
    }

    let avg_confidence = if groups_created > 0 {
        confidence_sum / groups_created as f64
    } else {
        0.0
    }; //

    info!( //
        "Service URL matching for cluster_id: {} complete. Created {} groups with avg confidence: {:.3}",
        cluster_id, groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        //
        method_type: MatchMethodType::ServiceUrlMatch, //
        groups_created,                                //
        entities_matched: distinct_services_processed_count, //
        avg_confidence,                                //
        avg_group_size: 2.0,                           //
    };

    Ok(ServiceMatchResult {
        //
        groups_created,                   //
        stats: method_stats.clone(),      //
        method_stats: vec![method_stats], //
    })
}

// calculate_matching_slugs and calculate_url_confidence functions remain the same
fn calculate_matching_slugs(url1: &str, url2: &str) -> usize {
    fn get_path_segments(url_str: &str) -> Vec<String> {
        // Changed from url to url_str to avoid conflict
        let parsed_url = match url::Url::parse(url_str) {
            Ok(u) => u,
            Err(_) => return Vec::new(), // Cannot parse, no path segments
        };
        parsed_url.path_segments().map_or(Vec::new(), |segments| {
            segments
                .filter(|s| !s.is_empty())
                .map(|s| s.to_lowercase())
                .collect()
        })
    }

    let segments1 = get_path_segments(url1); //
    let segments2 = get_path_segments(url2); //
    segments1
        .iter()
        .zip(segments2.iter())
        .filter(|&(s1, s2)| s1 == s2)
        .count() //
}

fn calculate_url_confidence(_url1: &str, _url2: &str, _domain: &str, matching_slugs: usize) -> f64 {
    //
    let mut confidence = 0.8; // Base for domain match
    if matching_slugs > 0 {
        //
        confidence += (matching_slugs as f64 * 0.05).min(0.15); // Boost for slugs, max boost 0.15
    }
    // Heuristic: if URLs are identical after ensuring they are valid and comparable
    if let (Ok(p_url1), Ok(p_url2)) = (url::Url::parse(_url1), url::Url::parse(_url2)) {
        if p_url1 == p_url2 {
            return 1.0; //
        }
    }
    confidence.min(0.95) // Cap confidence unless identical
}

fn create_empty_result_for_cluster(method_type: MatchMethodType) -> ServiceMatchResult {
    //
    let empty_stats = MatchMethodStats {
        //
        method_type,         //
        groups_created: 0,   //
        entities_matched: 0, //
        avg_confidence: 0.0, //
        avg_group_size: 0.0, //
    };
    ServiceMatchResult {
        //
        groups_created: 0,               //
        stats: empty_stats.clone(),      //
        method_stats: vec![empty_stats], //
    }
}
