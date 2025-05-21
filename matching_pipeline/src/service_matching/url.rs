// src/service_matching/url.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::PgPool;
use crate::models::{MatchMethodType, MatchValues, ServiceId, ServiceUrlMatchValue};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};
use crate::utils::extract_domain;

pub async fn find_matches(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
) -> Result<crate::results::ServiceMatchResult> {
    info!("Starting service URL matching...");

    let mut groups_created = 0;
    let mut total_services = 0;
    let mut avg_confidence = 0.0;

    // Fetch services with non-empty URLs
    let conn = pool
        .get()
        .await
        .context("Failed to get database connection")?;
    let rows = conn
        .query(
            "SELECT id, url FROM public.service 
         WHERE url IS NOT NULL AND url <> '' AND status = 'active'
         ORDER BY id",
            &[],
        )
        .await
        .context("Failed to query services with URLs")?;

    let services_count = rows.len();
    if services_count == 0 {
        info!("No services with URLs found for matching");
        return Ok(create_empty_result());
    }
    info!("Found {} services with URLs to process", services_count);

    // Build a map of normalized domains to services
    let mut domain_map: HashMap<String, Vec<(ServiceId, String)>> = HashMap::new();

    for row in rows {
        let service_id: String = row.get("id");
        let url: String = row.get("url");

        if let Some(domain) = extract_domain(&url) {
            if !domain.is_empty() {
                domain_map
                    .entry(domain.clone())
                    .or_default()
                    .push((ServiceId(service_id), url));
                total_services += 1;
            }
        }
    }
    info!("Built domain map with {} unique domains", domain_map.len());

    // Process potential matches
    let mut confidence_sum = 0.0;
    for (domain, services) in domain_map {
        if services.len() < 2 {
            continue; // No potential matches with this domain
        }

        // For each pair of services with the same domain
        for i in 0..services.len() {
            for j in (i + 1)..services.len() {
                let (service1_id, original_url1) = &services[i];
                let (service2_id, original_url2) = &services[j];

                // Skip self-matches
                if service1_id.0 == service2_id.0 {
                    continue;
                }

                // Calculate matching slugs for pre-RL confidence
                let matching_slug_count = calculate_matching_slugs(original_url1, original_url2);
                let pre_rl_confidence = calculate_url_confidence(
                    original_url1,
                    original_url2,
                    &domain,
                    matching_slug_count,
                );

                // Get feature context and apply RL if orchestrator is provided
                let tuned_confidence = if let Some(orchestrator_arc) = &service_orchestrator {
                    let mut orchestrator = orchestrator_arc.lock().await;

                    // Extract context features
                    let features = if let Some(cache) = &feature_cache {
                        let mut cache_guard = cache.lock().await;
                        cache_guard
                            .get_pair_features(pool, service1_id, service2_id)
                            .await?
                    } else {
                        ServiceMatchingOrchestrator::extract_pair_context_features(
                            pool,
                            service1_id,
                            service2_id,
                        )
                        .await?
                    };

                    // Get tuned confidence
                    let tuned = orchestrator.get_tuned_confidence(
                        &MatchMethodType::ServiceUrlMatch,
                        pre_rl_confidence,
                        &features,
                    )?;

                    // Log decision
                    let service_group_id = Uuid::new_v4().to_string();
                    let _ = orchestrator
                        .log_decision_snapshot(
                            pool,
                            &service_group_id,
                            pipeline_run_id,
                            &features,
                            &MatchMethodType::ServiceUrlMatch,
                            pre_rl_confidence,
                            tuned,
                        )
                        .await;

                    tuned
                } else {
                    pre_rl_confidence // Use raw confidence if no orchestrator
                };

                // Create a service group if confidence is sufficient
                if tuned_confidence >= 0.85 {
                    let match_values = ServiceUrlMatchValue {
                        original_url1: original_url1.clone(),
                        original_url2: original_url2.clone(),
                        normalized_shared_domain: domain.clone(),
                        matching_slug_count,
                    };

                    // Insert the service_group
                    let service_group_id = Uuid::new_v4().to_string();
                    let result = crate::service_matching::name::insert_service_group(
                        pool,
                        &service_group_id,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceUrlMatch,
                        MatchValues::ServiceUrl(match_values),
                    )
                    .await;

                    if result.is_ok() {
                        groups_created += 1;
                        confidence_sum += tuned_confidence;
                        debug!(
                            "Created service group for matched URLs. ID: {}, s1: {}, s2: {}, confidence: {:.3}",
                            service_group_id, service1_id.0, service2_id.0, tuned_confidence
                        );
                    }
                }
            }
        }
    }

    // Calculate average confidence
    if groups_created > 0 {
        avg_confidence = confidence_sum / groups_created as f64;
    }

    info!(
        "Service URL matching complete. Created {} groups with avg confidence: {:.3}",
        groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceUrlMatch,
        groups_created,
        entities_matched: total_services,
        avg_confidence,
        avg_group_size: 2.0,
    };

    // Create result struct
    Ok(ServiceMatchResult {
        groups_created,
        stats: crate::results::MatchMethodStats {
            method_type: MatchMethodType::ServiceUrlMatch,
            groups_created,
            entities_matched: total_services,
            avg_confidence,
            avg_group_size: 2.0,
        },
        method_stats: vec![method_stats],
    })
}

// Calculate number of matching URL path segments/slugs after the domain
fn calculate_matching_slugs(url1: &str, url2: &str) -> usize {
    fn get_path_segments(url: &str) -> Vec<String> {
        let path_start = url.find('/').map_or_else(|| url.len(), |i| i);
        let path = &url[path_start..];
        path.split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_lowercase())
            .collect()
    }

    let segments1 = get_path_segments(url1);
    let segments2 = get_path_segments(url2);

    let mut count = 0;
    let min_len = segments1.len().min(segments2.len());

    for i in 0..min_len {
        if segments1[i] == segments2[i] {
            count += 1;
        } else {
            break; // Stop counting when segments no longer match
        }
    }

    count
}

// Calculate URL confidence based on domain match and slug similarity
fn calculate_url_confidence(url1: &str, url2: &str, domain: &str, matching_slugs: usize) -> f64 {
    // Base confidence from domain match
    let mut confidence = 0.8;

    // Boost confidence based on matching path segments
    if matching_slugs > 0 {
        // Add up to 0.2 for matching slugs
        confidence += (matching_slugs as f64 * 0.05).min(0.2);
    }

    // Exact URL match gets max confidence
    if url1 == url2 {
        confidence = 1.0;
    }

    confidence
}

// Create empty result when no matches are found
fn create_empty_result() -> crate::results::ServiceMatchResult {
    let empty_stats = crate::results::MatchMethodStats {
        method_type: MatchMethodType::ServiceUrlMatch,
        groups_created: 0,
        entities_matched: 0,
        avg_confidence: 0.0,
        avg_group_size: 0.0,
    };

    crate::results::ServiceMatchResult {
        groups_created: 0,
        stats: empty_stats.clone(),
        method_stats: vec![empty_stats],
    }
}
