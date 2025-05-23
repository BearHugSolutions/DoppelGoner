// src/service_matching/email.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::{insert_service_group, PgPool};
use crate::models::{EmailMatchValue, MatchMethodType, MatchValues, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};

pub async fn find_matches_in_cluster(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String)>, // Input: Vec of (ServiceId, email_address)
    cluster_id: String,                            // For logging context
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service email matching for cluster_id: {} with {} services...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!(
            "No services provided for email matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceEmailMatch,
        ));
    }

    let mut groups_created = 0;
    let mut confidence_sum = 0.0;
    let mut distinct_services_processed_count = 0;

    let mut email_map: HashMap<String, Vec<(ServiceId, String)>> = HashMap::new();

    for (service_id, email) in services_in_cluster {
        let normalized_email = normalize_email(&email);
        if !normalized_email.is_empty() {
            email_map
                .entry(normalized_email.clone())
                .or_default()
                .push((service_id, email));
        }
    }
    distinct_services_processed_count = email_map
        .values()
        .flat_map(|v| v.iter().map(|(id, _)| id))
        .collect::<HashSet<_>>()
        .len();

    info!(
        "Built email map with {} unique normalized emails for cluster_id: {}",
        email_map.len(),
        cluster_id
    );

    for (normalized_email, services_with_same_email) in email_map {
        if services_with_same_email.len() < 2 {
            continue;
        }

        for i in 0..services_with_same_email.len() {
            for j in (i + 1)..services_with_same_email.len() {
                let (service1_id, original_email1) = &services_with_same_email[i];
                let (service2_id, original_email2) = &services_with_same_email[j];

                if service1_id.0 == service2_id.0 {
                    continue;
                }

                let pre_rl_confidence = 0.95;

                // Extract features once for both tuning and logging
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

                let tuned_confidence = if let Some(orchestrator_arc) = &service_orchestrator {
                    let orchestrator = orchestrator_arc.lock().await;
                    orchestrator.get_tuned_confidence(
                        &MatchMethodType::ServiceEmailMatch,
                        pre_rl_confidence,
                        &features,
                    )?
                } else {
                    pre_rl_confidence
                };

                if tuned_confidence >= 0.85 {
                    let match_values = EmailMatchValue {
                        original_email1: original_email1.clone(),
                        original_email2: original_email2.clone(),
                        normalized_shared_email: normalized_email.clone(),
                    };

                    // Generate service_group_id BEFORE inserting the group
                    let service_group_id = Uuid::new_v4().to_string();

                    // Attempt to insert the service group
                    let result = insert_service_group(
                        pool,
                        &service_group_id,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceEmailMatch,
                        MatchValues::ServiceEmail(match_values),
                    )
                    .await;

                    if result.is_ok() {
                        groups_created += 1;
                        confidence_sum += tuned_confidence;
                        debug!(
                            "Cluster_id: {}. Created service group for matched emails. ID: {}, s1: {}, s2: {}, confidence: {:.3}",
                           cluster_id, service_group_id, service1_id.0, service2_id.0, tuned_confidence
                        );

                        // Log the decision snapshot ONLY if the service group was successfully inserted
                        if let Some(orchestrator_arc) = &service_orchestrator {
                            let orchestrator = orchestrator_arc.lock().await;
                            let _ = orchestrator
                                .log_decision_snapshot(
                                    pool,
                                    &service_group_id, // Use the SAME ID that was just inserted
                                    pipeline_run_id,
                                    &features, // Use the already extracted features
                                    &MatchMethodType::ServiceEmailMatch,
                                    pre_rl_confidence,
                                    tuned_confidence,
                                )
                                .await;
                        }
                    } else {
                        warn!(
                            "Failed to insert service group for email match in cluster {}: {:?}",
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
    };

    info!(
        "Service email matching for cluster_id: {} complete. Created {} groups with avg confidence: {:.3}",
        cluster_id, groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmailMatch,
        groups_created,
        entities_matched: distinct_services_processed_count,
        avg_confidence,
        avg_group_size: 2.0,
    };

    Ok(ServiceMatchResult {
        groups_created,
        stats: method_stats.clone(),
        method_stats: vec![method_stats],
    })
}

// normalize_email function remains the same
fn normalize_email(email: &str) -> String {
    let email_trimmed = email.trim().to_lowercase();
    if let Some(at_pos) = email_trimmed.find('@') {
        let (local_part, domain_part) = email_trimmed.split_at(at_pos);
        let mut normalized_local = local_part.to_string();
        if let Some(plus_pos) = normalized_local.find('+') {
            normalized_local.truncate(plus_pos);
        }
        if domain_part.contains("gmail.com") || domain_part.contains("googlemail.com") {
            normalized_local = normalized_local.replace('.', "");
        }
        normalized_local + domain_part
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
