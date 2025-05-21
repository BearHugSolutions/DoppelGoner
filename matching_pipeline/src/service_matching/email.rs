// src/service_matching/email.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::PgPool;
use crate::models::{EmailMatchValue, MatchMethodType, MatchValues, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};

pub async fn find_matches(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
) -> Result<crate::results::ServiceMatchResult> {
    info!("Starting service email matching...");

    let mut groups_created = 0;
    let mut total_services = 0;
    let mut avg_confidence = 0.0;

    // Fetch services with non-empty emails
    let conn = pool
        .get()
        .await
        .context("Failed to get database connection")?;
    let rows = conn
        .query(
            "SELECT id, email FROM public.service 
         WHERE email IS NOT NULL AND email <> '' AND status = 'active'
         ORDER BY id",
            &[],
        )
        .await
        .context("Failed to query services with emails")?;

    let services_count = rows.len();
    if services_count == 0 {
        info!("No services with emails found for matching");
        return Ok(create_empty_result());
    }
    info!("Found {} services with emails to process", services_count);

    // Build a map of normalized emails to services
    let mut email_map: HashMap<String, Vec<(ServiceId, String)>> = HashMap::new();

    for row in rows {
        let service_id: String = row.get("id");
        let email: String = row.get("email");

        let normalized_email = normalize_email(&email);
        if !normalized_email.is_empty() {
            email_map
                .entry(normalized_email.clone())
                .or_default()
                .push((ServiceId(service_id), email));
            total_services += 1;
        }
    }
    info!(
        "Built email map with {} unique normalized emails",
        email_map.len()
    );

    // Process potential matches
    let mut confidence_sum = 0.0;
    for (normalized_email, services) in email_map {
        if services.len() < 2 {
            continue; // No potential matches with this email
        }

        // For each pair of services with the same normalized email
        for i in 0..services.len() {
            for j in (i + 1)..services.len() {
                let (service1_id, original_email1) = &services[i];
                let (service2_id, original_email2) = &services[j];

                // Skip self-matches
                if service1_id.0 == service2_id.0 {
                    continue;
                }

                // Email matches are high confidence by default
                let pre_rl_confidence = 0.95;

                // Get feature context and apply RL if orchestrator is provided
                let tuned_confidence = if let Some(orchestrator_arc) = &service_orchestrator {
                    let orchestrator = orchestrator_arc.lock().await;

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
                        &MatchMethodType::ServiceEmailMatch,
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
                            &MatchMethodType::ServiceEmailMatch,
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
                    let match_values = EmailMatchValue {
                        original_email1: original_email1.clone(),
                        original_email2: original_email2.clone(),
                        normalized_shared_email: normalized_email.clone(),
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
                        MatchMethodType::ServiceEmailMatch,
                        MatchValues::ServiceEmail(match_values),
                    )
                    .await;

                    if result.is_ok() {
                        groups_created += 1;
                        confidence_sum += tuned_confidence;
                        debug!(
                            "Created service group for matched emails. ID: {}, s1: {}, s2: {}, confidence: {:.3}",
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
        "Service email matching complete. Created {} groups with avg confidence: {:.3}",
        groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmailMatch,
        groups_created,
        entities_matched: total_services,
        avg_confidence,
        avg_group_size: 2.0,
    };

    // Create result struct
    Ok(ServiceMatchResult {
        groups_created,
        stats: MatchMethodStats {
            method_type: MatchMethodType::ServiceEmailMatch,
            groups_created,
            entities_matched: total_services,
            avg_confidence,
            avg_group_size: 2.0,
        },
        method_stats: vec![method_stats],
    })
}

// Normalize email addresses for comparison
fn normalize_email(email: &str) -> String {
    let email = email.trim().to_lowercase();

    // Handle common email formats and strip local part "plus addressing"
    if let Some(at_pos) = email.find('@') {
        let (local_part, domain) = email.split_at(at_pos);

        // Remove plus addressing if present
        let normalized_local_part = if let Some(plus_pos) = local_part.find('+') {
            local_part[0..plus_pos].to_string()
        } else {
            local_part.to_string()
        };

        // Remove dots from Gmail addresses (they're ignored by Gmail)
        let normalized_local_part = if domain.contains("gmail.com") {
            normalized_local_part.replace('.', "")
        } else {
            normalized_local_part
        };

        normalized_local_part + domain
    } else {
        email // Not a valid email, return as is
    }
}

// Create empty result when no matches are found
fn create_empty_result() -> crate::results::ServiceMatchResult {
    let empty_stats = crate::results::MatchMethodStats {
        method_type: MatchMethodType::ServiceEmailMatch,
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
