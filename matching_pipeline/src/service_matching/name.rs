// src/service_matching/name.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::PgPool;
use crate::models::{ServiceId, MatchMethodType, ServiceNameMatchValue, MatchValues};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};

pub async fn find_matches(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
) -> Result<crate::results::ServiceMatchResult> {
    info!("Starting service name matching...");
    
    let mut groups_created = 0;
    let mut total_services = 0;
    let mut avg_confidence = 0.0;
    
    // Fetch services with non-empty names
    let conn = pool.get().await.context("Failed to get database connection")?;
    let rows = conn.query(
        "SELECT id, name, organization_id FROM public.service 
         WHERE name IS NOT NULL AND name <> '' AND status = 'active'
         ORDER BY id",
        &[],
    ).await.context("Failed to query services")?;
    
    let services_count = rows.len();
    if services_count == 0 {
        info!("No services with names found for matching");
        return Ok(create_empty_result());
    }
    info!("Found {} services with names to process", services_count);
    
    // Build a map of normalized names to services
    let mut name_map: HashMap<String, Vec<(ServiceId, String)>> = HashMap::new();
    
    for row in rows {
        let service_id: String = row.get("id");
        let original_name: String = row.get("name");
        let normalized_name = normalize_service_name(&original_name);
        
        if !normalized_name.is_empty() {
            name_map.entry(normalized_name.clone())
                .or_default()
                .push((ServiceId(service_id), original_name));
            total_services += 1;
        }
    }
    info!("Built name map with {} unique normalized names", name_map.len());
    
    // Process potential matches
    let mut confidence_sum = 0.0;
    for (normalized_name, services) in name_map {
        if services.len() < 2 {
            continue; // No potential matches with this name
        }
        
        // For each pair of services with the same normalized name
        for i in 0..services.len() {
            for j in (i+1)..services.len() {
                let (service1_id, original_name1) = &services[i];
                let (service2_id, original_name2) = &services[j];
                
                // Skip self-matches
                if service1_id.0 == service2_id.0 {
                    continue;
                }
                
                // Calculate Jaro-Winkler similarity as pre-RL confidence
                let pre_rl_confidence = jaro_winkler_similarity(original_name1, original_name2);
                
                // Get feature context and apply RL if orchestrator is provided
                let tuned_confidence = if let Some(orchestrator_arc) = &service_orchestrator {
                    let mut orchestrator = orchestrator_arc.lock().await;
                    
                    // Extract context features
                    let features = if let Some(cache) = &feature_cache {
                        let mut cache_guard = cache.lock().await;
                        cache_guard.get_pair_features(pool, service1_id, service2_id).await?
                    } else {
                        ServiceMatchingOrchestrator::extract_pair_context_features(pool, service1_id, service2_id).await?
                    };
                    
                    // Get tuned confidence
                    let tuned = orchestrator.get_tuned_confidence(
                        &MatchMethodType::ServiceNameSimilarity,
                        pre_rl_confidence,
                        &features
                    )?;
                    
                    // Log decision
                    let service_group_id = Uuid::new_v4().to_string();
                    let _ = orchestrator.log_decision_snapshot(
                        pool,
                        &service_group_id,
                        pipeline_run_id,
                        &features,
                        &MatchMethodType::ServiceNameSimilarity,
                        pre_rl_confidence,
                        tuned
                    ).await;
                    
                    tuned
                } else {
                    pre_rl_confidence // Use raw confidence if no orchestrator
                };
                
                // Create a service group if confidence is sufficient
                if tuned_confidence >= 0.85 {
                    let match_values = ServiceNameMatchValue {
                        original_name1: original_name1.clone(),
                        original_name2: original_name2.clone(),
                        normalized_name1: normalized_name.clone(),
                        normalized_name2: normalized_name.clone(),
                    };
                    
                    // Insert the service_group
                    let service_group_id = Uuid::new_v4().to_string();
                    let result = insert_service_group(
                        pool,
                        &service_group_id,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceNameSimilarity,
                        MatchValues::ServiceName(match_values)
                    ).await;
                    
                    if result.is_ok() {
                        groups_created += 1;
                        confidence_sum += tuned_confidence;
                        debug!(
                            "Created service group for matched names. ID: {}, s1: {}, s2: {}, confidence: {:.3}",
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
        "Service name matching complete. Created {} groups with avg confidence: {:.3}",
        groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceNameSimilarity,
        groups_created,
        entities_matched: total_services,
        avg_confidence,
        avg_group_size: 2.0,
    };
    
    // Create result struct
    Ok(crate::results::ServiceMatchResult {
        groups_created,
        stats: crate::results::MatchMethodStats {
            method_type: MatchMethodType::ServiceNameSimilarity,
            groups_created,
            entities_matched: total_services,
            avg_confidence,
            avg_group_size: 2.0,
        },
        method_stats: vec![method_stats],
    })
}

// Helper function to normalize service names
fn normalize_service_name(name: &str) -> String {
    let mut normalized = name.to_lowercase();
    normalized = normalized
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect();
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

// Calculate Jaro-Winkler similarity between two strings
fn jaro_winkler_similarity(s1: &str, s2: &str) -> f64 {
    // Simple implementation - in production you would use a proper library
    // For this example, we'll return a placeholder value
    if s1 == s2 {
        1.0
    } else {
        let longest = s1.len().max(s2.len()) as f64;
        let distance = strsim::jaro_winkler(s1, s2);
        distance
    }
}

// Insert a new service_group record
pub(crate) async fn insert_service_group(
    pool: &PgPool,
    id: &str,
    service_id_1: &ServiceId,
    service_id_2: &ServiceId,
    confidence_score: f64,
    pre_rl_confidence_score: f64,
    method_type: MatchMethodType,
    match_values: MatchValues,
) -> Result<()> {
    let conn = pool.get().await.context("Failed to get database connection")?;
    
    // Ensure correct ordering of service IDs (smaller ID first)
    let (s1, s2) = if service_id_1.0 < service_id_2.0 {
        (service_id_1, service_id_2)
    } else {
        (service_id_2, service_id_1)
    };
    
    // Convert match_values to JSON
    let match_values_json = serde_json::to_value(match_values)
        .context("Failed to serialize match values to JSON")?;
    
    conn.execute(
        "INSERT INTO public.service_group
         (id, service_id_1, service_id_2, confidence_score, method_type, 
          match_values, pre_rl_confidence_score, confirmed_status, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
         ON CONFLICT (service_id_1, service_id_2, method_type) 
         DO UPDATE SET 
            confidence_score = EXCLUDED.confidence_score,
            match_values = EXCLUDED.match_values,
            pre_rl_confidence_score = EXCLUDED.pre_rl_confidence_score,
            updated_at = CURRENT_TIMESTAMP",
        &[
            &id, 
            &s1.0, 
            &s2.0, 
            &confidence_score, 
            &method_type.as_str(),
            &match_values_json,
            &pre_rl_confidence_score,
            &"PENDING_REVIEW",
        ],
    ).await.context("Failed to insert service_group record")?;
    
    Ok(())
}

// Create empty result when no matches are found
fn create_empty_result() -> crate::results::ServiceMatchResult {
    let empty_stats = crate::results::MatchMethodStats {
        method_type: MatchMethodType::ServiceNameSimilarity,
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