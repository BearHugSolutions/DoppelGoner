// src/service_matching/embedding.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use pgvector::Vector as PgVector;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::db::PgPool;
use crate::models::{MatchMethodType, MatchValues, ServiceEmbeddingMatchValue, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};
use crate::utils::cosine_similarity_candle;

pub async fn find_matches(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
) -> Result<crate::results::ServiceMatchResult> {
    info!("Starting service embedding similarity matching...");

    let mut groups_created = 0;
    let mut total_services = 0;
    let mut avg_confidence = 0.0;

    // Fetch services with embeddings
    let conn = pool
        .get()
        .await
        .context("Failed to get database connection")?;
    let rows = conn
        .query(
            "SELECT id, name, embedding_v2 FROM public.service 
         WHERE embedding_v2 IS NOT NULL AND status = 'active'
         ORDER BY id",
            &[],
        )
        .await
        .context("Failed to query services with embeddings")?;

    let services_count = rows.len();
    if services_count == 0 {
        info!("No services with embeddings found for matching");
        return Ok(create_empty_result());
    }
    info!(
        "Found {} services with embeddings to process",
        services_count
    );

    // Store service embeddings for comparison
    let mut service_embeddings: Vec<(ServiceId, String, Vec<f32>)> = Vec::new();

    for row in rows {
        let service_id: String = row.get("id");
        let name: String = row.get("name");

        // Extract embedding vector from pgvector
        if let Ok(Some(embedding_pg)) = row.try_get::<_, Option<PgVector>>("embedding_v2") {
            let embedding_vec = embedding_pg.to_vec();
            if !embedding_vec.is_empty() {
                service_embeddings.push((ServiceId(service_id), name, embedding_vec));
                total_services += 1;
            }
        }
    }
    info!(
        "Processed {} service embeddings for comparison",
        service_embeddings.len()
    );

    // Compare all pairs of embeddings
    let mut confidence_sum = 0.0;

    // For each pair of services
    for i in 0..service_embeddings.len() {
        for j in (i + 1)..service_embeddings.len() {
            let (service1_id, name1, embedding1) = &service_embeddings[i];
            let (service2_id, name2, embedding2) = &service_embeddings[j];

            // Skip self-matches
            if service1_id.0 == service2_id.0 {
                continue;
            }

            // Calculate cosine similarity
            let similarity = match cosine_similarity_candle(embedding1, embedding2) {
                Ok(sim) => sim,
                Err(e) => {
                    warn!("Failed to calculate cosine similarity: {}", e);
                    continue;
                }
            };

            // Only consider high similarities to avoid false positives
            if similarity < 0.8 {
                continue;
            }

            // Use similarity as pre-RL confidence
            let pre_rl_confidence = similarity;

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
                    &MatchMethodType::ServiceEmbeddingSimilarity,
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
                        &MatchMethodType::ServiceEmbeddingSimilarity,
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
                let match_values = ServiceEmbeddingMatchValue {
                    name1: name1.clone(),
                    name2: name2.clone(),
                    embedding_similarity: similarity,
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
                    MatchMethodType::ServiceEmbeddingSimilarity,
                    MatchValues::ServiceEmbedding(match_values),
                )
                .await;

                if result.is_ok() {
                    groups_created += 1;
                    confidence_sum += tuned_confidence;
                    debug!(
                        "Created service group for embedding similarity. ID: {}, s1: {}, s2: {}, similarity: {:.3}, confidence: {:.3}",
                        service_group_id, service1_id.0, service2_id.0, similarity, tuned_confidence
                    );
                }
            }
        }
    }

    // Calculate average confidence
    if groups_created > 0 {
        avg_confidence = confidence_sum / groups_created as f64;
    }

    info!(
        "Service embedding matching complete. Created {} groups with avg confidence: {:.3}",
        groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmbeddingSimilarity,
        groups_created,
        entities_matched: total_services,
        avg_confidence,
        avg_group_size: 2.0,
    };

    // Create result struct
    Ok(ServiceMatchResult {
        groups_created,
        stats: MatchMethodStats {
            method_type: MatchMethodType::ServiceEmbeddingSimilarity,
            groups_created,
            entities_matched: total_services,
            avg_confidence,
            avg_group_size: 2.0,
        },
        method_stats: vec![method_stats],
    })
}

// Create empty result when no matches are found
fn create_empty_result() -> ServiceMatchResult {
    let empty_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmbeddingSimilarity,
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
