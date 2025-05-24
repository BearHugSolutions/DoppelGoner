// src/service_matching/embedding.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

// Import the new transactional DB functions and PgPool
use crate::db::{insert_service_group_tx, insert_service_match_decision_detail, PgPool};
use crate::models::{MatchMethodType, MatchValues, ServiceEmbeddingMatchValue, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};
use crate::utils::cosine_similarity_candle;

pub async fn find_matches_in_cluster(
    pool: &PgPool,
    service_orchestrator: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String, Option<Vec<f32>>)>, // Input: Vec of (ServiceId, Name, Option<Embedding>)
    cluster_id: String,                                              // For logging context
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service embedding matching for cluster_id: {} with {} services...",
        cluster_id,
        services_in_cluster.len()
    );

    let service_embeddings: Vec<(ServiceId, String, Vec<f32>)> = services_in_cluster
        .into_iter()
        .filter_map(|(id, name, opt_embedding)| opt_embedding.map(|emb| (id, name, emb)))
        .collect();

    if service_embeddings.is_empty() {
        debug!(
            "No services with embeddings provided for matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceEmbeddingSimilarity,
        ));
    }

    let mut groups_created = 0;
    let mut confidence_sum = 0.0;
    let distinct_services_processed_count = service_embeddings.len();

    info!(
        "Processing {} service embeddings for comparison in cluster_id: {}",
        service_embeddings.len(),
        cluster_id
    );

    for i in 0..service_embeddings.len() {
        for j in (i + 1)..service_embeddings.len() {
            let (service1_id, name1, embedding1) = &service_embeddings[i];
            let (service2_id, name2, embedding2) = &service_embeddings[j];

            if service1_id.0 == service2_id.0 {
                continue;
            }

            let similarity = match cosine_similarity_candle(embedding1, embedding2) {
                Ok(sim) => sim,
                Err(e) => {
                    warn!("Cluster {}: Failed to calculate cosine similarity between {} and {}: {}", cluster_id, service1_id.0, service2_id.0, e);
                    continue;
                }
            };

            if similarity < 0.8 {
                continue;
            }

            let pre_rl_confidence = similarity;

            let features = if let Some(cache) = &feature_cache {
                let mut cache_guard = cache.lock().await;
                cache_guard
                    .get_pair_features(pool, service1_id, service2_id)
                    .await?
            } else {
                ServiceMatchingOrchestrator::extract_pair_context_features(
                    pool, service1_id, service2_id,
                ).await?
            };

            let (tuned_confidence, version) = if let Some(orchestrator_arc) = &service_orchestrator {
                let orchestrator = orchestrator_arc.lock().await;
                let conf = orchestrator.get_tuned_confidence(
                    &MatchMethodType::ServiceEmbeddingSimilarity, pre_rl_confidence, &features,
                )?;
                let ver = Some(orchestrator.confidence_tuner.version as i32);
                (conf, ver)
            } else {
                (pre_rl_confidence, None)
            };

            if tuned_confidence >= 0.85 {
                let match_values = ServiceEmbeddingMatchValue {
                    name1: name1.clone(),
                    name2: name2.clone(),
                    embedding_similarity: similarity,
                };
                let service_group_id = Uuid::new_v4().to_string();

                // --- Transaction Management ---
                let mut conn = pool.get().await.context("Failed to get DB connection")?;
                let mut transaction = conn.transaction().await.context("Failed to start transaction")?;

                let result: Result<()> = async {
                    insert_service_group_tx(
                        &mut transaction,
                        &service_group_id,
                        service1_id,
                        service2_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceEmbeddingSimilarity,
                        MatchValues::ServiceEmbedding(match_values.clone()),
                    ).await?;

                    insert_service_match_decision_detail(
                        &mut transaction,
                        &service_group_id,
                        pipeline_run_id,
                        serde_json::to_value(&features).context("Failed to serialize features")?,
                        MatchMethodType::ServiceEmbeddingSimilarity.as_str(),
                        pre_rl_confidence,
                        tuned_confidence,
                        version,
                    ).await?;

                    Ok(())
                }.await;

                match result {
                    Ok(_) => {
                        transaction.commit().await.context("Failed to commit transaction")?;
                        groups_created += 1;
                        confidence_sum += tuned_confidence;
                        debug!(
                            "Cluster_id: {}. Committed embedding group & decision. ID: {}, s1: {}, s2: {}, sim: {:.3}, conf: {:.3}",
                            cluster_id, service_group_id, service1_id.0, service2_id.0, similarity, tuned_confidence
                        );
                    }
                    Err(e) => {
                        transaction.rollback().await.context("Failed to rollback transaction")?;
                        warn!(
                            "Failed transaction for embedding match in cluster {}: {:?}. Rolled back.",
                            cluster_id, e
                        );
                    }
                }
                // --- End Transaction Management ---
            }
        }
    }

    let avg_confidence = if groups_created > 0 {
        confidence_sum / groups_created as f64
    } else {
        0.0
    };

    info!(
        "Service embedding matching for cluster_id: {} complete. Created {} groups with avg confidence: {:.3}",
        cluster_id, groups_created, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmbeddingSimilarity,
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