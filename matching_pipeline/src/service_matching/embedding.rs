// src/service_matching/embedding.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;
use pgvector::Vector as PgVector; // For handling pgvector types

use crate::db::{
    self, // For easier access to db functions like get_service_comparison_cache_entry
    insert_service_group_tx,
    insert_service_match_decision_detail,
    PgPool,
    NewServiceComparisonCacheEntry,
};
use crate::models::{MatchMethodType, MatchValues, ServiceEmbeddingMatchValue, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::reinforcement::service::service_feature_extraction; // For get_service_raw_data_and_signature
use crate::results::{MatchMethodStats, ServiceMatchResult};
// cosine_similarity_candle is not directly used for ANN query but for fallback or direct calculation if needed.
// The ANN query itself returns similarity.

const ANN_SIMILARITY_THRESHOLD: f64 = 0.8; // As per plan
const ANN_TOP_K: i64 = 10; // As per plan
const RL_CONFIDENCE_THRESHOLD: f64 = 0.85; // Standard threshold for making a match after RL

#[derive(Debug)]
struct CandidateService {
    id: ServiceId,
    name: String,
    // embedding: PgVector, // Not strictly needed if similarity is returned by query
    similarity: f64,
}

pub async fn find_matches_in_cluster(
    pool: &PgPool,
    service_orchestrator_option: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String, Option<Vec<f32>>)>, // (ServiceId, Name, Option<Embedding Vec<f32>>)
    cluster_id: String,
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service embedding matching for cluster_id: {} with {} services using ANN.",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!("No services for embedding matching in cluster_id: {}.", cluster_id);
        return Ok(create_empty_result_for_cluster(MatchMethodType::ServiceEmbeddingSimilarity));
    }

    let mut groups_created_count = 0;
    let mut total_confidence_sum = 0.0;
    let mut processed_service_ids = HashSet::new(); // To count distinct services processed

    let conn = pool.get().await.context("Failed to get DB connection for ANN query")?;

    for (anchor_service_id, anchor_service_name, anchor_embedding_opt) in &services_in_cluster {
        processed_service_ids.insert(anchor_service_id.0.clone());

        if anchor_embedding_opt.is_none() {
            debug!("Anchor service {} has no embedding, skipping ANN.", anchor_service_id.0);
            continue;
        }
        let anchor_embedding_vec = anchor_embedding_opt.as_ref().unwrap();
        let anchor_pg_embedding = PgVector::from(anchor_embedding_vec.clone());

        // ANN Query (Conceptual SQL from plan adapted to tokio_postgres)
        // This query finds services within the same cluster_id.
        let ann_query_sql = "
            WITH cluster_entity_ids AS (
                SELECT entity_id_1 AS entity_id FROM public.entity_group WHERE group_cluster_id = $1
                UNION
                SELECT entity_id_2 AS entity_id FROM public.entity_group WHERE group_cluster_id = $1
            ),
            services_in_target_cluster AS (
                SELECT s.id, s.name, s.embedding_v2
                FROM public.service s
                JOIN public.entity_feature ef ON s.id = ef.table_id AND ef.table_name = 'service'
                WHERE ef.entity_id IN (SELECT entity_id FROM cluster_entity_ids)
                  AND s.id != $2 -- Exclude the anchor service itself
                  AND s.embedding_v2 IS NOT NULL
                GROUP BY s.id, s.name, s.embedding_v2
            )
            SELECT
                stc.id,
                stc.name,
                stc.embedding_v2,
                1 - (stc.embedding_v2 <=> $3) AS similarity
            FROM services_in_target_cluster stc
            WHERE 1 - (stc.embedding_v2 <=> $3) > $4
            ORDER BY stc.embedding_v2 <=> $3
            LIMIT $5;
        ";

        let candidate_rows = conn.query(
            ann_query_sql,
            &[&cluster_id, &anchor_service_id.0, &anchor_pg_embedding, &ANN_SIMILARITY_THRESHOLD, &ANN_TOP_K],
        ).await.context(format!("ANN query failed for anchor service {}", anchor_service_id.0))?;

        if candidate_rows.is_empty() {
            debug!("No ANN candidates found for anchor service {}", anchor_service_id.0);
            continue;
        }
        info!("Found {} ANN candidates for anchor service {}", candidate_rows.len(), anchor_service_id.0);


        for candidate_row in candidate_rows {
            let candidate_service_id_str: String = candidate_row.get("id");
            let candidate_service_name: String = candidate_row.get("name");
            // let _candidate_pg_embedding: PgVector = candidate_row.get("embedding_v2");
            let raw_similarity: f64 = candidate_row.get("similarity");

            let candidate_service_id = ServiceId(candidate_service_id_str);
            processed_service_ids.insert(candidate_service_id.0.clone());


            // --- Service Comparison Cache Check ---
            let (anchor_raw_data, anchor_sig) = service_feature_extraction::get_service_raw_data_and_signature(&conn, anchor_service_id).await
                .context(format!("Failed to get raw_data/signature for anchor service {}", anchor_service_id.0))?;
            let (candidate_raw_data, candidate_sig) = service_feature_extraction::get_service_raw_data_and_signature(&conn, &candidate_service_id).await
                .context(format!("Failed to get raw_data/signature for candidate service {}", candidate_service_id.0))?;

            let method_type_str = MatchMethodType::ServiceEmbeddingSimilarity.as_str();

            if let Some(cached_entry) = db::get_service_comparison_cache_entry(
                pool,
                &anchor_service_id.0,
                &candidate_service_id.0,
                &anchor_sig,
                &candidate_sig,
                method_type_str,
            ).await.context("Service comparison cache check failed")? {
                info!("Cache HIT for ({}, {}) method {}. Result: {}", anchor_service_id.0, candidate_service_id.0, method_type_str, cached_entry.comparison_result);
                if cached_entry.comparison_result == "MATCH" {
                    // If it was a match, the group should already exist or be updated by insert_service_group_tx if called.
                    // We can count it if we want, but the main goal is to avoid re-processing.
                    // For stats, we might need to consider how to count cached matches if they contribute to groups_created.
                    // For now, let's assume insert_service_group_tx handles re-confirming confidence.
                }
                continue; // Skip to next candidate if valid cache entry found
            }
            info!("Cache MISS for ({}, {}) method {}. Proceeding with full comparison.", anchor_service_id.0, candidate_service_id.0, method_type_str);


            // --- Full Comparison Logic (Cache Miss) ---
            let pre_rl_confidence = raw_similarity; // Similarity from ANN is the pre-RL score

            let features = if let Some(ref cache_arc) = feature_cache { // Borrow feature_cache
                let mut cache_guard = cache_arc.lock().await;
                cache_guard.get_pair_features(pool, anchor_service_id, &candidate_service_id).await?
            } else if let Some(ref orch_arc) = service_orchestrator_option { // Borrow service_orchestrator_option
                let orch = orch_arc.lock().await; // Lock the orchestrator
                 orch.get_pair_features(pool, anchor_service_id, &candidate_service_id).await?
            }
            else {
                 // Fallback to direct extraction if neither cache nor orchestrator with cache is available
                ServiceMatchingOrchestrator::extract_pair_context_features(pool, anchor_service_id, &candidate_service_id).await?
            };


            let (tuned_confidence, tuner_version) = if let Some(ref orch_arc) = service_orchestrator_option {
                let orchestrator = orch_arc.lock().await;
                let conf = orchestrator.get_tuned_confidence(
                    &MatchMethodType::ServiceEmbeddingSimilarity,
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
                let match_values = ServiceEmbeddingMatchValue {
                    name1: anchor_service_name.clone(),
                    name2: candidate_service_name.clone(),
                    embedding_similarity: raw_similarity, // Store the raw similarity
                };
                let service_group_id_str = Uuid::new_v4().to_string();

                // Use a separate connection for the transaction to avoid deadlocks if `conn` is from a limited pool part.
                let mut tx_conn = pool.get().await.context("Failed to get DB connection for transaction")?;
                let mut transaction = tx_conn.transaction().await.context("Failed to start transaction")?;

                let tx_result: Result<()> = async {
                    insert_service_group_tx(
                        &mut transaction,
                        &service_group_id_str,
                        anchor_service_id,
                        &candidate_service_id,
                        tuned_confidence,
                        pre_rl_confidence,
                        MatchMethodType::ServiceEmbeddingSimilarity,
                        MatchValues::ServiceEmbedding(match_values),
                    ).await?;

                    insert_service_match_decision_detail(
                        &mut transaction,
                        &service_group_id_str,
                        pipeline_run_id,
                        serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                        MatchMethodType::ServiceEmbeddingSimilarity.as_str(),
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
                        debug!(
                            "Cluster_id: {}. Committed embedding group & decision. Anchor: {}, Candidate: {}, Sim: {:.3}, TunedConf: {:.3}",
                            cluster_id, anchor_service_id.0, candidate_service_id.0, raw_similarity, tuned_confidence
                        );
                    }
                    Err(e) => {
                        transaction.rollback().await.context("Failed to rollback transaction")?;
                        warn!(
                            "Transaction failed for embedding match ({} vs {}): {:?}. Rolled back.",
                             anchor_service_id.0, candidate_service_id.0, e
                        );
                        comparison_cache_result_val = "ERROR_IN_PROCESSING"; // Or keep NO_MATCH
                    }
                }
            }

            // --- Store result in Service Comparison Cache ---
            let new_cache_entry = NewServiceComparisonCacheEntry {
                service_id_1: &anchor_service_id.0,
                service_id_2: &candidate_service_id.0,
                signature_1: &anchor_sig,
                signature_2: &candidate_sig,
                method_type: method_type_str,
                pipeline_run_id: Some(pipeline_run_id),
                comparison_result: comparison_cache_result_val,
                confidence_score: Some(tuned_confidence), // Store tuned confidence
                snapshotted_features: Some(serde_json::to_value(&features).unwrap_or(serde_json::Value::Null)),
            };
            if let Err(e) = db::insert_service_comparison_cache_entry(pool, new_cache_entry).await {
                warn!("Failed to insert into service comparison cache for ({}, {}): {}", anchor_service_id.0, candidate_service_id.0, e);
            }

        }
    }

    let avg_confidence = if groups_created_count > 0 {
        total_confidence_sum / groups_created_count as f64
    } else {
        0.0
    };

    info!(
        "Service embedding matching (ANN) for cluster_id: {} complete. Created {} groups with avg_confidence: {:.3}",
        cluster_id, groups_created_count, avg_confidence
    );

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::ServiceEmbeddingSimilarity,
        groups_created: groups_created_count,
        entities_matched: processed_service_ids.len(), // Count of distinct services involved
        avg_confidence,
        avg_group_size: if groups_created_count > 0 { 2.0 } else { 0.0 }, // Assuming pairs for now
    };

    Ok(ServiceMatchResult {
        groups_created: groups_created_count,
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
