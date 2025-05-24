// src/service_matching/embedding.rs

use anyhow::{Context, Result};
use log::{debug, info, warn};
use pgvector::Vector as PgVector;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid; // For handling pgvector types

use crate::db::{
    self, // Now using db::upsert_service_group and db::insert_service_match_decision_detail_direct
    NewServiceComparisonCacheEntry,
    PgPool,
};
use crate::models::{MatchMethodType, MatchValues, ServiceEmbeddingMatchValue, ServiceId};
use crate::reinforcement::service::service_feature_cache_service::SharedServiceFeatureCache;
// For get_service_raw_data_and_signature (though not directly used here, signature::get_or_calculate_signature is)
use crate::reinforcement::service::service_feature_extraction;
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;
use crate::results::{MatchMethodStats, ServiceMatchResult};

const ANN_SIMILARITY_THRESHOLD: f64 = 0.8;
const ANN_TOP_K: i64 = 10;
const RL_CONFIDENCE_THRESHOLD: f64 = 0.85;

#[derive(Debug)]
struct CandidateService {
    id: ServiceId,
    name: String,
    similarity: f64,
}

pub async fn find_matches_in_cluster(
    pool: &PgPool, // Pass the pool directly
    service_orchestrator_option: Option<Arc<Mutex<ServiceMatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedServiceFeatureCache>,
    services_in_cluster: Vec<(ServiceId, String, Option<Vec<f32>>)>,
    cluster_id: String,
) -> Result<ServiceMatchResult> {
    info!(
        "Starting service embedding matching for cluster_id: {} with {} services using ANN (direct DB calls)...",
        cluster_id,
        services_in_cluster.len()
    );

    if services_in_cluster.is_empty() {
        debug!(
            "No services for embedding matching in cluster_id: {}.",
            cluster_id
        );
        return Ok(create_empty_result_for_cluster(
            MatchMethodType::ServiceEmbeddingSimilarity,
        ));
    }

    let mut groups_created_count = 0;
    let mut total_confidence_sum = 0.0;
    let mut processed_service_ids = HashSet::new();

    // Get a connection for the ANN query loop
    // This connection will be held for the duration of iterating through anchor services.
    // Consider if this is too long or if connections should be acquired per anchor if contention is an issue.
    let conn_for_ann = pool
        .get()
        .await
        .context("Failed to get DB connection for ANN query loop")?;

    for (anchor_service_id, anchor_service_name, anchor_embedding_opt) in &services_in_cluster {
        processed_service_ids.insert(anchor_service_id.0.clone());

        if anchor_embedding_opt.is_none() {
            debug!(
                "Anchor service {} has no embedding, skipping ANN.",
                anchor_service_id.0
            );
            continue;
        }
        let anchor_embedding_vec = anchor_embedding_opt.as_ref().unwrap();
        let anchor_pg_embedding = PgVector::from(anchor_embedding_vec.clone());

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
                  AND s.id != $2
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

        let candidate_rows = conn_for_ann // Use the acquired connection
            .query(
                ann_query_sql,
                &[
                    &cluster_id,
                    &anchor_service_id.0,
                    &anchor_pg_embedding,
                    &ANN_SIMILARITY_THRESHOLD,
                    &ANN_TOP_K,
                ],
            )
            .await
            .context(format!(
                "ANN query failed for anchor service {}",
                anchor_service_id.0
            ))?;

        if candidate_rows.is_empty() {
            debug!(
                "No ANN candidates found for anchor service {}",
                anchor_service_id.0
            );
            continue;
        }
        info!(
            "Found {} ANN candidates for anchor service {}",
            candidate_rows.len(),
            anchor_service_id.0
        );

        for candidate_row in candidate_rows {
            let candidate_service_id_str: String = candidate_row.get("id");
            let candidate_service_name: String = candidate_row.get("name");
            let raw_similarity: f64 = candidate_row.get("similarity");
            let candidate_service_id = ServiceId(candidate_service_id_str);
            processed_service_ids.insert(candidate_service_id.0.clone());

            // Signatures are fetched using the pool, managing their own connections
            let anchor_sig =
                match crate::signature::get_or_calculate_signature(pool, anchor_service_id).await {
                    Ok(sig) => sig,
                    Err(e) => {
                        warn!(
                            "Could not get/calc anchor sig {}: {}. Skipping pair.",
                            anchor_service_id.0, e
                        );
                        continue;
                    }
                };
            let candidate_sig =
                match crate::signature::get_or_calculate_signature(pool, &candidate_service_id)
                    .await
                {
                    Ok(sig) => sig,
                    Err(e) => {
                        warn!(
                            "Could not get/calc candidate sig {}: {}. Skipping pair.",
                            candidate_service_id.0, e
                        );
                        continue;
                    }
                };

            let method_type_str = MatchMethodType::ServiceEmbeddingSimilarity.as_str();

            if let Some(cached_entry) = db::get_service_comparison_cache_entry(
                pool, // Pass pool
                &anchor_service_id.0,
                &candidate_service_id.0,
                &anchor_sig,
                &candidate_sig,
                method_type_str,
            )
            .await
            .context("Service comparison cache check failed for embedding")?
            {
                info!(
                    "Cache HIT for EMBEDDING ({}, {}) method {}. Result: {}",
                    anchor_service_id.0,
                    candidate_service_id.0,
                    method_type_str,
                    cached_entry.comparison_result
                );
                continue;
            }
            info!(
                "Cache MISS for EMBEDDING ({}, {}) method {}. Proceeding.",
                anchor_service_id.0, candidate_service_id.0, method_type_str
            );

            let pre_rl_confidence = raw_similarity;

            let features = if let Some(ref cache_arc) = feature_cache {
                let mut cache_guard = cache_arc.lock().await;
                cache_guard
                    .get_pair_features(pool, anchor_service_id, &candidate_service_id)
                    .await?
            } else if let Some(ref orch_arc) = service_orchestrator_option {
                let orch = orch_arc.lock().await;
                orch.get_pair_features(pool, anchor_service_id, &candidate_service_id)
                    .await?
            } else {
                ServiceMatchingOrchestrator::extract_pair_context_features(
                    pool,
                    anchor_service_id,
                    &candidate_service_id,
                )
                .await?
            };

            let (tuned_confidence, tuner_version) =
                if let Some(ref orch_arc) = service_orchestrator_option {
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
                let match_values = ServiceEmbeddingMatchValue {
                    name1: anchor_service_name.clone(),
                    name2: candidate_service_name.clone(),
                    embedding_similarity: raw_similarity,
                };
                let proposed_service_group_id_str = Uuid::new_v4().to_string();

                // --- Refactored DB Insert Logic (No Transaction) ---
                match db::upsert_service_group(
                    pool,
                    &proposed_service_group_id_str,
                    anchor_service_id,
                    &candidate_service_id,
                    tuned_confidence,
                    pre_rl_confidence,
                    MatchMethodType::ServiceEmbeddingSimilarity,
                    MatchValues::ServiceEmbedding(match_values),
                )
                .await
                {
                    Ok(actual_service_group_id) => {
                        match db::insert_service_match_decision_detail_direct(
                            pool,
                            &actual_service_group_id,
                            pipeline_run_id,
                            serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                            MatchMethodType::ServiceEmbeddingSimilarity.as_str(),
                            pre_rl_confidence,
                            tuned_confidence,
                            tuner_version.map(|v| v as i32),
                        )
                        .await {
                            Ok(_) => {
                                debug!(
                                    "Cluster_id: {}. Committed embedding group and detail. Anchor: {}, Candidate: {}, Sim: {:.3}, TunedConf: {:.3}",
                                    cluster_id, anchor_service_id.0, candidate_service_id.0, raw_similarity, tuned_confidence
                                );
                                groups_created_count += 1;
                                total_confidence_sum += tuned_confidence;
                                comparison_cache_result_val = "MATCH";
                            }
                            Err(detail_err) => {
                                warn!(
                                    "Upserted service_group {} for embedding match but FAILED to insert decision detail: {:?}. Match recorded, RL context lost.",
                                    actual_service_group_id, detail_err
                                );
                                comparison_cache_result_val = "MATCH_NO_DETAIL";
                            }
                        }
                    }
                    Err(group_err) => {
                        warn!(
                            "FAILED to upsert service_group for embedding match ({} vs {}): {:?}. No decision detail recorded.",
                            anchor_service_id.0, candidate_service_id.0, group_err
                        );
                        comparison_cache_result_val = "ERROR_IN_PROCESSING";
                    }
                }
                // --- End Refactored DB Insert Logic ---
            }

            if comparison_cache_result_val != "ERROR_IN_PROCESSING" {
                let new_cache_entry = NewServiceComparisonCacheEntry {
                    service_id_1: &anchor_service_id.0,
                    service_id_2: &candidate_service_id.0,
                    signature_1: &anchor_sig,
                    signature_2: &candidate_sig,
                    method_type: method_type_str,
                    pipeline_run_id: Some(pipeline_run_id),
                    comparison_result: comparison_cache_result_val,
                    confidence_score: Some(tuned_confidence),
                    snapshotted_features: Some(
                        serde_json::to_value(&features).unwrap_or(serde_json::Value::Null),
                    ),
                };
                if let Err(e) = db::insert_service_comparison_cache_entry(pool, new_cache_entry).await {
                    warn!(
                        "Failed to insert into service comparison cache for EMBEDDING ({}, {}): {}",
                        anchor_service_id.0, candidate_service_id.0, e
                    );
                }
            }
        }
    }
    drop(conn_for_ann); // Release the connection used for ANN queries

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
