// src/matching/embedding.rs
use crate::matching::name::calculate_string_similarity_enhanced;
use crate::models::matching::{MatchMethodType, MatchValues, ServiceEmbeddingMatchValue, ServiceInfo, ServiceMatch};
use crate::models::stats_models::ServiceEmbeddingMatchingStats;
use crate::rl::{ServiceRLOrchestrator, SharedServiceFeatureCache};
use crate::utils::candle::cosine_similarity_candle;
use crate::utils::db_connect::PgPool;
use anyhow::Result;
use arroy::distances::Euclidean;
use arroy::{Database as ArroyDatabase, Reader, Writer};
use heed::EnvOpenOptions;
use indicatif::ProgressBar;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use rand::rngs::StdRng;
use rand::SeedableRng;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Mutex;

const LARGE_CLUSTER_THRESHOLD_EMB: usize = 2000;
const SERVICE_EMBEDDING_SIMILARITY_THRESHOLD: f64 = 0.80;

// Arroy ANN constants
const ARROY_DB_MAP_SIZE_MIB: usize = 2048; // 2 GiB for the LMDB map size
const K_NEAREST_NEIGHBORS: usize = 50;
const NUM_ARROY_TREES: NonZeroUsize = match NonZeroUsize::new(10) {
    Some(n) => n,
    None => panic!("Cannot create NonZeroUsize with 0"),
};
const ARROY_SEARCH_MULTIPLIER: NonZeroUsize = match NonZeroUsize::new(15) {
    Some(n) => n,
    None => panic!("Cannot create NonZeroUsize with 0"),
};

// ANN cache with TTL
static ANN_CACHE: Lazy<Mutex<HashMap<String, (Instant, Arc<ArroyDatabase<Euclidean>>)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
const CACHE_TTL: Duration = Duration::from_secs(3600); // 1 hour

/// Runs enhanced embedding matching for services with conditional RL integration.
pub async fn run_enhanced_embedding_matching(
    pool: &PgPool,
    services: &[ServiceInfo],
    service_rl_orchestrator: Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    service_feature_cache: Option<SharedServiceFeatureCache>,
    method_pb: Option<ProgressBar>,
) -> (Vec<ServiceMatch>, Vec<(String, String, Vec<f64>)>, ServiceEmbeddingMatchingStats) {
    let start_time = Instant::now();
    let mut stats = ServiceEmbeddingMatchingStats::default();

    let rl_enabled = service_rl_orchestrator.is_some();
    debug!(
        "Starting enhanced embedding matching for {} services (RL: {})",
        services.len(),
        if rl_enabled { "enabled" } else { "disabled" }
    );

    let mut matches = Vec::new();
    let mut features_for_rl_logging: Vec<(String, String, Vec<f64>)> = Vec::new();

    if services.len() > LARGE_CLUSTER_THRESHOLD_EMB {
        info!("Running embedding matching in LARGE CLUSTER (ANN) mode ({} services, RL: {})", services.len(), if rl_enabled { "enabled" } else { "disabled" });
        stats.large_cluster_mode = true;

        // Use ANN for large clusters
        let (ann_matches, ann_features, ann_stats) = run_ann_embedding_matching(
            pool,
            services,
            service_rl_orchestrator,
            service_feature_cache,
            method_pb.clone(),
        )
        .await;

        matches.extend(ann_matches);
        features_for_rl_logging.extend(ann_features);
        stats.ann_queries_run = ann_stats.ann_queries_run;
        stats.ann_candidates_processed = ann_stats.ann_candidates_processed;
        stats.ann_index_build_time = ann_stats.ann_index_build_time;
        stats.matches_found = matches.len();
        stats.avg_similarity = if !matches.is_empty() {
            matches.iter().map(|m| m.confidence_score).sum::<f64>() / matches.len() as f64
        } else {
            0.0
        };

        if let Some(pb) = method_pb {
            pb.finish_and_clear();
        }
        stats.processing_time = start_time.elapsed();
        return (matches, features_for_rl_logging, stats);
    } else {
        // Original O(N^2) logic for smaller clusters
        if let Some(pb) = &method_pb {
            pb.set_length(services.len() as u64 * services.len() as u64 / 2);
            pb.set_message(format!("Embedding Matching: Comparing all pairs (RL: {})...", if rl_enabled { "on" } else { "off" }));
            pb.set_position(0);
        }

        let mut pair_count_for_pb = 0;
        let mut similarities = Vec::new();
        for i in 0..services.len() {
            for j in (i + 1)..services.len() {
                // Get a fresh connection for this pair
                let conn = match pool.get().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        warn!("Failed to get connection for embedding pair ({}, {}): {}",
                              services[i].service_id, services[j].service_id, e);
                        continue;
                    }
                };

                if let Some((embedding_match, features_opt)) = match_services_by_embedding_enhanced(
                    &*conn,
                    &services[i],
                    &services[j],
                    &service_rl_orchestrator,
                    &service_feature_cache,
                    &mut stats,
                )
                .await
                {
                    matches.push(embedding_match.clone());
                    similarities.push(embedding_match.confidence_score);
                    if let Some(features) = features_opt {
                        let (s1_id, s2_id) = if services[i].service_id <= services[j].service_id {
                            (services[i].service_id.clone(), services[j].service_id.clone())
                        } else {
                            (services[j].service_id.clone(), services[i].service_id.clone())
                        };
                        features_for_rl_logging.push((s1_id, s2_id, features));
                    }
                }

                // Connection is automatically dropped here

                if let Some(pb) = &method_pb {
                    pair_count_for_pb += 1;
                    pb.set_position(pair_count_for_pb);
                }
            }
        }
        stats.matches_found = matches.len();
        stats.avg_similarity = if !similarities.is_empty() {
            similarities.iter().sum::<f64>() / similarities.len() as f64
        } else {
            0.0
        };
        stats.processing_time = start_time.elapsed();

        info!(
            "Enhanced embedding matching completed: {} matches from {} pairs in {:.2?} (RL: {})",
            stats.matches_found,
            stats.pairs_compared,
            stats.processing_time,
            if rl_enabled { "enabled" } else { "disabled" }
        );

        if let Some(pb) = method_pb {
            pb.finish_and_clear();
        }

        (matches, features_for_rl_logging, stats)
    }
}

/// Runs embedding matching using Approximate Nearest Neighbor (ANN) search for large clusters.
async fn run_ann_embedding_matching(
    pool: &PgPool,
    services: &[ServiceInfo],
    service_rl_orchestrator: Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    service_feature_cache: Option<SharedServiceFeatureCache>,
    method_pb: Option<ProgressBar>,
) -> (Vec<ServiceMatch>, Vec<(String, String, Vec<f64>)>, ServiceEmbeddingMatchingStats) {
    let mut stats = ServiceEmbeddingMatchingStats::default();
    let mut matches = Vec::new();
    let mut features_for_rl_logging: Vec<(String, String, Vec<f64>)> = Vec::new();

    let ann_start_time = Instant::now();

    // Create cache key from sorted service IDs
    let mut service_ids: Vec<String> = services.iter()
        .filter_map(|s| s.embedding.as_ref().map(|_| s.service_id.clone()))
        .collect();
    service_ids.sort();
    let cache_key = format!("{:x}", md5::compute(service_ids.join(",").as_bytes()));
    
    // Check cache
    let cached_index = {
        let mut cache = ANN_CACHE.lock().await;
        
        // Clean up expired entries
        cache.retain(|_, (time, _)| time.elapsed() < CACHE_TTL);
        
        if let Some((time, db)) = cache.get(&cache_key) {
            if time.elapsed() < CACHE_TTL {
                debug!("Using cached ANN index for key {}", cache_key);
                Some(db.clone())
            } else {
                None
            }
        } else {
            None
        }
    };

    let (env, db_ann) = if let Some(cached_db) = cached_index {
        // Use cached index - create a new environment for querying
        let temp_dir = match TempDir::new() {
            Ok(dir) => dir,
            Err(e) => {
                error!("Failed to create temporary directory for cached arroy: {}", e);
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
        };
        
        let env = Arc::new(match unsafe { EnvOpenOptions::new()
            .map_size(ARROY_DB_MAP_SIZE_MIB * 1024 * 1024)
            .open(temp_dir.path()) }
        {
            Ok(env) => env,
            Err(e) => {
                error!("Failed to open heed environment for cached arroy: {}", e);
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
        });
        
        (env, *cached_db)
    } else {
        // Build new index
        let build_start_time = Instant::now();
        
        // Create a temporary directory for the LMDB environment
        let temp_dir = match TempDir::new() {
            Ok(dir) => dir,
            Err(e) => {
                error!("Failed to create temporary directory for arroy: {}", e);
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
        };

        let env = Arc::new(match unsafe { EnvOpenOptions::new()
            .map_size(ARROY_DB_MAP_SIZE_MIB * 1024 * 1024)
            .open(temp_dir.path()) }
        {
            Ok(env) => env,
            Err(e) => {
                error!("Failed to open heed environment for arroy: {}", e);
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
        });

        let (dimension, _service_id_to_idx, _idx_to_service_info) = {
            let mut first_embedding_dim = 0;
            let mut service_id_to_idx_map = HashMap::new();
            let mut idx_to_service_info_map = HashMap::new();

            for (idx, service) in services.iter().enumerate() {
                if let Some(embedding) = &service.embedding {
                    if first_embedding_dim == 0 {
                        first_embedding_dim = embedding.len();
                    }
                    service_id_to_idx_map.insert(service.service_id.clone(), idx);
                    idx_to_service_info_map.insert(idx, service);
                } else {
                    warn!("Service {} has no embedding, skipping in ANN index.", service.service_id);
                }
            }
            
            if first_embedding_dim == 0 {
                error!("No services with embeddings found for ANN indexing.");
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
            (first_embedding_dim, service_id_to_idx_map, idx_to_service_info_map)
        };

        // Build the ANN index
        let mut wtxn = match env.write_txn() {
            Ok(txn) => txn,
            Err(e) => {
                error!("Failed to start write transaction for arroy: {}", e);
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
        };
        
        let db_ann: ArroyDatabase<Euclidean> = match env.create_database(&mut wtxn, None) {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to create arroy database: {}", e);
                stats.processing_time = ann_start_time.elapsed();
                return (Vec::new(), Vec::new(), stats);
            }
        };
        
        let writer = Writer::<Euclidean>::new(db_ann, 0, dimension);

        if let Some(pb) = &method_pb {
            pb.set_length(services.len() as u64 * 2);
            pb.set_message("Embedding Matching (ANN): Building index...");
            pb.set_position(0);
        }

        let mut added_embeddings = 0;
        for (idx, service) in services.iter().enumerate() {
            if let Some(embedding) = &service.embedding {
                if let Err(e) = writer.add_item(&mut wtxn, idx as u32, embedding) {
                    error!("Failed to add item to arroy index: {}", e);
                } else {
                    added_embeddings += 1;
                }
            }
            if let Some(pb) = &method_pb { pb.inc(1); }
        }

        if added_embeddings == 0 {
            error!("No embeddings were added to the ANN index.");
            stats.processing_time = ann_start_time.elapsed();
            return (Vec::new(), Vec::new(), stats);
        }
        
        let mut rng = StdRng::seed_from_u64(42);
        if let Err(e) = writer.builder(&mut rng).n_trees(NUM_ARROY_TREES.get()).build(&mut wtxn) {
            error!("Failed to build arroy index: {}", e);
            stats.processing_time = ann_start_time.elapsed();
            return (Vec::new(), Vec::new(), stats);
        }

        if let Err(e) = wtxn.commit() {
            error!("Failed to commit arroy index transaction: {}", e);
            stats.processing_time = ann_start_time.elapsed();
            return (Vec::new(), Vec::new(), stats);
        }
        
        stats.ann_index_build_time = build_start_time.elapsed();
        info!("Arroy index built with {} items in {:.2?}", added_embeddings, stats.ann_index_build_time);
        
        // Cache the new index
        {
            let mut cache = ANN_CACHE.lock().await;
            cache.insert(cache_key.clone(), (Instant::now(), Arc::new(db_ann)));
            debug!("Cached ANN index with key {}", cache_key);
        }
        
        (env, db_ann)
    };

    // Create service lookup maps
    let (_service_id_to_idx, idx_to_service_info) = {
        let mut service_id_to_idx_map = HashMap::new();
        let mut idx_to_service_info_map = HashMap::new();

        for (idx, service) in services.iter().enumerate() {
            if service.embedding.is_some() {
                service_id_to_idx_map.insert(service.service_id.clone(), idx);
                idx_to_service_info_map.insert(idx, service);
            }
        }
        (service_id_to_idx_map, idx_to_service_info_map)
    };

    // Query the ANN index
    let query_start_time = Instant::now();
    
    if let Some(pb) = &method_pb {
        pb.set_message("Embedding Matching (ANN): Querying for nearest neighbors...");
        pb.set_length(services.len() as u64);
        pb.set_position(0);
    }

    // Use a HashSet to store processed pairs
    let mut processed_pairs: HashSet<(String, String)> = HashSet::new();

    for (query_idx, query_service) in services.iter().enumerate() {
        if query_service.embedding.is_none() {
            if let Some(pb) = &method_pb { pb.inc(1); }
            continue;
        }

        stats.ann_queries_run += 1;
        
        let current_env_arc = Arc::clone(&env);
        let current_db_ann = db_ann;
        let current_query_embedding = query_service.embedding.clone();

        let neighbors_result = tokio::task::spawn_blocking(move || {
            let query_embedding_unwrapped = match current_query_embedding {
                Some(emb) => emb,
                None => {
                    return Err(anyhow::anyhow!("Query service has no embedding in blocking task"));
                }
            };

            let rtxn_for_query = match current_env_arc.read_txn() {
                Ok(txn) => txn,
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to start read transaction in blocking task for arroy query: {}", e));
                }
            };

            let reader_for_query = match Reader::<Euclidean>::open(&rtxn_for_query, 0, current_db_ann) {
                Ok(reader) => reader,
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to open arroy reader in blocking task for query: {}", e));
                }
            };

            let mut query_builder = reader_for_query.nns(K_NEAREST_NEIGHBORS);
            if let Some(search_k) = NonZeroUsize::new(K_NEAREST_NEIGHBORS * NUM_ARROY_TREES.get() * ARROY_SEARCH_MULTIPLIER.get()) {
                query_builder.search_k(search_k);
            }
            
            query_builder.by_vector(&rtxn_for_query, &query_embedding_unwrapped)
                .map_err(|e| anyhow::anyhow!("Failed to query arroy in blocking task: {}", e))
        }).await;

        match neighbors_result {
            Ok(Ok(neighbors)) => {
                for (neighbor_idx_u32, _distance) in neighbors {
                    let neighbor_idx = neighbor_idx_u32 as usize;
                    if query_idx == neighbor_idx { continue; }

                    let service1 = if query_idx < neighbor_idx { query_service } else { idx_to_service_info[&neighbor_idx] };
                    let service2 = if query_idx < neighbor_idx { idx_to_service_info[&neighbor_idx] } else { query_service };

                    let pair_key = if service1.service_id < service2.service_id {
                        (service1.service_id.clone(), service2.service_id.clone())
                    } else {
                        (service2.service_id.clone(), service1.service_id.clone())
                    };

                    // Only process if this pair hasn't been processed yet
                    if processed_pairs.insert(pair_key) {
                        stats.ann_candidates_processed += 1;
                        
                        // Get a fresh connection for this pair
                        let conn = match pool.get().await {
                            Ok(conn) => conn,
                            Err(e) => {
                                warn!("Failed to get connection for ANN pair ({}, {}): {}", 
                                      service1.service_id, service2.service_id, e);
                                continue;
                            }
                        };
                        
                        if let Some((embedding_match, features_opt)) = match_services_by_embedding_enhanced(
                            &*conn,
                            service1,
                            service2,
                            &service_rl_orchestrator,
                            &service_feature_cache,
                            &mut stats,
                        ).await {
                            matches.push(embedding_match);
                            if let Some(features) = features_opt {
                                let (s1_id, s2_id) = if service1.service_id <= service2.service_id {
                                    (service1.service_id.clone(), service2.service_id.clone())
                                } else {
                                    (service2.service_id.clone(), service1.service_id.clone())
                                };
                                features_for_rl_logging.push((s1_id, s2_id, features));
                            }
                        }
                    }
                }
            },
            Ok(Err(e)) => error!("Failed to query arroy in blocking task for service {}: {}", query_service.service_id, e),
            Err(join_err) => error!("Blocking task for arroy query failed with JoinError for service {}: {}", query_service.service_id, join_err),
        }
        if let Some(pb) = &method_pb { pb.inc(1); }
    }
    stats.processing_time = ann_start_time.elapsed();
    info!(
        "ANN embedding matching completed: {} matches found from {} queries ({:.2?} for queries). Total ANN time: {:.2?}",
        matches.len(),
        stats.ann_queries_run,
        query_start_time.elapsed(),
        stats.processing_time
    );

    (matches, features_for_rl_logging, stats)
}

/// Enhanced embedding similarity matching with better context and conditional RL integration.
async fn match_services_by_embedding_enhanced(
    conn: &tokio_postgres::Client,
    service1: &ServiceInfo,
    service2: &ServiceInfo,
    service_rl_orchestrator: &Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    service_feature_cache: &Option<SharedServiceFeatureCache>,
    stats: &mut ServiceEmbeddingMatchingStats,
) -> Option<(ServiceMatch, Option<Vec<f64>>)> {
    if let (Some(emb1), Some(emb2)) = (&service1.embedding, &service2.embedding) {
        if emb1.is_empty() || emb2.is_empty() {
            debug!("Skipping embedding match for empty embeddings: {} or {}", service1.service_id, service2.service_id);
            return None;
        }

        // Note: The stats.pairs_compared is now handled in the calling functions (O(N^2) or ANN)
        // to avoid double counting.

        if let Ok(similarity) = cosine_similarity_candle(emb1, emb2) {
            if similarity >= SERVICE_EMBEDDING_SIMILARITY_THRESHOLD {
                let name_boost = if let (Some(name1), Some(name2)) = (&service1.name, &service2.name) {
                    let name_sim = calculate_string_similarity_enhanced(name1, name2);
                    if name_sim > 0.7 {
                        stats.contextual_boosts += 1;
                        0.05
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };

                let pre_rl_confidence = (similarity + name_boost).min(0.98);
                let mut captured_features: Option<Vec<f64>> = None;
                let rl_enabled = service_rl_orchestrator.is_some();

                // MODIFIED: Conditional RL Integration
                let final_confidence = if rl_enabled {
                    if let Some(rl_orchestrator_arc) = service_rl_orchestrator {
                        // Extract features (always useful for caching)
                        let context_features_res = if let Some(cache_arc) = service_feature_cache {
                            let mut cache_guard = cache_arc.lock().await;
                            cache_guard.get_pair_features(conn, &service1.service_id, &service2.service_id).await
                        } else {
                            crate::rl::service_feature_extraction::extract_service_context_for_pair(
                                conn, &service1.service_id, &service2.service_id
                            ).await
                        };

                        if let Ok(features) = context_features_res {
                            captured_features = Some(features.clone());
                            let rl_guard = rl_orchestrator_arc.lock().await;
                            match rl_guard.get_tuned_confidence(
                                &MatchMethodType::ServiceEmbeddingSimilarity,
                                pre_rl_confidence,
                                &features,
                            ) {
                                Ok(tuned_conf) => {
                                    debug!("RL tuned confidence for embedding match ({}, {}): {:.3} -> {:.3}",
                                           service1.service_id, service2.service_id, pre_rl_confidence, tuned_conf);
                                    tuned_conf
                                }
                                Err(e) => {
                                    warn!("Failed to get tuned confidence: {}. Using pre-RL confidence.", e);
                                    pre_rl_confidence
                                }
                            }
                        } else {
                            warn!("Failed to extract features for service pair ({}, {}): {}. Using pre-RL confidence.",
                                  service1.service_id, service2.service_id, context_features_res.unwrap_err());
                            pre_rl_confidence
                        }
                    } else {
                        // This shouldn't happen if rl_enabled is true, but handle gracefully
                        warn!("RL enabled but no orchestrator available, using pre-RL confidence");
                        pre_rl_confidence
                    }
                } else {
                    // RL disabled - still extract features for caching if cache is available
                    if let Some(cache_arc) = service_feature_cache {
                        let context_features_res = {
                            let mut cache_guard = cache_arc.lock().await;
                            cache_guard.get_pair_features(conn, &service1.service_id, &service2.service_id).await
                        };

                        if let Ok(features) = context_features_res {
                            captured_features = Some(features);
                            debug!("Extracted features for caching (RL disabled) for pair ({}, {})",
                                   service1.service_id, service2.service_id);
                        }
                    }
                    pre_rl_confidence
                };

                debug!(
                    "Embedding match: {} <-> {} (similarity: {:.3}, pre-RL: {:.3}, final: {:.3}, RL: {})",
                    service1.service_id, service2.service_id, similarity, pre_rl_confidence, final_confidence,
                    if rl_enabled { "enabled" } else { "disabled" }
                );

                return Some((ServiceMatch {
                    service_id_1: service1.service_id.clone(),
                    service_id_2: service2.service_id.clone(),
                    match_method: MatchMethodType::ServiceEmbeddingSimilarity,
                    confidence_score: final_confidence,
                    match_values: MatchValues::ServiceEmbedding(ServiceEmbeddingMatchValue {
                        name1: service1.name.clone().unwrap_or_default(),
                        name2: service2.name.clone().unwrap_or_default(),
                        embedding_similarity: similarity,
                    }),
                }, captured_features));
            }
        }
    }
    None
}