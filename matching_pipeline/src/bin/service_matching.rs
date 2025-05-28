// src/bin/service_matching.rs
use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use dedupe_lib::models::{EmailMatchValue, MatchValues, ServiceEmbeddingMatchValue, ServiceId, ServiceNameMatchValue, ServiceUrlMatchValue};
use dedupe_lib::service_matching::types::{ServiceInfo, ServiceMatch, ServiceEmailMatch, ServiceEmailMatchingStats, ServiceUrlMatch, NormalizedServiceUrl, ServiceDomainType};
use dedupe_lib::service_matching::{service_email, service_name, service_url};
use dedupe_lib::utils::cosine_similarity_candle;
use dedupe_lib::{db, EntityId, MatchMethodType, PgPool};
// Add import for the new batch upsert struct
use dedupe_lib::db::NewServiceGroup;
use futures::stream::{self, StreamExt};
use log::{debug, info, warn, error};
use serde_json::json;
use tokio::task::JoinHandle;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, Semaphore};
use tokio_postgres::GenericClient;
use uuid::Uuid;
use xgraph::Graph;
use xgraph::leiden_clustering::{CommunityConfig, CommunityDetection};
use indicatif::{ProgressBar, ProgressStyle};

// Configuration constants
const BATCH_SIZE: usize = 1000;
const MAX_CONCURRENT_CLUSTERS: usize = 10;
const SERVICE_EMBEDDING_SIMILARITY_THRESHOLD: f64 = 0.80;

// Large Cluster Thresholds
const LARGE_CLUSTER_THRESHOLD_URL: usize = 500;
const LARGE_CLUSTER_THRESHOLD_NAME: usize = 500;
const LARGE_CLUSTER_THRESHOLD_EMB: usize = 2000;

// NEW: Match aggregation constants
const MATCH_AGGREGATION_ENABLED: bool = true;
const CROSS_VALIDATION_ENABLED: bool = true;
const MIN_CONFIDENCE_FOR_AGGREGATION: f64 = 0.6;

// NEW: Match method weights for aggregation
const EMAIL_MATCH_WEIGHT: f64 = 0.4;
const URL_MATCH_WEIGHT: f64 = 0.3;
const NAME_MATCH_WEIGHT: f64 = 0.2;
const EMBEDDING_MATCH_WEIGHT: f64 = 0.1;

#[derive(Debug)]
pub struct ServiceCluster {
    cluster_id: Option<String>,
    entities: Vec<EntityId>,
    services: Vec<ServiceInfo>,
}

// NEW: Enhanced match data for aggregation
#[derive(Debug, Clone)]
pub struct EnhancedServiceMatch {
    pub base_match: ServiceMatch,
    pub method_matches: Vec<MethodMatch>, // All methods that found this pair
    pub cross_validation_score: f64, // Penalty/bonus from cross-validation
    pub final_confidence: f64, // Aggregated confidence
}

#[derive(Debug, Clone)]
pub struct MethodMatch {
    pub method: MatchMethodType,
    pub confidence: f64,
    pub match_values: MatchValues,
    pub validation_flags: ValidationFlags,
    pub service_id_1: ServiceId, // Added to track service IDs
    pub service_id_2: ServiceId, // Added to track service IDs
}

#[derive(Debug, Clone, Default)]
pub struct ValidationFlags {
    pub incompatible_types: bool,
    pub conflicting_locations: bool,
    pub generic_indicators: bool,
    pub suspicious_patterns: bool,
}

#[derive(Debug)]
pub struct ServiceMatchingStats {
    total_clusters_processed: usize,
    total_services_compared: usize,
    total_matches_found: usize,
    matches_by_method: HashMap<MatchMethodType, usize>,
    processing_time: std::time::Duration,
    email_stats: Option<ServiceEmailMatchingStats>,
    url_stats: UrlMatchingStats,
    name_stats: NameMatchingStats,
    embedding_stats: EmbeddingMatchingStats,
    // NEW: Aggregation stats
    aggregated_matches: usize,
    cross_validation_rejections: usize,
    confidence_boosts: usize,
}

#[derive(Debug, Default)]
pub struct UrlMatchingStats {
    pub matches_found: usize,
    pub domain_only_matches: usize,
    pub path_matches: usize,
    pub ignored_domains: usize,
    pub avg_confidence: f64,
    pub processing_time: std::time::Duration,
    pub pairs_compared: usize,
    pub large_cluster_mode: bool,
}

#[derive(Debug, Default)]
pub struct NameMatchingStats {
    pub matches_found: usize,
    pub fuzzy_matches: usize,
    pub token_matches: usize,
    pub same_type_matches: usize,
    pub incompatible_type_rejections: usize,
    pub avg_confidence: f64,
    pub processing_time: std::time::Duration,
    pub pairs_compared: usize,
    pub large_cluster_mode: bool,
}

#[derive(Debug, Default)]
pub struct EmbeddingMatchingStats {
    pub matches_found: usize,
    pub pairs_compared: usize,
    pub avg_similarity: f64,
    pub processing_time: std::time::Duration,
    pub skipped_large_cluster: bool,
}

impl Default for ServiceMatchingStats {
    fn default() -> Self {
        Self {
            total_clusters_processed: 0,
            total_services_compared: 0,
            total_matches_found: 0,
            matches_by_method: HashMap::new(),
            processing_time: std::time::Duration::default(),
            email_stats: None,
            url_stats: UrlMatchingStats::default(),
            name_stats: NameMatchingStats::default(),
            embedding_stats: EmbeddingMatchingStats::default(),
            aggregated_matches: 0,
            cross_validation_rejections: 0,
            confidence_boosts: 0,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting enhanced service matching binary with cross-validation...");

    // Load environment variables
    let env_paths = [".env", ".env.local", "../.env"];
    let mut loaded_env = false;
    for path in env_paths.iter() {
        if Path::new(path).exists() {
            if let Err(e) = db::load_env_from_file(path) {
                warn!("Failed to load environment from {}: {}", path, e);
            } else {
                info!("Loaded environment variables from {}", path);
                loaded_env = true;
                break;
            }
        }
    }
    if !loaded_env {
        info!("No .env file found, using environment variables from system");
    }

    // Generate pipeline run ID
    let pipeline_run_id = format!("service-matching-{}", Utc::now().format("%Y%m%d-%H%M%S"));
    let run_timestamp = Utc::now().naive_utc();

    info!("Starting enhanced service matching pipeline with run ID: {}", pipeline_run_id);
    let pipeline_start = Instant::now();

    // Connect to database
    let pool = db::connect().await.context("Failed to connect to database")?;

    // Create initial pipeline run record
    db::create_initial_pipeline_run(&pool, &pipeline_run_id, run_timestamp, Some("Enhanced service matching pipeline with cross-validation")).await?;

    let stats = ServiceMatchingStats::default();

    // Phase 1: Get entities organized by cluster
    info!("Phase 1: Organizing entities by cluster...");
    let mut entity_clusters = get_entity_clusters(&pool).await?;
    info!("Found {} clusters with entities", entity_clusters.len());

    // Phase 2: Handle unmatched entities
    info!("Phase 2: Adding unmatched entities as individual clusters...");
    let unmatched_count = add_unmatched_entities(&pool, &mut entity_clusters).await?;
    info!("Added {} unmatched entities as individual clusters", unmatched_count);

    // Phase 3: Load services for each cluster and run matching strategies
    info!("Phase 3: Loading services and running enhanced matching strategies...");
    let service_clusters = load_service_clusters(&pool, entity_clusters).await?;
    info!("Loaded {} service clusters. Starting matching...", service_clusters.len());

    let (all_matches, updated_stats) = run_enhanced_service_matching_pipeline(&pool, service_clusters, &pipeline_run_id, stats).await?;
    info!("Found {} total service matches.", all_matches.len());

    // Phase 4: Batch insert service groups
    info!("Phase 4: Inserting service groups...");
    insert_service_groups_batch(&pool, all_matches, &pipeline_run_id).await?;

    // Phase 5: Consolidate service clusters
    info!("Phase 5: Consolidating service clusters...");
    let clusters_created = consolidate_service_clusters(&pool, &pipeline_run_id).await?;
    info!("Created {} service group clusters", clusters_created);

    // Phase 6: Create visualization edges
    info!("Phase 6: Creating visualization edges...");
    let edges_created = create_service_visualization_edges(&pool, &pipeline_run_id).await?;
    info!("Created {} visualization edges", edges_created);

    // Finalize stats
    let mut final_stats = updated_stats;
    final_stats.processing_time = pipeline_start.elapsed();

    // Log comprehensive statistics
    log_enhanced_statistics(&final_stats, &pipeline_run_id);

    Ok(())
}

/// Enhanced service matching pipeline with sophisticated strategies and cross-validation
async fn run_enhanced_service_matching_pipeline(
    pool: &PgPool,
    service_clusters: Vec<ServiceCluster>,
    pipeline_run_id: &str,
    mut stats: ServiceMatchingStats,
) -> Result<(Vec<ServiceMatch>, ServiceMatchingStats)> {
    let stats_mutex = Arc::new(Mutex::new(ServiceMatchingStats::default()));
    let all_matches = Arc::new(Mutex::new(Vec::new()));

    // Create semaphore to limit concurrent cluster processing tasks
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CLUSTERS));

    let total_clusters = service_clusters.len() as u64;
    let pb = ProgressBar::new(total_clusters);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .context("Failed to set progress bar style")?
        .progress_chars("#>-"));
    pb.set_message("Processing clusters...");
    let pb_arc = Arc::new(pb);

    let mut handles = Vec::new();

    for cluster in service_clusters {
        let pool_clone = pool.clone();
        let pipeline_run_id_clone = pipeline_run_id.to_string();
        let stats_mutex_clone = Arc::clone(&stats_mutex);
        let all_matches_clone = Arc::clone(&all_matches);
        let semaphore_clone = Arc::clone(&semaphore);
        let pb_clone = Arc::clone(&pb_arc);

        let handle: JoinHandle<()> = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap_or_else(|e| {
                error!("Semaphore acquisition failed: {}", e);
                panic!("Semaphore closed");
            });

            match run_enhanced_service_matching_for_cluster(&pool_clone, cluster, &pipeline_run_id_clone).await {
                Ok((cluster_matches, cluster_stats)) => {
                    let mut stats_guard = stats_mutex_clone.lock().await;
                    stats_guard.total_clusters_processed += 1;
                    stats_guard.total_matches_found += cluster_matches.len();

                    // Aggregate enhanced stats
                    if let Some(email_stats) = cluster_stats.email_stats {
                        if stats_guard.email_stats.is_none() {
                            stats_guard.email_stats = Some(ServiceEmailMatchingStats::default());
                        }
                        if let Some(ref mut aggregated_email_stats) = stats_guard.email_stats {
                            aggregated_email_stats.services_processed += email_stats.services_processed;
                            aggregated_email_stats.pairs_compared += email_stats.pairs_compared;
                            aggregated_email_stats.matches_found += email_stats.matches_found;
                            aggregated_email_stats.generic_email_matches += email_stats.generic_email_matches;
                            aggregated_email_stats.high_frequency_matches += email_stats.high_frequency_matches;
                        }
                    }

                    stats_guard.url_stats.matches_found += cluster_stats.url_stats.matches_found;
                    stats_guard.url_stats.domain_only_matches += cluster_stats.url_stats.domain_only_matches;
                    stats_guard.url_stats.path_matches += cluster_stats.url_stats.path_matches;
                    stats_guard.url_stats.ignored_domains += cluster_stats.url_stats.ignored_domains;
                    stats_guard.url_stats.pairs_compared += cluster_stats.url_stats.pairs_compared;
                    if cluster_stats.url_stats.large_cluster_mode { stats_guard.url_stats.large_cluster_mode = true; }

                    stats_guard.name_stats.matches_found += cluster_stats.name_stats.matches_found;
                    stats_guard.name_stats.fuzzy_matches += cluster_stats.name_stats.fuzzy_matches;
                    stats_guard.name_stats.token_matches += cluster_stats.name_stats.token_matches;
                    stats_guard.name_stats.same_type_matches += cluster_stats.name_stats.same_type_matches;
                    stats_guard.name_stats.incompatible_type_rejections += cluster_stats.name_stats.incompatible_type_rejections;
                    stats_guard.name_stats.pairs_compared += cluster_stats.name_stats.pairs_compared;
                    if cluster_stats.name_stats.large_cluster_mode { stats_guard.name_stats.large_cluster_mode = true; }

                    stats_guard.embedding_stats.matches_found += cluster_stats.embedding_stats.matches_found;
                    stats_guard.embedding_stats.pairs_compared += cluster_stats.embedding_stats.pairs_compared;
                    if cluster_stats.embedding_stats.skipped_large_cluster { stats_guard.embedding_stats.skipped_large_cluster = true; }

                    // NEW: Add aggregation stats
                    stats_guard.aggregated_matches += cluster_stats.aggregated_matches;
                    stats_guard.cross_validation_rejections += cluster_stats.cross_validation_rejections;
                    stats_guard.confidence_boosts += cluster_stats.confidence_boosts;

                    // Update method-specific counts
                    for service_match in &cluster_matches {
                        *stats_guard.matches_by_method.entry(service_match.match_method.clone()).or_insert(0) += 1;
                    }
                    drop(stats_guard);

                    let mut matches_guard = all_matches_clone.lock().await;
                    matches_guard.extend(cluster_matches);
                }
                Err(e) => {
                    error!("Failed to process cluster: {}", e);
                    let mut stats_guard = stats_mutex_clone.lock().await;
                    stats_guard.total_clusters_processed += 1;
                }
            }
            pb_clone.inc(1);
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        if let Err(e) = handle.await {
            error!("Cluster processing task failed (JoinError): {}", e);
        }
    }

    pb_arc.finish_with_message("Cluster processing complete.");

    let final_matches = Arc::try_unwrap(all_matches)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap matches mutex"))?
        .into_inner();

    let final_stats_from_tasks = Arc::try_unwrap(stats_mutex)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap stats mutex"))?
        .into_inner();

    // Merge the collected stats from tasks with the initial stats
    stats.total_clusters_processed = final_stats_from_tasks.total_clusters_processed;
    stats.total_matches_found = final_stats_from_tasks.total_matches_found;
    stats.matches_by_method = final_stats_from_tasks.matches_by_method;
    stats.email_stats = final_stats_from_tasks.email_stats;
    stats.url_stats = final_stats_from_tasks.url_stats;
    stats.name_stats = final_stats_from_tasks.name_stats;
    stats.embedding_stats = final_stats_from_tasks.embedding_stats;
    stats.aggregated_matches = final_stats_from_tasks.aggregated_matches;
    stats.cross_validation_rejections = final_stats_from_tasks.cross_validation_rejections;
    stats.confidence_boosts = final_stats_from_tasks.confidence_boosts;

    Ok((final_matches, stats))
}

/// Run enhanced matching strategies for a single cluster with cross-validation
async fn run_enhanced_service_matching_for_cluster(
    pool: &PgPool,
    cluster: ServiceCluster,
    pipeline_run_id: &str,
) -> Result<(Vec<ServiceMatch>, ServiceMatchingStats)> {
    let cluster_start = Instant::now();
    let mut cluster_stats = ServiceMatchingStats::default();

    let service_count = cluster.services.len();
    let cluster_id_str = cluster.cluster_id.as_deref().unwrap_or("unknown").to_string();

    debug!(
        "Processing cluster {:?} with {} services using enhanced strategies with cross-validation",
        &cluster_id_str,
        service_count,
    );

    if service_count < 2 {
        debug!("Skipping cluster {} with {} services (need at least 2)", &cluster_id_str, service_count);
        return Ok((Vec::new(), cluster_stats));
    }

    // Wrap services in Arc for safe sharing across tasks
    let services_arc = Arc::new(cluster.services);
    let pipeline_run_id_str = pipeline_run_id.to_string();
    let cluster_id_opt = cluster.cluster_id.clone();

    // --- Spawn Strategy Tasks ---

    // 1. Email Matching Task
    let pool_clone_email = pool.clone();
    let services_clone_email = Arc::clone(&services_arc);
    let pipeline_run_id_clone_email = pipeline_run_id_str.clone();
    let cluster_id_clone_email = cluster_id_opt.clone();
    let email_handle: JoinHandle<(Result<(Vec<ServiceEmailMatch>, ServiceEmailMatchingStats)>, std::time::Duration)> = tokio::spawn(async move {
        let start = Instant::now();
        let result = service_email::find_service_email_matches(
            &pool_clone_email,
            &services_clone_email,
            &pipeline_run_id_clone_email,
            cluster_id_clone_email.as_deref(),
        ).await;
        (result, start.elapsed())
    });

    // 2. URL Matching Task
    let services_clone_url = Arc::clone(&services_arc);
    let url_handle: JoinHandle<(Vec<ServiceUrlMatch>, UrlMatchingStats)> =
        tokio::spawn(async move { run_enhanced_url_matching(&services_clone_url).await });

    // 3. Name Matching Task
    let services_clone_name = Arc::clone(&services_arc);
    let name_handle: JoinHandle<(Vec<ServiceMatch>, NameMatchingStats)> =
        tokio::spawn(async move { run_enhanced_name_matching(&services_clone_name).await });

    // 4. Embedding Matching Task
    let services_clone_embedding = Arc::clone(&services_arc);
    let embedding_handle: JoinHandle<(Vec<ServiceMatch>, EmbeddingMatchingStats)> = tokio::spawn(async move {
        run_enhanced_embedding_matching(&services_clone_embedding).await
    });

    // --- Await and Process Results ---
    let (email_join_res, url_join_res, name_join_res, embedding_join_res) =
        tokio::join!(email_handle, url_handle, name_handle, embedding_handle);

    let mut email_time = std::time::Duration::default();
    let mut raw_matches: Vec<MethodMatch> = Vec::new();

    // Process Email Results
    match email_join_res {
        Ok((result, time)) => {
            email_time = time;
            match result {
                Ok((email_matches_raw, mut email_stats_from_task)) => {
                    email_stats_from_task.processing_time = time;
                    for email_match in email_matches_raw {
                        raw_matches.push(MethodMatch {
                            method: MatchMethodType::ServiceEmailMatch,
                            confidence: email_match.confidence_score,
                            match_values: email_match.match_values,
                            validation_flags: ValidationFlags {
                                generic_indicators: email_match.generic_email_detected,
                                ..Default::default()
                            },
                            service_id_1: email_match.service_id_1,
                            service_id_2: email_match.service_id_2,
                        });
                    }
                    cluster_stats.email_stats = Some(email_stats_from_task);
                }
                Err(e) => warn!("Email matching failed for cluster {:?}: {}", &cluster_id_opt, e),
            }
        }
        Err(e) => warn!("Email task failed (JoinError) for cluster {:?}: {}", &cluster_id_opt, e),
    }

    // Process URL Results
    match url_join_res {
        Ok((url_matches, url_stats_from_task)) => {
            for url_match in url_matches {
                raw_matches.push(MethodMatch {
                    method: MatchMethodType::ServiceUrlMatch,
                    confidence: url_match.confidence_score,
                    match_values: url_match.match_values,
                    validation_flags: ValidationFlags::default(),
                    service_id_1: url_match.service_id_1,
                    service_id_2: url_match.service_id_2,
                });
            }
            cluster_stats.url_stats = url_stats_from_task;
        }
        Err(e) => warn!("URL task failed (JoinError) for cluster {:?}: {}", &cluster_id_opt, e),
    }

    // Process Name Results
    match name_join_res {
        Ok((name_matches, name_stats_from_task)) => {
            for name_match in name_matches {
                // NEW: Extract validation flags from name matching
                let validation_flags = extract_name_validation_flags(&name_match);
                raw_matches.push(MethodMatch {
                    method: MatchMethodType::ServiceNameSimilarity,
                    confidence: name_match.confidence_score,
                    match_values: name_match.match_values,
                    validation_flags,
                    service_id_1: name_match.service_id_1,
                    service_id_2: name_match.service_id_2,
                });
            }
            cluster_stats.name_stats = name_stats_from_task;
        }
        Err(e) => warn!("Name task failed (JoinError) for cluster {:?}: {}", &cluster_id_opt, e),
    }

    // Process Embedding Results
    match embedding_join_res {
        Ok((embedding_matches, embedding_stats_from_task)) => {
            for embedding_match in embedding_matches {
                raw_matches.push(MethodMatch {
                    method: MatchMethodType::ServiceEmbeddingSimilarity,
                    confidence: embedding_match.confidence_score,
                    match_values: embedding_match.match_values,
                    validation_flags: ValidationFlags::default(),
                    service_id_1: embedding_match.service_id_1,
                    service_id_2: embedding_match.service_id_2,
                });
            }
            cluster_stats.embedding_stats = embedding_stats_from_task;
        }
        Err(e) => warn!("Embedding task failed (JoinError) for cluster {:?}: {}", &cluster_id_opt, e),
    }

    // NEW: Apply cross-validation and aggregation
    let (final_matches, aggregation_stats) = if MATCH_AGGREGATION_ENABLED {
        apply_cross_validation_and_aggregation(raw_matches, &services_arc)
    } else {
        // Convert raw matches to ServiceMatch without aggregation
        let simple_matches: Vec<ServiceMatch> = raw_matches.into_iter().map(|method_match| {
            ServiceMatch {
                service_id_1: method_match.service_id_1,
                service_id_2: method_match.service_id_2,
                match_method: method_match.method,
                confidence_score: method_match.confidence,
                match_values: method_match.match_values,
            }
        }).collect();
        (simple_matches, (0, 0, 0)) // (aggregated, rejected, boosted)
    };

    cluster_stats.aggregated_matches = aggregation_stats.0;
    cluster_stats.cross_validation_rejections = aggregation_stats.1;
    cluster_stats.confidence_boosts = aggregation_stats.2;

    let total_cluster_time = cluster_start.elapsed();

    debug!(
        "Cluster {:?} ENHANCED TIMING BREAKDOWN (Parallel Tasks + Cross-Validation):\n\
         ├─ Total cluster time: {:.2?}\n\
         ├─ Strategy times (approx):\n\
         │  ├─ Enhanced Email: {:.2?} ({} matches, {} pairs)\n\
         │  ├─ Enhanced URL: {:.2?} ({} matches, {} pairs, large_mode: {})\n\
         │  ├─ Enhanced Name: {:.2?} ({} matches, {} pairs, large_mode: {})\n\
         │  └─ Enhanced Embedding: {:.2?} ({} matches, {} pairs, skipped: {})\n\
         ├─ Cross-validation: {} aggregated, {} rejected, {} boosted\n\
         └─ Total final matches: {}",
        &cluster_id_str,
        total_cluster_time,
        email_time, cluster_stats.email_stats.as_ref().map(|s| s.matches_found).unwrap_or(0), cluster_stats.email_stats.as_ref().map(|s| s.pairs_compared).unwrap_or(0),
        cluster_stats.url_stats.processing_time, cluster_stats.url_stats.matches_found, cluster_stats.url_stats.pairs_compared, cluster_stats.url_stats.large_cluster_mode,
        cluster_stats.name_stats.processing_time, cluster_stats.name_stats.matches_found, cluster_stats.name_stats.pairs_compared, cluster_stats.name_stats.large_cluster_mode,
        cluster_stats.embedding_stats.processing_time, cluster_stats.embedding_stats.matches_found, cluster_stats.embedding_stats.pairs_compared, cluster_stats.embedding_stats.skipped_large_cluster,
        aggregation_stats.0, aggregation_stats.1, aggregation_stats.2,
        final_matches.len()
    );

    Ok((final_matches, cluster_stats))
}

/// NEW: Apply cross-validation and match aggregation
fn apply_cross_validation_and_aggregation(
    raw_matches: Vec<MethodMatch>,
    services: &[ServiceInfo],
) -> (Vec<ServiceMatch>, (usize, usize, usize)) {
    if !CROSS_VALIDATION_ENABLED {
        // Simple conversion without cross-validation
        let simple_matches: Vec<ServiceMatch> = raw_matches.into_iter().map(|method_match| {
            ServiceMatch {
                service_id_1: method_match.service_id_1,
                service_id_2: method_match.service_id_2,
                match_method: method_match.method,
                confidence_score: method_match.confidence,
                match_values: method_match.match_values,
            }
        }).collect();
        return (simple_matches, (0, 0, 0));
    }

    // Group matches by service pair
    let mut pair_matches: HashMap<(ServiceId, ServiceId), Vec<MethodMatch>> = HashMap::new();
    
    for method_match in raw_matches {
        let sid1 = &method_match.service_id_1;
        let sid2 = &method_match.service_id_2;
        let key = if sid1.0 <= sid2.0 { (sid1.clone(), sid2.clone()) } else { (sid2.clone(), sid1.clone()) };
        pair_matches.entry(key).or_default().push(method_match);
    }

    let mut final_matches = Vec::new();
    let mut aggregated_count = 0;
    let mut rejected_count = 0;
    let mut boosted_count = 0;

    for ((sid1, sid2), method_matches) in pair_matches {
        if method_matches.is_empty() {
            continue;
        }

        // Find the services for this pair
        let service1 = services.iter().find(|s| s.service_id == sid1);
        let service2 = services.iter().find(|s| s.service_id == sid2);

        if let (Some(s1), Some(s2)) = (service1, service2) {
            // Apply cross-validation
            let validation_result = cross_validate_service_pair(s1, s2, &method_matches);
            
            if validation_result.should_reject {
                rejected_count += 1;
                debug!("Cross-validation rejected match between {} and {}: {}", 
                       sid1.0, sid2.0, validation_result.rejection_reason.unwrap_or_default());
                continue;
            }

            // Aggregate multiple method matches
            if method_matches.len() > 1 {
                aggregated_count += 1;
                let aggregated_match = aggregate_method_matches(method_matches, validation_result.confidence_modifier);
                if validation_result.confidence_modifier > 1.0 {
                    boosted_count += 1;
                }
                final_matches.push(aggregated_match);
            } else {
                // Single method match, apply confidence modifier
                let mut single_match = method_matches.into_iter().next().unwrap();
                single_match.confidence = (single_match.confidence * validation_result.confidence_modifier).min(1.0);
                if validation_result.confidence_modifier > 1.0 {
                    boosted_count += 1;
                }
                
                final_matches.push(ServiceMatch {
                    service_id_1: sid1,
                    service_id_2: sid2,
                    match_method: single_match.method,
                    confidence_score: single_match.confidence,
                    match_values: single_match.match_values,
                });
            }
        }
    }

    (final_matches, (aggregated_count, rejected_count, boosted_count))
}

#[derive(Debug)]
struct CrossValidationResult {
    should_reject: bool,
    rejection_reason: Option<String>,
    confidence_modifier: f64, // Multiplier for confidence (1.0 = no change, >1.0 = boost, <1.0 = penalty)
}

/// NEW: Cross-validate a service pair using negative evidence
fn cross_validate_service_pair(
    service1: &ServiceInfo,
    service2: &ServiceInfo,
    method_matches: &[MethodMatch],
) -> CrossValidationResult {
    let mut confidence_modifier = 1.0;
    
    // Check for incompatible service types from name analysis
    let has_name_match = method_matches.iter().any(|m| m.method == MatchMethodType::ServiceNameSimilarity);
    let has_incompatible_types = method_matches.iter().any(|m| m.validation_flags.incompatible_types);
    
    if has_incompatible_types {
        return CrossValidationResult {
            should_reject: true,
            rejection_reason: Some("Incompatible service types detected".to_string()),
            confidence_modifier: 0.0,
        };
    }

    // Check for conflicting location indicators
    let has_conflicting_locations = method_matches.iter().any(|m| m.validation_flags.conflicting_locations);
    if has_conflicting_locations {
        confidence_modifier *= 0.7; // Penalty for conflicting locations
    }

    // Check for suspicious patterns (e.g., only generic email match with no other evidence)
    let only_generic_email = method_matches.len() == 1 && 
        method_matches[0].method == MatchMethodType::ServiceEmailMatch &&
        method_matches[0].validation_flags.generic_indicators;
    
    if only_generic_email && method_matches[0].confidence < 0.8 {
        return CrossValidationResult {
            should_reject: true,
            rejection_reason: Some("Only weak generic email match found".to_string()),
            confidence_modifier: 0.0,
        };
    }

    // Boost confidence for multiple complementary signals
    if method_matches.len() >= 2 {
        let has_strong_name = method_matches.iter().any(|m| 
            m.method == MatchMethodType::ServiceNameSimilarity && m.confidence > 0.85);
        let has_url_match = method_matches.iter().any(|m| 
            m.method == MatchMethodType::ServiceUrlMatch);
        let has_email_match = method_matches.iter().any(|m| 
            m.method == MatchMethodType::ServiceEmailMatch && !m.validation_flags.generic_indicators);
        
        if (has_strong_name && has_url_match) || (has_strong_name && has_email_match) {
            confidence_modifier *= 1.15; // 15% boost for complementary strong signals
        }
    }

    // Check for extremely different name patterns (even with other matches)
    if let (Some(name1), Some(name2)) = (&service1.name, &service2.name) {
        let name_similarity = service_name::calculate_string_similarity_enhanced(name1, name2);
        if name_similarity < 0.3 && method_matches.iter().any(|m| m.method != MatchMethodType::ServiceNameSimilarity) {
            // Very different names but other methods found matches - be cautious
            confidence_modifier *= 0.85;
        }
    }

    CrossValidationResult {
        should_reject: false,
        rejection_reason: None,
        confidence_modifier,
    }
}

/// NEW: Aggregate multiple method matches for the same service pair
fn aggregate_method_matches(method_matches: Vec<MethodMatch>, confidence_modifier: f64) -> ServiceMatch {
    let mut weighted_confidence = 0.0;
    let mut total_weight = 0.0;
    
    // Use the first match for basic structure
    let base_match = &method_matches[0];
    let sid1 = base_match.service_id_1.clone();
    let sid2 = base_match.service_id_2.clone();
    
    // Calculate weighted average confidence
    for method_match in &method_matches {
        let weight = match method_match.method {
            MatchMethodType::ServiceEmailMatch => EMAIL_MATCH_WEIGHT,
            MatchMethodType::ServiceUrlMatch => URL_MATCH_WEIGHT,
            MatchMethodType::ServiceNameSimilarity => NAME_MATCH_WEIGHT,
            MatchMethodType::ServiceEmbeddingSimilarity => EMBEDDING_MATCH_WEIGHT,
            _ => 0.1, // Default weight for unknown methods
        };
        
        weighted_confidence += method_match.confidence * weight;
        total_weight += weight;
    }
    
    let aggregated_confidence = if total_weight > 0.0 {
        (weighted_confidence / total_weight * confidence_modifier).min(1.0)
    } else {
        base_match.confidence
    };
    
    // Use the highest confidence method as the primary method for reporting
    let primary_method = method_matches.iter()
        .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal))
        .map(|m| m.method.clone())
        .unwrap_or(base_match.method.clone());
    
    ServiceMatch {
        service_id_1: sid1,
        service_id_2: sid2,
        match_method: primary_method,
        confidence_score: aggregated_confidence,
        match_values: base_match.match_values.clone(),
    }
}

/// NEW: Extract validation flags from name matching results
fn extract_name_validation_flags(name_match: &ServiceMatch) -> ValidationFlags {
    let mut flags = ValidationFlags::default();
    
    if let MatchValues::ServiceName(name_data) = &name_match.match_values {
        // Check for location conflicts (simplified)
        let location_keywords = ["north", "south", "east", "west", "downtown", "central"];
        let name1_locations: Vec<_> = location_keywords.iter()
            .filter(|&&kw| name_data.normalized_name1.contains(kw))
            .collect();
        let name2_locations: Vec<_> = location_keywords.iter()
            .filter(|&&kw| name_data.normalized_name2.contains(kw))
            .collect();
        
        if !name1_locations.is_empty() && !name2_locations.is_empty() {
            let shared_locations = name1_locations.iter()
                .filter(|&kw1| name2_locations.iter().any(|&kw2| kw1.to_owned() == kw2))
                .count();
            if shared_locations == 0 {
                flags.conflicting_locations = true;
            }
        }
    }
    
    flags
}

/// Phase 4: Batch insert service groups using optimized batch upsert
async fn insert_service_groups_batch(
    pool: &PgPool,
    matches: Vec<ServiceMatch>,
    pipeline_run_id: &str,
) -> Result<()> {
    if matches.is_empty() {
        info!("No service matches to insert.");
        return Ok(());
    }

    info!("Inserting {} service matches using batch upsert...", matches.len());
    let pb = ProgressBar::new(matches.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .context("Failed to set progress bar style for insert")?
        .progress_chars("#>-"));
    pb.set_message("Batch upserting matches...");

    // Convert ServiceMatch to NewServiceGroup for batch upsert
    let new_service_groups: Vec<NewServiceGroup> = matches.iter().map(|service_match| {
        NewServiceGroup {
            proposed_id: Uuid::new_v4().to_string(),
            service_id_1: service_match.service_id_1.clone(),
            service_id_2: service_match.service_id_2.clone(),
            confidence_score: service_match.confidence_score,
            pre_rl_confidence_score: service_match.confidence_score, // Using same value for now
            method_type: service_match.match_method.clone(),
            match_values: service_match.match_values.clone(),
        }
    }).collect();

    // Batch upsert service groups
    let inserted_ids = db::upsert_service_groups_batch(pool, &new_service_groups).await
        .context("Failed to batch upsert service groups")?;

    pb.set_length((matches.len() + inserted_ids.len()) as u64);
    pb.set_position(matches.len() as u64);
    pb.set_message("Creating decision details...");

    // Prepare service match decision details
    let decision_details: Vec<(String, String, serde_json::Value, String, f64, f64, Option<i32>)> = 
        inserted_ids.iter().zip(matches.iter()).map(|(group_id, service_match)| {
            let features_json = json!({
                "method_type": service_match.match_method.as_str(),
                "pipeline_run_id": pipeline_run_id,
                "service_id_1": service_match.service_id_1.0,
                "service_id_2": service_match.service_id_2.0
            });
            (
                group_id.clone(),
                pipeline_run_id.to_string(),
                features_json,
                service_match.match_method.as_str().to_string(),
                service_match.confidence_score,
                service_match.confidence_score,
                None
            )
        }).collect();

    // Batch insert decision details
    let _detail_ids = db::insert_service_match_decision_details_batch(pool, &decision_details).await
        .context("Failed to batch insert service match decision details")?;

    pb.set_position((matches.len() + inserted_ids.len()) as u64);
    pb.finish_with_message("Batch insert complete.");
    
    info!("Successfully inserted {} service groups and {} decision details", 
          inserted_ids.len(), _detail_ids.len());
    Ok(())
}

// REFACTORED URL Matching with Blocking for Large Clusters
async fn run_enhanced_url_matching(services: &[ServiceInfo]) -> (Vec<ServiceUrlMatch>, UrlMatchingStats) {
    let start_time = Instant::now();
    let mut stats = UrlMatchingStats::default();

    if services.len() > LARGE_CLUSTER_THRESHOLD_URL {
        stats.large_cluster_mode = true;
        warn!("Running URL matching in LARGE CLUSTER (blocking) mode ({} services)", services.len());
        let (matches, blocked_stats) = run_blocked_url_matching(services).await;
        stats.matches_found = blocked_stats.matches_found;
        stats.domain_only_matches = blocked_stats.domain_only_matches;
        stats.path_matches = blocked_stats.path_matches;
        stats.ignored_domains = blocked_stats.ignored_domains;
        stats.avg_confidence = blocked_stats.avg_confidence;
        stats.pairs_compared = blocked_stats.pairs_compared;
        stats.processing_time = start_time.elapsed();
        return (matches, stats);
    }

    // Original O(N^2) logic for smaller clusters
    let mut matches = Vec::new();
    let mut confidence_scores = Vec::new();

    for i in 0..services.len() {
        for j in (i + 1)..services.len() {
            stats.pairs_compared += 1;
            if let Some(url_match) = service_url::match_services_by_url(&services[i], &services[j]) {
                matches.push(url_match.clone());
                confidence_scores.push(url_match.confidence_score);
                if url_match.match_values.get_url_matching_slugs() > 0 {
                    stats.path_matches += 1;
                } else {
                    stats.domain_only_matches += 1;
                }
            }
        }
    }
    stats.matches_found = matches.len();
    stats.avg_confidence = if !confidence_scores.is_empty() { confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64 } else { 0.0 };
    stats.processing_time = start_time.elapsed();
    (matches, stats)
}

async fn run_blocked_url_matching(services: &[ServiceInfo]) -> (Vec<ServiceUrlMatch>, UrlMatchingStats) {
    let mut matches = Vec::new();
    let mut stats = UrlMatchingStats::default();
    let mut confidence_scores = Vec::new();
    let mut domain_groups: HashMap<String, Vec<&ServiceInfo>> = HashMap::new();

    // 1. Normalize and Group by Domain
    for service in services {
        if let Some(url_str) = &service.url {
            if let Some(normalized) = service_url::normalize_service_url(url_str) {
                if service_url::is_ignored_service_domain(&normalized.domain) {
                    stats.ignored_domains += 1;
                    continue;
                }
                domain_groups.entry(normalized.domain.clone()).or_default().push(service);
            }
        }
    }

    // 2. Compare within each group
    for (_domain, group) in domain_groups {
        if group.len() < 2 { continue; }

        for i in 0..group.len() {
            for j in (i + 1)..group.len() {
                stats.pairs_compared += 1;
                if let Some(url_match) = service_url::match_services_by_url(group[i], group[j]) {
                    matches.push(url_match.clone());
                    confidence_scores.push(url_match.confidence_score);
                    if url_match.match_values.get_url_matching_slugs() > 0 {
                        stats.path_matches += 1;
                    } else {
                        stats.domain_only_matches += 1;
                    }
                }
            }
        }
    }

    stats.matches_found = matches.len();
    stats.avg_confidence = if !confidence_scores.is_empty() { confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64 } else { 0.0 };
    (matches, stats)
}

// REFACTORED Name Matching with Indexing for Large Clusters
async fn run_enhanced_name_matching(services: &[ServiceInfo]) -> (Vec<ServiceMatch>, NameMatchingStats) {
    let start_time = Instant::now();
    let mut stats = NameMatchingStats::default();

    if services.len() > LARGE_CLUSTER_THRESHOLD_NAME {
        stats.large_cluster_mode = true;
        warn!("Running Name matching in LARGE CLUSTER (indexing) mode ({} services)", services.len());
        let (matches, indexed_stats) = run_indexed_name_matching(services).await;
        stats.matches_found = indexed_stats.matches_found;
        stats.fuzzy_matches = indexed_stats.fuzzy_matches;
        stats.token_matches = indexed_stats.token_matches;
        stats.same_type_matches = indexed_stats.same_type_matches;
        stats.incompatible_type_rejections = indexed_stats.incompatible_type_rejections;
        stats.avg_confidence = indexed_stats.avg_confidence;
        stats.pairs_compared = indexed_stats.pairs_compared;
        stats.processing_time = start_time.elapsed();
        return (matches, stats);
    }

    // Original O(N^2) logic for smaller clusters
    let mut matches = Vec::new();
    let mut confidence_scores = Vec::new();
    for i in 0..services.len() {
        for j in (i + 1)..services.len() {
            stats.pairs_compared += 1;
            if let Some(name_match) = service_name::match_services_by_name_enhanced(&services[i], &services[j]) {
                matches.push(name_match.clone());
                confidence_scores.push(name_match.confidence_score);
            }
        }
    }
    stats.matches_found = matches.len();
    stats.avg_confidence = if !confidence_scores.is_empty() { confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64 } else { 0.0 };
    stats.processing_time = start_time.elapsed();
    (matches, stats)
}

async fn run_indexed_name_matching(services: &[ServiceInfo]) -> (Vec<ServiceMatch>, NameMatchingStats) {
    let mut matches = Vec::new();
    let mut stats = NameMatchingStats::default();
    let mut confidence_scores = Vec::new();
    let mut inverted_index: HashMap<String, Vec<usize>> = HashMap::new();
    let mut service_data_map: HashMap<usize, &ServiceInfo> = HashMap::new();

    // 1. Build Inverted Index & Store ServiceData
    for (idx, service) in services.iter().enumerate() {
        service_data_map.insert(idx, service);
        if let Some(name) = &service.name {
            let (normalized, stype) = service_name::normalize_service_name_advanced(name);
            let (tokens, _bigrams) = service_name::tokenize_service_name(&normalized, stype);
            for token in tokens {
                inverted_index.entry(token).or_default().push(idx);
            }
        }
    }

    // 2. Generate Candidate Pairs
    let mut candidate_pairs: HashSet<(usize, usize)> = HashSet::new();
    for (_token, service_indices) in inverted_index {
        if service_indices.len() >= 2 && service_indices.len() < 1000 {
            for i in 0..service_indices.len() {
                for j in (i + 1)..service_indices.len() {
                    let idx1 = service_indices[i];
                    let idx2 = service_indices[j];
                    candidate_pairs.insert(if idx1 < idx2 { (idx1, idx2) } else { (idx2, idx1) });
                }
            }
        }
    }

    // 3. Process Candidate Pairs
    for (idx1, idx2) in candidate_pairs {
        stats.pairs_compared += 1;
        let service1 = service_data_map[&idx1];
        let service2 = service_data_map[&idx2];

        if let Some(name_match) = service_name::match_services_by_name_enhanced(service1, service2) {
            matches.push(name_match.clone());
            confidence_scores.push(name_match.confidence_score);
        }
    }

    stats.matches_found = matches.len();
    stats.avg_confidence = if !confidence_scores.is_empty() { confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64 } else { 0.0 };
    (matches, stats)
}

// REFACTORED Embedding Matching with Skipping for Large Clusters
async fn run_enhanced_embedding_matching(services: &[ServiceInfo]) -> (Vec<ServiceMatch>, EmbeddingMatchingStats) {
    let start_time = Instant::now();
    let mut stats = EmbeddingMatchingStats::default();

    if services.len() > LARGE_CLUSTER_THRESHOLD_EMB {
        warn!("Skipping embedding matching for large cluster ({} services). Consider ANN.", services.len());
        stats.skipped_large_cluster = true;
        stats.processing_time = start_time.elapsed();
        return (Vec::new(), stats);
    }

    // Original O(N^2) logic for smaller clusters
    let mut matches = Vec::new();
    let mut similarities = Vec::new();
    for i in 0..services.len() {
        for j in (i + 1)..services.len() {
            stats.pairs_compared += 1;
            if let Some(embedding_match) = match_services_by_embedding_enhanced(&services[i], &services[j]) {
                matches.push(embedding_match.clone());
                similarities.push(embedding_match.confidence_score);
            }
        }
    }
    stats.matches_found = matches.len();
    stats.avg_similarity = if !similarities.is_empty() { similarities.iter().sum::<f64>() / similarities.len() as f64 } else { 0.0 };
    stats.processing_time = start_time.elapsed();
    (matches, stats)
}

/// Enhanced embedding similarity matching with better context
fn match_services_by_embedding_enhanced(service1: &ServiceInfo, service2: &ServiceInfo) -> Option<ServiceMatch> {
    if let (Some(emb1), Some(emb2)) = (&service1.embedding, &service2.embedding) {
        if let Ok(similarity) = cosine_similarity_candle(emb1, emb2) {
            if similarity >= SERVICE_EMBEDDING_SIMILARITY_THRESHOLD {
                // Boost confidence if names are also similar
                let name_boost = if let (Some(name1), Some(name2)) = (&service1.name, &service2.name) {
                    let name_sim = service_name::calculate_string_similarity_enhanced(name1, name2);
                    if name_sim > 0.7 { 0.05 } else { 0.0 }
                } else {
                    0.0
                };

                let adjusted_confidence = (similarity + name_boost).min(0.98);

                return Some(ServiceMatch {
                    service_id_1: service1.service_id.clone(),
                    service_id_2: service2.service_id.clone(),
                    match_method: MatchMethodType::ServiceEmbeddingSimilarity,
                    confidence_score: adjusted_confidence,
                    match_values: MatchValues::ServiceEmbedding(ServiceEmbeddingMatchValue {
                        name1: service1.name.clone().unwrap_or_default(),
                        name2: service2.name.clone().unwrap_or_default(),
                        embedding_similarity: similarity,
                    }),
                });
            }
        }
    }
    None
}

/// Log comprehensive enhanced statistics
fn log_enhanced_statistics(stats: &ServiceMatchingStats, pipeline_run_id: &str) {
    info!("Enhanced service matching pipeline completed successfully!");
    info!("Pipeline run ID: {}", pipeline_run_id);
    info!("Total processing time: {:.2?}", stats.processing_time);
    info!("Clusters processed: {}", stats.total_clusters_processed);
    info!("Total matches found: {}", stats.total_matches_found);

    // NEW: Cross-validation stats
    info!("CROSS-VALIDATION STATS:");
    info!("  Aggregated matches: {}", stats.aggregated_matches);
    info!("  Cross-validation rejections: {}", stats.cross_validation_rejections);
    info!("  Confidence boosts: {}", stats.confidence_boosts);

    // Method breakdown
    for (method, count) in &stats.matches_by_method {
        info!("  {}: {} matches", method.as_str(), count);
    }

    // Enhanced email statistics
    if let Some(email_stats) = &stats.email_stats {
        info!("ENHANCED EMAIL MATCHING STATS:");
        info!("  Services processed: {}", email_stats.services_processed);
        info!("  Pairs compared: {}", email_stats.pairs_compared);
        info!("  Matches found: {}", email_stats.matches_found);
        info!("  Generic email matches: {}", email_stats.generic_email_matches);
        info!("  High frequency matches: {}", email_stats.high_frequency_matches);
        info!("  Average confidence: {:.3}", email_stats.avg_confidence);
        info!("  Processing time: {:.2?}", email_stats.processing_time);
    }

    // Enhanced URL statistics
    info!("ENHANCED URL MATCHING STATS:");
    info!("  Total matches: {}", stats.url_stats.matches_found);
    info!("  Pairs compared: {}", stats.url_stats.pairs_compared);
    info!("  Domain-only matches: {}", stats.url_stats.domain_only_matches);
    info!("  Path matches: {}", stats.url_stats.path_matches);
    info!("  Ignored domains: {}", stats.url_stats.ignored_domains);
    info!("  Average confidence: {:.3}", stats.url_stats.avg_confidence);
    info!("  Processing time: {:.2?}", stats.url_stats.processing_time);
    info!("  Large cluster (blocking) mode used: {}", stats.url_stats.large_cluster_mode);

    // Enhanced name statistics
    info!("ENHANCED NAME MATCHING STATS:");
    info!("  Total matches: {}", stats.name_stats.matches_found);
    info!("  Pairs compared: {}", stats.name_stats.pairs_compared);
    info!("  Fuzzy matches: {}", stats.name_stats.fuzzy_matches);
    info!("  Token matches: {}", stats.name_stats.token_matches);
    info!("  Same type matches: {}", stats.name_stats.same_type_matches);
    info!("  Incompatible rejections: {}", stats.name_stats.incompatible_type_rejections);
    info!("  Average confidence: {:.3}", stats.name_stats.avg_confidence);
    info!("  Processing time: {:.2?}", stats.name_stats.processing_time);
    info!("  Large cluster (indexing) mode used: {}", stats.name_stats.large_cluster_mode);

    // Enhanced embedding statistics
    info!("ENHANCED EMBEDDING MATCHING STATS:");
    info!("  Total matches: {}", stats.embedding_stats.matches_found);
    info!("  Pairs compared: {}", stats.embedding_stats.pairs_compared);
    info!("  Average similarity: {:.3}", stats.embedding_stats.avg_similarity);
    info!("  Processing time: {:.2?}", stats.embedding_stats.processing_time);
    info!("  Skipped large clusters: {}", stats.embedding_stats.skipped_large_cluster);
}

// Include all the remaining functions from the original implementation
// (get_entity_clusters, add_unmatched_entities, load_service_clusters, etc.)

/// Phase 1: Get entities organized by cluster from entity_group table
async fn get_entity_clusters(pool: &PgPool) -> Result<HashMap<Option<String>, Vec<EntityId>>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let query = "
        SELECT DISTINCT entity_id_1, entity_id_2, group_cluster_id
        FROM public.entity_group
        WHERE group_cluster_id IS NOT NULL
    ";

    debug!("Querying entity_group table...");
    let rows = conn.query(query, &[]).await.context("Failed to query entity_group")?;
    debug!("Found {} rows in entity_group.", rows.len());

    let mut clusters: HashMap<Option<String>, Vec<EntityId>> = HashMap::new();

    for row in rows {
        let entity_id_1: String = row.get("entity_id_1");
        let entity_id_2: String = row.get("entity_id_2");
        let cluster_id: Option<String> = row.get("group_cluster_id");

        let cluster_key = cluster_id.clone();
        let cluster_entities = clusters.entry(cluster_key).or_insert_with(Vec::new);

        cluster_entities.push(EntityId(entity_id_1));
        cluster_entities.push(EntityId(entity_id_2));
    }

    for entities in clusters.values_mut() {
        entities.sort_by(|a, b| a.0.cmp(&b.0));
        entities.dedup_by(|a, b| a.0 == b.0);
    }

    debug!("Organized {} entities into {} clusters",
          clusters.values().map(|v| v.len()).sum::<usize>(),
          clusters.len());

    Ok(clusters)
}

/// Phase 2: Add unmatched entities as individual pseudo-clusters
async fn add_unmatched_entities(
    pool: &PgPool,
    clusters: &mut HashMap<Option<String>, Vec<EntityId>>
) -> Result<usize> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let clustered_entities: HashSet<EntityId> = clusters
        .values()
        .flatten()
        .cloned()
        .collect();

    let query = "SELECT id FROM public.entity";
    debug!("Querying all entities...");
    let rows = conn.query(query, &[]).await.context("Failed to query all entities")?;
    debug!("Found {} total entities.", rows.len());

    let mut unmatched_count = 0;

    for row in rows {
        let entity_id = EntityId(row.get::<_, String>("id"));

        if !clustered_entities.contains(&entity_id) {
            let pseudo_cluster_key = Some(format!("individual-{}", entity_id.0));
            clusters.insert(pseudo_cluster_key, vec![entity_id]);
            unmatched_count += 1;
        }
    }
    Ok(unmatched_count)
}

/// Load services for each cluster
async fn load_service_clusters(
    pool: &PgPool,
    entity_clusters: HashMap<Option<String>, Vec<EntityId>>
) -> Result<Vec<ServiceCluster>> {
    if entity_clusters.is_empty() {
        return Ok(Vec::new());
    }

    let conn = pool.get().await.context("Failed to get DB connection")?;

    let mut all_entity_ids = Vec::new();
    let mut entity_to_cluster: HashMap<EntityId, Option<String>> = HashMap::new();

    for (cluster_id, entities) in &entity_clusters {
        for entity_id in entities {
            all_entity_ids.push(entity_id.0.clone());
            entity_to_cluster.insert(entity_id.clone(), cluster_id.clone());
        }
    }

    if all_entity_ids.is_empty() {
        return Ok(Vec::new());
    }

    debug!("Loading services for {} entities across {} clusters",
          all_entity_ids.len(), entity_clusters.len());

    let query = "
        SELECT s.id, s.name, s.email, s.url, s.embedding_v2, ef.entity_id
        FROM public.service s
        JOIN public.entity_feature ef ON ef.table_id = s.id AND ef.table_name = 'service'
        WHERE ef.entity_id = ANY($1)
        ORDER BY ef.entity_id, s.id
    ";

    let rows = conn.query(query, &[&all_entity_ids]).await
        .context("Failed to query services for all clusters")?;

    debug!("Retrieved {} service rows from database", rows.len());

    let mut cluster_services: HashMap<Option<String>, Vec<ServiceInfo>> = HashMap::new();

    for row in rows {
        let service_id = ServiceId(row.get::<_, String>("id"));
        let entity_id = EntityId(row.get::<_, String>("entity_id"));
        let name: Option<String> = row.get("name");
        let email: Option<String> = row.get("email");
        let url: Option<String> = row.get("url");

        let embedding: Option<Vec<f32>> = row.get::<_, Option<pgvector::Vector>>("embedding_v2")
            .map(|v| v.to_vec());

        let service_info = ServiceInfo {
            service_id,
            entity_id: entity_id.clone(),
            name,
            email,
            url,
            embedding,
        };

        if let Some(cluster_id) = entity_to_cluster.get(&entity_id) {
            cluster_services
                .entry(cluster_id.clone())
                .or_insert_with(Vec::new)
                .push(service_info);
        }
    }

    let mut service_clusters = Vec::new();
    for (cluster_id, entities) in entity_clusters {
        if let Some(services) = cluster_services.remove(&cluster_id) {
            if !services.is_empty() {
                service_clusters.push(ServiceCluster {
                    cluster_id,
                    entities,
                    services,
                });
            }
        }
    }

    debug!("Created {} service clusters with services", service_clusters.len());
    Ok(service_clusters)
}

/// Phase 5: Consolidate service clusters using graph clustering
async fn consolidate_service_clusters(pool: &PgPool, pipeline_run_id: &str) -> Result<usize> {
    info!("Starting service cluster consolidation using Leiden algorithm...");
    let conn = pool.get().await.context("Failed to get DB connection")?;
    let query = "SELECT id, service_id_1, service_id_2, confidence_score FROM public.service_group WHERE service_group_cluster_id IS NULL";
    debug!("Querying unclustered service groups...");
    let rows = conn.query(query, &[]).await.context("Failed to query service groups")?;
    if rows.is_empty() { info!("No unclustered service groups found."); return Ok(0); }
    info!("Found {} unclustered service groups to process.", rows.len());
    debug!("Building graph for clustering...");
    let mut graph: Graph<f64, String, ()> = Graph::new(false);
    let mut node_map: HashMap<String, xgraph::graph::graph::NodeId> = HashMap::new();
    let mut group_confidences: HashMap<String, f64> = HashMap::new();
    for row in &rows {
        let group_id: String = row.get("id");
        let confidence: Option<f64> = row.get("confidence_score");
        let node_id = graph.add_node(group_id.clone());
        node_map.insert(group_id.clone(), node_id);
        group_confidences.insert(group_id, confidence.unwrap_or(0.0));
    }
    let mut service_to_groups: HashMap<String, Vec<String>> = HashMap::new();
    for row in &rows {
        let group_id: String = row.get("id");
        let service_id_1: String = row.get("service_id_1");
        let service_id_2: String = row.get("service_id_2");
        service_to_groups.entry(service_id_1).or_default().push(group_id.clone());
        service_to_groups.entry(service_id_2).or_default().push(group_id.clone());
    }
    for groups in service_to_groups.values() {
        for i in 0..groups.len() {
            for j in (i + 1)..groups.len() {
                let group1 = &groups[i];
                let group2 = &groups[j];
                if let (Some(&node1), Some(&node2)) = (node_map.get(group1), node_map.get(group2)) {
                    let conf1 = group_confidences.get(group1).unwrap_or(&0.0);
                    let conf2 = group_confidences.get(group2).unwrap_or(&0.0);
                    let edge_weight = (conf1 + conf2) / 2.0;
                    if edge_weight > 0.0 { let _ = graph.add_edge(node1, node2, edge_weight, ()); }
                }
            }
        }
    }
    debug!("Running Leiden clustering...");
    let config = CommunityConfig { resolution: 1.0, deterministic: true, seed: Some(42), iterations: 10, gamma: 1.0 };
    let communities = graph.detect_communities_with_config(config).context("Leiden clustering failed")?;
    info!("Found {} communities from {} service groups", communities.len(), rows.len());
    let mut clusters_created = 0;
    let pb = ProgressBar::new(communities.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .context("Failed to set progress bar style for cluster creation")?
        .progress_chars("#>-"));
    pb.set_message("Creating clusters...");
    for (community_id, nodes_in_community) in communities.into_iter() {
        pb.inc(1);
        if nodes_in_community.len() < 2 { debug!("Skipping single-node community {}", community_id); continue; }
        let cluster_id = Uuid::new_v4().to_string();
        let cluster_name = format!("ServiceCluster-{}", community_id);
        let now = Utc::now().naive_utc();
        conn.execute("INSERT INTO public.service_group_cluster (id, name, description, created_at, updated_at, service_count, service_group_count) VALUES ($1, $2, $3, $4, $5, 0, $6)", &[&cluster_id, &cluster_name, &format!("Leiden-generated service cluster"), &now, &now, &(nodes_in_community.len() as i32)]).await.context("Failed to insert service_group_cluster")?;
        let group_ids: Vec<String> = nodes_in_community.iter().filter_map(|&node_idx| graph.nodes.get(node_idx).map(|node| node.data.clone())).collect();
        if !group_ids.is_empty() {
            conn.execute("UPDATE public.service_group SET service_group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP WHERE id = ANY($2)", &[&cluster_id, &group_ids]).await.context("Failed to update service groups with cluster ID")?;
            clusters_created += 1;
            debug!("Created service cluster {} with {} groups", cluster_id, group_ids.len());
        }
    }
    pb.finish_with_message("Cluster creation complete.");
    Ok(clusters_created)
}

/// Phase 6: Create visualization edges for frontend
async fn create_service_visualization_edges(pool: &PgPool, pipeline_run_id: &str) -> Result<usize> {
    info!("Creating service visualization edges...");
    let conn = pool.get().await.context("Failed to get DB connection")?;
    debug!("Cleaning existing visualization edges for run ID {}", pipeline_run_id);
    conn.execute("DELETE FROM public.service_edge_visualization WHERE pipeline_run_id = $1", &[&pipeline_run_id]).await.context("Failed to clean existing visualization edges")?;
    let cluster_query = "SELECT id FROM public.service_group_cluster";
    debug!("Querying service group clusters...");
    let cluster_rows = conn.query(cluster_query, &[]).await.context("Failed to query clusters")?;
    debug!("Found {} clusters for visualization.", cluster_rows.len());
    let mut total_edges = 0;
    let pb = ProgressBar::new(cluster_rows.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .context("Failed to set progress bar style for edge creation")?
        .progress_chars("#>-"));
    pb.set_message("Creating edges...");
    for cluster_row in cluster_rows {
        pb.inc(1);
        let cluster_id: String = cluster_row.get("id");
        debug!("Processing cluster {} for visualization", cluster_id);
        let services_query = "SELECT DISTINCT service_id_1, service_id_2 FROM public.service_group WHERE service_group_cluster_id = $1";
        let service_rows = conn.query(services_query, &[&cluster_id]).await.context("Failed to query services in cluster")?;
        let mut services = HashSet::new();
        for row in &service_rows {
            services.insert(row.get::<_, String>("service_id_1"));
            services.insert(row.get::<_, String>("service_id_2"));
        }
        let services_vec: Vec<String> = services.into_iter().collect();
        if services_vec.len() < 2 { debug!("Skipping cluster {} with less than 2 services", cluster_id); continue; }
        for i in 0..services_vec.len() {
            for j in (i + 1)..services_vec.len() {
                let service1 = &services_vec[i];
                let service2 = &services_vec[j];
                let methods_query = "SELECT confidence_score, method_type, match_values FROM public.service_group WHERE service_group_cluster_id = $1 AND ((service_id_1 = $2 AND service_id_2 = $3) OR (service_id_1 = $3 AND service_id_2 = $2))";
                let method_rows = conn.query(methods_query, &[&cluster_id, service1, service2]).await.context("Failed to query service group methods")?;
                if !method_rows.is_empty() {
                    let mut method_confidences = Vec::new();
                    let mut method_details = Vec::new();
                    for method_row in method_rows {
                        let confidence: Option<f64> = method_row.get("confidence_score");
                        let method_type: String = method_row.get("method_type");
                        let match_values: Option<serde_json::Value> = method_row.get("match_values");
                        if let Some(conf) = confidence {
                            method_confidences.push(conf);
                            method_details.push(json!({ "method_type": method_type, "confidence": conf, "match_values": match_values }));
                        }
                    }
                    if !method_confidences.is_empty() {
                        let edge_weight = 1.0 - method_confidences.iter().fold(1.0, |acc, &conf| acc * (1.0 - conf.max(0.0).min(1.0)));
                        let details_json = json!({ "methods": method_details, "method_count": method_confidences.len() });
                        let edge_id = Uuid::new_v4().to_string();
                        let (s1, s2) = if service1 < service2 { (service1, service2) } else { (service2, service1) };
                        conn.execute("INSERT INTO public.service_edge_visualization (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id) VALUES ($1, $2, $3, $4, $5, $6, $7)", &[&edge_id, &cluster_id, s1, s2, &edge_weight, &details_json, &pipeline_run_id]).await.context("Failed to insert visualization edge")?;
                        total_edges += 1;
                    }
                }
            }
        }
    }
    pb.finish_with_message("Edge creation complete.");
    Ok(total_edges)
}

// Extension trait to help with match values (used by run_enhanced_url_matching)
trait MatchValuesExt {
    fn get_url_matching_slugs(&self) -> usize;
}

impl MatchValuesExt for MatchValues {
    fn get_url_matching_slugs(&self) -> usize {
        match self {
            MatchValues::ServiceUrl(url_match) => url_match.matching_slug_count,
            _ => 0, // Only ServiceUrl has matching_slug_count
        }
    }
}