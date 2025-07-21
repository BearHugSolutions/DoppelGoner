// src/matching/manager.rs
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Context;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use indicatif::MultiProgress;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use log::{debug, info, warn, error};
use anyhow::Result;

use crate::matching::db::create_rl_decision_batch_data;
use crate::matching::db::insert_service_groups_batch;
use crate::matching::email::find_service_email_matches;
use crate::matching::embedding::run_enhanced_embedding_matching;
use crate::matching::name::run_enhanced_name_matching;
use crate::models::matching::MatchMethodType;
use crate::models::matching::MethodMatch;
use crate::models::matching::ServiceMatch;
use crate::models::stats_models::ServiceCluster;
use crate::models::stats_models::ServiceMatchingStats;
use crate::rl::{ServiceRLOrchestrator, SharedServiceFeatureCache};
use crate::utils::db_connect::PgPool;

static MAX_CONCURRENT_CLUSTERS: Lazy<usize> = Lazy::new(|| {
    std::env::var("MAX_CONCURRENT_CLUSTERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| num_cpus::get().min(8)) // Cap at 8 to avoid connection starvation
});
const CLUSTER_PROCESSING_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes
const CLUSTER_BATCH_SIZE: usize = 50;

/// Runs the enhanced service matching pipeline across multiple clusters in parallel.
/// Aggregates all matches and RL decision data for a single batch insertion at the end.
/// NEW: Updated to accept and pass through service contributor filter
pub async fn run_enhanced_service_matching_pipeline(
    pool: &PgPool,
    service_clusters: Vec<ServiceCluster>,
    pipeline_run_id: &str,
    mut stats: ServiceMatchingStats,
    service_rl_orchestrator: Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    service_feature_cache: Option<SharedServiceFeatureCache>,
    service_contributor_filter: Option<crate::utils::service_contributor_filter::ServiceContributorFilterConfig>, // FIXED: Changed to owned type
    multi_progress: Option<MultiProgress>,
) -> Result<(Vec<ServiceMatch>, ServiceMatchingStats)> {
    let stats_mutex = Arc::new(Mutex::new(ServiceMatchingStats::default()));
    let all_matches = Arc::new(Mutex::new(Vec::new()));
    let all_rl_features_data = Arc::new(Mutex::new(Vec::new()));

    // Create semaphore to limit concurrent cluster processing tasks
    let semaphore = Arc::new(Semaphore::new(*MAX_CONCURRENT_CLUSTERS));

    let total_clusters = service_clusters.len() as u64;
    
    // Create progress bar for this phase
    let pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(total_clusters));
        pb.set_style(ProgressStyle::default_bar()
            .template("    {spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .context("Failed to set progress bar style")?
            .progress_chars("#>-"));
        pb.set_message(format!("Processing clusters... (RL enabled: {}, Filtering enabled: {})", 
                              service_rl_orchestrator.is_some(),
                              service_contributor_filter.as_ref().map_or(false, |f| f.enabled)));
        Some(pb)
    } else {
        let pb = ProgressBar::new(total_clusters); 
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .context("Failed to set progress bar style")?
            .progress_chars("#>-"));
        pb.set_message(format!("Processing clusters... (RL enabled: {}, Filtering enabled: {})", 
                              service_rl_orchestrator.is_some(),
                              service_contributor_filter.as_ref().map_or(false, |f| f.enabled)));
        Some(pb)
    };
    let pb_arc = Arc::new(pb.unwrap());

    // Process clusters in batches to avoid overwhelming the system
    for (batch_idx, batch) in service_clusters.chunks(CLUSTER_BATCH_SIZE).enumerate() {
        let batch_start = Instant::now();
        let mut batch_handles = Vec::new();
        
        info!("Processing batch {}/{} ({} clusters)", 
            batch_idx + 1, 
            (service_clusters.len() + CLUSTER_BATCH_SIZE - 1) / CLUSTER_BATCH_SIZE,
            batch.len()
        );

        for cluster in batch {
            let pool_clone = pool.clone();
            let pipeline_run_id_clone = pipeline_run_id.to_string();
            let stats_mutex_clone = Arc::clone(&stats_mutex);
            let all_matches_clone = Arc::clone(&all_matches);
            let all_rl_features_data_clone = Arc::clone(&all_rl_features_data);
            let semaphore_clone = Arc::clone(&semaphore);
            let pb_clone = Arc::clone(&pb_arc);
            
            // Clone RL components for each task
            let rl_orchestrator_clone = service_rl_orchestrator.clone();
            let feature_cache_clone = service_feature_cache.clone();
            let multi_progress_clone = multi_progress.clone();
            let cluster_clone = cluster.clone(); // Clone cluster for move
            
            // Extract cluster_id_str here, before moving cluster_clone
            let cluster_id_str = cluster_clone.cluster_id.as_deref().unwrap_or("unknown").to_string();

            // FIXED: Clone the filter config for each task
            let service_contributor_filter_clone = service_contributor_filter.clone();

            let handle: JoinHandle<()> = tokio::spawn(async move {
                let _permit = match semaphore_clone.acquire().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        error!("Semaphore closed");
                        return;
                    }
                };

                // Use the owned cluster_id_str
                let cluster_id_for_log = cluster_id_str.clone(); 
                
                // Wrap cluster processing in timeout
                // FIXED: Pass owned filter to cluster processing
                match timeout(CLUSTER_PROCESSING_TIMEOUT, run_enhanced_service_matching_for_cluster(
                    &pool_clone, 
                    cluster_clone, // Pass the owned clone here
                    &pipeline_run_id_clone,
                    rl_orchestrator_clone,
                    feature_cache_clone,
                    service_contributor_filter_clone, // FIXED: Pass owned filter through
                    multi_progress_clone,
                )).await {
                    Ok(Ok((cluster_matches, cluster_rl_features, cluster_stats))) => {
                        let mut stats_guard = stats_mutex_clone.lock().await;
                        stats_guard.total_clusters_processed += 1;
                        stats_guard.total_matches_found += cluster_matches.len();

                        // Aggregate enhanced stats
                        stats_guard.email_stats.services_processed += cluster_stats.email_stats.services_processed;
                        stats_guard.email_stats.pairs_compared += cluster_stats.email_stats.pairs_compared;
                        stats_guard.email_stats.matches_found += cluster_stats.email_stats.matches_found;
                        stats_guard.email_stats.generic_email_matches += cluster_stats.email_stats.generic_email_matches;
                        stats_guard.email_stats.high_frequency_matches += cluster_stats.email_stats.high_frequency_matches;

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
                        drop(matches_guard);

                        let mut rl_features_guard = all_rl_features_data_clone.lock().await;
                        rl_features_guard.extend(cluster_rl_features);
                        drop(rl_features_guard);
                    }
                    Ok(Err(e)) => {
                        error!("Failed to process cluster {}: {}", cluster_id_for_log, e);
                        let mut stats_guard = stats_mutex_clone.lock().await;
                        stats_guard.total_clusters_processed += 1;
                    }
                    Err(_) => {
                        error!("Cluster {} processing timed out after {:?}", cluster_id_for_log, CLUSTER_PROCESSING_TIMEOUT);
                        let mut stats_guard = stats_mutex_clone.lock().await;
                        stats_guard.total_clusters_processed += 1;
                    }
                }
                pb_clone.inc(1);
            });
            batch_handles.push(handle);
        }

        // Wait for batch completion
        for handle in batch_handles {
            if let Err(e) = handle.await {
                error!("Batch task failed (JoinError): {}", e);
            }
        }
        
        info!("Batch {} completed in {:.2?}", batch_idx + 1, batch_start.elapsed());
    }

    pb_arc.finish_with_message("Cluster processing complete.");

    let final_matches = Arc::try_unwrap(all_matches)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap matches mutex"))?
        .into_inner();

    let final_rl_features = Arc::try_unwrap(all_rl_features_data)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap RL features mutex"))?
        .into_inner();

    let final_stats_from_tasks = Arc::try_unwrap(stats_mutex)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap stats mutex"))?
        .into_inner();

    // Merge the collected stats from tasks with the initial stats
    stats.total_clusters_processed = final_stats_from_tasks.total_clusters_processed;
    stats.total_matches_found = final_stats_from_tasks.total_matches_found;
    stats.matches_by_method = final_stats_from_tasks.matches_by_method;
    stats.email_stats = final_stats_from_tasks.email_stats;
    stats.name_stats = final_stats_from_tasks.name_stats;
    stats.embedding_stats = final_stats_from_tasks.embedding_stats;
    stats.aggregated_matches = final_stats_from_tasks.aggregated_matches;
    stats.cross_validation_rejections = final_stats_from_tasks.cross_validation_rejections;
    stats.confidence_boosts = final_stats_from_tasks.confidence_boosts;

    let mut rl_tuner_version: Option<u32> = None;
    if let Some(rl_orch) = &service_rl_orchestrator {
        let rl_guard = rl_orch.lock().await;
        rl_tuner_version = Some(rl_guard.confidence_tuner.version);
        drop(rl_guard); // Explicit drop
    }

    let rl_tuner_version_i32 = rl_tuner_version.map(|v| v as i32);
    let rl_decision_data = if !final_rl_features.is_empty() {
        debug!("Preparing {} RL decision details for batch insertion.", final_rl_features.len());
        Some(create_rl_decision_batch_data(
            &final_matches,
            pipeline_run_id,
            &final_rl_features,
            rl_tuner_version_i32,
        ))
    } else {
        info!("No RL features collected, skipping RL decision data creation.");
        None
    };

    info!(
        "Total matches found across all clusters: {}. Preparing for batch insertion (RL logging: {}).",
        final_matches.len(),
        rl_decision_data.is_some()
    );

    // Call the batch insertion function with all consolidated data
    insert_service_groups_batch(
        pool,
        final_matches.clone(),
        pipeline_run_id,
        rl_decision_data,
    ).await.context("Failed to perform final batch insertion of service groups and RL decision details")?;

    info!(
        "Enhanced service matching pipeline completed: {} total matches from {} clusters (RL enabled: {}, Filtering enabled: {})",
        final_matches.len(),
        stats.total_clusters_processed,
        service_rl_orchestrator.is_some(),
        service_contributor_filter.as_ref().map_or(false, |f| f.enabled)
    );

    Ok((final_matches, stats))
}

/// Run enhanced matching strategies for a single cluster with cross-validation and RL integration
/// FIXED: Updated to accept owned service contributor filter
async fn run_enhanced_service_matching_for_cluster(
    pool: &PgPool,
    cluster: ServiceCluster, // Takes ownership of ServiceCluster
    pipeline_run_id: &str,
    service_rl_orchestrator: Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    service_feature_cache: Option<SharedServiceFeatureCache>,
    service_contributor_filter: Option<crate::utils::service_contributor_filter::ServiceContributorFilterConfig>, // FIXED: Changed to owned type
    multi_progress: Option<MultiProgress>, 
) -> Result<(Vec<ServiceMatch>, Vec<(String, String, Vec<f64>)>, ServiceMatchingStats)> {
    let cluster_start = Instant::now();
    let mut cluster_stats = ServiceMatchingStats::default();

    let service_count = cluster.services.len();
    let cluster_id_str = cluster.cluster_id.as_deref().unwrap_or("unknown").to_string(); // Owned string

    debug!(
        "Processing cluster {:?} with {} services using enhanced strategies with RL integration (RL enabled: {}, Filtering enabled: {})",
        &cluster_id_str, // Use reference here
        service_count,
        service_rl_orchestrator.is_some(),
        service_contributor_filter.as_ref().map_or(false, |f| f.enabled)
    );

    if service_count < 2 {
        debug!("Skipping cluster {} with {} services (need at least 2)", &cluster_id_str, service_count);
        return Ok((Vec::new(), Vec::new(), cluster_stats));
    }

    // Wrap services in Arc for safe sharing across tasks
    let services_arc = Arc::new(cluster.services);
    let pipeline_run_id_str = pipeline_run_id.to_string();
    let cluster_id_opt = cluster.cluster_id.clone(); // Keep this as Option<String> for the clone

    let cluster_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(3)); // 3 strategies
        pb.set_style(
            ProgressStyle::default_bar()
                .template("      {spinner:.yellow} [{elapsed_precise}] {bar:20.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message(format!("Cluster {}: Processing {} services...", &cluster_id_str, service_count));
        Some(pb)
    } else {
        None
    };

    let mut raw_matches: Vec<MethodMatch> = Vec::new();
    let mut all_cluster_rl_features: Vec<(String, String, Vec<f64>)> = Vec::new();

    // 1. Email Matching - now uses pool instead of connection
    if let Some(pb) = &cluster_pb { 
        pb.set_message(format!("Cluster {}: Email matching...", &cluster_id_str)); 
    }
    
    match find_service_email_matches(
        pool, // Pass pool instead of connection
        &services_arc,
        &pipeline_run_id_str,
        cluster_id_opt.as_deref(), // Use as_deref here
        service_rl_orchestrator.clone(),
        service_feature_cache.clone(),
        None,
    ).await {
        Ok((email_matches_raw, email_rl_features, email_stats_from_task)) => {
            for email_match in email_matches_raw {
                raw_matches.push(MethodMatch {
                    method: MatchMethodType::ServiceEmailMatch,
                    confidence: email_match.confidence_score,
                    match_values: email_match.match_values,
                    service_id_1: email_match.service_id_1,
                    service_id_2: email_match.service_id_2,
                });
            }
            all_cluster_rl_features.extend(email_rl_features);
            cluster_stats.email_stats = email_stats_from_task;
        }
        Err(e) => warn!("Email matching failed for cluster {:?}: {}", &cluster_id_opt, e),
    }
    
    if let Some(pb) = &cluster_pb { pb.inc(1); }

    // 2. Name Matching - now uses pool instead of connection
    if let Some(pb) = &cluster_pb { 
        pb.set_message(format!("Cluster {}: Name matching...", &cluster_id_str)); 
    }
    
    let (name_matches, name_rl_features, name_stats_from_task) = run_enhanced_name_matching(
        pool, // Pass pool instead of connection
        &services_arc,
        // service_rl_orchestrator.clone(),
        None,
        service_feature_cache.clone(),
        None,
    ).await;
    
    for name_match in name_matches {
        raw_matches.push(MethodMatch {
            method: MatchMethodType::ServiceNameSimilarity,
            confidence: name_match.confidence_score,
            match_values: name_match.match_values,
            service_id_1: name_match.service_id_1,
            service_id_2: name_match.service_id_2,
        });
    }
    all_cluster_rl_features.extend(name_rl_features);
    cluster_stats.name_stats = name_stats_from_task;
    
    if let Some(pb) = &cluster_pb { pb.inc(1); }

    // 3. Embedding Matching - now uses pool instead of connection
    if let Some(pb) = &cluster_pb { 
        pb.set_message(format!("Cluster {}: Embedding matching...", &cluster_id_str)); 
    }
    
    let (embedding_matches, embedding_rl_features, embedding_stats_from_task) = 
        run_enhanced_embedding_matching(
            pool, // Pass pool instead of connection
            &services_arc,
            // service_rl_orchestrator.clone(),
            None,
            service_feature_cache.clone(),
            None,
        ).await;
    
    for embedding_match in embedding_matches {
        raw_matches.push(MethodMatch {
            method: MatchMethodType::ServiceEmbeddingSimilarity,
            confidence: embedding_match.confidence_score,
            match_values: embedding_match.match_values,
            service_id_1: embedding_match.service_id_1,
            service_id_2: embedding_match.service_id_2,
        });
    }
    all_cluster_rl_features.extend(embedding_rl_features);
    cluster_stats.embedding_stats = embedding_stats_from_task;
    
    if let Some(pb) = &cluster_pb { pb.inc(1); }

    // Convert raw matches to ServiceMatch
    let simple_matches: Vec<ServiceMatch> = raw_matches.into_iter().map(|method_match| {
        ServiceMatch {
            service_id_1: method_match.service_id_1,
            service_id_2: method_match.service_id_2,
            match_method: method_match.method,
            confidence_score: method_match.confidence,
            match_values: method_match.match_values,
        }
    }).collect();

    cluster_stats.aggregated_matches = 0;
    cluster_stats.cross_validation_rejections = 0;
    cluster_stats.confidence_boosts = 0;

    let total_cluster_time = cluster_start.elapsed();

    if let Some(pb) = &cluster_pb {
        pb.finish_and_clear();
    }

    debug!(
        "Cluster {:?} completed in {:.2?} - {} matches found (Filtering enabled: {})",
        &cluster_id_str,
        total_cluster_time,
        simple_matches.len(),
        service_contributor_filter.as_ref().map_or(false, |f| f.enabled)
    );

    Ok((simple_matches, all_cluster_rl_features, cluster_stats))
}