// src/bin/from_phase5_5.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::join_all; // Changed to join_all to handle results individually
use log::{debug, info, warn};
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Semaphore}, // Added Semaphore
    task::JoinHandle,
};
use uuid::Uuid;

use dedupe_lib::{
    db::{self, PgPool},
    models::{self, *},
    reinforcement::service::{
        service_feature_cache_prewarmer::{
            extract_and_store_all_service_features_and_prewarm_cache,
            prewarm_service_pair_features_cache,
        },
        service_feature_cache_service::{create_shared_service_cache, SharedServiceFeatureCache},
        service_orchestrator::{self, ServiceMatchingOrchestrator},
    },
    results::{self, MatchMethodStats, PipelineStats, ServiceMatchResult},
    service_cluster_visualization, service_consolidate_clusters, service_matching,
};

// Define the concurrency limit for matching tasks
const MAX_CONCURRENT_MATCHING_TASKS: usize = 20;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Starting HSDS pipeline from Phase 5.5 (Service Context Feature Extraction)");
    let start_time = Instant::now();

    // Try to load .env file if it exists
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

    // Connect to the database
    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;
    info!("Successfully connected to the database");

    // Capture timing information for each phase
    let mut phase_times = HashMap::new();

    // Run the pipeline from phase 5.5 onwards
    let stats: results::PipelineStats = run_pipeline_from_phase5_5(&pool, &mut phase_times).await?;

    let elapsed = start_time.elapsed();
    info!(
        "Pipeline (from Phase 5.5) completed in {:.2?}. Processed: {} entities, {} groups, {} clusters, {} service matches",
        elapsed,
        stats.total_entities,
        stats.total_groups,
        stats.total_clusters,
        stats.total_service_matches
    );

    // Generate and store report
    let description = Some(
        "Pipeline run from Phase 5.5 (service context feature extraction) onwards".to_string(),
    );
    results::generate_report(&pool, stats, &phase_times, description).await?;

    Ok(())
}

async fn run_pipeline_from_phase5_5(
    pool: &PgPool,
    phase_times: &mut HashMap<String, Duration>,
) -> Result<results::PipelineStats> {
    info!("Initializing pipeline state from database...");

    let run_id_uuid = Uuid::new_v4();
    let run_id = run_id_uuid.to_string();
    let run_id_clone = run_id.clone();
    let run_timestamp = Utc::now().naive_utc();
    let description = Some("Pipeline run from Phase 5.5 onwards".to_string());

    // Create initial pipeline_run record
    info!("Creating pipeline_run record for Phase 5.5+ execution");
    db::create_initial_pipeline_run(pool, &run_id, run_timestamp, description.as_deref())
        .await
        .context("Failed to create initial pipeline_run record")?;

    // Load current state from database
    let current_state = load_pipeline_state(pool).await?;

    let mut stats = results::PipelineStats {
        run_id,
        run_timestamp,
        description: Some("Pipeline run from Phase 5.5 onwards".to_string()),

        // Use loaded state
        total_entities: current_state.total_entities,
        total_groups: current_state.total_groups,
        total_clusters: current_state.total_clusters, // Load existing clusters
        total_service_matches: 0,                     // Will be updated in Phase 6
        total_visualization_edges: 0,
        total_service_clusters: 0,
        total_service_visualization_edges: 0,

        // Initialize timing fields (phases not run in this execution are set to 0)
        entity_processing_time: 0.0,          // Not run in this execution
        context_feature_extraction_time: 0.0, // Not run in this execution
        service_context_feature_extraction_time: 0.0,
        matching_time: 0.0,                       // Not run in this execution
        clustering_time: 0.0,                     // Not run in this execution
        visualization_edge_calculation_time: 0.0, // Not run in this execution
        service_matching_time: 0.0,
        total_processing_time: 0.0,
        service_clustering_time: 0.0,
        service_visualization_edge_calculation_time: 0.0,

        method_stats: Vec::new(),
        cluster_stats: None,
        service_stats: None,
    };

    info!(
        "Pipeline state loaded. Found {} entities, {} groups, {} clusters",
        stats.total_entities, stats.total_groups, stats.total_clusters
    );

    // Initialize service RL orchestrator
    info!("Initializing service RL orchestrator");
    let service_orchestrator_instance = ServiceMatchingOrchestrator::new(pool)
        .await
        .context("Failed to initialize service orchestrator")?;
    let service_orchestrator = Arc::new(Mutex::new(service_orchestrator_instance));

    let service_feature_cache = create_shared_service_cache();

    // Set the feature cache on the service orchestrator
    {
        let mut orchestrator = service_orchestrator.lock().await;
        orchestrator.set_feature_cache(service_feature_cache.clone());
        info!("Feature cache attached to service RL orchestrator");
    }

    info!("Starting from Phase 5.5...");

    // Phase 5.5: Service Context Feature Extraction and Cache Pre-warming
    info!("Phase 5.5: Service Context Feature Extraction and Cache Pre-warming");
    let phase5_5_start = Instant::now();

    // Extract and store individual service features
    let service_features_count =
        extract_and_store_all_service_features_and_prewarm_cache(pool, &service_feature_cache)
            .await?;

    // Pre-warm service pair features cache
    let max_service_pairs = 100; // Example value, adjust as needed
    match prewarm_service_pair_features_cache(pool, &service_feature_cache, max_service_pairs).await {
        Ok(pairs_count) => info!(
            "Successfully pre-warmed service pair features cache for {} likely service pairs.",
            pairs_count
        ),
        Err(e) => warn!(
            "Service pair features cache pre-warming failed: {}. Proceeding with on-demand feature extraction.",
            e
        ),
    }

    let phase5_5_duration = phase5_5_start.elapsed();
    phase_times.insert(
        "service_context_feature_extraction_and_prewarming".to_string(),
        phase5_5_duration,
    );
    stats.service_context_feature_extraction_time = phase5_5_duration.as_secs_f64();
    info!(
        "Extracted and pre-warmed context features for {} services in {:.2?}. Phase 5.5 complete.",
        service_features_count, phase5_5_duration
    );
    info!("Pipeline progress: [1/4] phases from Phase 5.5 (25%)");

    // PHASE 6: Service matching
    info!("Phase 6: Service matching");
    let phase6_start = Instant::now();

    // Run the service matching pipeline
    let service_match_result = run_service_matching_pipeline(
        pool,
        &run_id_clone,
        service_orchestrator.clone(),
        service_feature_cache.clone(),
    )
    .await
    .context("Service matching failed")?;

    stats.total_service_matches = service_match_result.groups_created;
    stats
        .method_stats
        .extend(service_match_result.method_stats.clone());

    if stats.service_stats.is_none() {
        // Create basic service stats from the result
        stats.service_stats = Some(results::ServiceMatchStats {
            total_matches: service_match_result.groups_created,
            avg_similarity: service_match_result.stats.avg_confidence,
            high_similarity_matches: service_match_result
                .method_stats
                .iter()
                .filter(|s| s.avg_confidence >= 0.9)
                .map(|s| s.groups_created)
                .sum(),
            medium_similarity_matches: service_match_result
                .method_stats
                .iter()
                .filter(|s| s.avg_confidence >= 0.8 && s.avg_confidence < 0.9)
                .map(|s| s.groups_created)
                .sum(),
            low_similarity_matches: service_match_result
                .method_stats
                .iter()
                .filter(|s| s.avg_confidence < 0.8)
                .map(|s| s.groups_created)
                .sum(),
            clusters_with_matches: 0, // This might need more sophisticated calculation based on results
        });
    }

    let phase6_duration = phase6_start.elapsed();
    phase_times.insert("service_matching".to_string(), phase6_duration);
    stats.service_matching_time = phase6_duration.as_secs_f64();
    info!(
        "Service matching processed in {:.2?}. Found {} service matches. Phase 6 complete.",
        phase6_duration, stats.total_service_matches
    );
    info!("Pipeline progress: [2/4] phases from Phase 5.5 (50%)");

    // Phase 7: Service cluster consolidation
    info!("Phase 7: Service cluster consolidation");
    let phase7_start = Instant::now();

    // Ensure consolidation tables exist before processing
    service_consolidate_clusters::ensure_consolidation_tables_exist(pool)
        .await
        .context("Failed to ensure consolidation tables exist")?;

    // Custom config with performance optimizations
    let consolidation_config = service_consolidate_clusters::ConsolidationConfig {
        similarity_threshold: 0.5, // Lower threshold for more broad clustering
        embedding_batch_size: 200, // Larger batches if memory allows
        db_batch_size: 100,        // Larger DB batches for I/O efficiency
        max_cache_size: 15000,     // Larger cache for better hit rates
        min_cluster_size: 3,       // Only consolidate clusters with 3+ services
        embedding_cache_duration_secs: 3600, // Explicitly set or use default (adjust as needed)
    };

    match service_consolidate_clusters::consolidate_service_clusters(
        pool,
        &run_id_clone,
        Some(consolidation_config),
    )
    .await
    {
        Ok(merged_clusters) => {
            stats.total_service_clusters = merged_clusters;
            info!(
                "Successfully consolidated {} service cluster pairs",
                merged_clusters
            );
        }
        Err(e) => {
            warn!("Service cluster consolidation encountered an error: {}. Continuing with existing clusters.", e);
            // Optionally, fetch current cluster count if consolidation fails
            stats.total_service_clusters = 0;
        }
    }

    let phase7_duration = phase7_start.elapsed();
    phase_times.insert("service_clustering".to_string(), phase7_duration);
    stats.service_clustering_time = phase7_duration.as_secs_f64();
    info!(
        "Service cluster consolidation phase completed in {:.2?}. Phase 7 complete.",
        phase7_duration
    );
    info!("Pipeline progress: [3/4] phases from Phase 5.5 (75%)");

    // Phase 8: Service Visualization Edge Calculation
    info!("Phase 8: Calculating service relationship edges for cluster visualization");
    let phase8_start = Instant::now();
    service_cluster_visualization::ensure_visualization_tables_exist(pool).await?;
    stats.total_service_visualization_edges =
        service_cluster_visualization::calculate_visualization_edges(pool, &run_id_clone).await?;
    let phase8_duration = phase8_start.elapsed();
    phase_times.insert(
        "service_visualization_edge_calculation".to_string(),
        phase8_duration,
    );
    stats.service_visualization_edge_calculation_time = phase8_duration.as_secs_f64();
    info!(
        "Calculated {} service relationship edges for visualization in {:.2?}. Phase 8 complete.",
        stats.total_service_visualization_edges, phase8_duration
    );
    info!("Pipeline progress: [4/4] phases from Phase 5.5 (100%)");

    // Calculate total processing time for phases run in this execution
    stats.total_processing_time = stats.service_context_feature_extraction_time
        + stats.service_matching_time
        + stats.service_clustering_time
        + stats.service_visualization_edge_calculation_time;

    Ok(stats)
}

#[derive(Debug)]
struct PipelineState {
    total_entities: usize,
    total_groups: usize,
    total_clusters: usize,
}

async fn load_pipeline_state(pool: &PgPool) -> Result<PipelineState> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Get total entities count
    let entities_row = conn
        .query_one("SELECT COUNT(*) FROM public.entity", &[])
        .await
        .context("Failed to count entities")?;
    let total_entities: i64 = entities_row.get(0);

    // Get total entity groups (pairwise links) count
    let groups_row = conn
        .query_one("SELECT COUNT(*) FROM public.entity_group", &[])
        .await
        .context("Failed to count entity groups")?;
    let total_groups: i64 = groups_row.get(0);

    // Get total clusters count
    let clusters_row = conn
        .query_one("SELECT COUNT(*) FROM public.entity_group_cluster", &[])
        .await
        .context("Failed to count clusters")?;
    let total_clusters: i64 = clusters_row.get(0);

    info!(
        "Loaded pipeline state: {} entities, {} entity groups, {} clusters",
        total_entities, total_groups, total_clusters
    );

    Ok(PipelineState {
        total_entities: total_entities as usize,
        total_groups: total_groups as usize,
        total_clusters: total_clusters as usize,
    })
}

async fn run_service_matching_pipeline(
    pool: &PgPool,
    pipeline_run_id: &str,
    service_orchestrator: Arc<Mutex<ServiceMatchingOrchestrator>>,
    feature_cache: SharedServiceFeatureCache,
) -> Result<results::ServiceMatchResult> {
    info!("Starting cluster-scoped service matching pipeline...");
    let start_time = Instant::now();

    // 1. Get all cluster IDs
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for clusters")?;
    let cluster_rows = conn
        .query(
            "SELECT id FROM public.entity_group_cluster WHERE id IS NOT NULL",
            &[],
        )
        .await
        .context("Failed to query group_cluster IDs")?;

    let cluster_ids: Vec<String> = cluster_rows.into_iter().map(|row| row.get("id")).collect();

    if cluster_ids.is_empty() {
        info!("No entity clusters found. Service matching will be skipped.");
        return Ok(results::ServiceMatchResult {
            groups_created: 0,
            stats: results::MatchMethodStats {
                method_type: MatchMethodType::Custom("service_combined_skipped".to_string()),
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
            },
            method_stats: vec![],
        });
    }
    info!(
        "Found {} entity clusters for cluster-scoped service matching. Processing each with a concurrency limit of {}...",
        cluster_ids.len(),
        MAX_CONCURRENT_MATCHING_TASKS
    );

    // Create a semaphore to limit concurrent tasks
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_MATCHING_TASKS));
    let mut all_tasks: Vec<JoinHandle<Result<ServiceMatchResult, anyhow::Error>>> = Vec::new();

    for cluster_id_str in cluster_ids {
        // 2. For each cluster_id, get its services' relevant details
        // It's important to get a new connection here or ensure the previous one is released
        // if the loop is long-running or if connection pooling is tight.
        // For simplicity, we'll re-acquire a connection from the pool for this query.
        let mut conn_for_service_details = pool
            .get()
            .await
            .context(format!("Failed to get DB connection for services in cluster {}", cluster_id_str))?;

        let service_detail_rows = conn_for_service_details
            .query(
                "WITH cluster_entities AS (
                SELECT entity_id_1 AS entity_id FROM public.entity_group WHERE group_cluster_id = $1
                UNION
                SELECT entity_id_2 AS entity_id FROM public.entity_group WHERE group_cluster_id = $1
            )
            SELECT
                s.id,
                s.name,
                s.url,
                s.email,
                s.embedding_v2
            FROM public.service s
            JOIN public.entity_feature ef ON s.id = ef.table_id AND ef.table_name = 'service'
            WHERE ef.entity_id IN (SELECT entity_id FROM cluster_entities)
              AND s.status = 'active'
            GROUP BY s.id, s.name, s.url, s.email, s.embedding_v2",
                &[&cluster_id_str],
            )
            .await
            .context(format!(
                "Failed to query services for cluster {}",
                cluster_id_str
            ))?;

        if service_detail_rows.is_empty() {
            debug!(
                "No active services found for cluster_id: {}. Skipping.",
                cluster_id_str
            );
            continue;
        }

        // Prepare data for each matching method
        let mut services_for_name_matcher: Vec<(ServiceId, String)> = Vec::new();
        let mut services_for_url_matcher: Vec<(ServiceId, String)> = Vec::new();
        let mut services_for_email_matcher: Vec<(ServiceId, String)> = Vec::new();
        let mut services_for_embedding_matcher: Vec<(ServiceId, String, Option<Vec<f32>>)> =
            Vec::new();

        for row in &service_detail_rows {
            let service_id_val: String = row.get("id");
            let service_id = ServiceId(service_id_val);
            let name_val: Option<String> = row.get("name");
            let url_val: Option<String> = row.get("url");
            let email_val: Option<String> = row.get("email");
            let embedding_pg_opt: Option<pgvector::Vector> = row.get("embedding_v2");

            if let Some(name) = name_val.clone() {
                if !name.is_empty() {
                    services_for_name_matcher.push((service_id.clone(), name.clone()));
                    if let Some(embedding_pg) = embedding_pg_opt.clone() {
                        services_for_embedding_matcher.push((
                            service_id.clone(),
                            name,
                            Some(embedding_pg.to_vec()),
                        ));
                    } else {
                        services_for_embedding_matcher.push((service_id.clone(), name, None));
                    }
                }
            }
            if let Some(url) = url_val {
                if !url.is_empty() {
                    services_for_url_matcher.push((service_id.clone(), url));
                }
            }
            if let Some(email) = email_val {
                if !email.is_empty() {
                    services_for_email_matcher.push((service_id.clone(), email));
                }
            }
        }

        // Spawn matching tasks for each method type, respecting the semaphore
        if !services_for_name_matcher.is_empty() {
            let task_pool: PgPool = pool.clone();
            let task_orchestrator: Arc<Mutex<ServiceMatchingOrchestrator>> =
                service_orchestrator.clone();
            let task_run_id: String = pipeline_run_id.to_string();
            let task_feature_cache: SharedServiceFeatureCache = feature_cache.clone();
            let task_cluster_id: String = cluster_id_str.clone();
            let semaphore_clone = semaphore.clone(); // Clone Arc for the new task

            all_tasks.push(tokio::spawn(async move {
                let permit = semaphore_clone
                    .acquire_owned()
                    .await
                    .context("Failed to acquire semaphore permit for name matching")?;
                // Permit is held by _permit_guard and released when it goes out of scope
                let _permit_guard = permit;

                let result = service_matching::name::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_name_matcher,
                    task_cluster_id,
                )
                .await;
                result // Return the Result<ServiceMatchResult, anyhow::Error>
            }));
        }

        if !services_for_url_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let semaphore_clone = semaphore.clone();

            all_tasks.push(tokio::spawn(async move {
                let permit = semaphore_clone
                    .acquire_owned()
                    .await
                    .context("Failed to acquire semaphore permit for URL matching")?;
                let _permit_guard = permit;

                let result = service_matching::url::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_url_matcher,
                    task_cluster_id,
                )
                .await;
                result
            }));
        }

        if !services_for_email_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let semaphore_clone = semaphore.clone();

            all_tasks.push(tokio::spawn(async move {
                let permit = semaphore_clone
                    .acquire_owned()
                    .await
                    .context("Failed to acquire semaphore permit for email matching")?;
                let _permit_guard = permit;

                let result = service_matching::email::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_email_matcher,
                    task_cluster_id,
                )
                .await;
                result
            }));
        }

        if !services_for_embedding_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let semaphore_clone = semaphore.clone();

            all_tasks.push(tokio::spawn(async move {
                let permit = semaphore_clone
                    .acquire_owned()
                    .await
                    .context("Failed to acquire semaphore permit for embedding matching")?;
                let _permit_guard = permit;

                let result = service_matching::embedding::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_embedding_matcher,
                    task_cluster_id,
                )
                .await;
                result
            }));
        }
    }

    // Aggregate results from all tasks
    let mut total_groups_created_all_methods = 0;
    let mut aggregated_method_stats: HashMap<String, MatchMethodStats> = HashMap::new();

    // Use join_all to wait for all tasks and collect their results
    let task_join_results = join_all(all_tasks).await;

    for join_result in task_join_results {
        match join_result {
            Ok(task_outcome_result) => { // Task completed its execution (didn't panic)
                match task_outcome_result { // Check the Result from the task's async block
                    Ok(service_match_result) => {
                        total_groups_created_all_methods += service_match_result.groups_created;
                        for stat in service_match_result.method_stats {
                            let method_key = stat.method_type.as_str().to_string();
                            let entry =
                                aggregated_method_stats
                                    .entry(method_key)
                                    .or_insert_with(|| MatchMethodStats {
                                        method_type: stat.method_type.clone(),
                                        groups_created: 0,
                                        entities_matched: 0,
                                        avg_confidence: 0.0,
                                        avg_group_size: 2.0, // Default for pairwise
                                    });
                            // Correctly update average confidence
                            let total_confidence_points_before =
                                entry.avg_confidence * entry.groups_created as f64;
                            let current_confidence_points =
                                stat.avg_confidence * stat.groups_created as f64;

                            entry.groups_created += stat.groups_created;
                            entry.entities_matched += stat.entities_matched; // Assuming this is additive

                            if entry.groups_created > 0 {
                                entry.avg_confidence = (total_confidence_points_before
                                    + current_confidence_points)
                                    / entry.groups_created as f64;
                            } else {
                                entry.avg_confidence = 0.0; // Avoid division by zero
                            }
                        }
                    }
                    Err(e) => {
                        // An error occurred within the task's logic (e.g., DB error, permit error)
                        warn!("A service matching task returned an error: {:?}", e);
                    }
                }
            }
            Err(join_err) => {
                // The task panicked or was cancelled
                warn!(
                    "A service matching task failed to join (e.g., panicked or was cancelled): {:?}",
                    join_err
                );
                // Depending on requirements, you might want to propagate this as a fatal error for the pipeline
                // For now, we log and continue aggregating other results.
                // return Err(anyhow::Error::from(join_err)
                //     .context("A service matching task panicked or was cancelled"));
            }
        }
    }


    // Process feedback and save models
    info!("Processing human feedback for service confidence tuner");
    let mut orchestrator = service_orchestrator.lock().await;
    if let Err(e) = orchestrator.process_feedback_and_update_tuner(pool).await {
        warn!("Failed to process feedback and update tuner: {}", e);
    }
    if let Err(e) = orchestrator.save_models(pool).await {
        warn!("Failed to save RL models: {}", e);
    }

    if let Some((hits, misses, ind_hits, ind_misses)) = orchestrator.get_feature_cache_stats().await
    {
        let hit_rate = if hits + misses > 0 {
            (hits as f64 / (hits + misses) as f64) * 100.0
        } else {
            0.0
        };
        let individual_hit_rate = if ind_hits + ind_misses > 0 {
            (ind_hits as f64 / (ind_hits + ind_misses) as f64) * 100.0
        } else {
            0.0
        };
        info!(
            "Feature cache statistics - Pair features: {} hits, {} misses, {:.2}% hit rate",
            hits, misses, hit_rate
        );
        info!(
            "Feature cache statistics - Individual features: {} hits, {} misses, {:.2}% hit rate",
            ind_hits, ind_misses, individual_hit_rate
        );
    }

    let final_method_stats: Vec<MatchMethodStats> =
        aggregated_method_stats.values().cloned().collect();

    let combined_stats = if !final_method_stats.is_empty() {
        let total_avg_confidence_points: f64 = final_method_stats
            .iter()
            .map(|s| s.avg_confidence * s.groups_created as f64)
            .sum();
        let total_groups_for_avg: usize = final_method_stats.iter().map(|s| s.groups_created).sum();
        let avg_confidence = if total_groups_for_avg > 0 {
            total_avg_confidence_points / total_groups_for_avg as f64
        } else {
            0.0
        };

        results::MatchMethodStats {
            method_type: MatchMethodType::Custom("service_combined_all_clusters".to_string()),
            groups_created: total_groups_created_all_methods,
            entities_matched: final_method_stats // Sum of entities matched by each method
                .iter()
                .map(|s| s.entities_matched)
                .sum::<usize>(),
            avg_confidence,
            avg_group_size: if total_groups_created_all_methods > 0 {
                 // A more accurate avg_group_size would require tracking individual group sizes
                 // For now, using 2.0 as a placeholder if groups were created.
                2.0
            } else {
                0.0
            },
        }
    } else {
        results::MatchMethodStats {
            method_type: MatchMethodType::Custom("service_combined_no_matches".to_string()),
            groups_created: 0,
            entities_matched: 0,
            avg_confidence: 0.0,
            avg_group_size: 0.0,
        }
    };

    info!(
        "Cluster-scoped service matching complete in {:.2?}. Created {} service groups total across all clusters.",
        start_time.elapsed(), total_groups_created_all_methods
    );

    Ok(results::ServiceMatchResult {
        groups_created: total_groups_created_all_methods,
        stats: combined_stats,
        method_stats: final_method_stats,
    })
}
