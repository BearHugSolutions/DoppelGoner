// src/bin/from_phase4.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::join_all; // Changed to join_all
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
    cluster_visualization, consolidate_clusters,
    db::{self, PgPool},
    models::{self, *},
    reinforcement::{
        entity::{
            feature_cache_service::{create_shared_cache, SharedFeatureCache},
            orchestrator::MatchingOrchestrator,
        },
        service::{
            service_feature_cache_prewarmer::{
                extract_and_store_all_service_features_and_prewarm_cache,
                prewarm_service_pair_features_cache,
            },
            service_feature_cache_service::{
                create_shared_service_cache, SharedServiceFeatureCache,
            },
            service_orchestrator::{self, ServiceMatchingOrchestrator},
        },
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

    info!("Starting HSDS pipeline from Phase 4 (Cluster consolidation)");
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

    // Run the pipeline from phase 4 onwards
    let stats: results::PipelineStats = run_pipeline_from_phase4(&pool, &mut phase_times).await?;

    let elapsed = start_time.elapsed();
    info!(
        "Pipeline (from Phase 4) completed in {:.2?}. Processed: {} entities, {} groups, {} clusters, {} service matches",
        elapsed,
        stats.total_entities,
        stats.total_groups,
        stats.total_clusters,
        stats.total_service_matches
    );

    // Generate and store report
    let description = Some("Pipeline run from Phase 4 (cluster consolidation) onwards".to_string());
    results::generate_report(&pool, stats, &phase_times, description).await?;

    Ok(())
}

async fn run_pipeline_from_phase4(
    pool: &PgPool,
    phase_times: &mut HashMap<String, Duration>,
) -> Result<results::PipelineStats> {
    info!("Initializing pipeline state from database...");

    let run_id_uuid = Uuid::new_v4();
    let run_id = run_id_uuid.to_string();
    let run_id_clone = run_id.clone();
    let run_timestamp = Utc::now().naive_utc();
    let description = Some("Pipeline run from Phase 4 onwards".to_string());

    // Create initial pipeline_run record
    info!("Creating pipeline_run record for Phase 4+ execution");
    db::create_initial_pipeline_run(pool, &run_id, run_timestamp, description.as_deref())
        .await
        .context("Failed to create initial pipeline_run record")?;

    // Load current state from database
    let current_state = load_pipeline_state(pool).await?;

    let mut stats = results::PipelineStats {
        run_id, // run_id is moved here
        run_timestamp,
        description: Some("Pipeline run from Phase 4 onwards".to_string()),

        // Use loaded state
        total_entities: current_state.total_entities,
        total_groups: current_state.total_groups,
        total_clusters: 0,        // Will be updated in Phase 4
        total_service_matches: 0, // Will be updated in Phase 6
        total_visualization_edges: 0,
        total_service_clusters: 0,
        total_service_visualization_edges: 0,

        // Initialize timing fields
        entity_processing_time: 0.0,          // Not run in this execution
        context_feature_extraction_time: 0.0, // Not run in this execution
        service_context_feature_extraction_time: 0.0,
        matching_time: 0.0, // Not run in this execution
        clustering_time: 0.0,
        visualization_edge_calculation_time: 0.0,
        service_matching_time: 0.0,
        total_processing_time: 0.0,
        service_clustering_time: 0.0,
        service_visualization_edge_calculation_time: 0.0,

        method_stats: Vec::new(),
        cluster_stats: None,
        service_stats: None,
    };

    info!(
        "Pipeline state loaded. Found {} entities, {} groups",
        stats.total_entities, stats.total_groups
    );

    // Initialize the ML reinforcement orchestrator for entities
    info!("Initializing ML-guided entity matching reinforcement orchestrator");
    let entity_reinforcement_orchestrator_instance = MatchingOrchestrator::new(pool)
        .await
        .context("Failed to initialize entity ML reinforcement orchestrator")?;
    let entity_reinforcement_orchestrator = Arc::new(Mutex::new(entity_reinforcement_orchestrator_instance));

    // Create and set entity feature cache on the orchestrator (though not used in this partial run directly for matching)
    let entity_feature_cache: SharedFeatureCache = create_shared_cache();
    {
        let mut orchestrator = entity_reinforcement_orchestrator.lock().await;
        orchestrator.set_feature_cache(entity_feature_cache.clone());
        info!("Entity feature cache attached to entity RL orchestrator (for potential future use in consolidation)");
    }

    info!("Starting from Phase 4...");

    // Phase 4: Cluster consolidation
    info!("Phase 4: Cluster consolidation");
    let phase4_start = Instant::now();
    stats.total_clusters =
        consolidate_clusters_helper(pool, entity_reinforcement_orchestrator, run_id_clone.clone()).await?; // Pass run_id_clone
    let phase4_duration = phase4_start.elapsed();
    phase_times.insert("cluster_consolidation".to_string(), phase4_duration);
    stats.clustering_time = phase4_duration.as_secs_f64();
    info!(
        "Formed {} clusters in {:.2?}. Phase 4 complete.",
        stats.total_clusters, phase4_duration
    );
    info!("Pipeline progress: [1/5] phases from Phase 4 (20%)");

    // Check for cluster changes AND existing visualization data
    let clusters_changed = stats.total_clusters > 0;
    let existing_viz_edges =
        cluster_visualization::visualization_edges_exist(pool, &run_id_clone).await?;

    if clusters_changed || !existing_viz_edges {
        // PHASE 5: Visualization edge calculation
        info!("Phase 5: Calculating entity relationship edges for cluster visualization");
        let phase5_start = Instant::now();
        cluster_visualization::ensure_visualization_tables_exist(pool).await?;
        stats.total_visualization_edges =
            cluster_visualization::calculate_visualization_edges(pool, &run_id_clone).await?;
        let phase5_duration = phase5_start.elapsed();
        phase_times.insert(
            "visualization_edge_calculation".to_string(),
            phase5_duration,
        );
        stats.visualization_edge_calculation_time = phase5_duration.as_secs_f64();
        info!(
            "Calculated {} entity relationship edges for visualization in {:.2?}. Phase 5 complete.",
            stats.total_visualization_edges, phase5_duration
        );
    } else {
        info!("No new clusters formed or existing clusters merged in Phase 4, and visualization edges already exist for this run. Skipping Phase 5 (Visualization Edge Calculation).");
        stats.total_visualization_edges = 0;
        stats.visualization_edge_calculation_time = 0.0;
    }
    info!("Pipeline progress: [2/5] phases from Phase 4 (40%)");

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

    // Phase 5.5: Service Context Feature Extraction and Cache Pre-warming
    info!("Phase 5.5: Service Context Feature Extraction and Cache Pre-warming");
    let phase5_5_start = Instant::now();

    let service_features_count =
        extract_and_store_all_service_features_and_prewarm_cache(pool, &service_feature_cache)
            .await?;

    let max_service_pairs = 100;
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
    info!("Pipeline progress: [2.5/5] phases from Phase 4 (~50%)");


    // PHASE 6: Service matching
    info!("Phase 6: Service matching");
    let phase6_start = Instant::now();

    let service_match_result = run_service_matching_pipeline(
        pool,
        &run_id_clone, // Use run_id_clone
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
            clusters_with_matches: 0,
        });
    }

    let phase6_duration = phase6_start.elapsed();
    phase_times.insert("service_matching".to_string(), phase6_duration);
    stats.service_matching_time = phase6_duration.as_secs_f64();
    info!(
        "Service matching processed in {:.2?}. Found {} service matches. Phase 6 complete.",
        phase6_duration, stats.total_service_matches
    );
    info!("Pipeline progress: [3/5] phases from Phase 4 (60%)");

    // Phase 7: Service cluster consolidation
    info!("Phase 7: Service cluster consolidation");
    let phase7_start = Instant::now();

    service_consolidate_clusters::ensure_consolidation_tables_exist(pool)
        .await
        .context("Failed to ensure consolidation tables exist")?;

    let consolidation_config = service_consolidate_clusters::ConsolidationConfig {
        similarity_threshold: 0.5,
        embedding_batch_size: 200,
        db_batch_size: 100,
        max_cache_size: 15000,
        min_cluster_size: 3,
        embedding_cache_duration_secs: 3600,
    };

    match service_consolidate_clusters::consolidate_service_clusters(
        pool,
        &run_id_clone, // Use run_id_clone
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
    info!("Pipeline progress: [4/5] phases from Phase 4 (80%)");

    // Phase 8: Service Visualization Edge Calculation
    info!("Phase 8: Calculating service relationship edges for cluster visualization");
    let phase8_start = Instant::now();
    service_cluster_visualization::ensure_visualization_tables_exist(pool).await?;
    stats.total_service_visualization_edges =
        service_cluster_visualization::calculate_visualization_edges(pool, &run_id_clone).await?; // Use run_id_clone
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
    info!("Pipeline progress: [5/5] phases from Phase 4 (100%)");

    stats.total_processing_time = stats.clustering_time
        + stats.visualization_edge_calculation_time
        + stats.service_context_feature_extraction_time
        + stats.service_matching_time
        + stats.service_clustering_time
        + stats.service_visualization_edge_calculation_time;

    Ok(stats)
}

#[derive(Debug)]
struct PipelineState {
    total_entities: usize,
    total_groups: usize,
}

async fn load_pipeline_state(pool: &PgPool) -> Result<PipelineState> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let entities_row = conn
        .query_one("SELECT COUNT(*) FROM public.entity", &[])
        .await
        .context("Failed to count entities")?;
    let total_entities: i64 = entities_row.get(0);

    let groups_row = conn
        .query_one("SELECT COUNT(*) FROM public.entity_group", &[])
        .await
        .context("Failed to count entity groups")?;
    let total_groups: i64 = groups_row.get(0);

    info!(
        "Loaded pipeline state: {} entities, {} entity groups",
        total_entities, total_groups
    );

    Ok(PipelineState {
        total_entities: total_entities as usize,
        total_groups: total_groups as usize,
    })
}

async fn consolidate_clusters_helper(
    pool: &PgPool,
    _reinforcement_orchestrator: Arc<Mutex<MatchingOrchestrator>>, // Keep for potential future use
    run_id: String, // run_id is passed as String
) -> Result<usize> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let unassigned_query =
        "SELECT COUNT(*) FROM public.entity_group WHERE group_cluster_id IS NULL";
    let groups_row = conn
        .query_one(unassigned_query, &[])
        .await
        .context("Failed to count groups")?;
    let unassigned_groups: i64 = groups_row.get(0);

    info!(
        "Found {} groups that need cluster assignment",
        unassigned_groups
    );

    if unassigned_groups == 0 {
        info!("No groups require clustering. Skipping consolidation.");
        return Ok(0);
    };

    consolidate_clusters::process_clusters(pool, &run_id) // Pass run_id as &str
        .await
        .context("Failed to process clusters")
}

async fn run_service_matching_pipeline(
    pool: &PgPool,
    pipeline_run_id: &str,
    service_orchestrator: Arc<Mutex<ServiceMatchingOrchestrator>>,
    feature_cache: SharedServiceFeatureCache,
) -> Result<results::ServiceMatchResult> {
    info!(
        "Starting cluster-scoped service matching pipeline with concurrency limit of {}...",
        MAX_CONCURRENT_MATCHING_TASKS
    );
    let start_time = Instant::now();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_MATCHING_TASKS));

    let conn_for_clusters = pool
        .get()
        .await
        .context("Failed to get DB connection for clusters")?;
    let cluster_rows = conn_for_clusters
        .query(
            "SELECT id FROM public.entity_group_cluster WHERE id IS NOT NULL",
            &[],
        )
        .await
        .context("Failed to query group_cluster IDs")?;
    
    let cluster_ids: Vec<String> = cluster_rows.into_iter().map(|row| row.get("id")).collect();
    drop(conn_for_clusters); // Release connection early

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
        "Found {} entity clusters for cluster-scoped service matching. Processing each with concurrency limit {}...",
        cluster_ids.len(),
        MAX_CONCURRENT_MATCHING_TASKS
    );

    let mut all_tasks: Vec<JoinHandle<Result<ServiceMatchResult, anyhow::Error>>> = Vec::new();

    for cluster_id_str in cluster_ids {
        let mut conn_for_service_details = pool.get().await.context(format!(
            "Failed to get DB connection for service details in cluster {}",
            cluster_id_str
        ))?;

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
        drop(conn_for_service_details);


        if service_detail_rows.is_empty() {
            debug!(
                "No active services found for cluster_id: {}. Skipping.",
                cluster_id_str
            );
            continue;
        }

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

        if !services_for_name_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let sem_clone = semaphore.clone();
            all_tasks.push(tokio::spawn(async move {
                let permit = sem_clone.acquire_owned().await.context("Permit acq failed (name)")?;
                let _p_guard = permit;
                service_matching::name::find_matches_in_cluster(
                    &task_pool, Some(task_orchestrator), &task_run_id, Some(task_feature_cache), services_for_name_matcher, task_cluster_id)
                .await
            }));
        }

        if !services_for_url_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let sem_clone = semaphore.clone();
            all_tasks.push(tokio::spawn(async move {
                let permit = sem_clone.acquire_owned().await.context("Permit acq failed (url)")?;
                let _p_guard = permit;
                service_matching::url::find_matches_in_cluster(
                    &task_pool, Some(task_orchestrator), &task_run_id, Some(task_feature_cache), services_for_url_matcher, task_cluster_id)
                .await
            }));
        }

        if !services_for_email_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let sem_clone = semaphore.clone();
            all_tasks.push(tokio::spawn(async move {
                let permit = sem_clone.acquire_owned().await.context("Permit acq failed (email)")?;
                let _p_guard = permit;
                service_matching::email::find_matches_in_cluster(
                    &task_pool, Some(task_orchestrator), &task_run_id, Some(task_feature_cache), services_for_email_matcher, task_cluster_id)
                .await
            }));
        }

        if !services_for_embedding_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();
            let sem_clone = semaphore.clone();
            all_tasks.push(tokio::spawn(async move {
                let permit = sem_clone.acquire_owned().await.context("Permit acq failed (embed)")?;
                let _p_guard = permit;
                service_matching::embedding::find_matches_in_cluster(
                    &task_pool, Some(task_orchestrator), &task_run_id, Some(task_feature_cache), services_for_embedding_matcher, task_cluster_id)
                .await
            }));
        }
    }

    let mut total_groups_created_all_methods = 0;
    let mut aggregated_method_stats: HashMap<String, MatchMethodStats> = HashMap::new();

    let task_join_results = join_all(all_tasks).await; // Changed to join_all

    for join_result in task_join_results {
        match join_result {
            Ok(task_outcome_result) => {
                match task_outcome_result {
                    Ok(service_match_result_for_method_in_cluster) => {
                        total_groups_created_all_methods +=
                            service_match_result_for_method_in_cluster.groups_created;
                        for stat in service_match_result_for_method_in_cluster.method_stats {
                            let method_key = stat.method_type.as_str().to_string();
                            let entry =
                                aggregated_method_stats
                                    .entry(method_key)
                                    .or_insert_with(|| MatchMethodStats {
                                        method_type: stat.method_type.clone(),
                                        groups_created: 0,
                                        entities_matched: 0,
                                        avg_confidence: 0.0,
                                        avg_group_size: 2.0,
                                    });
                            let total_confidence_points_before =
                                entry.avg_confidence * entry.groups_created as f64;
                            let current_confidence_points =
                                stat.avg_confidence * stat.groups_created as f64;
                            
                            entry.groups_created += stat.groups_created;
                            entry.entities_matched += stat.entities_matched;
                            if entry.groups_created > 0 {
                                entry.avg_confidence = (total_confidence_points_before
                                    + current_confidence_points)
                                    / entry.groups_created as f64;
                            } else {
                                entry.avg_confidence = 0.0;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("A service matching task returned an error: {:?}", e);
                    }
                }
            }
            Err(join_err) => {
                warn!(
                    "A service matching task failed to join (e.g., panicked): {:?}",
                    join_err
                );
            }
        }
    }

    info!("Processing human feedback for service confidence tuner");
    let mut orchestrator = service_orchestrator.lock().await;
    if let Err(e) = orchestrator.process_feedback_and_update_tuner(pool).await {
         warn!("Failed to process feedback and update tuner: {}",e);
    }
    if let Err(e) = orchestrator.save_models(pool).await {
        warn!("Failed to save models: {}",e);
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
            entities_matched: final_method_stats
                .iter()
                .map(|s| s.entities_matched)
                .sum::<usize>(),
            avg_confidence,
            avg_group_size: if total_groups_created_all_methods > 0 { 2.0 } else {0.0}, // Default for pairwise
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
