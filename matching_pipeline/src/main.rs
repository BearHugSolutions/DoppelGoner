// src/main.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::join_all;
use log::{debug, info, warn};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinHandle,
};
use uuid::Uuid;

use dedupe_lib::{
    consolidate_clusters,
    db::{self, PgPool},
    entity_organizations,
    matching,
    models::{self, *}, // Import all models
    reinforcement::{
        entity::{
            feature_cache_prewarmer::{
                extract_and_store_all_entity_features_and_prewarm_cache,
                prewarm_pair_features_cache,
            },
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
    results::{self, AnyMatchResult, MatchMethodStats, PipelineStats, ServiceMatchResult},
};

// Define the concurrency limit for matching tasks
const MAX_CONCURRENT_MATCHING_TASKS: usize = 10; // Default, can be overridden by BatchConfig

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting HSDS entity grouping and clustering pipeline");
    let start_time = Instant::now();

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

    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;
    info!("Successfully connected to the database");

    let mut phase_times = HashMap::new();
    let stats: results::PipelineStats = run_pipeline(&pool, &mut phase_times).await?;
    let elapsed = start_time.elapsed();
    info!(
        "Pipeline completed in {:.2?}. Processed: {} entities, {} groups, {} clusters, {} service matches",
        elapsed,
        stats.total_entities,
        stats.total_groups,
        stats.total_clusters,
        stats.total_service_matches
    );

    let description = Some("Full pipeline run with standard matching configuration".to_string());
    results::generate_report(&pool, stats, &phase_times, description).await?;
    Ok(())
}

async fn run_pipeline(
    pool: &PgPool,
    phase_times: &mut HashMap<String, Duration>,
) -> Result<results::PipelineStats> {
    let run_id_uuid = Uuid::new_v4();
    let run_id = run_id_uuid.to_string();
    let run_timestamp = Utc::now().naive_utc();
    let description = Some("Regular pipeline run".to_string());

    db::create_initial_pipeline_run(pool, &run_id, run_timestamp, description.as_deref())
        .await
        .context("Failed to create initial pipeline_run record")?;

    let mut stats = results::PipelineStats {
        run_id: run_id.clone(),
        run_timestamp,
        description,
        total_entities: 0,
        total_groups: 0,
        total_clusters: 0,
        total_service_matches: 0,
        total_visualization_edges: 0,
        total_service_clusters: 0,
        total_service_visualization_edges: 0,
        entity_processing_time: 0.0,
        context_feature_extraction_time: 0.0,
        service_context_feature_extraction_time: 0.0,
        matching_time: 0.0,
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

    info!("Pipeline started. Progress: [0/4] phases (0%)");

    // Phase 1: Entity identification
    let phase1_start = Instant::now();
    stats.total_entities = identify_entities(pool).await?;
    let phase1_duration = phase1_start.elapsed();
    phase_times.insert("entity_identification".to_string(), phase1_duration);
    stats.entity_processing_time = phase1_duration.as_secs_f64();
    info!(
        "Identified {} entities in {:.2?}. Phase 1 complete.",
        stats.total_entities, phase1_duration
    );
    info!("Pipeline progress: [1/4] phases (25%)");

    // Phase 2: Entity Context Feature Extraction and Cache Pre-warming
    let entity_feature_cache: SharedFeatureCache = create_shared_cache();
    let phase2_start = Instant::now();
    if stats.total_entities > 0 {
        match extract_and_store_all_entity_features_and_prewarm_cache(pool, &entity_feature_cache)
            .await
        {
            Ok(count) => info!(
                "Entity context features & cache pre-warmed for {} entities.",
                count
            ),
            Err(e) => warn!(
                "Entity context feature extraction/cache pre-warming failed: {}.",
                e
            ),
        }
        let max_pairs = 100; // Example value, adjust as needed
        match prewarm_pair_features_cache(pool, &entity_feature_cache, max_pairs).await {
            Ok(count) => info!("Pre-warmed pair features cache for {} entity pairs.", count),
            Err(e) => warn!("Pair features cache pre-warming failed: {}.", e),
        }
    } else {
        info!("Skipping entity context feature extraction: no entities.");
    }
    let phase2_duration = phase2_start.elapsed();
    phase_times.insert(
        "entity_context_feature_extraction_and_prewarming".to_string(),
        phase2_duration,
    );
    stats.context_feature_extraction_time = phase2_duration.as_secs_f64();
    info!("Entity Context Feature Extraction and Cache Pre-warming complete in {:.2?}. Phase 2 complete.", phase2_duration);
    info!("Pipeline progress: [2/4] phases (50%)");

    // Initialize Entity MatchingOrchestrator
    let entity_matching_orchestrator_instance = MatchingOrchestrator::new(pool)
        .await
        .context("Failed to initialize entity MatchingOrchestrator")?;
    let entity_matching_orchestrator = Arc::new(Mutex::new(entity_matching_orchestrator_instance));
    {
        let mut orchestrator_guard = entity_matching_orchestrator.lock().await;
        orchestrator_guard.set_feature_cache(entity_feature_cache.clone());
    }

    // Phase 3: Entity matching
    let phase3_start = Instant::now();
    let (total_groups, method_stats_match) = run_entity_matching_pipeline(
        pool,
        entity_matching_orchestrator.clone(),
        stats.run_id.clone(),
        entity_feature_cache.clone(),
    )
    .await?;
    stats.total_groups = total_groups;
    stats.method_stats.extend(method_stats_match);
    let phase3_duration = phase3_start.elapsed();
    phase_times.insert("entity_matching".to_string(), phase3_duration);
    stats.matching_time = phase3_duration.as_secs_f64();
    info!(
        "Created {} entity groups in {:.2?}. Phase 3 complete.",
        stats.total_groups, phase3_duration
    );
    info!("Pipeline progress: [3/4] phases (75%)");

    // Phase 4: Cluster consolidation & Visualization Edge Calculation
    let phase4_start = Instant::now();
    let (clusters_created, viz_edges_created) =
        consolidate_clusters_helper(pool, stats.run_id.clone()).await?;
    stats.total_clusters = clusters_created;
    stats.total_visualization_edges = viz_edges_created;
    let phase4_duration = phase4_start.elapsed();
    phase_times.insert("cluster_consolidation".to_string(), phase4_duration);
    stats.clustering_time = phase4_duration.as_secs_f64();
    // Visualization edge calculation is now part of the clustering time and not a separate step
    stats.visualization_edge_calculation_time = 0.0;
    info!(
        "Formed {} clusters and {} visualization edges in {:.2?}. Phase 4 complete.",
        stats.total_clusters, stats.total_visualization_edges, phase4_duration
    );
    info!("Pipeline progress: [4/4] phases (100%)");

    stats.total_processing_time = phase_times.values().map(|d| d.as_secs_f64()).sum();
    Ok(stats)
}

async fn identify_entities(pool: &PgPool) -> Result<usize> {
    info!("Phase 1: Entity identification starting...");
    let org_entities = entity_organizations::extract_entities(pool)
        .await
        .context("Failed to extract entities from organizations")?;
    info!(
        "Discovered/created {} org-entity mappings.",
        org_entities.len()
    );

    entity_organizations::link_entity_features(pool, &org_entities)
        .await
        .context("Failed to link entity features")?;
    let updated_features_count = entity_organizations::update_entity_features(pool)
        .await
        .context("Failed to update entity features")?;
    info!(
        "Added {} new feature links to existing entities.",
        updated_features_count
    );

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for counting entities")?;
    let total_entities_row = conn
        .query_one("SELECT COUNT(*) FROM public.entity", &[])
        .await
        .context("Failed to count total entities")?;
    let total_entities_count: i64 = total_entities_row.get(0);
    info!(
        "Entity identification complete. Total entities: {}",
        total_entities_count
    );
    Ok(total_entities_count as usize)
}

async fn run_entity_matching_pipeline(
    pool: &PgPool,
    entity_matching_orchestrator: Arc<Mutex<MatchingOrchestrator>>,
    run_id: String,
    entity_feature_cache: SharedFeatureCache,
) -> Result<(usize, Vec<MatchMethodStats>)> {
    info!(
        "Initializing entity matching pipeline with feature caching and concurrency limit of {}...",
        MAX_CONCURRENT_MATCHING_TASKS
    );
    let start_time_matching = Instant::now();
    let mut tasks: Vec<JoinHandle<Result<AnyMatchResult, anyhow::Error>>> = Vec::new();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_MATCHING_TASKS));

    // --- Spawn Email Matching Task (Entity) ---
    let pool_clone_email = pool.clone();
    let orchestrator_clone_email = entity_matching_orchestrator.clone();
    let run_id_clone_email = run_id.clone();
    let feature_cache_clone_email = entity_feature_cache.clone();
    let semaphore_clone_email = semaphore.clone();
    tasks.push(tokio::spawn(async move {
        let permit = semaphore_clone_email
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit for entity email matching")?;
        let _permit_guard = permit; // Guard ensures permit is released when task finishes
        matching::email::find_matches(
            &pool_clone_email,
            Some(orchestrator_clone_email),
            &run_id_clone_email,
            Some(feature_cache_clone_email),
        )
        .await
        .context("Entity email matching failed")
    }));

    // --- Spawn Phone Matching Task (Entity) ---
    let pool_clone_phone = pool.clone();
    let orchestrator_clone_phone = entity_matching_orchestrator.clone();
    let run_id_clone_phone = run_id.clone();
    let feature_cache_clone_phone = entity_feature_cache.clone();
    let semaphore_clone_phone = semaphore.clone();
    tasks.push(tokio::spawn(async move {
        let permit = semaphore_clone_phone
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit for entity phone matching")?;
        let _permit_guard = permit;
        matching::phone::find_matches(
            &pool_clone_phone,
            Some(orchestrator_clone_phone),
            &run_id_clone_phone,
            Some(feature_cache_clone_phone),
        )
        .await
        .context("Entity phone matching failed")
    }));

    // --- Spawn URL Matching Task (Entity) ---
    let pool_clone_url = pool.clone();
    let orchestrator_clone_url = entity_matching_orchestrator.clone();
    let run_id_clone_url = run_id.clone();
    let feature_cache_clone_url = entity_feature_cache.clone();
    let semaphore_clone_url = semaphore.clone();
    tasks.push(tokio::spawn(async move {
        let permit = semaphore_clone_url
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit for entity URL matching")?;
        let _permit_guard = permit;
        matching::url::find_matches(
            &pool_clone_url,
            Some(orchestrator_clone_url),
            &run_id_clone_url,
            Some(feature_cache_clone_url),
        )
        .await
        .context("Entity URL matching failed")
    }));

    // --- Spawn Address Matching Task (Entity) ---
    let pool_clone_address = pool.clone();
    let orchestrator_clone_address = entity_matching_orchestrator.clone();
    let run_id_clone_address = run_id.clone();
    let feature_cache_clone_address = entity_feature_cache.clone();
    let semaphore_clone_address = semaphore.clone();
    tasks.push(tokio::spawn(async move {
        let permit = semaphore_clone_address
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit for entity address matching")?;
        let _permit_guard = permit;
        matching::address::find_matches(
            &pool_clone_address,
            Some(orchestrator_clone_address),
            &run_id_clone_address,
            Some(feature_cache_clone_address),
        )
        .await
        .context("Entity address matching failed")
    }));

    // --- Spawn Name Matching Task (Entity) ---
    let pool_clone_name = pool.clone();
    let orchestrator_clone_name = entity_matching_orchestrator.clone();
    let run_id_clone_name = run_id.clone();
    let feature_cache_clone_name = entity_feature_cache.clone();
    let semaphore_clone_name = semaphore.clone();
    tasks.push(tokio::spawn(async move {
        let permit = semaphore_clone_name
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit for entity name matching")?;
        let _permit_guard = permit;
        matching::name::find_matches(
            &pool_clone_name,
            Some(orchestrator_clone_name),
            &run_id_clone_name,
            Some(feature_cache_clone_name),
        )
        .await
        .context("Entity name matching failed")
    }));

    // --- Spawn Geospatial Matching Task (Entity) ---
    let pool_clone_geo = pool.clone();
    let orchestrator_clone_geo = entity_matching_orchestrator.clone();
    let run_id_clone_geo = run_id.clone();
    let feature_cache_clone_geo = entity_feature_cache.clone();
    let semaphore_clone_geo = semaphore.clone();
    tasks.push(tokio::spawn(async move {
        let permit = semaphore_clone_geo
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit for entity geo matching")?;
        let _permit_guard = permit;
        matching::geospatial::find_matches(
            &pool_clone_geo,
            Some(orchestrator_clone_geo),
            &run_id_clone_geo,
            Some(feature_cache_clone_geo),
        )
        .await
        .context("Entity geospatial matching failed")
    }));

    let join_handle_results = join_all(tasks).await;
    let mut total_groups = 0;
    let mut method_stats_vec = Vec::new();

    for join_result in join_handle_results {
        match join_result {
            Ok(task_outcome_result) => {
                match task_outcome_result {
                    Ok(any_match_result) => {
                        total_groups += any_match_result.groups_created();
                        method_stats_vec.push(any_match_result.stats().clone());
                    }
                    Err(e) => {
                        warn!("An entity matching task returned an error: {:?}", e);
                    }
                }
            }
            Err(e) => {
                warn!("An entity matching task failed to join (e.g., panicked): {:?}", e);
            }
        }
    }

    // Report and clear entity feature cache stats
    {
        let mut cache_guard = entity_feature_cache.lock().await;
        let (hits, misses, ind_hits, ind_misses) = cache_guard.get_stats();
        let hit_rate = if hits + misses > 0 {
            (hits as f64 / (hits + misses) as f64) * 100.0
        } else {
            0.0
        };
        let ind_hit_rate = if ind_hits + ind_misses > 0 {
            (ind_hits as f64 / (ind_hits + ind_misses) as f64) * 100.0
        } else {
            0.0
        };
        info!("Entity Feature Cache - Pair: {} hits, {} misses ({:.2}%). Individual: {} hits, {} misses ({:.2}%).", hits, misses, hit_rate, ind_hits, ind_misses, ind_hit_rate);
        cache_guard.clear(); // Clear after reporting for the run
    }

    info!(
        "Entity matching processed in {:.2?}. Total groups: {}.",
        start_time_matching.elapsed(),
        total_groups
    );
    Ok((total_groups, method_stats_vec))
}

async fn consolidate_clusters_helper(
    pool: &PgPool,
    run_id: String,
) -> Result<(usize, usize)> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for consolidate_clusters_helper")?;
    let groups_row = conn
        .query_one(
            "SELECT COUNT(*) FROM public.entity_group WHERE group_cluster_id IS NULL",
            &[],
        )
        .await
        .context("Failed to count unassigned groups")?;
    let unassigned_groups: i64 = groups_row.get(0);
    info!(
        "Found {} groups needing cluster assignment.",
        unassigned_groups
    );
    if unassigned_groups == 0 {
        info!("No groups require clustering. Skipping consolidation.");
        return Ok((0, 0)); // No clusters formed if no groups to assign
    }

    // This single call now handles clustering and creating visualization edge data.
    let clusters_created = consolidate_clusters::process_clusters(pool, &run_id)
        .await
        .context("Failed to process clusters")?;

    // After the process, we query the db to get the count of edges for the report.
    let viz_edges_created = db::count_visualization_edges(pool, &run_id).await? as usize;

    Ok((clusters_created, viz_edges_created))
}
