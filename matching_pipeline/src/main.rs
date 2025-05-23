// src/main.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::try_join_all;
use log::{debug, info, warn};
use std::{
    any::Any,
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinHandle};
use uuid::Uuid;

use dedupe_lib::{
    cluster_visualization, config, consolidate_clusters,
    db::{self, PgPool},
    entity_organizations, matching,
    models::{self, *},
    reinforcement::{
        self,
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
    results::{
        self, AddressMatchResult, AnyMatchResult, EmailMatchResult, GeospatialMatchResult,
        MatchMethodStats, NameMatchResult, PhoneMatchResult, PipelineStats, ServiceMatchResult,
        UrlMatchResult,
    },
    service_cluster_visualization, service_consolidate_clusters, service_matching,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Starting HSDS entity grouping and clustering pipeline");
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

    // Connect to the database (now async)
    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;
    info!("Successfully connected to the database");

    // Capture timing information for each phase
    let mut phase_times = HashMap::new();

    // Run the pipeline in sequence
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

    // Generate and store report
    let description = Some("Full pipeline run with standard matching configuration".to_string());
    results::generate_report(&pool, stats, &phase_times, description).await?;

    Ok(())
}

async fn run_pipeline(
    pool: &PgPool,
    phase_times: &mut HashMap<String, Duration>,
) -> Result<results::PipelineStats> {
    // Initialize the ML reinforcement_orchestrator
    info!("Initializing ML-guided matching reinforcement_orchestrator");

    let run_id_uuid = Uuid::new_v4();
    let run_id = run_id_uuid.to_string();
    let run_id_clone = run_id.clone();
    let run_timestamp = Utc::now().naive_utc();
    let description = Some("Regular pipeline run".to_string()); // Default description

    // Create initial pipeline_run record to satisfy foreign key constraints
    info!("Creating initial pipeline_run record");
    db::create_initial_pipeline_run(pool, &run_id, run_timestamp, description.as_deref())
        .await
        .context("Failed to create initial pipeline_run record")?;

    // Initialize more complete stats structure
    let mut stats = results::PipelineStats {
        run_id,
        run_timestamp,
        description: Some("Regular pipeline run".to_string()),

        total_entities: 0,
        total_groups: 0,
        total_clusters: 0,
        total_service_matches: 0,
        total_visualization_edges: 0, // New field added
        total_service_clusters: 0,
        total_service_visualization_edges: 0,

        // Initialize timing fields - will be updated later
        entity_processing_time: 0.0,
        context_feature_extraction_time: 0.0,
        service_context_feature_extraction_time: 0.0,
        matching_time: 0.0,
        clustering_time: 0.0,
        visualization_edge_calculation_time: 0.0, // New timing field
        service_matching_time: 0.0,
        total_processing_time: 0.0,
        service_clustering_time: 0.0,
        service_visualization_edge_calculation_time: 0.0,

        // These will be populated during the pipeline
        method_stats: Vec::new(),
        cluster_stats: None,
        service_stats: None,
    };

    info!("Pipeline started. Progress: [0/8] phases (0%)"); // Updated number of phases

    // Phase 1: Entity identification
    info!("Phase 1: Entity identification");
    let phase1_start = Instant::now();
    stats.total_entities = identify_entities(pool).await?;
    let phase1_duration = phase1_start.elapsed();
    phase_times.insert("entity_identification".to_string(), phase1_duration);
    stats.entity_processing_time = phase1_duration.as_secs_f64();
    info!(
        "Identified {} entities in {:.2?}",
        stats.total_entities,
        phase1_start.elapsed()
    );
    info!("Pipeline progress: [1/8] phases (12.5%)");

    let entity_feature_cache: SharedFeatureCache = create_shared_cache(); //

    // Phase 2: Entity Context Feature Extraction and Cache Pre-warming
    info!("Phase 2: Entity Context Feature Extraction and Cache Pre-warming");
    let phase2_start = Instant::now();
    if stats.total_entities > 0 {
        // Call the new function from entity_feature_extraction module
        match extract_and_store_all_entity_features_and_prewarm_cache(pool, &entity_feature_cache).await {
            Ok(features_count) => info!(
                "Successfully extracted/retrieved context features and pre-warmed cache for {} entities.",
                features_count
            ),
            Err(e) => warn!(
                "Entity context feature extraction and cache pre-warming failed: {}. Proceeding with potentially slower matching.",
                e
            ),
        }
        let max_pairs = 100;
        match prewarm_pair_features_cache(pool, &entity_feature_cache, max_pairs).await {
            Ok(pairs_count) => info!(
                "Successfully pre-warmed pair features cache for {} likely entity pairs.",
                pairs_count
            ),
            Err(e) => warn!(
                "Pair features cache pre-warming failed: {}. Proceeding with on-demand feature extraction.",
                e
            ),
        }
    } else {
        info!("Skipping entity context feature extraction and cache pre-warming as no entities were identified.");
    }
    let phase2_duration = phase2_start.elapsed();
    phase_times.insert(
        "entity_context_feature_extraction_and_prewarming".to_string(),
        phase2_duration,
    );
    stats.context_feature_extraction_time = phase2_duration.as_secs_f64();
    info!(
        "Entity Context Feature Extraction and Cache Pre-warming complete in {:.2?}. Phase 2 complete.",
        phase2_duration
    );
    info!("Pipeline progress: [2/8] phases (25%)");

    // Now initialize the ML reinforcement_orchestrator
    info!("Initializing ML-guided matching reinforcement_orchestrator");
    let reinforcement_orchestrator_instance = MatchingOrchestrator::new(pool)
        .await
        .context("Failed to initialize ML reinforcement_orchestrator")?;
    let reinforcement_orchestrator = Arc::new(Mutex::new(reinforcement_orchestrator_instance));

    // Set the *pre-warmed* feature cache on the orchestrator
    {
        let mut orchestrator = reinforcement_orchestrator.lock().await;
        orchestrator.set_feature_cache(entity_feature_cache.clone()); // Use the cache we just pre-warmed
        info!("Pre-warmed entity feature cache attached to RL orchestrator");
    }

    // Phase 3: Entity matching
    info!("Phase 3: Entity matching");
    let phase3_start = Instant::now();
    let (total_groups, method_stats_match) = run_matching_pipeline(
        pool,
        reinforcement_orchestrator.clone(), // This orchestrator instance now holds the pre-warmed cache
        stats.run_id.clone(),               // Use run_id from stats struct
        entity_feature_cache.clone(),       // Pass the pre-warmed entity_feature_cache
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
    info!("Pipeline progress: [3/8] phases (37.5%)");

    // Phase 4: Cluster consolidation
    info!("Phase 4: Cluster consolidation");
    let phase4_start = Instant::now();
    stats.total_clusters =
        consolidate_clusters_helper(pool, reinforcement_orchestrator, run_id_clone.clone()).await?;
    let phase4_duration = phase4_start.elapsed();
    phase_times.insert("cluster_consolidation".to_string(), phase4_duration);
    stats.clustering_time = phase4_duration.as_secs_f64();
    info!(
        "Formed {} clusters in {:.2?}. Phase 4 complete.",
        stats.total_clusters, phase4_duration
    );
    info!("Pipeline progress: [4/8] phases (50%)");

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
        stats.total_visualization_edges = 0; // Explicitly set to 0 as it's skipped
        stats.visualization_edge_calculation_time = 0.0; // Explicitly set to 0
    }
    info!("Pipeline progress: [5/8] phases (62.5%)");

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

    info!("Phase 5.5: Service Context Feature Extraction and Cache Pre-warming");
    let phase5_5_start = Instant::now();

    // Extract and store individual service features
    let service_features_count =
        extract_and_store_all_service_features_and_prewarm_cache(pool, &service_feature_cache)
            .await?;

    // Pre-warm service pair features cache
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
    info!("Pipeline progress: [6/8] phases (75%)");

    info!("Phase 7: Service cluster consolidation");
    let phase7_start = Instant::now();

    // Ensure consolidation tables exist before processing
    service_consolidate_clusters::ensure_consolidation_tables_exist(pool)
        .await
        .context("Failed to ensure consolidation tables exist")?;

    // Custom config with performance optimizations
    let consolidation_config = service_consolidate_clusters::ConsolidationConfig {
        similarity_threshold: 0.5, // Lower threshold for more broad clustering
        embedding_batch_size: 200,  // Larger batches if memory allows
        db_batch_size: 100,         // Larger DB batches for I/O efficiency
        max_cache_size: 15000,      // Larger cache for better hit rates
        min_cluster_size: 3,        // Only consolidate clusters with 3+ services
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
            stats.total_service_clusters = 0; // Or fetch current cluster count
        }
    }

    let phase7_duration = phase7_start.elapsed();
    phase_times.insert("service_clustering".to_string(), phase7_duration);
    stats.service_clustering_time = phase7_duration.as_secs_f64();
    info!(
        "Service cluster consolidation phase completed in {:.2?}. Phase 7 complete.",
        phase7_duration
    );
    info!("Pipeline progress: [7/8] phases (87%)");

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
    info!("Pipeline progress: [8/8] phases (100%)");

    // Calculate total processing time
    stats.total_processing_time = stats.entity_processing_time
        + stats.context_feature_extraction_time
        + stats.matching_time
        + stats.clustering_time
        + stats.visualization_edge_calculation_time
        + stats.service_matching_time
        + stats.service_clustering_time
        + stats.service_visualization_edge_calculation_time;

    Ok(stats)
}

async fn identify_entities(pool: &PgPool) -> Result<usize> {
    info!("Phase 1: Entity identification starting...");
    // First, extract entities from organizations (creates new ones if necessary)
    let org_entities = entity_organizations::extract_entities(pool)
        .await
        .context("Failed to extract entities from organizations")?;
    info!(
        "Discovered or created {} mapping(s) between organization and entity tables.",
        org_entities.len()
    );

    // Then, link entities to their features (services, phones, etc.)
    // This step ensures entity_feature table is up-to-date for all entities based on their organization_id
    // It doesn't directly return the list of entities being linked, but processes based on `org_entities`
    // and also handles existing links.
    entity_organizations::link_entity_features(pool, &org_entities) // Pass all current entities
        .await
        .context("Failed to link entity features")?;

    // Update existing entities with new features added since the last time features were linked
    // This ensures that if new records (services, phones, locations, contacts) have been added
    // that reference organizations with existing entities, they'll be properly linked
    let updated_features_count = entity_organizations::update_entity_features(pool)
        .await
        .context("Failed to update entity features for existing entities")?;
    info!(
        "Added {} new feature links to existing entities",
        updated_features_count
    );

    // After potential new entities are created and features linked,
    // get the total count of entities from the database.
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
        "Entity identification phase complete. Total entities in system: {}",
        total_entities_count
    );
    Ok(total_entities_count as usize)
}

// Modified section of src/main.rs to implement feature caching
// Replace the run_matching_pipeline function with this version

// src/main.rs - Updated run_matching_pipeline function with feature cache

async fn run_matching_pipeline(
    pool: &PgPool,
    reinforcement_orchestrator: Arc<Mutex<MatchingOrchestrator>>, // This orchestrator has the pre-warmed cache set on it
    run_id: String,
    // Add the pre-warmed feature cache as a parameter
    feature_cache: SharedFeatureCache,
) -> Result<(usize, Vec<MatchMethodStats>)> {
    info!("Initializing matching pipeline with pre-warmed feature caching...");
    let start_time_matching = Instant::now();

    let _run_id_uuid = match Uuid::parse_str(&run_id) {
        // Renamed to avoid conflict, original name run_id_uuid
        Ok(uuid) => uuid,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to parse run_id as UUID: {}", e));
        }
    };

    // DO NOT create a new feature cache here. Use the one passed in.
    // The feature cache is already set on the `reinforcement_orchestrator` passed to this function.
    // The individual matching tasks below (email, phone, etc.) will clone this `reinforcement_orchestrator`
    // or receive the `feature_cache` directly.

    let mut tasks: Vec<JoinHandle<Result<AnyMatchResult, anyhow::Error>>> = Vec::new();

    // --- Spawn Email Matching Task ---
    let pool_clone_email = pool.clone();
    let orchestrator_clone_email = reinforcement_orchestrator.clone(); // Orchestrator carries its cache
    let run_id_clone_email = run_id.clone();
    let feature_cache_clone_email = feature_cache.clone(); // Pass the pre-warmed cache
    tasks.push(tokio::spawn(async move {
        info!("Starting email matching task...");
        let result = matching::email::find_matches(
            &pool_clone_email,
            Some(orchestrator_clone_email),
            &run_id_clone_email,
            Some(feature_cache_clone_email), // Pass feature cache explicitly
        )
        .await
        .context("Email matching task failed");
        info!(
            "Email matching task finished successfully: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // --- Spawn Phone Matching Task ---
    let pool_clone_phone = pool.clone();
    let orchestrator_clone_phone = reinforcement_orchestrator.clone();
    let run_id_clone_phone = run_id.clone();
    let feature_cache_clone_phone = feature_cache.clone(); // Pass the pre-warmed cache
    tasks.push(tokio::spawn(async move {
        info!("Starting phone matching task...");
        let result = matching::phone::find_matches(
            &pool_clone_phone,
            Some(orchestrator_clone_phone),
            &run_id_clone_phone,
            Some(feature_cache_clone_phone), // Pass feature cache explicitly
        )
        .await
        .context("Phone matching task failed");
        info!(
            "Phone matching task finished successfully: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // --- Spawn URL Matching Task ---
    let pool_clone_url = pool.clone();
    let orchestrator_clone_url = reinforcement_orchestrator.clone();
    let run_id_clone_url = run_id.clone();
    let feature_cache_clone_url = feature_cache.clone(); // Pass the pre-warmed cache
    tasks.push(tokio::spawn(async move {
        info!("Starting URL matching task...");
        let result = matching::url::find_matches(
            &pool_clone_url,
            Some(orchestrator_clone_url),
            &run_id_clone_url,
            Some(feature_cache_clone_url), // Pass feature cache explicitly
        )
        .await
        .context("URL matching task failed");
        info!(
            "URL matching task finished successfully: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // --- Spawn Address Matching Task ---
    let pool_clone_address = pool.clone();
    let orchestrator_clone_address = reinforcement_orchestrator.clone();
    let run_id_clone_address = run_id.clone();
    let feature_cache_clone_address = feature_cache.clone(); // Pass the pre-warmed cache
    tasks.push(tokio::spawn(async move {
        info!("Starting address matching task...");
        let result = matching::address::find_matches(
            &pool_clone_address,
            Some(orchestrator_clone_address),
            &run_id_clone_address,
            Some(feature_cache_clone_address), // Pass feature cache explicitly
        )
        .await
        .context("Address matching task failed");
        info!(
            "Address matching task finished successfully: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // --- Spawn Name Matching Task ---
    let pool_clone_name = pool.clone();
    let orchestrator_clone_name = reinforcement_orchestrator.clone();
    let run_id_clone_name = run_id.clone();
    let feature_cache_clone_name = feature_cache.clone(); // Pass the pre-warmed cache
    tasks.push(tokio::spawn(async move {
        info!("Starting name matching task...");
        let result = matching::name::find_matches(
            &pool_clone_name,
            Some(orchestrator_clone_name),
            &run_id_clone_name,
            Some(feature_cache_clone_name), // Pass feature cache explicitly
        )
        .await
        .context("Name matching task failed");
        info!(
            "Name matching task finished successfully: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // --- Spawn Geospatial Matching Task ---
    let pool_clone_geo = pool.clone();
    let orchestrator_clone_geo = reinforcement_orchestrator.clone();
    let run_id_clone_geo = run_id.clone();
    let feature_cache_clone_geo = feature_cache.clone(); // Pass the pre-warmed cache
    tasks.push(tokio::spawn(async move {
        info!("Starting geospatial matching task...");
        let result = matching::geospatial::find_matches(
            &pool_clone_geo,
            Some(orchestrator_clone_geo),
            &run_id_clone_geo,
            Some(feature_cache_clone_geo), // Pass feature cache explicitly
        )
        .await
        .context("Geospatial matching task failed");
        info!(
            "Geospatial matching task finished: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // The cache statistics reported here will now reflect the usage of the pre-warmed cache.
    // Clearing the cache here is appropriate as it's done after all matching tasks.
    let join_handle_results = try_join_all(tasks).await;

    let mut total_groups = 0;
    let mut method_stats_vec = Vec::new();
    let mut completed_methods = 0;
    let total_matching_methods = 6;

    match join_handle_results {
        Ok(individual_task_results) => {
            for (idx, task_result) in individual_task_results.into_iter().enumerate() {
                match task_result {
                    Ok(any_match_result) => {
                        info!(
                            "Processing result for task index {}: method {:?}, groups created {}.",
                            idx,
                            any_match_result.stats().method_type,
                            any_match_result.groups_created()
                        );
                        total_groups += any_match_result.groups_created();
                        method_stats_vec.push(any_match_result.stats().clone());
                        completed_methods += 1;
                    }
                    Err(task_err) => {
                        warn!(
                            "Matching task at index {} failed internally: {:?}",
                            idx, task_err
                        );
                        return Err(
                            task_err.context(format!("Matching task at index {} failed", idx))
                        );
                    }
                }
            }
        }
        Err(join_err) => {
            warn!(
                "A matching task failed to join (e.g., panicked): {:?}",
                join_err
            );
            return Err(
                anyhow::Error::from(join_err).context("A matching task panicked or was cancelled")
            );
        }
    }

    if completed_methods != total_matching_methods
        && method_stats_vec.len() != total_matching_methods
    {
        warn!(
            "Expected {} completed matching methods, but only {} results processed successfully. Check logs for task errors.",
             total_matching_methods, completed_methods
         );
    }

    // Report cache statistics from the passed (and now used) cache
    {
        let mut cache_guard = feature_cache.lock().await; // Use the passed feature_cache
        let (hits, misses, ind_hits, ind_misses) = cache_guard.get_stats();

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

        cache_guard.clear(); // Clear the cache to free memory
    }

    info!(
        "All matching strategies processed in {:.2?}. Total groups from successful tasks: {}, Methods processed: {}/{}.",
        start_time_matching.elapsed(),
        total_groups,
        completed_methods,
        total_matching_methods
    );

    Ok((total_groups, method_stats_vec))
}

async fn consolidate_clusters_helper(
    pool: &PgPool,
    reinforcement_orchestrator: Arc<Mutex<MatchingOrchestrator>>,
    run_id: String,
) -> Result<usize> {
    // Get the unassigned group count before we start
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Parse the run_id into a UUID type to fix the type mismatch
    let run_id_uuid = match Uuid::parse_str(&run_id) {
        Ok(uuid) => uuid,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to parse run_id as UUID: {}", e));
        }
    };

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
        return Ok(0); // No clusters created
    };

    // Call the actual implementation
    let clusters_created = consolidate_clusters::process_clusters(pool, &run_id)
        .await
        .context("Failed to process clusters")?;

    Ok(clusters_created)
}

async fn run_service_matching_pipeline(
    pool: &PgPool,
    pipeline_run_id: &str,
    service_orchestrator: Arc<Mutex<ServiceMatchingOrchestrator>>,
    feature_cache: SharedServiceFeatureCache,
) -> Result<results::ServiceMatchResult> {
    info!("Starting cluster-scoped service matching pipeline...");
    let start_time = Instant::now();

    // 1. Get all cluster IDs - FIXED: Changed from group_cluster to entity_group_cluster
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
        .context("Failed to query entity_group_cluster IDs")?;

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
        "Found {} entity clusters for cluster-scoped service matching. Processing each...",
        cluster_ids.len()
    );

    let mut all_tasks: Vec<JoinHandle<Result<ServiceMatchResult, anyhow::Error>>> = Vec::new();

    for cluster_id_str in cluster_ids {
        // 2. For each cluster_id, get its services' relevant details
        let service_detail_rows = conn
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

        // Prepare data for each matching method (as in your previous correct version)
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
                    services_for_name_matcher.push((service_id.clone(), name.clone())); // Also clone name here
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

        // --- Spawn Name Matching Task ---
        if !services_for_name_matcher.is_empty() {
            // These task-specific clones ensure each task owns its required data.
            let task_pool: PgPool = pool.clone();
            let task_orchestrator: Arc<Mutex<ServiceMatchingOrchestrator>> =
                service_orchestrator.clone();
            let task_run_id: String = pipeline_run_id.to_string(); // Owned String
            let task_feature_cache: SharedServiceFeatureCache = feature_cache.clone();
            let task_cluster_id: String = cluster_id_str.clone(); // Owned String
                                                                  // services_for_name_matcher is already an owned Vec, will be moved into the closure.

            all_tasks.push(tokio::spawn(async move {
                // async move takes ownership of the task_* variables
                service_matching::name::find_matches_in_cluster(
                    &task_pool,                // Pass reference to owned pool
                    Some(task_orchestrator),   // Arc is cloned and moved
                    &task_run_id,              // Pass reference to owned String
                    Some(task_feature_cache),  // Arc is cloned and moved
                    services_for_name_matcher, // Vec is moved
                    task_cluster_id,           // String is moved
                )
                .await // Ensure the future returned by find_matches_in_cluster is awaited
            }));
        }

        // --- Spawn URL Matching Task ---
        if !services_for_url_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();

            all_tasks.push(tokio::spawn(async move {
                service_matching::url::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_url_matcher,
                    task_cluster_id,
                )
                .await
            }));
        }

        // --- Spawn Email Matching Task ---
        if !services_for_email_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();

            all_tasks.push(tokio::spawn(async move {
                service_matching::email::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_email_matcher,
                    task_cluster_id,
                )
                .await
            }));
        }

        // --- Spawn Embedding Matching Task ---
        if !services_for_embedding_matcher.is_empty() {
            let task_pool = pool.clone();
            let task_orchestrator = service_orchestrator.clone();
            let task_run_id = pipeline_run_id.to_string();
            let task_feature_cache = feature_cache.clone();
            let task_cluster_id = cluster_id_str.clone();

            all_tasks.push(tokio::spawn(async move {
                service_matching::embedding::find_matches_in_cluster(
                    &task_pool,
                    Some(task_orchestrator),
                    &task_run_id,
                    Some(task_feature_cache),
                    services_for_embedding_matcher,
                    task_cluster_id,
                )
                .await
            }));
        }
    }
    // Aggregate results from all tasks (all methods, all clusters)
    let mut total_groups_created_all_methods = 0;
    let mut aggregated_method_stats: HashMap<String, MatchMethodStats> = HashMap::new();

    let task_results = futures::future::try_join_all(all_tasks).await;

    match task_results {
        Ok(individual_task_results) => {
            for task_result in individual_task_results {
                match task_result {
                    Ok(service_match_result_for_method_in_cluster) => {
                        total_groups_created_all_methods +=
                            service_match_result_for_method_in_cluster.groups_created;
                        // Each ServiceMatchResult from find_matches_in_cluster should ideally contain one method_stat
                        for stat in service_match_result_for_method_in_cluster.method_stats {
                            let method_key = stat.method_type.as_str().to_string();
                            let entry =
                                aggregated_method_stats
                                    .entry(method_key)
                                    .or_insert_with(|| MatchMethodStats {
                                        method_type: stat.method_type.clone(),
                                        groups_created: 0,
                                        entities_matched: 0, // entities_matched needs careful aggregation or redefinition for service matching
                                        avg_confidence: 0.0,
                                        avg_group_size: 2.0,
                                    });
                            // Weighted average for confidence
                            let total_confidence_points =
                                entry.avg_confidence * entry.groups_created as f64;
                            let current_confidence_points =
                                stat.avg_confidence * stat.groups_created as f64;
                            entry.groups_created += stat.groups_created;
                            // entities_matched: sum of unique services involved in new groups by this method.
                            // This is harder to aggregate correctly without more info from find_matches_in_cluster.
                            // For now, summing stat.entities_matched might overestimate if same service matched by same method in different contexts.
                            // Let's assume entities_matched from find_matches_in_cluster refers to distinct services processed by that method in that cluster call.
                            entry.entities_matched += stat.entities_matched;
                            if entry.groups_created > 0 {
                                entry.avg_confidence = (total_confidence_points
                                    + current_confidence_points)
                                    / entry.groups_created as f64;
                            } else {
                                entry.avg_confidence = 0.0;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("A service matching task failed: {:?}", e);
                        // Optionally continue or return error
                    }
                }
            }
        }
        Err(join_err) => {
            warn!(
                "A service matching task failed to join (e.g., panicked): {:?}",
                join_err
            );
            return Err(anyhow::Error::from(join_err)
                .context("A service matching task panicked or was cancelled"));
        }
    }

    // Process any new feedback to update the service confidence tuner
    info!("Processing human feedback for service confidence tuner");
    let mut orchestrator = service_orchestrator.lock().await;
    let _ = orchestrator.process_feedback_and_update_tuner(pool).await; //

    // Save the updated service confidence tuner
    let _ = orchestrator.save_models(pool).await; //

    if let Some((hits, misses, ind_hits, ind_misses)) = orchestrator.get_feature_cache_stats().await
    {
        //
        // ... (logging for cache stats as before) ...
        let hit_rate = if hits + misses > 0 {
            (hits as f64 / (hits + misses) as f64) * 100.0
        } else {
            0.0
        }; //
        let individual_hit_rate = if ind_hits + ind_misses > 0 {
            (ind_hits as f64 / (ind_hits + ind_misses) as f64) * 100.0
        } else {
            0.0
        }; //
        info!(
            "Feature cache statistics - Pair features: {} hits, {} misses, {:.2}% hit rate",
            hits, misses, hit_rate
        ); //
        info!(
            "Feature cache statistics - Individual features: {} hits, {} misses, {:.2}% hit rate",
            ind_hits, ind_misses, individual_hit_rate
        ); //
    }

    let final_method_stats: Vec<MatchMethodStats> =
        aggregated_method_stats.values().cloned().collect();

    let combined_stats = if !final_method_stats.is_empty() {
        //
        let total_avg_confidence_points: f64 = final_method_stats
            .iter()
            .map(|s| s.avg_confidence * s.groups_created as f64)
            .sum(); //
        let total_groups_for_avg: usize = final_method_stats.iter().map(|s| s.groups_created).sum(); //
        let avg_confidence = if total_groups_for_avg > 0 {
            total_avg_confidence_points / total_groups_for_avg as f64
        } else {
            0.0
        }; //

        results::MatchMethodStats {
            //
            method_type: MatchMethodType::Custom("service_combined_all_clusters".to_string()), //
            groups_created: total_groups_created_all_methods,                                  //
            entities_matched: final_method_stats
                .iter()
                .map(|s| s.entities_matched)
                .sum::<usize>(), // Sum of unique entities involved, per method type
            avg_confidence,                                                                    //
            avg_group_size: 2.0,                                                               //
        }
    } else {
        results::MatchMethodStats {
            //
            method_type: MatchMethodType::Custom("service_combined_no_matches".to_string()), //
            groups_created: 0,                                                               //
            entities_matched: 0,                                                             //
            avg_confidence: 0.0,                                                             //
            avg_group_size: 2.0,                                                             //
        }
    };

    info!(
        "Cluster-scoped service matching complete in {:.2?}. Created {} service groups total across all clusters.",
        start_time.elapsed(), total_groups_created_all_methods
    );

    Ok(results::ServiceMatchResult {
        //
        groups_created: total_groups_created_all_methods, //
        stats: combined_stats,                            //
        method_stats: final_method_stats,                 //
    })
}
