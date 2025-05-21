// src/main.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::try_join_all;
use log::{info, warn};
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
            service_feature_cache_service::{
                create_shared_service_cache, SharedServiceFeatureCache,
            },
            service_feature_extraction::{
                extract_and_store_all_service_context_features, get_stored_service_features,
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
        let max_pairs = 10000;
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
        //
    }
    let phase2_duration = phase2_start.elapsed(); //
                                                  // Updated key for clarity
    phase_times.insert(
        "entity_context_feature_extraction_and_prewarming".to_string(),
        phase2_duration,
    );
    stats.context_feature_extraction_time = phase2_duration.as_secs_f64(); //
    info!(
        "Entity Context Feature Extraction and Cache Pre-warming complete in {:.2?}. Phase 2 complete.",
        phase2_duration
    ); //
    info!("Pipeline progress: [2/8] phases (25%)"); //

    // Now initialize the ML reinforcement_orchestrator
    info!("Initializing ML-guided matching reinforcement_orchestrator"); //
    let reinforcement_orchestrator_instance = MatchingOrchestrator::new(pool) //
        .await
        .context("Failed to initialize ML reinforcement_orchestrator")?; //
    let reinforcement_orchestrator = Arc::new(Mutex::new(reinforcement_orchestrator_instance)); //

    // Set the *pre-warmed* feature cache on the orchestrator
    {
        let mut orchestrator = reinforcement_orchestrator.lock().await; //
        orchestrator.set_feature_cache(entity_feature_cache.clone()); // Use the cache we just pre-warmed
        info!("Pre-warmed entity feature cache attached to RL orchestrator"); //
    }

    // Phase 3: Entity matching
    info!("Phase 3: Entity matching"); //
    let phase3_start = Instant::now(); //
    let (total_groups, method_stats_match) = run_matching_pipeline(
        pool,
        reinforcement_orchestrator.clone(), // This orchestrator instance now holds the pre-warmed cache
        stats.run_id.clone(),               // Use run_id from stats struct
        entity_feature_cache.clone(),       // Pass the pre-warmed entity_feature_cache
    )
    .await?;
    stats.total_groups = total_groups; //
    stats.method_stats.extend(method_stats_match); //
    let phase3_duration = phase3_start.elapsed(); //
    phase_times.insert("entity_matching".to_string(), phase3_duration); //
    stats.matching_time = phase3_duration.as_secs_f64(); //
    info!(
        "Created {} entity groups in {:.2?}. Phase 3 complete.",
        stats.total_groups, phase3_duration
    ); //
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

    info!("Phase 5.5: Service Context Feature Extraction");
    let phase5_5_start = Instant::now();
    let service_features_count =
        extract_and_store_all_service_context_features(pool, &service_feature_cache).await?;
    let phase5_5_duration = phase5_5_start.elapsed();
    phase_times.insert(
        "service_context_feature_extraction".to_string(),
        phase5_5_duration,
    );
    stats.service_context_feature_extraction_time = phase5_5_duration.as_secs_f64();
    info!(
        "Extracted and stored context features for {} services in {:.2?}. Phase 5.5 complete.",
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

    // Phase 7: Service Clustering - now we can use the same service_orchestrator
    info!("Phase 7: Service cluster consolidation");
    let phase7_start = Instant::now();
    stats.total_service_clusters = service_consolidate_clusters::process_clusters(
        pool,
        Some(&service_orchestrator),
        &run_id_clone,
    )
    .await?;
    let phase7_duration = phase7_start.elapsed();
    phase_times.insert("service_clustering".to_string(), phase7_duration);
    stats.service_clustering_time = phase7_duration.as_secs_f64();
    info!(
        "Formed {} service clusters in {:.2?}. Phase 7 complete.",
        stats.total_service_clusters, phase7_duration
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
            Some(&orchestrator_clone_phone), // Note: some find_matches take Option<&Arc<Mutex<...>>
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
    let clusters_created =
        consolidate_clusters::process_clusters(pool, Some(&reinforcement_orchestrator), &run_id)
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
    info!("Starting service matching pipeline with parallel execution...");
    let start_time = Instant::now();

    // The vector of JoinHandles will hold tasks that resolve to Result<ServiceMatchResult, anyhow::Error>
    let mut tasks: Vec<JoinHandle<Result<ServiceMatchResult, anyhow::Error>>> = Vec::new();

    // --- Spawn Name Matching Task ---
    let pool_clone_name = pool.clone();
    let orchestrator_clone_name = service_orchestrator.clone();
    let run_id_clone_name = pipeline_run_id.to_string();
    let feature_cache_clone_name = feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        info!("Starting service name matching task...");
        let result = service_matching::name::find_matches(
            &pool_clone_name,
            Some(orchestrator_clone_name),
            &run_id_clone_name,
            Some(feature_cache_clone_name),
        )
        .await
        .context("Service name matching task failed");
        info!(
            "Service name matching task finished: {:?}",
            result.as_ref().map(|r| r.groups_created)
        );
        result
    }));

    // --- Spawn URL Matching Task ---
    let pool_clone_url = pool.clone();
    let orchestrator_clone_url = service_orchestrator.clone();
    let run_id_clone_url = pipeline_run_id.to_string();
    let feature_cache_clone_url = feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        info!("Starting service URL matching task...");
        let result = service_matching::url::find_matches(
            &pool_clone_url,
            Some(orchestrator_clone_url),
            &run_id_clone_url,
            Some(feature_cache_clone_url),
        )
        .await
        .context("Service URL matching task failed");
        info!(
            "Service URL matching task finished: {:?}",
            result.as_ref().map(|r| r.groups_created)
        );
        result
    }));

    // --- Spawn Email Matching Task ---
    let pool_clone_email = pool.clone();
    let orchestrator_clone_email = service_orchestrator.clone();
    let run_id_clone_email = pipeline_run_id.to_string();
    let feature_cache_clone_email = feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        info!("Starting service email matching task...");
        let result = service_matching::email::find_matches(
            &pool_clone_email,
            Some(orchestrator_clone_email),
            &run_id_clone_email,
            Some(feature_cache_clone_email),
        )
        .await
        .context("Service email matching task failed");
        info!(
            "Service email matching task finished: {:?}",
            result.as_ref().map(|r| r.groups_created)
        );
        result
    }));

    // --- Spawn Embedding Matching Task ---
    let pool_clone_embedding = pool.clone();
    let orchestrator_clone_embedding = service_orchestrator.clone();
    let run_id_clone_embedding = pipeline_run_id.to_string();
    let feature_cache_clone_embedding = feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        info!("Starting service embedding matching task...");
        let result = service_matching::embedding::find_matches(
            &pool_clone_embedding,
            Some(orchestrator_clone_embedding),
            &run_id_clone_embedding,
            Some(feature_cache_clone_embedding),
        )
        .await
        .context("Service embedding matching task failed");
        info!(
            "Service embedding matching task finished: {:?}",
            result.as_ref().map(|r| r.groups_created)
        );
        result
    }));

    // try_join_all awaits all JoinHandles.
    let join_handle_results = try_join_all(tasks).await;

    let mut total_groups_created = 0;
    let mut method_stats: Vec<results::MatchMethodStats> = Vec::new();
    let mut completed_methods = 0;
    let total_matching_methods = 4; // name, url, email, embedding

    // Handle results from all tasks
    match join_handle_results {
        Ok(individual_task_results) => {
            for (idx, task_result) in individual_task_results.into_iter().enumerate() {
                match task_result {
                    Ok(match_result) => {
                        info!(
                            "Processing result for service matching task index {}: method {:?}, groups created {}.",
                            idx,
                            match_result.stats.method_type,
                            match_result.groups_created
                        );
                        total_groups_created += match_result.groups_created;
                        method_stats.extend(match_result.method_stats);
                        completed_methods += 1;
                    }
                    Err(task_err) => {
                        warn!(
                            "Service matching task at index {} failed internally: {:?}",
                            idx, task_err
                        );
                        return Err(task_err
                            .context(format!("Service matching task at index {} failed", idx)));
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

    if completed_methods != total_matching_methods {
        warn!(
            "Expected {} completed service matching methods, but only {} results processed successfully. Check logs for task errors.",
             total_matching_methods, completed_methods
         );
    }

    // Process any new feedback to update the service confidence tuner
    info!("Processing human feedback for service confidence tuner");
    let mut orchestrator = service_orchestrator.lock().await;
    let _ = orchestrator.process_feedback_and_update_tuner(pool).await;

    // Save the updated service confidence tuner
    let _ = orchestrator.save_models(pool).await;

    // Report cache statistics
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

    // Elapsed time
    let elapsed = start_time.elapsed();

    info!(
        "Service matching complete in {:.2?}. Created {} service groups total.",
        elapsed, total_groups_created
    );

    // Create combined stats for the overall result
    let combined_stats = if !method_stats.is_empty() {
        // Calculate average confidence across all methods
        let avg_confidence = method_stats
            .iter()
            .filter(|s| s.groups_created > 0)
            .map(|s| s.avg_confidence * s.groups_created as f64)
            .sum::<f64>()
            / method_stats
                .iter()
                .filter(|s| s.groups_created > 0)
                .map(|s| s.groups_created as f64)
                .sum::<f64>()
                .max(1.0); // Avoid division by zero

        results::MatchMethodStats {
            method_type: models::MatchMethodType::Custom("service_combined".to_string()),
            groups_created: total_groups_created,
            entities_matched: method_stats
                .iter()
                .map(|s| s.entities_matched)
                .sum::<usize>(),
            avg_confidence,
            avg_group_size: 2.0,
        }
    } else {
        // Default stats if no methods completed successfully
        results::MatchMethodStats {
            method_type: models::MatchMethodType::Custom("service_combined".to_string()),
            groups_created: 0,
            entities_matched: 0,
            avg_confidence: 0.0,
            avg_group_size: 2.0,
        }
    };

    // Return combined results
    Ok(ServiceMatchResult {
        groups_created: total_groups_created,
        stats: combined_stats,
        method_stats,
    })
}
