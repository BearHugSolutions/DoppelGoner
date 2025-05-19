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
    models::*,
    reinforcement::{self, MatchingOrchestrator},
    results::{
        self, AddressMatchResult, AnyMatchResult, EmailMatchResult, GeospatialMatchResult,
        MatchMethodStats, NameMatchResult, PhoneMatchResult, PipelineStats, UrlMatchResult,
    },
    service_matching,
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

        // Initialize timing fields - will be updated later
        entity_processing_time: 0.0,
        context_feature_extraction_time: 0.0,
        matching_time: 0.0,
        clustering_time: 0.0,
        visualization_edge_calculation_time: 0.0, // New timing field
        service_matching_time: 0.0,
        total_processing_time: 0.0,

        // These will be populated during the pipeline
        method_stats: Vec::new(),
        cluster_stats: None,
        service_stats: None,
    };

    info!("Pipeline started. Progress: [0/6] phases (0%)"); // Updated number of phases

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
    info!("Pipeline progress: [1/6] phases (17%)");

    // Phase 2: Context Feature Extraction
    info!("Phase 2: Context Feature Extraction");
    let phase2_start = Instant::now();
    if stats.total_entities > 0 {
        match entity_organizations::extract_and_store_all_entity_context_features(pool).await {
            Ok(features_count) => info!("Successfully extracted and stored context features for {} entities.", features_count),
            Err(e) => warn!("Context feature extraction failed: {}. Proceeding without ML-guided features for some operations potentially.", e),
        }
    } else {
        info!("Skipping context feature extraction as no entities were identified.");
    }
    let phase2_duration = phase2_start.elapsed();
    phase_times.insert("context_feature_extraction".to_string(), phase2_duration);
    stats.context_feature_extraction_time = phase2_duration.as_secs_f64();
    info!(
        "Context Feature Extraction complete in {:.2?}. Phase 2 complete.",
        phase2_duration
    );
    info!("Pipeline progress: [2/6] phases (33%)");

    // Now initialize the ML reinforcement_orchestrator as features should be available
    info!("Initializing ML-guided matching reinforcement_orchestrator");
    let reinforcement_orchestrator = reinforcement::MatchingOrchestrator::new(pool)
        .await
        .context("Failed to initialize ML reinforcement_orchestrator")?;
    let reinforcement_orchestrator = Mutex::new(reinforcement_orchestrator);

    // Phase 3: Entity matching
    info!("Phase 3: Entity matching");
    info!("Initializing ML-guided matching reinforcement_orchestrator");
    let reinforcement_orchestrator_instance = reinforcement::MatchingOrchestrator::new(pool)
        .await
        .context("Failed to initialize ML reinforcement_orchestrator")?;
    // Wrap in Arc and Mutex for shared, mutable access across tasks
    let reinforcement_orchestrator = Arc::new(Mutex::new(reinforcement_orchestrator_instance));

    let phase3_start = Instant::now();
    let (total_groups, method_stats_match) = run_matching_pipeline(
        pool,
        reinforcement_orchestrator.clone(),
        run_id_clone.clone(),
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
    info!("Pipeline progress: [3/6] phases (50%)");

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
    info!("Pipeline progress: [4/6] phases (67%)");

    // NEW PHASE 5: Visualization edge calculation
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
    info!("Pipeline progress: [5/6] phases (83%)");

    // PHASE 6: Service matching
    info!("Phase 6: Service matching");
    let phase6_start = Instant::now();
    let service_match_stats_result = service_matching::semantic_geospatial::match_services(pool)
        .await
        .context("failed to match services")?;
    stats.total_service_matches = service_match_stats_result.groups_created;
    stats.method_stats.push(service_match_stats_result.stats);
    let phase6_duration = phase6_start.elapsed();
    phase_times.insert("service_matching".to_string(), phase6_duration);
    stats.service_matching_time = phase6_duration.as_secs_f64();
    info!(
        "Service matching processed in {:.2?}. Phase 6 complete.",
        phase6_duration
    );
    info!("Pipeline progress: [6/6] phases (100%)");

    // Calculate total processing time
    stats.total_processing_time = stats.entity_processing_time
        + stats.context_feature_extraction_time
        + stats.matching_time
        + stats.clustering_time
        + stats.visualization_edge_calculation_time
        + stats.service_matching_time;

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

async fn run_matching_pipeline(
    pool: &PgPool,
    reinforcement_orchestrator: Arc<Mutex<reinforcement::MatchingOrchestrator>>,
    run_id: String,
) -> Result<(usize, Vec<MatchMethodStats>)> {
    info!("Parallelizing matching strategies...");
    let start_time_matching = Instant::now();

    // Parse the run_id into a UUID type
    // This will fix the type mismatch between String and PostgreSQL UUID
    let run_id_uuid = match Uuid::parse_str(&run_id) {
        Ok(uuid) => uuid,
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to parse run_id as UUID: {}", e));
        }
    };

    // The vector of JoinHandles will hold tasks that resolve to Result<AnyMatchResult, anyhow::Error>
    let mut tasks: Vec<JoinHandle<Result<AnyMatchResult, anyhow::Error>>> = Vec::new();

    // --- Spawn Email Matching Task ---
    let pool_clone_email = pool.clone();
    let orchestrator_clone_email = reinforcement_orchestrator.clone();
    let run_id_clone_email = run_id.clone(); // Keep using string for compatibility with existing API
    tasks.push(tokio::spawn(async move {
        info!("Starting email matching task...");
        let result = matching::email::find_matches(
            &pool_clone_email,
            Some(orchestrator_clone_email),
            &run_id_clone_email,
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
    tasks.push(tokio::spawn(async move {
        info!("Starting phone matching task...");
        let result = matching::phone::find_matches(
            &pool_clone_phone,
            Some(&orchestrator_clone_phone),
            &run_id_clone_phone,
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
    tasks.push(tokio::spawn(async move {
        info!("Starting URL matching task...");
        let result = matching::url::find_matches(
            &pool_clone_url,
            Some(orchestrator_clone_url),
            &run_id_clone_url,
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
    tasks.push(tokio::spawn(async move {
        info!("Starting address matching task...");
        let result = matching::address::find_matches(
            &pool_clone_address,
            Some(orchestrator_clone_address),
            &run_id_clone_address,
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
    tasks.push(tokio::spawn(async move {
        info!("Starting name matching task...");
        let result = matching::name::find_matches(
            &pool_clone_name,
            Some(orchestrator_clone_name),
            &run_id_clone_name,
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
    tasks.push(tokio::spawn(async move {
        info!("Starting geospatial matching task...");
        let result = matching::geospatial::find_matches(
            &pool_clone_geo,
            Some(orchestrator_clone_geo),
            &run_id_clone_geo,
        )
        .await
        .context("Geospatial matching task failed");
        info!(
            "Geospatial matching task finished: {:?}",
            result.as_ref().map(|r| r.groups_created())
        );
        result
    }));

    // try_join_all awaits all JoinHandles.
    // Each JoinHandle resolves to a Result<InnerTaskResult, JoinError>.
    // Our InnerTaskResult is Result<AnyMatchResult, anyhow::Error>.
    let join_handle_results: Result<
        Vec<Result<AnyMatchResult, anyhow::Error>>,
        tokio::task::JoinError,
    > = try_join_all(tasks).await;

    let mut total_groups = 0;
    let mut method_stats_vec = Vec::new();
    let mut completed_methods = 0;
    let total_matching_methods = 6;

    // First, handle potential JoinError from try_join_all
    match join_handle_results {
        Ok(individual_task_results) => {
            // individual_task_results is Vec<Result<AnyMatchResult, anyhow::Error>>
            for (idx, task_result) in individual_task_results.into_iter().enumerate() {
                match task_result {
                    Ok(any_match_result) => {
                        // Task completed successfully and returned Ok(AnyMatchResult)
                        info!(
                            "Processing result for task index {}: method {:?}, groups created {}.",
                            idx,
                            any_match_result.stats().method_type,
                            any_match_result.groups_created()
                        );
                        total_groups += any_match_result.groups_created();
                        method_stats_vec.push(any_match_result.stats().clone()); // Clone stats
                        completed_methods += 1;
                    }
                    Err(task_err) => {
                        // Task completed but returned an Err from its own logic
                        warn!(
                            "Matching task at index {} failed internally: {:?}",
                            idx, task_err
                        );
                        // Depending on requirements, you might want to collect all errors
                        // or return the first one. try_join_all behavior makes this tricky
                        // if you want to wait for all even if some fail internally.
                        // For now, let's return the first internal task error.
                        return Err(
                            task_err.context(format!("Matching task at index {} failed", idx))
                        );
                    }
                }
            }
        }
        Err(join_err) => {
            // One of the tasks panicked or was cancelled
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
        // This state could occur if try_join_all itself didn't error (no panics),
        // but one or more of the inner Results was an Err, and we decided to continue
        // rather than returning early. The current logic returns on the first Err.
    }

    info!(
        "All matching strategies processed in {:.2?}. Total groups from successful tasks: {}, Methods processed: {}/{}.",
        start_time_matching.elapsed(),
        total_groups,
        completed_methods, // or method_stats_vec.len()
        total_matching_methods
    );

    Ok((total_groups, method_stats_vec))
}

async fn consolidate_clusters_helper(
    pool: &PgPool,
    reinforcement_orchestrator: Arc<Mutex<reinforcement::MatchingOrchestrator>>,
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
