// src/main.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::future::try_join_all;
use log::{debug, info, warn};
use std::{
    // any::Any, // Appears unused
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinHandle};
use uuid::Uuid;

use dedupe_lib::{
    cluster_visualization, // config, // 'config' seems unused in this file
    consolidate_clusters,
    db::{self, PgPool},
    entity_organizations, matching,
    models::{self, *}, // Imports all from models
    reinforcement::{
        // self as reinforcement_root, // Alias if needed, but direct use is fine
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
            service_orchestrator::{self, ServiceMatchingOrchestrator}, // service_orchestrator alias might be confusing
        },
    },
    results::{
        self, // AddressMatchResult, // Unused directly
        AnyMatchResult, // EmailMatchResult, // Unused directly
        // GeospatialMatchResult, // Unused directly
        MatchMethodStats, // NameMatchResult, // Unused directly
        // PhoneMatchResult, // Unused directly
        PipelineStats, ServiceMatchResult,
        // UrlMatchResult, // Unused directly
    },
    service_cluster_visualization, service_consolidate_clusters, service_matching,
};

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

    let pool = db::connect().await.context("Failed to connect to database")?;
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
        run_id: run_id.clone(), // Clone run_id for stats
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

    info!("Pipeline started. Progress: [0/8] phases (0%)");

    // Phase 1: Entity identification
    let phase1_start = Instant::now();
    stats.total_entities = identify_entities(pool).await?;
    let phase1_duration = phase1_start.elapsed();
    phase_times.insert("entity_identification".to_string(), phase1_duration);
    stats.entity_processing_time = phase1_duration.as_secs_f64();
    info!("Identified {} entities in {:.2?}. Phase 1 complete.", stats.total_entities, phase1_duration);
    info!("Pipeline progress: [1/8] phases (12.5%)");


    // Phase 2: Entity Context Feature Extraction and Cache Pre-warming
    let entity_feature_cache: SharedFeatureCache = create_shared_cache();
    let phase2_start = Instant::now();
    if stats.total_entities > 0 {
        match extract_and_store_all_entity_features_and_prewarm_cache(pool, &entity_feature_cache).await {
            Ok(count) => info!("Entity context features & cache pre-warmed for {} entities.", count),
            Err(e) => warn!("Entity context feature extraction/cache pre-warming failed: {}.", e),
        }
        let max_pairs = 100; // Configuration for prewarming
        match prewarm_pair_features_cache(pool, &entity_feature_cache, max_pairs).await {
            Ok(count) => info!("Pre-warmed pair features cache for {} entity pairs.", count),
            Err(e) => warn!("Pair features cache pre-warming failed: {}.", e),
        }
    } else {
        info!("Skipping entity context feature extraction: no entities.");
    }
    let phase2_duration = phase2_start.elapsed();
    phase_times.insert("entity_context_feature_extraction_and_prewarming".to_string(), phase2_duration);
    stats.context_feature_extraction_time = phase2_duration.as_secs_f64();
    info!("Entity Context Feature Extraction and Cache Pre-warming complete in {:.2?}. Phase 2 complete.", phase2_duration);
    info!("Pipeline progress: [2/8] phases (25%)");


    // Initialize Entity MatchingOrchestrator
    let entity_matching_orchestrator_instance = MatchingOrchestrator::new(pool).await
        .context("Failed to initialize entity MatchingOrchestrator")?;
    let entity_matching_orchestrator = Arc::new(Mutex::new(entity_matching_orchestrator_instance));
    {
        let mut orchestrator_guard = entity_matching_orchestrator.lock().await;
        orchestrator_guard.set_feature_cache(entity_feature_cache.clone());
    }


    // Phase 3: Entity matching
    let phase3_start = Instant::now();
    let (total_groups, method_stats_match) = run_entity_matching_pipeline( // Renamed for clarity
        pool,
        entity_matching_orchestrator.clone(),
        stats.run_id.clone(),
        entity_feature_cache.clone(), // Pass the entity feature cache
    ).await?;
    stats.total_groups = total_groups;
    stats.method_stats.extend(method_stats_match);
    let phase3_duration = phase3_start.elapsed();
    phase_times.insert("entity_matching".to_string(), phase3_duration);
    stats.matching_time = phase3_duration.as_secs_f64();
    info!("Created {} entity groups in {:.2?}. Phase 3 complete.", stats.total_groups, phase3_duration);
    info!("Pipeline progress: [3/8] phases (37.5%)");


    // Phase 4: Cluster consolidation
    let phase4_start = Instant::now();
    stats.total_clusters = consolidate_clusters_helper(pool, entity_matching_orchestrator, stats.run_id.clone()).await?;
    let phase4_duration = phase4_start.elapsed();
    phase_times.insert("cluster_consolidation".to_string(), phase4_duration);
    stats.clustering_time = phase4_duration.as_secs_f64();
    info!("Formed {} clusters in {:.2?}. Phase 4 complete.", stats.total_clusters, phase4_duration);
    info!("Pipeline progress: [4/8] phases (50%)");


    // Phase 5: Visualization edge calculation
    let clusters_changed = stats.total_clusters > 0; // Simplified condition
    let existing_viz_edges = cluster_visualization::visualization_edges_exist(pool, &stats.run_id).await?;
    if clusters_changed || !existing_viz_edges {
        let phase5_start = Instant::now();
        cluster_visualization::ensure_visualization_tables_exist(pool).await?;
        stats.total_visualization_edges = cluster_visualization::calculate_visualization_edges(pool, &stats.run_id).await?;
        let phase5_duration = phase5_start.elapsed();
        phase_times.insert("visualization_edge_calculation".to_string(), phase5_duration);
        stats.visualization_edge_calculation_time = phase5_duration.as_secs_f64();
        info!("Calculated {} entity viz edges in {:.2?}. Phase 5 complete.", stats.total_visualization_edges, phase5_duration);
    } else {
        info!("Skipping entity viz edge calculation. Phase 5 complete.");
        stats.visualization_edge_calculation_time = 0.0;
    }
    info!("Pipeline progress: [5/8] phases (62.5%)");


    // Initialize ServiceMatchingOrchestrator and its feature cache
    info!("Initializing ServiceMatchingOrchestrator...");
    let mut service_orchestrator_instance = ServiceMatchingOrchestrator::new(pool).await
        .context("Failed to initialize ServiceMatchingOrchestrator")?;
    let service_feature_vector_cache = create_shared_service_cache(); // Cache for *feature vectors*
    service_orchestrator_instance.set_feature_cache(service_feature_vector_cache.clone());
    let service_matching_orchestrator = Arc::new(Mutex::new(service_orchestrator_instance));


    // Phase 5.5: Service Context Feature Extraction and Cache Pre-warming
    info!("Phase 5.5: Service Context Feature Extraction and Cache Pre-warming");
    let phase5_5_start = Instant::now();
    let service_features_count = extract_and_store_all_service_features_and_prewarm_cache(pool, &service_feature_vector_cache).await?;
    let max_service_pairs = 100; // Configuration for prewarming
    match prewarm_service_pair_features_cache(pool, &service_feature_vector_cache, max_service_pairs).await {
        Ok(pairs_count) => info!("Pre-warmed service pair features cache for {} pairs.", pairs_count),
        Err(e) => warn!("Service pair features cache pre-warming failed: {}.", e),
    }
    let phase5_5_duration = phase5_5_start.elapsed();
    phase_times.insert("service_context_feature_extraction_and_prewarming".to_string(), phase5_5_duration);
    stats.service_context_feature_extraction_time = phase5_5_duration.as_secs_f64();
    info!("Service Context Feature Extraction/Pre-warming complete in {:.2?} ({} services). Phase 5.5 complete.", phase5_5_duration, service_features_count);


    // Phase 6: Service matching
    info!("Phase 6: Service matching");
    let phase6_start = Instant::now();
    let service_match_run_result = run_service_matching_pipeline(
        pool,
        &stats.run_id,
        service_matching_orchestrator.clone(), // Pass the orchestrator
        service_feature_vector_cache.clone(), // Pass the feature vector cache explicitly
    ).await.context("Service matching failed")?;
    stats.total_service_matches = service_match_run_result.groups_created;
    stats.method_stats.extend(service_match_run_result.method_stats.clone()); // Add service method stats
    // Populate service_stats (simplified)
    if stats.service_stats.is_none() {
        stats.service_stats = Some(results::ServiceMatchStats {
            total_matches: service_match_run_result.groups_created,
            avg_similarity: service_match_run_result.stats.avg_confidence,
            high_similarity_matches: service_match_run_result.method_stats.iter().filter(|s| s.avg_confidence >= 0.9).map(|s| s.groups_created).sum(),
            medium_similarity_matches: service_match_run_result.method_stats.iter().filter(|s| s.avg_confidence >= 0.8 && s.avg_confidence < 0.9).map(|s| s.groups_created).sum(),
            low_similarity_matches: service_match_run_result.method_stats.iter().filter(|s| s.avg_confidence < 0.8).map(|s| s.groups_created).sum(),
            clusters_with_matches: 0, // This would need more complex tracking if required
        });
    }
    let phase6_duration = phase6_start.elapsed();
    phase_times.insert("service_matching".to_string(), phase6_duration);
    stats.service_matching_time = phase6_duration.as_secs_f64();
    info!("Service matching processed in {:.2?}. Found {} service matches. Phase 6 complete.", phase6_duration, stats.total_service_matches);
    info!("Pipeline progress: [6/8] phases (75%)");


    // Phase 7: Service cluster consolidation
    info!("Phase 7: Service cluster consolidation");
    let phase7_start = Instant::now();
    service_consolidate_clusters::ensure_consolidation_tables_exist(pool).await?;
    let consolidation_config = service_consolidate_clusters::ConsolidationConfig::default(); // Using default
    match service_consolidate_clusters::consolidate_service_clusters(pool, &stats.run_id, Some(consolidation_config)).await {
        Ok(merged_clusters) => {
            stats.total_service_clusters = merged_clusters; // This is count of *merged pairs*, not total final clusters
            info!("Successfully consolidated/merged {} service cluster pairs.", merged_clusters);
        }
        Err(e) => warn!("Service cluster consolidation error: {}. Continuing.", e),
    }
    let phase7_duration = phase7_start.elapsed();
    phase_times.insert("service_clustering".to_string(), phase7_duration);
    stats.service_clustering_time = phase7_duration.as_secs_f64();
    info!("Service cluster consolidation complete in {:.2?}. Phase 7 complete.", phase7_duration);
    info!("Pipeline progress: [7/8] phases (87.5%)");

    // Phase 8: Service Visualization Edge Calculation
    info!("Phase 8: Calculating service relationship edges for cluster visualization");
    let phase8_start = Instant::now();
    service_cluster_visualization::ensure_visualization_tables_exist(pool).await?;
    stats.total_service_visualization_edges = service_cluster_visualization::calculate_visualization_edges(pool, &stats.run_id).await?;
    let phase8_duration = phase8_start.elapsed();
    phase_times.insert("service_visualization_edge_calculation".to_string(), phase8_duration);
    stats.service_visualization_edge_calculation_time = phase8_duration.as_secs_f64();
    info!("Calculated {} service viz edges in {:.2?}. Phase 8 complete.", stats.total_service_visualization_edges, phase8_duration);
    info!("Pipeline progress: [8/8] phases (100%)");


    // Calculate total processing time
    stats.total_processing_time = phase_times.values().map(|d| d.as_secs_f64()).sum();
    Ok(stats)
}

async fn identify_entities(pool: &PgPool) -> Result<usize> {
    info!("Phase 1: Entity identification starting...");
    let org_entities = entity_organizations::extract_entities(pool).await
        .context("Failed to extract entities from organizations")?;
    info!("Discovered/created {} org-entity mappings.", org_entities.len());

    entity_organizations::link_entity_features(pool, &org_entities).await
        .context("Failed to link entity features")?;
    let updated_features_count = entity_organizations::update_entity_features(pool).await
        .context("Failed to update entity features")?;
    info!("Added {} new feature links to existing entities.", updated_features_count);

    let conn = pool.get().await.context("Failed to get DB connection for counting entities")?;
    let total_entities_row = conn.query_one("SELECT COUNT(*) FROM public.entity", &[]).await
        .context("Failed to count total entities")?;
    let total_entities_count: i64 = total_entities_row.get(0);
    info!("Entity identification complete. Total entities: {}", total_entities_count);
    Ok(total_entities_count as usize)
}

// Renamed to avoid conflict with the service matching pipeline runner
async fn run_entity_matching_pipeline(
    pool: &PgPool,
    entity_matching_orchestrator: Arc<Mutex<MatchingOrchestrator>>,
    run_id: String,
    entity_feature_cache: SharedFeatureCache, // For entity features
) -> Result<(usize, Vec<MatchMethodStats>)> {
    info!("Initializing entity matching pipeline with feature caching...");
    let start_time_matching = Instant::now();
    let mut tasks: Vec<JoinHandle<Result<AnyMatchResult, anyhow::Error>>> = Vec::new();

    // --- Spawn Email Matching Task (Entity) ---
    let pool_clone_email = pool.clone();
    let orchestrator_clone_email = entity_matching_orchestrator.clone();
    let run_id_clone_email = run_id.clone();
    let feature_cache_clone_email = entity_feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        matching::email::find_matches(
            &pool_clone_email, Some(orchestrator_clone_email), &run_id_clone_email, Some(feature_cache_clone_email)
        ).await.context("Entity email matching failed")
    }));

    // --- Spawn Phone Matching Task (Entity) ---
    let pool_clone_phone = pool.clone();
    let orchestrator_clone_phone = entity_matching_orchestrator.clone();
    let run_id_clone_phone = run_id.clone();
    let feature_cache_clone_phone = entity_feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        matching::phone::find_matches(
            &pool_clone_phone, Some(orchestrator_clone_phone), &run_id_clone_phone, Some(feature_cache_clone_phone)
        ).await.context("Entity phone matching failed")
    }));

    // --- Spawn URL Matching Task (Entity) ---
    let pool_clone_url = pool.clone();
    let orchestrator_clone_url = entity_matching_orchestrator.clone();
    let run_id_clone_url = run_id.clone();
    let feature_cache_clone_url = entity_feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        matching::url::find_matches(
            &pool_clone_url, Some(orchestrator_clone_url), &run_id_clone_url, Some(feature_cache_clone_url)
        ).await.context("Entity URL matching failed")
    }));

     // --- Spawn Address Matching Task (Entity) ---
    let pool_clone_address = pool.clone();
    let orchestrator_clone_address = entity_matching_orchestrator.clone();
    let run_id_clone_address = run_id.clone();
    let feature_cache_clone_address = entity_feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        matching::address::find_matches(
            &pool_clone_address, Some(orchestrator_clone_address), &run_id_clone_address, Some(feature_cache_clone_address)
        ).await.context("Entity address matching failed")
    }));

    // --- Spawn Name Matching Task (Entity) ---
    let pool_clone_name = pool.clone();
    let orchestrator_clone_name = entity_matching_orchestrator.clone();
    let run_id_clone_name = run_id.clone();
    let feature_cache_clone_name = entity_feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        matching::name::find_matches(
            &pool_clone_name, Some(orchestrator_clone_name), &run_id_clone_name, Some(feature_cache_clone_name)
        ).await.context("Entity name matching failed")
    }));

    // --- Spawn Geospatial Matching Task (Entity) ---
    let pool_clone_geo = pool.clone();
    let orchestrator_clone_geo = entity_matching_orchestrator.clone();
    let run_id_clone_geo = run_id.clone();
    let feature_cache_clone_geo = entity_feature_cache.clone();
    tasks.push(tokio::spawn(async move {
        matching::geospatial::find_matches(
            &pool_clone_geo, Some(orchestrator_clone_geo), &run_id_clone_geo, Some(feature_cache_clone_geo)
        ).await.context("Entity geospatial matching failed")
    }));


    let join_handle_results = try_join_all(tasks).await;
    let mut total_groups = 0;
    let mut method_stats_vec = Vec::new();

    match join_handle_results {
        Ok(results_vec) => { // Renamed from results to results_vec to avoid conflict
            for res in results_vec {
                match res {
                    Ok(any_match_result) => {
                        total_groups += any_match_result.groups_created();
                        method_stats_vec.push(any_match_result.stats().clone());
                    }
                    Err(e) => return Err(e.context("A matching task failed internally")),
                }
            }
        }
        Err(e) => return Err(anyhow::Error::from(e).context("A matching task panicked or was cancelled")),
    }
    
    // Report and clear entity feature cache stats
    {
        let mut cache_guard = entity_feature_cache.lock().await;
        let (hits, misses, ind_hits, ind_misses) = cache_guard.get_stats();
        let hit_rate = if hits + misses > 0 { (hits as f64 / (hits + misses) as f64) * 100.0 } else { 0.0 };
        let ind_hit_rate = if ind_hits + ind_misses > 0 { (ind_hits as f64 / (ind_hits + ind_misses) as f64) * 100.0 } else { 0.0 };
        info!("Entity Feature Cache - Pair: {} hits, {} misses ({:.2}%). Individual: {} hits, {} misses ({:.2}%).", hits, misses, hit_rate, ind_hits, ind_misses, ind_hit_rate);
        cache_guard.clear();
    }

    info!("Entity matching processed in {:.2?}. Total groups: {}.", start_time_matching.elapsed(), total_groups);
    Ok((total_groups, method_stats_vec))
}


async fn consolidate_clusters_helper(
    pool: &PgPool,
    _reinforcement_orchestrator: Arc<Mutex<MatchingOrchestrator>>, // Orchestrator might be used for advanced consolidation in future
    run_id: String,
) -> Result<usize> {
    let conn = pool.get().await.context("Failed to get DB connection for consolidate_clusters_helper")?;
    let groups_row = conn.query_one("SELECT COUNT(*) FROM public.entity_group WHERE group_cluster_id IS NULL", &[])
        .await.context("Failed to count unassigned groups")?;
    let unassigned_groups: i64 = groups_row.get(0);
    info!("Found {} groups needing cluster assignment.", unassigned_groups);
    if unassigned_groups == 0 {
        info!("No groups require clustering. Skipping consolidation.");
        return Ok(0);
    }
    consolidate_clusters::process_clusters(pool, &run_id).await.context("Failed to process clusters")
}

// This is the main service matching pipeline runner, called from run_pipeline
async fn run_service_matching_pipeline(
    pool: &PgPool,
    pipeline_run_id: &str,
    service_orchestrator: Arc<Mutex<ServiceMatchingOrchestrator>>, // Orchestrator for RL
    feature_cache: SharedServiceFeatureCache, // Cache for *service feature vectors*
) -> Result<results::ServiceMatchResult> {
    info!("Starting cluster-scoped service matching pipeline...");
    let start_time = Instant::now();

    let conn = pool.get().await.context("Failed to get DB connection for clusters")?;
    let cluster_rows = conn.query(
        "SELECT id FROM public.entity_group_cluster WHERE id IS NOT NULL", &[]
    ).await.context("Failed to query entity_group_cluster IDs")?;
    let cluster_ids: Vec<String> = cluster_rows.into_iter().map(|row| row.get("id")).collect();

    if cluster_ids.is_empty() {
        info!("No entity clusters found. Service matching skipped.");
        // Corrected: Directly initialize ServiceMatchResult for an empty case.
        // This assumes ServiceMatchResult has a constructor or you define one (e.g., ServiceMatchResult::new_empty).
        // For now, creating a default one. You might need to adjust this based on your results.rs definition.
        let skipped_method_type = MatchMethodType::Custom("service_combined_skipped".to_string());
        return Ok(ServiceMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(skipped_method_type),
            method_stats: Vec::new(),
        });
    }
    info!("Found {} entity clusters for service matching. Processing...", cluster_ids.len());

    let mut all_tasks: Vec<JoinHandle<Result<ServiceMatchResult, anyhow::Error>>> = Vec::new();

    for cluster_id_str in cluster_ids {
        // Fetch service details for the current cluster
        let service_detail_rows = conn.query(
            "WITH cluster_entities AS (
                SELECT entity_id_1 AS entity_id FROM public.entity_group WHERE group_cluster_id = $1
                UNION
                SELECT entity_id_2 AS entity_id FROM public.entity_group WHERE group_cluster_id = $1
            )
            SELECT s.id, s.name, s.url, s.email, s.embedding_v2
            FROM public.service s
            JOIN public.entity_feature ef ON s.id = ef.table_id AND ef.table_name = 'service'
            WHERE ef.entity_id IN (SELECT entity_id FROM cluster_entities) AND s.status = 'active'
            GROUP BY s.id, s.name, s.url, s.email, s.embedding_v2",
            &[&cluster_id_str],
        ).await.context(format!("Failed to query services for cluster {}", cluster_id_str))?;

        if service_detail_rows.is_empty() {
            debug!("No active services in cluster_id: {}. Skipping.", cluster_id_str);
            continue;
        }

        let mut services_for_name: Vec<(ServiceId, String)> = Vec::new();
        let mut services_for_url: Vec<(ServiceId, String)> = Vec::new();
        let mut services_for_email: Vec<(ServiceId, String)> = Vec::new();
        let mut services_for_embedding: Vec<(ServiceId, String, Option<Vec<f32>>)> = Vec::new();

        for row in &service_detail_rows {
            let service_id_val: String = row.get("id");
            let service_id = ServiceId(service_id_val);
            let name_val: Option<String> = row.get("name");
            let url_val: Option<String> = row.get("url");
            let email_val: Option<String> = row.get("email");
            let embedding_pg_opt: Option<pgvector::Vector> = row.get("embedding_v2");
            let embedding_f32_opt = embedding_pg_opt.map(|pg_vec| pg_vec.to_vec());

            if let Some(name) = name_val.clone() {
                if !name.is_empty() {
                    services_for_name.push((service_id.clone(), name.clone()));
                    services_for_embedding.push((service_id.clone(), name, embedding_f32_opt.clone()));
                }
            }
            if let Some(url) = url_val { if !url.is_empty() { services_for_url.push((service_id.clone(), url)); }}
            if let Some(email) = email_val { if !email.is_empty() { services_for_email.push((service_id.clone(), email)); }}
        }

        if !services_for_name.is_empty() {
            let task_pool = pool.clone(); let task_orch = service_orchestrator.clone(); let task_fc = feature_cache.clone(); let task_prid = pipeline_run_id.to_string(); let task_cid = cluster_id_str.clone();
            all_tasks.push(tokio::spawn(async move { service_matching::name::find_matches_in_cluster(&task_pool, Some(task_orch), &task_prid, Some(task_fc), services_for_name, task_cid).await }));
        }
        if !services_for_url.is_empty() {
            let task_pool = pool.clone(); let task_orch = service_orchestrator.clone(); let task_fc = feature_cache.clone(); let task_prid = pipeline_run_id.to_string(); let task_cid = cluster_id_str.clone();
            all_tasks.push(tokio::spawn(async move { service_matching::url::find_matches_in_cluster(&task_pool, Some(task_orch), &task_prid, Some(task_fc), services_for_url, task_cid).await }));
        }
        if !services_for_email.is_empty() {
            let task_pool = pool.clone(); let task_orch = service_orchestrator.clone(); let task_fc = feature_cache.clone(); let task_prid = pipeline_run_id.to_string(); let task_cid = cluster_id_str.clone();
            all_tasks.push(tokio::spawn(async move { service_matching::email::find_matches_in_cluster(&task_pool, Some(task_orch), &task_prid, Some(task_fc), services_for_email, task_cid).await }));
        }
        if !services_for_embedding.is_empty() { 
            let task_pool = pool.clone(); let task_orch = service_orchestrator.clone(); let task_fc = feature_cache.clone(); let task_prid = pipeline_run_id.to_string(); let task_cid = cluster_id_str.clone();
            all_tasks.push(tokio::spawn(async move { service_matching::embedding::find_matches_in_cluster(&task_pool, Some(task_orch), &task_prid, Some(task_fc), services_for_embedding, task_cid).await }));
        }
    }

    let mut total_groups_created_all_methods = 0;
    let mut aggregated_method_stats: HashMap<String, MatchMethodStats> = HashMap::new();

    let task_join_results = try_join_all(all_tasks).await;
    match task_join_results {
        Ok(results_per_task) => {
            for res_task in results_per_task {
                match res_task {
                    Ok(method_result_in_cluster) => {
                        total_groups_created_all_methods += method_result_in_cluster.groups_created;
                        for stat in method_result_in_cluster.method_stats { 
                            // Corrected: Use default_for as suggested by compiler
                            let entry = aggregated_method_stats.entry(stat.method_type.as_str().to_string()).or_insert_with(|| MatchMethodStats::default_for(stat.method_type.clone()));
                            entry.groups_created += stat.groups_created;
                            entry.entities_matched += stat.entities_matched; 
                            let total_old_confidence_points = entry.avg_confidence * (entry.groups_created - stat.groups_created) as f64;
                            let current_confidence_points = stat.avg_confidence * stat.groups_created as f64;
                            if entry.groups_created > 0 {
                                entry.avg_confidence = (total_old_confidence_points + current_confidence_points) / entry.groups_created as f64;
                            } else {
                                entry.avg_confidence = 0.0;
                            }
                        }
                    }
                    Err(e) => warn!("A service matching task failed: {:?}", e),
                }
            }
        }
        Err(e) => return Err(anyhow::Error::from(e).context("Service matching task panicked or was cancelled")),
    }
    
    if let Ok(mut orchestrator_guard) = service_orchestrator.try_lock() { 
        info!("Processing human feedback for service confidence tuner (end of service matching)...");
        if let Err(e) = orchestrator_guard.process_feedback_and_update_tuner(pool).await {
            warn!("Failed to process feedback and update service tuner: {}", e);
        }
        if let Err(e) = orchestrator_guard.save_models(pool).await {
            warn!("Failed to save service confidence tuner models: {}", e);
        }
        if let Some((hits, misses, ind_hits, ind_misses)) = orchestrator_guard.get_feature_cache_stats().await {
            let hit_rate = if hits + misses > 0 { (hits as f64 / (hits + misses) as f64) * 100.0 } else { 0.0 };
            let ind_hit_rate = if ind_hits + ind_misses > 0 { (ind_hits as f64 / (ind_hits + ind_misses) as f64) * 100.0 } else { 0.0 };
            info!("Service Feature Vector Cache - Pair: {} hits, {} misses ({:.2}%). Individual: {} hits, {} misses ({:.2}%).", hits, misses, hit_rate, ind_hits, ind_misses, ind_hit_rate);
        }
    } else {
        warn!("Could not acquire lock on service_orchestrator to process feedback or get cache stats at the end of service matching.");
    }

    let final_method_stats_vec: Vec<MatchMethodStats> = aggregated_method_stats.values().cloned().collect();
    
    // Corrected: Directly calculate combined_stats here
    let combined_stats_method_name = "service_combined_all_clusters";
    let combined_stats = if final_method_stats_vec.is_empty() {
        MatchMethodStats::default_for(MatchMethodType::Custom(
            format!("{}_no_methods", combined_stats_method_name),
        ))
    } else {
        let mut total_entities_matched_across_methods = 0;
        let mut total_weighted_confidence = 0.0;
        let mut total_groups_for_avg_confidence = 0;

        for stat in &final_method_stats_vec {
            total_entities_matched_across_methods += stat.entities_matched;
            total_weighted_confidence += stat.avg_confidence * stat.groups_created as f64;
            total_groups_for_avg_confidence += stat.groups_created;
        }

        let avg_confidence = if total_groups_for_avg_confidence > 0 {
            total_weighted_confidence / total_groups_for_avg_confidence as f64
        } else {
            0.0
        };

        MatchMethodStats {
            method_type: MatchMethodType::Custom(combined_stats_method_name.to_string()),
            groups_created: total_groups_created_all_methods,
            entities_matched: total_entities_matched_across_methods,
            avg_confidence,
            avg_group_size: if total_groups_created_all_methods > 0 { 2.0 } else { 0.0 },
        }
    };


    info!("Cluster-scoped service matching complete in {:.2?}. Created {} service groups.", start_time.elapsed(), total_groups_created_all_methods);
    Ok(results::ServiceMatchResult {
        groups_created: total_groups_created_all_methods,
        stats: combined_stats,
        method_stats: final_method_stats_vec,
    })
}
