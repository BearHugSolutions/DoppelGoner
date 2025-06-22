use anyhow::{Context, Result};
use chrono::Utc;
use dedupe_lib::clustering::entity_clustering::run_entity_clustering;
use dedupe_lib::entity_identification::{identify_entities, link_and_update_entity_features};
use dedupe_lib::matching::manager::run_entity_matching_pipeline;
use dedupe_lib::rl::extract_and_store_all_contextual_features;
use dedupe_lib::rl::feature_cache::{create_shared_cache, SharedFeatureCache};
use dedupe_lib::rl::orchestrator::RLOrchestrator;
use dedupe_lib::utils::db_connect::{connect, get_pool_status};
use dedupe_lib::utils::get_memory_usage;
use dedupe_lib::utils::progress_bars::progress_config::ProgressConfig;
use dedupe_lib::utils::{env::load_env, instantiate_run::create_initial_pipeline_run};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use std::collections::HashMap;
use std::sync::Arc; // Import Arc
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging and environment
    env_logger::init();
    info!("Starting HSDS organization grouping and clustering pipeline");
    load_env();

    // Load progress configuration from environment
    // Wrap progress_config in an Arc so it can be shared and cloned for multiple async tasks/closures
    let progress_config_arc = Arc::new(ProgressConfig::from_env());
    info!(
        "Progress tracking: enabled={}, detailed={}",
        progress_config_arc.enabled, progress_config_arc.detailed
    );

    // Initialize progress tracking if enabled
    let multi_progress = progress_config_arc.create_multi_progress();

    // Create main pipeline progress bar
    let main_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(4));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Initializing pipeline...");
        Some(pb)
    } else {
        None
    };

    let pool = connect().await.context("Failed to connect to database")?;
    info!("Successfully connected to the database");

    info!("Instantiating variables for run");
    let mut phase_times = HashMap::new();
    let run_id_uuid = Uuid::new_v4();
    let run_id = run_id_uuid.to_string();
    let run_timestamp = Utc::now().naive_utc();
    let description = Some("Regular pipeline run with progress tracking".to_string());

    let mut stats =
        create_initial_pipeline_run(&pool, &run_id, run_timestamp, description.as_deref())
            .await
            .context("Failed to create initial pipeline run")?;
    info!(
        "Successfully created initial pipeline run with ID: {}",
        run_id
    );

    // Helper closure to update progress message with common stats.
    // This closure now takes owned copies of ProgressBar, Arc<ProgressConfig>, and PgPool.
    // When called, the actual variables `main_pb`, `progress_config_arc`, and `pool` will be cloned
    // and those clones moved into the async block.
    let update_main_pb_message = |
        pb_clone: ProgressBar, // Take an owned clone of the ProgressBar
        phase_name: String,    // Take an owned String for phase_name
        current_step: usize,
        config_arc: Arc<ProgressConfig>, // Take an Arc clone
        db_pool_clone: dedupe_lib::utils::db_connect::PgPool // Take an owned clone of the Pool
    | async move { // The async block owns all its captured variables
        if config_arc.should_show_memory() || config_arc.should_show_db_connection_stats() {
            let mut parts = Vec::new();
            if config_arc.should_show_memory() {
                let memory_mb = get_memory_usage().await;
                parts.push(format!("Memory: {} MB", memory_mb));
            }
            if config_arc.should_show_db_connection_stats() {
                let (size, available, recycled) = get_pool_status(&db_pool_clone);
                parts.push(format!("DB: {}/{} (used/total)", size - available, size));
            }
            pb_clone.set_message(format!("{}: {} ({})", phase_name, current_step, parts.join(", ")));
        } else {
            pb_clone.set_message(format!("{}: {}", phase_name, current_step));
        }
    };


    // Phase 1: Entity Identification & Feature Linking
    if let Some(pb) = &main_pb {
        // Clone `pb`, `progress_config_arc`, and `pool` for this specific call to the closure
        update_main_pb_message(
            pb.clone(),
            "Phase 1: Entity identification and feature linking".to_string(),
            0,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase1_start = Instant::now();

    info!("Phase 1: Entity identification and feature linking starting...");

    let total_entities = identify_entities(&pool, multi_progress.clone())
        .await
        .context("Failed to identify entities")?;
    stats.total_entities = total_entities;

    let total_entity_features = link_and_update_entity_features(&pool, multi_progress.clone())
        .await
        .context("Failed to link entity features")?;
    stats.total_entity_features = total_entity_features;

    let phase1_duration = phase1_start.elapsed();
    phase_times.insert(
        "entity_identification_and_linking".to_string(),
        phase1_duration,
    );
    stats.entity_processing_time = phase1_duration.as_secs_f64();

    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 1 complete".to_string(),
            1,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 2: RL Contextual Feature Extraction
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 2: RL contextual feature extraction".to_string(),
            1,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let entity_feature_cache: SharedFeatureCache = create_shared_cache();
    let phase2_start = Instant::now();

    info!("Phase 2: RL contextual feature extraction starting...");

    let contextual_features_processed = extract_and_store_all_contextual_features(
        &pool,
        &entity_feature_cache,
        multi_progress.clone(),
    )
    .await
    .context("Failed to extract contextual features")?;

    let phase2_duration = phase2_start.elapsed();
    phase_times.insert("contextual_feature_extraction".to_string(), phase2_duration);
    stats.context_feature_extraction_time = phase2_duration.as_secs_f64();

    info!(
        "Phase 2 completed: Processed contextual features for {} entities",
        contextual_features_processed
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            "Phase 2 complete".to_string(),
            2,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 3: Entity Matching
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 3: Entity matching".to_string(),
            2,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase3_start = Instant::now();

    // Initialize Entity RLOrchestrator
    let entity_matching_orchestrator_instance = RLOrchestrator::new(&pool)
        .await
        .context("Failed to initialize entity RLOrchestrator")?;
    let entity_matching_orchestrator = Arc::new(Mutex::new(entity_matching_orchestrator_instance));

    let (total_groups, method_stats_match) = run_entity_matching_pipeline(
        &pool,
        entity_matching_orchestrator.clone(),
        stats.run_id.clone(),
        entity_feature_cache.clone(),
        multi_progress.clone(),
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

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            format!("Phase 3 complete: {} groups", stats.total_groups),
            3,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 4: Clustering
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 4: Clustering".to_string(),
            3,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase4_start = Instant::now();

    let total_clusters = run_entity_clustering(&pool, &run_id, multi_progress.clone()).await?;

    let phase4_duration = phase4_start.elapsed();
    phase_times.insert("clustering".to_string(), phase4_duration);
    stats.total_clusters = total_clusters;
    stats.clustering_time = phase4_duration.as_secs_f64();

    info!(
        "Created {} clusters in {:.2?}. Phase 4 complete.",
        stats.total_clusters, phase4_duration
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            format!("Pipeline complete: {} clusters", stats.total_clusters),
            4,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
        pb.finish(); // Finish the main progress bar
    }

    // Print comprehensive summary
    let total_time = phase1_duration + phase2_duration + phase3_duration + phase4_duration;

    info!("=== Pipeline Summary ===");
    info!("Run ID: {}", run_id);
    info!("Total entities: {}", stats.total_entities);
    info!("Total entity features: {}", stats.total_entity_features);
    info!("Total groups created: {}", stats.total_groups);
    info!("Total clusters created: {}", stats.total_clusters);
    info!("=== Timing Breakdown ===");
    info!("Phase 1 (Entity ID & Features): {:.2?}", phase1_duration);
    info!("Phase 2 (Feature Extraction): {:.2?}", phase2_duration);
    info!("Phase 3 (Entity Matching): {:.2?}", phase3_duration);
    info!("Phase 4 (Clustering): {:.2?}", phase4_duration);
    info!("Total execution time: {:.2?}", total_time);

    // Use the original `progress_config_arc` here after all calls
    if progress_config_arc.should_show_memory() {
        let final_memory_mb = get_memory_usage().await;
        info!("Final memory usage: {} MB", final_memory_mb);
    }

    // Print method-specific statistics
    if progress_config_arc.should_show_cache_stats() && !stats.method_stats.is_empty() {
        info!("=== Method Statistics ===");
        for method_stat in &stats.method_stats {
            info!(
                "{:?}: {} groups, {} entities, avg confidence: {:.3}",
                method_stat.method_type,
                method_stat.groups_created,
                method_stat.entities_matched,
                method_stat.avg_confidence
            );
        }
    }

    // Final log of connection pool status
    let (pool_size, available_connections, in_use_connections) = get_pool_status(&pool);
    info!(
        "Final DB Connection Pool Status: Total: {}, Available: {}, In Use: {}",
        pool_size,
        available_connections,
        in_use_connections
    );

    info!("Pipeline completed successfully!");
    Ok(())
}
