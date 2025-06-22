use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::Context;
use chrono::Utc;
use dedupe_lib::{
    candidate_generation::candidate_generation::{add_unmatched_entities, get_entity_clusters, load_service_clusters},
    clustering::create_clusters::run_service_clustering,
    matching::{db::insert_service_groups_batch, manager::run_enhanced_service_matching_pipeline},
    models::stats_models::ServiceMatchingStats,
    // NEW: RL imports
    rl::{create_shared_service_cache, extract_and_store_all_service_contextual_features, ServiceRLOrchestrator},
    utils::{db_connect::{connect, get_pool_status}, env::load_env, get_memory_usage, instantiate_run::create_initial_pipeline_run, progress_config::ProgressConfig} // Import get_pool_status
};
use indicatif::{ProgressBar, ProgressStyle};
use log::{info, warn};
use tokio::sync::Mutex;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Initialize logging and environment
    env_logger::init();
    info!("Starting HSDS service grouping and clustering pipeline with RL integration");
    load_env();

    // Load progress configuration from environment
    // Wrap progress_config in an Arc so it can be shared and cloned for multiple async tasks/closures
    let progress_config_arc = Arc::new(ProgressConfig::from_env());
    info!(
        "Progress tracking: enabled={}, detailed={}",
        progress_config_arc.enabled, progress_config_arc.detailed
    );

    // Initialize multi-progress tracking if enabled
    let multi_progress = progress_config_arc.create_multi_progress();

    // Create main pipeline progress bar
    let main_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(6)); // Updated to 6 phases with RL
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Initializing pipeline with RL...");
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
    let description = Some("Regular pipeline run with RL integration and progress tracking".to_string());

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

    // NEW: Phase 1: Initialize RL Infrastructure
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 1: Initialize RL Infrastructure".to_string(),
            0,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let rl_phase_start = Instant::now();

    info!("Initializing Service RL Infrastructure...");

    // Initialize Service RL Orchestrator
    let mut service_rl_orchestrator = ServiceRLOrchestrator::new(&pool).await
        .context("Failed to initialize ServiceRLOrchestrator")?;
    info!("ServiceRLOrchestrator initialized successfully");

    // Create and set up service feature cache
    let service_feature_cache = create_shared_service_cache();
    service_rl_orchestrator.set_feature_cache(service_feature_cache.clone());
    info!("Service feature cache initialized and connected to orchestrator");

    // Wrap orchestrator in Arc<Mutex> for sharing across tasks
    let service_rl_orchestrator_arc = Arc::new(Mutex::new(service_rl_orchestrator));

    let rl_phase_duration = rl_phase_start.elapsed();
    phase_times.insert("RL_Infrastructure_Setup".to_string(), rl_phase_duration);


    if let Some(pb) = &main_pb {
        pb.inc(1);
        // Ensure consistent use of the helper closure
        update_main_pb_message(
            pb.clone(),
            "Phase 1 complete".to_string(),
            1,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 2: Prepare Candidates for Matching
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 2: Prepare Candidates for Matching".to_string(),
            1,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase2_start = Instant::now();

    let mut entity_clusters = get_entity_clusters(&pool).await?;
    info!("Found {} clusters with entities", entity_clusters.len());
    let unmatched_count = add_unmatched_entities(&pool, &mut entity_clusters).await?;
    info!("Added {} unmatched entities as individual clusters", unmatched_count);
    let service_clusters = load_service_clusters(&pool, entity_clusters).await?;
    info!("Loaded {} service clusters... Starting RL feature extraction...", service_clusters.len());

    let phase2_duration = phase2_start.elapsed();
    phase_times.insert("Prepare_Candidates_for_Matching".to_string(), phase2_duration);
    stats.entity_processing_time = phase2_duration.as_secs_f64();

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

    // NEW: Phase 3: Service RL Feature Extraction
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 3: Service RL Feature Extraction".to_string(),
            2,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase3_start = Instant::now();

    info!("Starting service contextual feature extraction phase...");
    let feature_extraction_count = extract_and_store_all_service_contextual_features(
        &pool,
        &service_feature_cache,
        // Pass multi_progress conditionally
        if progress_config_arc.should_show_detailed() { multi_progress.clone() } else { None },
    ).await.context("Failed to extract service contextual features")?;

    info!("Service feature extraction completed. Processed {} services.", feature_extraction_count);

    let phase3_duration = phase3_start.elapsed();
    phase_times.insert("Service_Feature_Extraction".to_string(), phase3_duration);
    stats.service_matching_time = phase3_duration.as_secs_f64(); // Temporary assignment, will be updated in phase 4

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            "Phase 3 complete".to_string(),
            3,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 4: Match Services with RL Integration
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 4: Match Services with RL".to_string(),
            3,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase4_start = Instant::now();

    let service_matching_stats = ServiceMatchingStats::default();
    let (matches, final_stats) = run_enhanced_service_matching_pipeline(
        &pool,
        service_clusters,
        &run_id,
        service_matching_stats,
        // Some(service_rl_orchestrator_arc.clone()),
        None,
        Some(service_feature_cache.clone()),
        if progress_config_arc.should_show_detailed() { multi_progress.clone() } else { None },
    ).await?;

    let phase4_duration = phase4_start.elapsed();
    phase_times.insert("Match_Services_with_RL".to_string(), phase4_duration);
    stats.service_matching_time = phase4_duration.as_secs_f64();

    info!(
        "Service matching with RL completed: {} matches found across {} clusters in {:.2?}",
        matches.len(),
        final_stats.total_clusters_processed,
        phase4_duration
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            "Phase 4 complete".to_string(),
            4,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 5: Update Database with RL Decision Logging
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 5: Update Database with RL Decisions".to_string(),
            4,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase5_start = Instant::now();

    // Insert service groups with RL decision logging
    // Note: For now we pass None for RL decision data as the individual matching methods
    // would need to be updated to collect and return this data. This is a placeholder
    // for the full RL decision logging integration.
    insert_service_groups_batch(&pool, matches, &run_id, None).await
        .context("Failed to insert service groups")?;

    let phase5_duration = phase5_start.elapsed();
    phase_times.insert("Update_Database_with_RL".to_string(), phase5_duration);

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            "Phase 5 complete".to_string(),
            5,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }

    // Phase 6: Service Clustering
    if let Some(pb) = &main_pb {
        update_main_pb_message(
            pb.clone(),
            "Phase 6: Service Clustering".to_string(),
            5,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
    }
    let phase6_start = Instant::now();

    run_service_clustering(
        &pool,
        &run_id,
        // Pass multi_progress conditionally
        if progress_config_arc.should_show_detailed() { multi_progress.clone() } else { None },
    ).await?;

    let phase6_duration = phase6_start.elapsed();
    phase_times.insert("Service_Clustering".to_string(), phase6_duration);

    if let Some(pb) = &main_pb {
        pb.inc(1);
        update_main_pb_message(
            pb.clone(),
            "Phase 6 complete".to_string(),
            6,
            progress_config_arc.clone(),
            pool.clone(),
        ).await;
        pb.finish(); // Finish the main progress bar
    }

    // NEW: Save RL Models and Display Statistics (with narrow lock scope)
    info!("Saving RL models and displaying statistics...");
    let rl_models_saved = {
        let mut rl_guard = service_rl_orchestrator_arc.lock().await;
        rl_guard.save_models(&pool).await.is_ok()
    };

    if rl_models_saved {
        info!("Successfully saved RL models to database");
    } else {
        warn!("Failed to save RL models");
    }

    // Display RL statistics without holding lock
    let tuner_stats = {
        let rl_guard = service_rl_orchestrator_arc.lock().await;
        rl_guard.get_confidence_tuner_stats()
    };
    info!("=== Service RL Statistics ===");
    info!("{}", tuner_stats);

    if progress_config_arc.should_show_cache_stats() {
        if let Some(cache_stats) = {
            let rl_guard = service_rl_orchestrator_arc.lock().await;
            rl_guard.get_feature_cache_stats().await
        } {
            info!("Service Feature Cache Stats - Hits: {}, Misses: {}, Individual Hits: {}, Individual Misses: {}",
                    cache_stats.0, cache_stats.1, cache_stats.2, cache_stats.3);
        }
    }

    // Calculate total pipeline time
    let total_pipeline_time: std::time::Duration = phase_times.values().sum();
    stats.total_processing_time = total_pipeline_time.as_secs_f64();

    info!("=== Pipeline Phase Timing Summary ===");
    for (phase, duration) in &phase_times {
        info!("  {}: {:.2?}", phase, duration);
    }
    info!("  Total Pipeline Time: {:.2?}", total_pipeline_time);

    // Using the last clone of main_pb, which might be finished,
    // or if multi_progress is None, this block is skipped.
    // The previous pb.finish() handles the final message.

    info!("=== Service Matching Pipeline with RL Integration Complete ===");
    info!("Total matches found: {}", final_stats.total_matches_found);
    info!("Total clusters processed: {}", final_stats.total_clusters_processed);
    info!("RL feature extractions: {}", feature_extraction_count);
    info!("Total pipeline time: {:.2?}", total_pipeline_time);

    // Final log of connection pool status (using the original `pool` variable)
    let (pool_size, available_connections, in_use_connections) = get_pool_status(&pool);
    info!(
        "Final DB Connection Pool Status: Total: {}, Available: {}, In Use: {}",
        pool_size,
        available_connections,
        in_use_connections
    );

    Ok(())
}
