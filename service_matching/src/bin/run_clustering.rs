// src/bin/run_clustering.rs

use anyhow::{Context, Result};
use chrono::Utc;
use dedupe_lib::clustering::create_clusters::run_service_clustering;
use dedupe_lib::utils::instantiate_run::create_initial_pipeline_run;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use std::time::Instant;
use uuid::Uuid;

use dedupe_lib::utils::db_connect::connect;
use dedupe_lib::utils::env::load_env;
use dedupe_lib::utils::progress_config::ProgressConfig;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging and environment
    env_logger::init();
    info!("Starting standalone entity clustering pipeline");
    load_env();

    // Load progress configuration from environment
    let progress_config = ProgressConfig::from_env();
    info!(
        "Progress tracking: enabled={}, detailed={}",
        progress_config.enabled, progress_config.detailed
    );

    // Initialize progress tracking if enabled
    let multi_progress = progress_config.create_multi_progress();

    // Create a main progress bar for this binary
    let main_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(1)); // Only one main step for clustering
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Connecting to database...");
        Some(pb)
    } else {
        None
    };

    // Connect to the database
    let pool = connect().await.context("Failed to connect to database")?;
    info!("Successfully connected to the database");

    // Generate a new pipeline run ID for this standalone clustering operation
    // This ensures that the clustering results can be associated with a specific run.
    let run_id_uuid = Uuid::new_v4();
    let run_id = run_id_uuid.to_string();
    let run_timestamp = Utc::now().naive_utc();
    let description = Some("Standalone entity clustering run".to_string());

    // Create an entry in the pipeline_run table.
    // Even if only clustering is run, it's good practice to log this "run".
    let _stats = create_initial_pipeline_run(&pool, &run_id, run_timestamp, description.as_deref())
        .await
        .context("Failed to create initial pipeline run entry for clustering")?;
    info!("Created pipeline run entry with ID: {}", run_id);

    if let Some(pb) = &main_pb {
        pb.set_message("Phase 4: Starting clustering...");
    }

    let phase4_start = Instant::now();

    // Call the entity clustering function
    let total_clusters = run_service_clustering(&pool, &run_id, multi_progress.clone())
        .await
        .context("Service clustering failed")?;

    let phase4_duration = phase4_start.elapsed();

    info!(
        "Clustering completed. Created {} clusters in {:.2?}.",
        total_clusters, phase4_duration
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.finish_with_message(format!(
            "Clustering complete: {} clusters created in {:.2?}",
            total_clusters, phase4_duration
        ));
    }

    info!("Standalone service clustering pipeline completed successfully!");
    Ok(())
}