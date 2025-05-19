// src/bin/match_url.rs
use anyhow::{Context, Result};
use log::{info, warn};
use std::{path::Path, sync::Arc, time::Instant};
use tokio::sync::Mutex;
use uuid::Uuid;

use dedupe_lib::{db, matching, reinforcement};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Starting URL matching pipeline component");
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

    // Generate a run ID for traceability
    let run_id = Uuid::new_v4().to_string();
    info!("Run ID: {}", run_id);

    // Initialize the ML reinforcement orchestrator
    info!("Initializing ML-guided matching reinforcement orchestrator");
    let reinforcement_orchestrator_instance = reinforcement::MatchingOrchestrator::new(&pool)
        .await
        .context("Failed to initialize ML reinforcement orchestrator")?;

    let reinforcement_orchestrator = Arc::new(Mutex::new(reinforcement_orchestrator_instance));

    // Run the URL matching component
    info!("Running URL matching...");
    match matching::url::find_matches(&pool, Some(reinforcement_orchestrator), &run_id).await {
        Ok(result) => {
            info!(
                "URL matching completed successfully. Created {} new entity groups.",
                result.groups_created()
            );
            info!(
                "Method statistics: {} groups created, {} entities matched, Avg confidence: {:.2}, Avg group size: {:.2}",
                result.stats().groups_created,
                result.stats().entities_matched,
                result.stats().avg_confidence,
                result.stats().avg_group_size
            );
        }
        Err(e) => {
            warn!("URL matching failed: {}", e);
            return Err(e);
        }
    }

    let elapsed = start_time.elapsed();
    info!("URL matching completed in {:.2?}", elapsed);

    Ok(())
}
