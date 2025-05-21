// src/bin/extract_service_context_features.rs
use anyhow::{Context, Result};
use futures::future::join_all;
use log::{debug, info, warn};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::Semaphore;

use dedupe_lib::{
    db,
    models::ServiceId,
    reinforcement::service::{
        service_feature_cache_prewarmer::extract_and_store_all_service_context_features,
        service_feature_cache_service::create_shared_service_cache, service_feature_extraction,
    },
};

// Constants for parallel execution
const BATCH_SIZE: usize = 100;
const MAX_CONCURRENT_TASKS: usize = 20; // Adjust based on your hardware/DB capacity

async fn process_service_batch(
    pool: &db::PgPool,
    services: Vec<ServiceId>,
    semaphore: Arc<Semaphore>,
    processed_counter: Arc<AtomicUsize>,
    total_services: usize,
) -> Result<usize> {
    let mut tasks = Vec::with_capacity(services.len());

    for service_id in services {
        let service_pool = pool.clone();
        let service_semaphore = semaphore.clone();
        let service_counter = processed_counter.clone();

        let task = tokio::spawn(async move {
            // Acquire semaphore permit to limit concurrent DB connections
            let _permit = service_semaphore
                .acquire()
                .await
                .expect("Semaphore acquisition failed");

            let conn = match service_pool.get().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!(
                        "Failed to get DB connection for service {}: {}",
                        service_id.0, e
                    );
                    return Err(anyhow::anyhow!("DB connection error"));
                }
            };

            // Get or extract features for this service
            let result =
                service_feature_extraction::get_stored_service_features(&conn, &service_id).await;

            // Update counter regardless of success/failure
            let current = service_counter.fetch_add(1, Ordering::Relaxed) + 1;
            if current % 50 == 0 || current == total_services {
                info!(
                    "Processed {}/{} services ({:.1}%)",
                    current,
                    total_services,
                    (current as f64 / total_services as f64) * 100.0
                );
            }

            match result {
                Ok(_) => Ok(1),
                Err(e) => {
                    warn!(
                        "Failed to extract features for service {}: {}",
                        service_id.0, e
                    );
                    Err(anyhow::anyhow!("Feature extraction error"))
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete and collect results
    let results = join_all(tasks).await;

    // Count successes
    let mut success_count = 0;
    for result in results {
        match result {
            Ok(Ok(count)) => success_count += count,
            Ok(Err(_)) => {} // Already logged in task
            Err(e) => warn!("Task panicked: {}", e),
        }
    }

    Ok(success_count)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Starting parallel service context feature extraction pipeline component");
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

    // Get all service IDs
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for retrieving services")?;

    info!("Retrieving all service IDs from database...");
    let service_rows = conn
        .query("SELECT id FROM public.service ORDER BY id", &[])
        .await
        .context("Failed to retrieve service IDs")?;

    let service_ids: Vec<ServiceId> = service_rows
        .iter()
        .map(|row| ServiceId(row.get(0)))
        .collect();

    let total_services = service_ids.len();

    if total_services == 0 {
        info!("No services found in the database. No features to extract.");
        return Ok(());
    }

    info!("Found {} services to process", total_services);

    // Create a shared service feature cache
    info!("Creating service feature cache...");
    let feature_cache = create_shared_service_cache();

    // Create semaphore to limit concurrent database connections
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_TASKS));

    // Create counter for progress tracking
    let processed_counter = Arc::new(AtomicUsize::new(0));

    // Create batches of service IDs
    let service_batches: Vec<Vec<ServiceId>> = service_ids
        .chunks(BATCH_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect();

    info!("Processing {} services in {} batches of up to {} services each with max {} concurrent tasks",
        total_services, service_batches.len(), BATCH_SIZE, MAX_CONCURRENT_TASKS);

    // Process all batches
    let mut total_success = 0;
    for (batch_idx, batch) in service_batches.clone().into_iter().enumerate() {
        debug!(
            "Processing batch {}/{}",
            batch_idx + 1,
            service_batches.len()
        );
        let batch_size = batch.len();

        let success_count = process_service_batch(
            &pool,
            batch,
            semaphore.clone(),
            processed_counter.clone(),
            total_services,
        )
        .await?;

        total_success += success_count;

        if success_count < batch_size {
            warn!(
                "Batch {}/{}: {} of {} services were successfully processed",
                batch_idx + 1,
                service_batches.len(),
                success_count,
                batch_size
            );
        }
    }

    info!(
        "Successfully extracted features for {}/{} services",
        total_success, total_services
    );

    // Store the features in the shared cache as well
    info!("Populating shared feature cache with extracted features...");
    match extract_and_store_all_service_context_features(&pool, &feature_cache).await {
        Ok(features_count) => info!(
            "Successfully populated cache with features for {} services.",
            features_count
        ),
        Err(e) => warn!(
            "Error during cache population: {}. This is non-critical as features are already in DB.",
            e
        ),
    }

    let elapsed = start_time.elapsed();
    info!(
        "Service context feature extraction completed in {:.2?}",
        elapsed
    );

    Ok(())
}
