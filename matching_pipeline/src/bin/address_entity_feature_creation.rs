// src/bin/address_entity_feature_creation.rs

use anyhow::{Context, Result};
use chrono::Utc;
use dedupe_lib::{
    db::{self, PgPool},
    models::{EntityFeature, EntityId},
};
use futures::future::join_all;
use log::{debug, info, warn};
use std::{
    collections::HashSet,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

// Constants for parallel execution
const BATCH_SIZE: usize = 1000; // How many entities to process info for at a time in memory
const MAX_CONCURRENT_DB_TASKS: usize = 10; // Max concurrent tasks that hit the DB for inserts/checks
const MAX_CONCURRENT_ENTITY_PROCESSING_TASKS: usize = 50; // Max concurrent tasks for in-memory processing of entities

// Struct to hold intermediate data for linking
#[derive(Debug, Clone)] // Added Clone
struct EntityLocationAddressLink {
    entity_id: String,
    address_id: String,
}

async fn fetch_existing_features(pool: &PgPool) -> Result<HashSet<(String, String, String)>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for existing features")?;
    let rows = conn
        .query(
            "SELECT entity_id, table_name, table_id FROM public.entity_feature WHERE table_name = 'address'",
            &[],
        )
        .await
        .context("Failed to query existing address entity_features")?;

    let mut existing_features = HashSet::new();
    for row in rows {
        let entity_id: String = row.get("entity_id");
        let table_name: String = row.get("table_name");
        let table_id: String = row.get("table_id");
        existing_features.insert((entity_id, table_name, table_id));
    }
    info!(
        "Fetched {} existing 'address' entity_feature records.",
        existing_features.len()
    );
    Ok(existing_features)
}

async fn fetch_entity_location_address_links_batch(
    pool: &PgPool,
    offset: usize,
    limit: usize,
) -> Result<Vec<EntityLocationAddressLink>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for entity-location-address links")?;

    // Query to link entities to addresses via locations
    // Added DISTINCT to prevent the source query from returning duplicate pairs.
    // Adjusted ORDER BY to use selected columns for stability with DISTINCT and pagination.
    let query_str = "
        SELECT DISTINCT e.id AS entity_id, a.id AS address_id
        FROM public.entity e
        JOIN public.location l ON e.organization_id = l.organization_id
        JOIN public.address a ON l.id = a.location_id
        ORDER BY entity_id, address_id
        OFFSET $1 LIMIT $2";

    let rows = conn
        .query(query_str, &[&(offset as i64), &(limit as i64)])
        .await
        .context("Failed to query entity-location-address links")?;

    let mut links = Vec::new();
    for row in rows {
        links.push(EntityLocationAddressLink {
            entity_id: row.get("entity_id"),
            address_id: row.get("address_id"),
        });
    }
    Ok(links)
}

async fn insert_new_features_batch(
    pool: &PgPool,
    features_to_insert: Vec<EntityFeature>,
    semaphore: Arc<Semaphore>,
    inserted_counter: Arc<AtomicUsize>,
) -> Result<()> {
    if features_to_insert.is_empty() {
        return Ok(());
    }

    let _permit = semaphore
        .acquire()
        .await
        .expect("Semaphore acquisition failed for DB insert task");

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch insert")?;

    // Use a transaction for batch insert
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch insert")?;

    let mut query = String::from(
        "INSERT INTO public.entity_feature (id, entity_id, table_name, table_id, created_at) VALUES ",
    );
    // Removed unused params_count and params Vec for string formatted query
    // let mut params_count = 0;
    // let mut params: Vec<String> = Vec::new(); 

    for (i, feature) in features_to_insert.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }
        // Manually construct the values part of the query string.
        // Ensure proper escaping if data can contain special characters. Here, UUIDs and 'address' are safe.
        query.push_str(&format!(
            "('{}', '{}', '{}', '{}', '{}')",
            feature.id.replace('\'', "''"), 
            feature.entity_id.0.replace('\'', "''"),
            feature.table_name.replace('\'', "''"), 
            feature.table_id.replace('\'', "''"),
            feature.created_at // NaiveDateTime formats to ISO 8601 by default, which is SQL compatible
        ));
    }
    
    match transaction.execute(query.as_str(), &[]).await {
        Ok(count) => {
            transaction
                .commit()
                .await
                .context("Failed to commit transaction")?;
            let num_inserted = count as usize;
            inserted_counter.fetch_add(num_inserted, Ordering::Relaxed);
            debug!("Successfully inserted {} address features.", num_inserted);
        }
        Err(e) => {
            let _ = transaction.rollback().await; 
            warn!("Error inserting batch of address features: {}. Batch details: {} features.", e, features_to_insert.len());
            if !features_to_insert.is_empty() {
                warn!("First feature in failed batch: entity_id: {}, address_id: {}", features_to_insert[0].entity_id.0, features_to_insert[0].table_id);
            }
            return Err(e.into());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting one-time address entity feature creation binary.");
    let start_time = Instant::now();

    let env_paths = [".env", ".env.local", "../.env", "../../.env"];
    let mut loaded_env = false;
    for path_str in env_paths.iter() {
        let path = Path::new(path_str);
        if path.exists() {
            if let Err(e) = db::load_env_from_file(path_str) {
                warn!("Failed to load environment from {}: {}", path_str, e);
            } else {
                info!("Loaded environment variables from {}", path_str);
                loaded_env = true;
                break;
            }
        } else {
            debug!("Env file not found at: {}", path_str);
        }
    }
    if !loaded_env {
        info!("No .env file found in checked paths, using system environment variables.");
    }

    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;
    info!("Successfully connected to the database.");

    let existing_features = Arc::new(
        fetch_existing_features(&pool)
            .await
            .context("Failed to fetch existing features")?,
    );
    info!(
        "Found {} existing address features to exclude.",
        existing_features.len()
    );

    let total_processed_links = Arc::new(AtomicUsize::new(0));
    let new_features_identified_count = Arc::new(AtomicUsize::new(0)); // Renamed for clarity
    let total_inserted_count = Arc::new(AtomicUsize::new(0));

    let db_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DB_TASKS));
    let processing_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_ENTITY_PROCESSING_TASKS));

    let mut offset = 0;
    loop {
        info!(
            "Fetching entity-location-address links batch: offset {}, limit {}",
            offset, BATCH_SIZE
        );
        let links_batch = fetch_entity_location_address_links_batch(&pool, offset, BATCH_SIZE)
            .await
            .context(format!(
                "Failed to fetch links batch (offset: {})",
                offset
            ))?;

        if links_batch.is_empty() {
            info!("No more entity-location-address links to process.");
            break;
        }
        let current_batch_size = links_batch.len();
        info!(
            "Processing {} links from offset {}.",
            current_batch_size, offset
        );

        let mut tasks = Vec::new();
        // The link_chunk processing tasks.
        // Each task processes a sub-chunk of links_batch.
        for link_chunk in links_batch.chunks(MAX_CONCURRENT_ENTITY_PROCESSING_TASKS / 5 + 1) { // Smaller chunks for parallel processing
            let permit = processing_semaphore.clone().acquire_owned().await.unwrap();
            let chunk_to_process = link_chunk.to_vec(); 
            let existing_features_clone = existing_features.clone();
            let new_features_identified_count_clone = new_features_identified_count.clone();
            let total_processed_links_clone = total_processed_links.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = permit; 
                let mut features_for_this_processing_chunk = Vec::new();
                let now = Utc::now().naive_utc();
                for link in chunk_to_process {
                    total_processed_links_clone.fetch_add(1, Ordering::Relaxed);
                    let feature_key = (
                        link.entity_id.clone(),
                        "address".to_string(),
                        link.address_id.clone(),
                    );

                    // Check against the initially fetched existing_features.
                    // Duplicates arising from concurrent processing of the same link
                    // (if fetch_entity_location_address_links_batch somehow returned duplicates despite DISTINCT)
                    // or from different links mapping to the same feature key will be handled later.
                    if !existing_features_clone.contains(&feature_key) {
                        features_for_this_processing_chunk.push(EntityFeature {
                            id: Uuid::new_v4().to_string(),
                            entity_id: EntityId(link.entity_id),
                            table_name: "address".to_string(),
                            table_id: link.address_id,
                            created_at: now,
                        });
                        new_features_identified_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
                features_for_this_processing_chunk
            }));
        }
        
        let mut all_new_features_for_main_batch = Vec::new();
        for task_result in join_all(tasks).await {
            match task_result {
                Ok(chunk_features) => all_new_features_for_main_batch.extend(chunk_features),
                Err(e) => warn!("Entity processing task panicked: {}", e),
            }
        }

        // Deduplicate features collected from all parallel processing tasks for the current main batch.
        // This is crucial to prevent trying to insert the same feature multiple times if different
        // processing tasks generated it (e.g., if links_batch itself had duplicates not caught by DISTINCT,
        // or if different links resolve to the same feature key).
        let mut unique_features_to_insert_for_main_batch = Vec::new();
        let mut seen_in_this_main_batch = HashSet::new();
        for feature in all_new_features_for_main_batch {
            let key = (feature.entity_id.0.clone(), feature.table_name.clone(), feature.table_id.clone());
            if seen_in_this_main_batch.insert(key) {
                unique_features_to_insert_for_main_batch.push(feature);
            }
        }

        if !unique_features_to_insert_for_main_batch.is_empty() {
            info!(
                "Found {} unique new address features to insert from this batch of links (total links in batch: {}, after internal deduplication).",
                unique_features_to_insert_for_main_batch.len(),
                current_batch_size
            );
            
            let mut insert_tasks = Vec::new();
            // Chunk the unique features for batch insertion into the DB.
            for insert_chunk in unique_features_to_insert_for_main_batch.chunks(BATCH_SIZE / 5) { 
                if insert_chunk.is_empty() { continue; }
                let features_to_insert_clone = insert_chunk.to_vec();
                let pool_clone = pool.clone();
                let db_semaphore_clone = db_semaphore.clone();
                let total_inserted_count_clone = total_inserted_count.clone();
                
                insert_tasks.push(tokio::spawn(async move {
                    if let Err(e) = insert_new_features_batch(
                        &pool_clone,
                        features_to_insert_clone,
                        db_semaphore_clone,
                        total_inserted_count_clone,
                    )
                    .await
                    {
                        // Log error, but script continues with other batches.
                        // The error from insert_new_features_batch is already logged there.
                        // No need to re-log e, but can add context if needed.
                    }
                }));
            }
            join_all(insert_tasks).await; 
        } else {
            info!("No new unique address features to insert from this batch of links (total links in batch: {}).", current_batch_size);
        }
        
        offset += current_batch_size;
        if current_batch_size < BATCH_SIZE {
            info!("Processed last batch of links.");
            break; 
        }
    }

    let elapsed = start_time.elapsed();
    info!(
        "Address entity feature creation completed in {:.2?}.",
        elapsed
    );
    info!("Total entity-location-address links processed: {}", total_processed_links.load(Ordering::Relaxed));
    info!("Total new address features identified (pre-batch deduplication): {}", new_features_identified_count.load(Ordering::Relaxed));
    info!("Total new address features inserted: {}", total_inserted_count.load(Ordering::Relaxed));

    Ok(())
}