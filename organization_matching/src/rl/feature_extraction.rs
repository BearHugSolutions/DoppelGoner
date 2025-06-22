// src/rl/feature_extraction.rs
use anyhow::{Context, Result};
use futures::future;
use log::{debug, error, info, warn, Level as LogLevel};
use pgvector::Vector as PgVector;
use tokio_postgres::{Client as PgConnection, Row as PgRow};
use uuid::Uuid;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use candle_core::{Device, Error as CandleError};
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::models::rl_models::FeatureMetadata;
use crate::rl::feature_cache::SharedFeatureCache;
use crate::utils::candle::cosine_similarity_candle;
use crate::utils::db_connect::PgPool;

// Helper to get a Candle device (same as before)
fn get_candle_device() -> Result<Device, CandleError> {
    #[cfg(all(feature = "metal", target_os = "macos"))]
    {
        info!("Using Metal device for Candle operations in feature_extraction.");
        return Device::new_metal(0);
    }
    #[cfg(all(feature = "cuda"))] // Simplified CUDA check
    {
        info!("Using CUDA device for Candle operations in feature_extraction.");
        return Device::new_cuda(0);
    }
    info!("Using CPU device for Candle operations in feature_extraction.");
    Ok(Device::Cpu)
}

type SingleFeatureFuture<'a> =
    Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + 'a>>;

// Helper function to wrap a future with progress logging (same as before)
async fn wrap_with_progress<F, T, E>(
    task_future: F,
    counter: Arc<AtomicUsize>,
    total_tasks: usize,
    task_description: String,
    entity_context: String,
    log_level: LogLevel,
) -> Result<T, E>
where
    F: Future<Output = Result<T, E>> + Send,
    T: Send,
    E: std::fmt::Debug + Send,
{
    match task_future.await {
        Ok(result) => {
            let completed_count = counter.fetch_add(1, Ordering::Relaxed) + 1;
            log::log!(
                log_level,
                "{}: Task '{}' completed ({}/{}).",
                entity_context,
                task_description,
                completed_count,
                total_tasks
            );
            Ok(result)
        }
        Err(e) => {
            let completed_count = counter.fetch_add(1, Ordering::Relaxed) + 1;
            log::log!(
                LogLevel::Warn,
                "{}: Task '{}' failed ({}/{}), error: {:?}",
                entity_context,
                task_description,
                completed_count,
                total_tasks,
                e
            );
            Err(e)
        }
    }
}

/// Extracts and stores contextual features for all identified entities in PARALLEL.
/// This function should be called from the RL phase of the pipeline.
///
/// It iterates through all entities in the 'public.entity' table and ensures
/// that contextual features (the 31-element vectors for ML) are extracted and stored.
/// Uses `get_stored_entity_features` to avoid recalculating existing, complete features.
pub async fn extract_and_store_all_contextual_features(
    pool: &PgPool,
    _feature_cache: &SharedFeatureCache,
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    info!("Starting PARALLEL extraction and storage of contextual features for all entities...");
    let start_time = std::time::Instant::now();

    // Create main progress bar for this phase
    let main_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(3)); // Setup, Processing, Finalization
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.green} [{elapsed_precise}] {bar:25.cyan/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Setting up feature extraction...");
        Some(pb)
    } else {
        None
    };

    // Get a connection to fetch the list of all entity IDs
    let conn_for_list = pool
        .get()
        .await
        .context("Failed to get DB connection for entity list")?;
    let entity_rows = conn_for_list
        .query("SELECT id FROM public.entity", &[]) // EntityId is String, so direct mapping
        .await
        .context("Failed to query entity IDs for contextual feature extraction")?;
    drop(conn_for_list); // Release the connection as soon as the list is fetched

    if entity_rows.is_empty() {
        info!("No entities found to process for contextual features.");
        if let Some(pb) = &main_pb {
            pb.finish_with_message("No entities to process");
        }
        return Ok(0);
    }

    let entity_ids: Vec<String> = entity_rows
        .into_iter()
        .map(|row| row.get(0)) // Assuming EntityId(String)
        .collect();

    let total_entities_to_process = entity_ids.len();
    info!(
        "Found {} entities to process for contextual features.",
        total_entities_to_process
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message(format!(
            "Processing {} entities...",
            total_entities_to_process
        ));
    }

    // Create detailed progress bar for entity processing
    let entity_progress_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(total_entities_to_process as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("      {spinner:.blue} [{elapsed_precise}] {bar:20.yellow/red} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Processing entities...");
        Some(pb)
    } else {
        None
    };

    // --- Parallel Processing Logic ---
    // Set desired concurrency. This value should be tuned based on your DB capacity and machine resources.
    // Keeping it at 30 to stay well within the preferred 40 connection limit,
    // assuming other parts of the pipeline might use some connections.
    // This could be made configurable (e.g., from config.rs or an environment variable).
    let desired_concurrency = 30;

    // Atomic counters for tracking progress in a thread-safe manner
    let succeeded_count = Arc::new(AtomicUsize::new(0));
    let processed_for_log_count = Arc::new(AtomicUsize::new(0));

    // Clone progress bar for use in async tasks
    let entity_pb_clone = entity_progress_pb.clone();

    stream::iter(entity_ids)
        .map(|entity_id| {
            let pool_clone = pool.clone(); // Clone the pool for each spawned task
            let succeeded_clone = Arc::clone(&succeeded_count);
            let processed_log_clone = Arc::clone(&processed_for_log_count);
            let pb_clone = entity_pb_clone.clone();
            // Shadow total_entities_to_process to avoid capturing the outer variable directly in the 'static future
            let total_entities = total_entities_to_process;

            tokio::spawn(async move { // Spawn a new asynchronous task for each entity
                match pool_clone.get().await { // Each task gets its own connection from the pool
                    Ok(conn_guard) => {
                        // Call `get_stored_entity_features` which handles resumability.
                        // It will call the actual feature extraction logic only if necessary.
                        match get_stored_entity_features(&*conn_guard, &entity_id).await {
                            Ok(_features) => {
                                // If features are successfully retrieved or generated.
                                succeeded_clone.fetch_add(1, Ordering::Relaxed);
                                debug!("Contextual features ensured for entity {}", entity_id);
                            }
                            Err(e) => {
                                warn!("Failed to ensure contextual features for entity {}: {}", entity_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get DB connection for entity {}: {}", entity_id, e);
                    }
                }
                
                // Update progress bars
                if let Some(ref pb) = pb_clone {
                    pb.inc(1);
                }
                
                // Log progress periodically
                let current_processed = processed_log_clone.fetch_add(1, Ordering::Relaxed) + 1;
                if current_processed % 100 == 0 || current_processed == total_entities { // Log every 100 entities or at the very end
                    info!(
                        "Contextual feature extraction progress: {}/{} entity processing tasks dispatched/completed.",
                        current_processed, total_entities
                    );
                    
                    // Update progress bar message every 100 entities
                    if let Some(ref pb) = pb_clone {
                        if current_processed % 100 == 0 {
                            pb.set_message(format!("Processing entities... ({}/{})", current_processed, total_entities));
                        }
                    }
                }
            })
        })
        .buffer_unordered(desired_concurrency) // Limit the number of concurrent tasks
        .for_each(|result| async {
            if let Err(e) = result { // Handle potential errors from spawned tasks (e.g., panics)
                warn!("A contextual feature extraction task panicked or failed to join: {:?}", e);
            }
        })
        .await;

    let final_succeeded_count = succeeded_count.load(Ordering::Relaxed);

    // Finish entity progress bar
    if let Some(pb) = &entity_progress_pb {
        pb.finish_with_message(format!("Processed {} entities", total_entities_to_process));
    }

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message(format!(
            "Finalizing... ({} successful)",
            final_succeeded_count
        ));
    }

    let elapsed = start_time.elapsed();
    info!(
        "Parallel contextual feature extraction and storage complete in {:.2?}. Ensured features for {} out of {} entities.",
        elapsed, final_succeeded_count, total_entities_to_process
    );

    // Create summary progress bar to show final results
    let summary_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(1));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("      {spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.set_message(format!(
            "Feature extraction complete: {}/{} entities successful in {:.1?}",
            final_succeeded_count, total_entities_to_process, elapsed
        ));
        pb.finish();
        Some(pb)
    } else {
        None
    };

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.finish_with_message(format!(
            "Feature extraction complete: {}/{} entities",
            final_succeeded_count, total_entities_to_process
        ));
    }

    Ok(final_succeeded_count)
}

// Feature metadata (remains the same, defines the 12 individual + 7 pairwise features)
pub fn get_feature_metadata() -> Vec<FeatureMetadata> {
    vec![
        // --- Individual Entity Features (Indices 0-11) ---
        FeatureMetadata {
            name: "name_complexity".to_string(),
            description: "Complexity of the organization name.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "data_completeness".to_string(),
            description: "Completeness of core organization data fields.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "has_email".to_string(),
            description: "Boolean indicating if an email exists.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "has_phone".to_string(),
            description: "Boolean indicating if a phone number exists.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "has_url".to_string(),
            description: "Boolean indicating if a URL exists.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "has_address".to_string(),
            description: "Boolean indicating if an address exists.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "has_location".to_string(),
            description: "Boolean indicating if location coordinates exist.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "organization_size".to_string(),
            description: "Estimated size of the organization based on features.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "service_count".to_string(),
            description: "Normalized count of services offered.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "embedding_centroid_distance".to_string(),
            description:
                "Distance from organization embedding to global centroid (Candle calculated)."
                    .to_string(),
            min_value: 0.0,
            max_value: 2.0,
        },
        FeatureMetadata {
            name: "service_semantic_coherence".to_string(),
            description:
                "Average semantic similarity between an org's services (Candle calculated)."
                    .to_string(),
            min_value: -1.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "embedding_quality".to_string(),
            description: "Quality score of the organization's embedding.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        // --- Pairwise Features (Indices 12-18 when combined) ---
        // These are calculated between two entities. When constructing the full 31-element vector,
        // features 0-11 are for entity1, 0-11 (again) for entity2, and then these 7.
        FeatureMetadata {
            name: "name_similarity".to_string(),
            description: "Similarity between two entity names.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "embedding_similarity".to_string(),
            description: "Cosine similarity between two entity embeddings (Candle calculated)."
                .to_string(),
            min_value: -1.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "max_service_similarity".to_string(),
            description:
                "Max semantic similarity between services of two entities (Candle calculated)."
                    .to_string(),
            min_value: -1.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "geographic_distance".to_string(),
            description: "Normalized geographic proximity between entities.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "shared_domain".to_string(),
            description: "Boolean if entities share the same website domain.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "shared_phone".to_string(),
            description: "Boolean if entities share the same phone number.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "service_geo_semantic_score".to_string(),
            description: "Hybrid score combining service similarity (Candle) and geo-proximity."
                .to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
    ]
}

#[derive(Debug)]
struct RawEntityOrgData {
    org_name: Option<String>,
    org_description: Option<String>,
    org_email: Option<String>,
    org_url: Option<String>,
    org_tax_id: Option<String>,
    org_legal_status: Option<String>,
    has_phone_flag: bool,
    has_address_flag: bool,
    has_location_coords_flag: bool,
    service_count_val: i64,
    location_count_val: i64,
}

/// Extracts the 12 individual features for a single entity.
/// If features are already stored, they are retrieved; otherwise, they are calculated and stored.
pub async fn get_stored_entity_features(
    conn: &PgConnection, // Changed to direct PgConnection
    entity_id: &str,
) -> Result<Vec<f64>> {
    let entity_context = format!("Entity {}", entity_id);
    debug!(
        "{} Checking for stored contextual features...",
        entity_context
    );
    let rows = conn
        .query(
            "SELECT feature_name, feature_value
         FROM clustering_metadata.entity_context_features
         WHERE entity_id = $1
         ORDER BY feature_name", // Order by name to ensure consistent reconstruction if needed
            &[&entity_id],
        )
        .await
        .context(format!(
            "Failed to query stored contextual features for entity {}",
            entity_id
        ))?;

    let metadata: Vec<FeatureMetadata> = get_feature_metadata(); // Full metadata
    const INDIVIDUAL_FEATURE_COUNT: usize = 12; // We only care about the first 12 for individual entities

    if !rows.is_empty() {
        debug!(
            "{} Found {} stored contextual feature rows, reconstructing vector...",
            entity_context,
            rows.len()
        );
        let mut feature_map = HashMap::new();
        for row in rows {
            let name: String = row.get(0);
            let value: f64 = row.get(1);
            feature_map.insert(name, value);
        }

        let mut features_vec: Vec<f64> = Vec::with_capacity(INDIVIDUAL_FEATURE_COUNT);
        let mut all_present = true;
        for i in 0..INDIVIDUAL_FEATURE_COUNT {
            if let Some(value) = feature_map.get(&metadata[i].name) {
                features_vec.push(*value);
            } else {
                warn!(
                    "{}: Missing stored contextual feature '{}', will need to re-extract all.",
                    entity_context, metadata[i].name
                );
                all_present = false;
                break;
            }
        }

        if all_present && features_vec.len() == INDIVIDUAL_FEATURE_COUNT {
            debug!(
                "{} Successfully reconstructed all {} stored contextual features.",
                entity_context, INDIVIDUAL_FEATURE_COUNT
            );
            return Ok(features_vec);
        } else {
            warn!(
                "{}: Stored contextual features incomplete (found {}, expected {}) or mismatched. Re-extracting all.",
                entity_context, features_vec.len(), INDIVIDUAL_FEATURE_COUNT
            );
        }
    } else {
        info!(
            "{}: No stored contextual features found, extracting now.",
            entity_context
        );
    }
    // If not all features were present or rows were empty, extract and store them.
    extract_and_store_entity_features(conn, entity_id, &metadata.as_slice()).await
}

/// Extracts, stores, and returns the 12 individual features for a single entity.
/// This is called by `get_stored_entity_features` if features are missing or incomplete.
async fn extract_and_store_entity_features(
    conn: &PgConnection,
    entity_id: &str,
    all_feature_metadata: &[FeatureMetadata], // Pass full metadata for names
) -> Result<Vec<f64>> {
    let entity_context = format!("Entity {}", entity_id);
    const INDIVIDUAL_FEATURE_COUNT: usize = 12;

    info!(
        "{} Extracting 9 basic contextual features (consolidated query)...",
        entity_context
    );
    let basic_features_tasks_completed = Arc::new(AtomicUsize::new(0));
    let total_basic_feature_tasks = 1; // For the single consolidated query

    let consolidated_query_future = async {
        let query = "
            SELECT
                o.name AS org_name, o.description AS org_description, o.email AS org_email,
                o.url AS org_url, o.tax_id AS org_tax_id, o.legal_status AS org_legal_status,
                EXISTS (SELECT 1 FROM public.phone p JOIN public.entity_feature ef ON ef.table_id = p.id WHERE ef.entity_id = e.id AND ef.table_name = 'phone' AND p.number IS NOT NULL AND p.number <> '') AS has_phone_flag,
                EXISTS (SELECT 1 FROM public.address a JOIN public.location l_addr ON a.location_id = l_addr.id JOIN public.entity_feature ef_addr ON ef_addr.table_id = l_addr.id WHERE ef_addr.entity_id = e.id AND ef_addr.table_name = 'location') AS has_address_flag,
                EXISTS (SELECT 1 FROM public.location l JOIN public.entity_feature ef_loc ON ef_loc.table_id = l.id WHERE ef_loc.entity_id = e.id AND ef_loc.table_name = 'location' AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL) AS has_location_coords_flag,
                (SELECT COUNT(*) FROM public.service s JOIN public.entity_feature ef_s ON ef_s.table_id = s.id WHERE ef_s.entity_id = e.id AND ef_s.table_name = 'service') AS service_count_val,
                (SELECT COUNT(*) FROM public.location l_loc_count JOIN public.entity_feature ef_lc ON ef_lc.table_id = l_loc_count.id WHERE ef_lc.entity_id = e.id AND ef_lc.table_name = 'location') AS location_count_val
            FROM public.entity e
            JOIN public.organization o ON e.organization_id = o.id
            WHERE e.id = $1";

        conn.query_one(query, &[&entity_id])
            .await
            .map_err(anyhow::Error::from)
            .context(format!(
                "Consolidated query for basic contextual features failed for entity {}",
                entity_id
            ))
    };

    let row = wrap_with_progress(
        consolidated_query_future,
        basic_features_tasks_completed.clone(),
        total_basic_feature_tasks,
        "Consolidated Basic Contextual Features Query".to_string(),
        entity_context.clone(),
        LogLevel::Info,
    )
    .await?;

    let raw_data = RawEntityOrgData {
        org_name: row.try_get("org_name").ok(),
        org_description: row.try_get("org_description").ok(),
        org_email: row.try_get("org_email").ok(),
        org_url: row.try_get("org_url").ok(),
        org_tax_id: row.try_get("org_tax_id").ok(),
        org_legal_status: row.try_get("org_legal_status").ok(),
        has_phone_flag: row.try_get("has_phone_flag").unwrap_or(false),
        has_address_flag: row.try_get("has_address_flag").unwrap_or(false),
        has_location_coords_flag: row.try_get("has_location_coords_flag").unwrap_or(false),
        service_count_val: row.try_get("service_count_val").unwrap_or(0),
        location_count_val: row.try_get("location_count_val").unwrap_or(0),
    };

    let basic_features_vec = vec![
        calculate_name_complexity_from_data(&raw_data), // feature 0
        calculate_data_completeness_from_data(&raw_data), // feature 1
        calculate_has_email_from_data(&raw_data),       // feature 2
        calculate_has_phone_from_data(&raw_data),       // feature 3
        calculate_has_url_from_data(&raw_data),         // feature 4
        calculate_has_address_from_data(&raw_data),     // feature 5
        calculate_has_location_from_data(&raw_data),    // feature 6
        calculate_organization_size_from_data(&raw_data), // feature 7
        calculate_service_count_feature_from_data(&raw_data), // feature 8
    ];
    info!(
        "{} Completed calculation of 9 basic contextual features.",
        entity_context
    );

    let candle_device =
        get_candle_device().map_err(|e| anyhow::anyhow!("Failed to get candle device: {}", e))?;

    info!(
        "{} Starting extraction of 3 enhanced contextual features (using Candle)...",
        entity_context
    );
    let enhanced_tasks_completed = Arc::new(AtomicUsize::new(0));
    let num_enhanced_feature_tasks = 3;

    let enhanced_futures: [SingleFeatureFuture<'_>; 3] = [
        Box::pin(wrap_with_progress(
            extract_embedding_centroid_distance(conn, entity_id, candle_device.clone()),
            enhanced_tasks_completed.clone(),
            num_enhanced_feature_tasks,
            all_feature_metadata[9].name.clone(),
            entity_context.clone(),
            LogLevel::Debug,
        )),
        Box::pin(wrap_with_progress(
            extract_service_semantic_coherence(conn, entity_id, candle_device.clone()),
            enhanced_tasks_completed.clone(),
            num_enhanced_feature_tasks,
            all_feature_metadata[10].name.clone(),
            entity_context.clone(),
            LogLevel::Debug,
        )),
        Box::pin(wrap_with_progress(
            extract_embedding_quality_score(conn, entity_id),
            enhanced_tasks_completed.clone(),
            num_enhanced_feature_tasks,
            all_feature_metadata[11].name.clone(),
            entity_context.clone(),
            LogLevel::Debug,
        )),
    ];
    let enhanced_results = future::try_join_all(Vec::from(enhanced_futures)).await?;
    info!(
        "{} Completed extraction of 3 enhanced contextual features.",
        entity_context
    );

    let mut features = Vec::with_capacity(INDIVIDUAL_FEATURE_COUNT);
    features.extend(basic_features_vec);
    features.extend(enhanced_results);

    // Store these 12 features
    debug!(
        "{} Storing {} extracted individual contextual features...",
        entity_context,
        features.len()
    );
    store_individual_entity_features(conn, entity_id, &features, all_feature_metadata).await?;
    debug!(
        "{} Successfully stored individual contextual features.",
        entity_context
    );

    Ok(features)
}

/// Extracts the full 31-element feature vector for a PAIR of entities with improved error handling.
/// This version includes detailed logging to pinpoint exact failure points during extraction.
pub async fn extract_context_for_pair(
    pool: &PgPool,
    entity1_id: &str,
    entity2_id: &str,
) -> Result<Vec<f64>> {
    // Format a consistent context string for all log messages related to this pair
    let pair_context = format!("Pair ({}, {})", entity1_id, entity2_id);

    // Get DB connection with detailed error context
    let conn_guard = match pool.get().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("{} Failed to get DB connection: {}", pair_context, e);
            return Err(anyhow::anyhow!(
                "DB connection failure for {}: {}",
                pair_context,
                e
            ));
        }
    };
    let conn: &PgConnection = &*conn_guard;

    info!(
        "{} Starting contextual feature extraction (31 features)...",
        pair_context
    );
    debug!("{} Getting feature metadata...", pair_context);
    let all_feature_metadata = get_feature_metadata();

    // Setup progress tracking
    let context_steps_completed = Arc::new(AtomicUsize::new(0));
    let total_context_steps = 3; // Entity1 features, Entity2 features, Pairwise features

    // Get ML device with detailed error context
    debug!("{} Initializing Candle device...", pair_context);
    let candle_device = match get_candle_device() {
        Ok(device) => device,
        Err(e) => {
            error!("{} Failed to initialize Candle device: {}", pair_context, e);
            return Err(anyhow::anyhow!(
                "ML device initialization failed for {}: {}",
                pair_context,
                e
            ));
        }
    };

    info!(
        "{} Extracting individual contextual features for both entities...",
        pair_context
    );

    // --- TASK 1: Get Entity 1 Features ---
    debug!(
        "{} Starting extraction for entity1 ({})",
        pair_context, entity1_id
    );
    let entity1_features_task = wrap_with_progress(
        get_stored_entity_features(conn, entity1_id),
        context_steps_completed.clone(),
        total_context_steps,
        format!("Contextual features for entity1 ({})", entity1_id),
        pair_context.clone(),
        LogLevel::Info,
    );

    // --- TASK 2: Get Entity 2 Features ---
    debug!(
        "{} Starting extraction for entity2 ({})",
        pair_context, entity2_id
    );
    let entity2_features_task = wrap_with_progress(
        get_stored_entity_features(conn, entity2_id),
        context_steps_completed.clone(),
        total_context_steps,
        format!("Contextual features for entity2 ({})", entity2_id),
        pair_context.clone(),
        LogLevel::Info,
    );

    // --- TASK 3: Calculate Pairwise Features ---
    debug!("{} Starting pairwise feature calculations...", pair_context);
    let pair_context_for_inner = pair_context.clone();
    let device_for_pair_calc = candle_device.clone();

    let pair_features_calculation_task_inner = async move {
        let pair_calc_tasks_completed_inner = Arc::new(AtomicUsize::new(0));
        const PAIRWISE_METADATA_OFFSET: usize = 12;
        const NUM_PAIRWISE_FEATURES: usize = 7;

        // Individual feature tracking map for better error reporting
        let mut feature_outcomes = HashMap::with_capacity(NUM_PAIRWISE_FEATURES);

        let feature_tasks: Vec<(
            String,
            Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>>,
        )> = vec![
            // Name similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET].name.clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    calculate_name_similarity(conn, entity1_id, entity2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET].name.clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Embedding similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 1]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    calculate_embedding_similarity(
                        conn,
                        entity1_id,
                        entity2_id,
                        device_for_pair_calc.clone(),
                    ),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 1]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Max service similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 2]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    calculate_max_service_similarity(
                        conn,
                        entity1_id,
                        entity2_id,
                        device_for_pair_calc.clone(),
                    ),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 2]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Geographic distance
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 3]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    calculate_geographic_distance(conn, entity1_id, entity2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 3]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Shared domain
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 4]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    check_shared_domain(conn, entity1_id, entity2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 4]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Shared phone
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 5]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    check_shared_phone(conn, entity1_id, entity2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 5]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Service geo semantic score
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 6]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    calculate_service_geo_semantic_score(
                        conn,
                        entity1_id,
                        entity2_id,
                        device_for_pair_calc.clone(),
                    ),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 6]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
        ];

        // Create a vector to store feature results
        let mut feature_results = Vec::with_capacity(NUM_PAIRWISE_FEATURES);
        let mut success_count = 0;
        let mut failure_count = 0;

        // Process each feature task and track outcomes
        for (feature_name, task) in feature_tasks {
            match task.await {
                Ok(value) => {
                    feature_results.push(value);
                    feature_outcomes.insert(feature_name.clone(), "success".to_string());
                    success_count += 1;
                    debug!(
                        "{} Successfully extracted feature: {}",
                        pair_context_for_inner, feature_name
                    );
                }
                Err(e) => {
                    // Log the specific feature failure with detailed error
                    warn!(
                        "{} Failed to extract feature '{}': {}",
                        pair_context_for_inner, feature_name, e
                    );
                    feature_outcomes.insert(feature_name.clone(), format!("failed: {}", e));

                    // Use a default value (0.0) for missing features to maintain vector structure
                    feature_results.push(0.0);
                    failure_count += 1;
                }
            }
        }

        // Log overall pairwise feature extraction results
        if failure_count > 0 {
            warn!(
                "{} Completed pairwise feature extraction with {} successes and {} failures: {:?}",
                pair_context_for_inner, success_count, failure_count, feature_outcomes
            );

            // If there are too many failures, return an error
            if failure_count > NUM_PAIRWISE_FEATURES / 2 {
                return Err(anyhow::anyhow!(
                    "Too many pairwise feature failures ({}/{}): {:?}",
                    failure_count,
                    NUM_PAIRWISE_FEATURES,
                    feature_outcomes
                ));
            }
        } else {
            info!(
                "{} Successfully extracted all {} pairwise features",
                pair_context_for_inner, NUM_PAIRWISE_FEATURES
            );
        }

        Ok(feature_results)
    };

    let pair_features_task_logged = wrap_with_progress(
        pair_features_calculation_task_inner,
        context_steps_completed.clone(),
        total_context_steps,
        "All Pair-Specific Calculations".to_string(),
        pair_context.clone(),
        LogLevel::Info,
    );

    // Execute all three tasks concurrently
    info!(
        "{} Running all feature extraction tasks concurrently...",
        pair_context
    );
    let (entity1_features_res, entity2_features_res, pair_features_res) = tokio::join!(
        entity1_features_task,
        entity2_features_task,
        pair_features_task_logged
    );

    // --- PROCESS RESULTS WITH DETAILED ERROR HANDLING ---

    // Entity 1 features
    let entity1_features = match entity1_features_res {
        Ok(features) => {
            debug!(
                "{} Successfully extracted {} features for entity1",
                pair_context,
                features.len()
            );
            features
        }
        Err(e) => {
            error!(
                "{} Failed to extract features for entity1: {}",
                pair_context, e
            );
            return Err(anyhow::anyhow!(
                "Feature extraction failed for entity1 ({}): {}",
                entity1_id,
                e
            ));
        }
    };

    // Entity 2 features
    let entity2_features = match entity2_features_res {
        Ok(features) => {
            debug!(
                "{} Successfully extracted {} features for entity2",
                pair_context,
                features.len()
            );
            features
        }
        Err(e) => {
            error!(
                "{} Failed to extract features for entity2: {}",
                pair_context, e
            );
            return Err(anyhow::anyhow!(
                "Feature extraction failed for entity2 ({}): {}",
                entity2_id,
                e
            ));
        }
    };

    // Pairwise features
    let pair_features = match pair_features_res {
        Ok(features) => {
            debug!(
                "{} Successfully calculated {} pairwise features",
                pair_context,
                features.len()
            );
            features
        }
        Err(e) => {
            error!(
                "{} Failed to calculate pairwise features: {}",
                pair_context, e
            );
            return Err(anyhow::anyhow!(
                "Pairwise feature calculation failed for pair ({}, {}): {}",
                entity1_id,
                entity2_id,
                e
            ));
        }
    };

    // Assemble final context vector
    let mut final_context_vector = Vec::with_capacity(12 + 12 + 7);
    final_context_vector.extend(entity1_features.clone());
    final_context_vector.extend(entity2_features.clone());
    final_context_vector.extend(pair_features.clone());

    // Validate final vector
    if final_context_vector.len() != 31 {
        warn!(
            "{} Final context vector length is {}, expected 31. Entity1: {}, Entity2: {}, Pairwise: {}",
            pair_context,
            final_context_vector.len(),
            entity1_features.len(),
            entity2_features.len(),
            pair_features.len()
        );

        // You could consider returning an error here if vector length is critical
    }

    info!(
        "{} Successfully extracted complete context vector (length: {})",
        pair_context,
        final_context_vector.len()
    );

    Ok(final_context_vector)
}

/// Stores the 12 individual entity features into `clustering_metadata.entity_context_features`.
async fn store_individual_entity_features(
    conn: &PgConnection,
    entity_id: &str,
    features: &[f64],                         // Should be the 12 features
    all_feature_metadata: &[FeatureMetadata], // Used for names
) -> Result<()> {
    if features.len() != 12 {
        warn!(
            "Attempting to store {} contextual features for entity_id: {}, but expected 12. Skipping store.",
            features.len(),
            entity_id
        );
        return Ok(()); // Or return an error
    }

    let mut query = String::from(
        "INSERT INTO clustering_metadata.entity_context_features (id, entity_id, feature_name, feature_value, created_at) VALUES ",
    );
    let mut params_data: Vec<(String, String, String, f64, chrono::NaiveDateTime)> =
        Vec::with_capacity(12);
    let now = chrono::Utc::now().naive_utc();

    for i in 0..12 {
        // Iterate only for the first 12 individual features
        let feature_meta = &all_feature_metadata[i]; // Safe: metadata has at least 12 elements
        let feature_value = features[i]; // Safe: features has 12 elements
        let feature_id_str = Uuid::new_v4().to_string();
        params_data.push((
            feature_id_str,
            entity_id.to_string(),
            feature_meta.name.clone(),
            feature_value,
            now,
        ));
        if i > 0 {
            query.push_str(", ");
        }
        let base = i * 5;
        query.push_str(&format!(
            "(${p1}, ${p2}, ${p3}, ${p4}, ${p5})",
            p1 = base + 1,
            p2 = base + 2,
            p3 = base + 3,
            p4 = base + 4,
            p5 = base + 5
        ));
    }
    query.push_str(" ON CONFLICT (entity_id, feature_name) DO UPDATE SET feature_value = EXCLUDED.feature_value, created_at = EXCLUDED.created_at");

    let mut sql_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(12 * 5);
    for (id_val, entity_id_val, name_val, value_val, created_at_val) in &params_data {
        sql_params.push(id_val);
        sql_params.push(entity_id_val);
        sql_params.push(name_val);
        sql_params.push(value_val);
        sql_params.push(created_at_val);
    }

    match conn.execute(query.as_str(), &sql_params[..]).await {
        Ok(rows_affected) => {
            debug!(
                "Stored/updated 12 individual contextual features for entity_id: {} ({} rows affected).",
                entity_id, rows_affected
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                "Error storing individual contextual features for entity_id: {}. Query: [{}], Error: {}",
                entity_id, query, e
            );
            Err(anyhow::Error::from(e)
                .context("Storing individual entity contextual features failed"))
        }
    }
}

// --- Synchronous Calculation Helpers for Basic Features (from RawEntityOrgData) ---
fn calculate_name_complexity_from_data(data: &RawEntityOrgData) -> f64 {
    if let Some(name) = &data.org_name {
        let length = name.len() as f64;
        let word_count = name.split_whitespace().count() as f64;
        (length / 100.0).min(1.0) * 0.5 + (word_count / 10.0).min(1.0) * 0.5
    } else {
        0.0
    }
}
fn calculate_data_completeness_from_data(data: &RawEntityOrgData) -> f64 {
    let mut score = 0.0;
    let mut total_fields = 0.0;
    total_fields += 1.0;
    if data.org_name.is_some() {
        score += 1.0;
    }
    total_fields += 1.0;
    if data.org_description.as_ref().map_or(false, |d| d.len() > 5) {
        score += 1.0;
    }
    total_fields += 1.0;
    if data.org_email.is_some() {
        score += 1.0;
    }
    total_fields += 1.0;
    if data.org_url.is_some() {
        score += 1.0;
    }
    total_fields += 1.0;
    if data.org_tax_id.is_some() {
        score += 1.0;
    }
    total_fields += 1.0;
    if data.org_legal_status.is_some() {
        score += 1.0;
    }
    if total_fields == 0.0 {
        0.0
    } else {
        score / total_fields
    }
}
fn calculate_has_email_from_data(data: &RawEntityOrgData) -> f64 {
    if data.org_email.is_some() {
        1.0
    } else {
        0.0
    }
}
fn calculate_has_phone_from_data(data: &RawEntityOrgData) -> f64 {
    if data.has_phone_flag {
        1.0
    } else {
        0.0
    }
}
fn calculate_has_url_from_data(data: &RawEntityOrgData) -> f64 {
    if data.org_url.is_some() {
        1.0
    } else {
        0.0
    }
}
fn calculate_has_address_from_data(data: &RawEntityOrgData) -> f64 {
    if data.has_address_flag {
        1.0
    } else {
        0.0
    }
}
fn calculate_has_location_from_data(data: &RawEntityOrgData) -> f64 {
    if data.has_location_coords_flag {
        1.0
    } else {
        0.0
    }
}
fn calculate_organization_size_from_data(data: &RawEntityOrgData) -> f64 {
    let service_score = (data.service_count_val as f64 / 10.0).min(1.0);
    let location_score = (data.location_count_val as f64 / 5.0).min(1.0);
    let phone_score = if data.has_phone_flag { 1.0 } else { 0.0 };
    service_score * 0.5 + location_score * 0.3 + phone_score * 0.2
}
fn calculate_service_count_feature_from_data(data: &RawEntityOrgData) -> f64 {
    (data.service_count_val as f64 / 10.0).min(1.0)
}

// --- Enhanced Feature Extraction Functions (using Candle) ---
async fn extract_embedding_centroid_distance(
    conn: &PgConnection,
    entity_id: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!(
        "Extracting embedding centroid distance for entity {} using Candle",
        entity_id
    );
    let org_embedding_row = conn.query_opt("SELECT o.embedding FROM public.organization o JOIN public.entity e ON e.organization_id = o.id WHERE e.id = $1 AND o.embedding IS NOT NULL", &[&entity_id]).await.context("DB query for org_embedding failed")?;
    let centroid_embedding_row = conn.query_opt("SELECT avg(embedding) as centroid_embedding FROM public.organization WHERE embedding IS NOT NULL", &[]).await.context("DB query for centroid_embedding failed")?;
    match (org_embedding_row, centroid_embedding_row) {
        (Some(org_row), Some(centroid_row)) => {
            let org_pg_vec: Option<PgVector> = org_row.try_get(0).ok();
            let centroid_pg_vec: Option<PgVector> = centroid_row.try_get("centroid_embedding").ok();
            if let (Some(org_pg_vec_val), Some(centroid_pg_vec_val)) = (org_pg_vec, centroid_pg_vec)
            {
                let org_vec_f32 = org_pg_vec_val.to_vec();
                let centroid_vec_f32 = centroid_pg_vec_val.to_vec();
                if org_vec_f32.is_empty() || centroid_vec_f32.is_empty() {
                    warn!(
                        "Empty embedding vector for entity {} or centroid. Max distance.",
                        entity_id
                    );
                    return Ok(2.0);
                }
                if org_vec_f32.len() != centroid_vec_f32.len() {
                    warn!("Embedding dimension mismatch for entity {}. Org: {}, Centroid: {}. Max distance.", entity_id, org_vec_f32.len(), centroid_vec_f32.len());
                    return Ok(2.0);
                }
                let similarity = cosine_similarity_candle(&org_vec_f32, &centroid_vec_f32)
                    .map_err(|e| anyhow::anyhow!("Candle similarity failed: {}", e))?;
                Ok((1.0 - similarity).max(0.0).min(2.0)) // Cosine distance
            } else {
                warn!(
                    "Could not retrieve org/centroid embedding for entity {}.",
                    entity_id
                );
                Ok(2.0)
            }
        }
        _ => {
            warn!(
                "Org embedding or centroid not found for entity {}.",
                entity_id
            );
            Ok(2.0)
        }
    }
}
async fn extract_service_semantic_coherence(
    conn: &PgConnection,
    entity_id: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!(
        "Extracting service semantic coherence for entity {} using Candle",
        entity_id
    );
    let service_embeddings_rows = conn.query("SELECT s.embedding_v2 FROM public.service s JOIN public.entity_feature ef ON ef.table_id = s.id WHERE ef.entity_id = $1 AND ef.table_name = 'service' AND s.embedding_v2 IS NOT NULL", &[&entity_id]).await.context("DB query for service_embeddings failed")?;
    let service_embeddings_f32: Vec<Vec<f32>> = service_embeddings_rows
        .into_iter()
        .filter_map(|row| {
            row.get::<_, Option<PgVector>>(0)
                .map(|pg_vec| pg_vec.to_vec())
        })
        .filter(|v| !v.is_empty())
        .collect();
    if service_embeddings_f32.len() < 2 {
        return Ok(0.0);
    }
    let first_dim = service_embeddings_f32[0].len();
    if !service_embeddings_f32.iter().all(|v| v.len() == first_dim) {
        warn!(
            "Service embeddings for entity {} have inconsistent dimensions.",
            entity_id
        );
        return Ok(0.0);
    }
    let mut total_similarity = 0.0;
    let mut pair_count = 0;
    for i in 0..service_embeddings_f32.len() {
        for j in (i + 1)..service_embeddings_f32.len() {
            let sim =
                cosine_similarity_candle(&service_embeddings_f32[i], &service_embeddings_f32[j])
                    .map_err(|e| {
                        anyhow::anyhow!("Candle similarity for service coherence failed: {}", e)
                    })?;
            total_similarity += sim;
            pair_count += 1;
        }
    }
    if pair_count == 0 {
        Ok(0.0)
    } else {
        Ok(total_similarity / pair_count as f64)
    }
}
async fn extract_embedding_quality_score(conn: &PgConnection, entity_id: &str) -> Result<f64> {
    let row_opt = conn.query_opt("SELECT CASE WHEN o.embedding IS NULL THEN 0.0 WHEN o.description IS NULL OR LENGTH(o.description) = 0 THEN 0.1 WHEN LENGTH(o.description) < 20 THEN 0.3 WHEN LENGTH(o.description) < 100 THEN 0.6 ELSE 0.9 END::DOUBLE PRECISION as embedding_quality FROM public.entity e JOIN public.organization o ON e.organization_id = o.id WHERE e.id = $1", &[&entity_id]).await.context(format!("DB query failed for embedding_quality_score for entity {}", entity_id))?;
    Ok(row_opt.map_or(0.0, |row| {
        get_guaranteed_f64_from_row(&row, 0, "embedding_quality_score", &entity_id)
    }))
}

// --- Pairwise Feature Functions (using Candle where applicable) ---
async fn calculate_name_similarity(
    conn: &PgConnection,
    entity1: &str,
    entity2: &str,
) -> Result<f64> {
    let row_opt = conn.query_opt("SELECT similarity(LOWER(o1.name), LOWER(o2.name))::DOUBLE PRECISION as name_similarity FROM public.entity e1 JOIN public.organization o1 ON e1.organization_id = o1.id, public.entity e2 JOIN public.organization o2 ON e2.organization_id = o2.id WHERE e1.id = $1 AND e2.id = $2 AND o1.name IS NOT NULL AND o2.name IS NOT NULL", &[&entity1, &entity2]).await.context("DB query for name_similarity failed")?;
    Ok(row_opt.map_or(0.0, |row| {
        get_guaranteed_f64_from_row(
            &row,
            0,
            "name_similarity",
            &format!("{},{}", entity1, entity2),
        )
    }))
}
async fn calculate_embedding_similarity(
    conn: &PgConnection,
    entity1: &str,
    entity2: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!(
        "Calculating embedding similarity for pair ({}, {}) using Candle",
        entity1, entity2
    );
    let row_opt = conn.query_opt("SELECT o1.embedding as emb1, o2.embedding as emb2 FROM public.entity e1 JOIN public.organization o1 ON e1.organization_id = o1.id, public.entity e2 JOIN public.organization o2 ON e2.organization_id = o2.id WHERE e1.id = $1 AND e2.id = $2", &[&entity1, &entity2]).await.context("DB query for embeddings in embedding_similarity failed")?;
    if let Some(row) = row_opt {
        let emb1_pg: Option<PgVector> = row.try_get("emb1").ok();
        let emb2_pg: Option<PgVector> = row.try_get("emb2").ok();
        if let (Some(e1), Some(e2)) = (emb1_pg, emb2_pg) {
            let v1_f32 = e1.to_vec();
            let v2_f32 = e2.to_vec();
            if v1_f32.is_empty() || v2_f32.is_empty() {
                warn!(
                    "Empty embedding(s) for pair ({}, {}). Sim 0.0.",
                    entity1, entity2
                );
                return Ok(0.0);
            }
            if v1_f32.len() != v2_f32.len() {
                warn!(
                    "Embedding dim mismatch for pair ({}, {}). V1: {}, V2: {}. Sim 0.0.",
                    entity1,
                    entity2,
                    v1_f32.len(),
                    v2_f32.len()
                );
                return Ok(0.0);
            }
            cosine_similarity_candle(&v1_f32, &v2_f32)
                .map_err(|e| anyhow::anyhow!("Candle similarity failed: {}", e))
        } else {
            Ok(0.0)
        }
    } else {
        Ok(0.0)
    }
}
async fn calculate_max_service_similarity(
    conn: &PgConnection,
    entity1: &str,
    entity2: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!(
        "Calculating max service similarity for pair ({}, {}) using Candle",
        entity1, entity2
    );
    let services1_rows = conn.query("SELECT s.embedding_v2 FROM public.service s JOIN public.entity_feature ef ON ef.table_id = s.id WHERE ef.entity_id = $1 AND ef.table_name = 'service' AND s.embedding_v2 IS NOT NULL", &[&entity1]).await.context(format!("DB query for services of e1 ({}) failed", entity1))?;
    let services2_rows = conn.query("SELECT s.embedding_v2 FROM public.service s JOIN public.entity_feature ef ON ef.table_id = s.id WHERE ef.entity_id = $1 AND ef.table_name = 'service' AND s.embedding_v2 IS NOT NULL", &[&entity2]).await.context(format!("DB query for services of e2 ({}) failed", entity2))?;
    let s1_embeddings: Vec<Vec<f32>> = services1_rows
        .into_iter()
        .filter_map(|r| r.get::<_, Option<PgVector>>(0).map(|pg_v| pg_v.to_vec()))
        .filter(|v| !v.is_empty())
        .collect();
    let s2_embeddings: Vec<Vec<f32>> = services2_rows
        .into_iter()
        .filter_map(|r| r.get::<_, Option<PgVector>>(0).map(|pg_v| pg_v.to_vec()))
        .filter(|v| !v.is_empty())
        .collect();
    if s1_embeddings.is_empty() || s2_embeddings.is_empty() {
        return Ok(0.0);
    }
    if !s1_embeddings.is_empty()
        && !s2_embeddings.is_empty()
        && (s1_embeddings[0].len() != s2_embeddings[0].len())
    {
        warn!("Max service sim: Embeddings for pair ({}, {}) have different dimensions. S1: {}, S2: {}. Sim 0.0.", entity1, entity2, s1_embeddings[0].len(), s2_embeddings[0].len());
        return Ok(0.0);
    }
    let mut max_similarity = -1.0f64;
    for emb1 in &s1_embeddings {
        for emb2 in &s2_embeddings {
            if emb1.len() != emb2.len() {
                warn!(
                    "Skipping service pair due to dim mismatch. Emb1: {}, Emb2: {}",
                    emb1.len(),
                    emb2.len()
                );
                continue;
            }
            let current_sim = cosine_similarity_candle(emb1, emb2)
                .map_err(|e| anyhow::anyhow!("Candle sim for max service sim failed: {}", e))?;
            if current_sim > max_similarity {
                max_similarity = current_sim;
            }
        }
    }
    Ok(max_similarity)
}
async fn calculate_geographic_distance(
    conn: &PgConnection,
    entity1: &str,
    entity2: &str,
) -> Result<f64> {
    let row = conn.query_one("WITH loc_distances AS (SELECT ST_Distance(l1.geom, l2.geom) as distance FROM public.location l1 JOIN public.entity_feature ef1 ON ef1.table_id = l1.id, public.location l2 JOIN public.entity_feature ef2 ON ef2.table_id = l2.id WHERE ef1.entity_id = $1 AND ef1.table_name = 'location' AND ef2.entity_id = $2 AND ef2.table_name = 'location' AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL) SELECT CASE WHEN COUNT(distance) = 0 THEN 0.0 WHEN MIN(distance) > 10000 THEN 0.0 ELSE 1.0 - (MIN(distance) / 10000.0) END::DOUBLE PRECISION FROM loc_distances", &[&entity1, &entity2]).await.context("DB query for geographic_distance failed")?;
    Ok(get_guaranteed_f64_from_row(
        &row,
        0,
        "geographic_distance",
        &format!("{},{}", entity1, entity2),
    ))
}
async fn check_shared_domain(conn: &PgConnection, entity1: &str, entity2: &str) -> Result<f64> {
    let row_opt = conn.query_opt("WITH domain_extract AS (SELECT regexp_replace(LOWER(o1.url), '^https?://(www\\.)?|/.*$', '', 'g') as domain1, regexp_replace(LOWER(o2.url), '^https?://(www\\.)?|/.*$', '', 'g') as domain2 FROM public.entity e1 JOIN public.organization o1 ON e1.organization_id = o1.id, public.entity e2 JOIN public.organization o2 ON e2.organization_id = o2.id WHERE e1.id = $1 AND e2.id = $2 AND o1.url IS NOT NULL AND o1.url <> '' AND o2.url IS NOT NULL AND o2.url <> '') SELECT CASE WHEN domain1 = domain2 AND domain1 <> '' THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION FROM domain_extract", &[&entity1, &entity2]).await.context("DB query for shared_domain failed")?;
    Ok(row_opt.map_or(0.0, |row| {
        get_guaranteed_f64_from_row(
            &row,
            0,
            "shared_domain",
            &format!("{},{}", entity1, entity2),
        )
    }))
}
async fn check_shared_phone(conn: &PgConnection, entity1: &str, entity2: &str) -> Result<f64> {
    let row = conn.query_one("SELECT CASE WHEN EXISTS (SELECT 1 FROM public.phone p1 JOIN public.entity_feature ef1 ON ef1.table_id = p1.id, public.phone p2 JOIN public.entity_feature ef2 ON ef2.table_id = p2.id WHERE ef1.entity_id = $1 AND ef1.table_name = 'phone' AND ef2.entity_id = $2 AND ef2.table_name = 'phone' AND regexp_replace(p1.number, '[^0-9]', '', 'g') = regexp_replace(p2.number, '[^0-9]', '', 'g') AND regexp_replace(p1.number, '[^0-9]', '', 'g') <> '') THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION", &[&entity1, &entity2]).await.context("DB query for shared_phone failed")?;
    Ok(get_guaranteed_f64_from_row(
        &row,
        0,
        "shared_phone",
        &format!("{},{}", entity1, entity2),
    ))
}
async fn calculate_service_geo_semantic_score(
    conn: &PgConnection,
    entity1: &str,
    entity2: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!(
        "Calculating service_geo_semantic_score for pair ({}, {}) using Candle",
        entity1, entity2
    );
    let rows = conn.query("SELECT s1.embedding_v2 as emb1, s2.embedding_v2 as emb2, ST_Distance(l1.geom, l2.geom) as geo_distance FROM public.service s1 JOIN public.entity_feature ef1 ON ef1.table_id = s1.id JOIN public.service_at_location sal1 ON sal1.service_id = s1.id JOIN public.location l1 ON l1.id = sal1.location_id, public.service s2 JOIN public.entity_feature ef2 ON ef2.table_id = s2.id JOIN public.service_at_location sal2 ON sal2.service_id = s2.id JOIN public.location l2 ON l2.id = sal2.location_id WHERE ef1.entity_id = $1 AND ef1.table_name = 'service' AND s1.embedding_v2 IS NOT NULL AND l1.geom IS NOT NULL AND ef2.entity_id = $2 AND ef2.table_name = 'service' AND s2.embedding_v2 IS NOT NULL AND l2.geom IS NOT NULL", &[&entity1, &entity2]).await.context("DB query for service_geo_semantic_score failed")?;
    if rows.is_empty() {
        return Ok(0.0);
    }
    let mut total_weighted_score = 0.0;
    let mut count = 0;
    for row in rows {
        let emb1_pg: Option<PgVector> = row.try_get("emb1").ok();
        let emb2_pg: Option<PgVector> = row.try_get("emb2").ok();
        let geo_distance: Option<f64> = row.try_get("geo_distance").ok();
        if let (Some(e1), Some(e2), Some(dist)) = (emb1_pg, emb2_pg, geo_distance) {
            let v1_f32 = e1.to_vec();
            let v2_f32 = e2.to_vec();
            if v1_f32.is_empty() || v2_f32.is_empty() {
                continue;
            }
            if v1_f32.len() != v2_f32.len() {
                warn!(
                    "Service geo semantic: dim mismatch. V1: {}, V2: {}.",
                    v1_f32.len(),
                    v2_f32.len()
                );
                continue;
            }
            let semantic_sim = cosine_similarity_candle(&v1_f32, &v2_f32).map_err(|e| {
                anyhow::anyhow!("Candle sim for service_geo_semantic failed: {}", e)
            })?;
            let geo_proximity = if dist > 10000.0 {
                0.0
            } else {
                1.0 - (dist / 10000.0)
            };
            total_weighted_score += semantic_sim * geo_proximity;
            count += 1;
        }
    }
    if count == 0 {
        Ok(0.0)
    } else {
        Ok((total_weighted_score / count as f64).max(0.0).min(1.0))
    }
}

// --- Helper functions for robust row data extraction ---
fn get_guaranteed_f64_from_row(
    row: &PgRow,
    index: usize,
    feature_name: &str,
    context: &str,
) -> f64 {
    match row.try_get::<_, f64>(index) {
        Ok(value) => value,
        Err(e) => {
            warn!("Error deserializing guaranteed f64 for '{}' for context '{}': {}. Defaulting to 0.0.", feature_name, context, e);
            0.0
        }
    }
}
