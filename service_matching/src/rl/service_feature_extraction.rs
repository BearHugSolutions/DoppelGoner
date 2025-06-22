// src/rl/service_feature_extraction.rs
use anyhow::{Context, Result};
use candle_core::cpu::kernels::VecOps;
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

use crate::rl::service_feature_cache::SharedServiceFeatureCache;
use crate::utils::candle::cosine_similarity_candle;
use crate::utils::db_connect::PgPool;

// Helper to get a Candle device
fn get_candle_device() -> Result<Device, CandleError> {
    #[cfg(all(feature = "metal", target_os = "macos"))]
    {
        info!("Using Metal device for Candle operations in service_feature_extraction.");
        return Device::new_metal(0);
    }
    #[cfg(all(feature = "cuda"))]
    {
        info!("Using CUDA device for Candle operations in service_feature_extraction.");
        return Device::new_cuda(0);
    }
    info!("Using CPU device for Candle operations in service_feature_extraction.");
    Ok(Device::Cpu)
}

type SingleFeatureFuture<'a> =
    Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + 'a>>;

// Helper function to wrap a future with progress logging
async fn wrap_with_progress<F, T, E>(
    task_future: F,
    counter: Arc<AtomicUsize>,
    total_tasks: usize,
    task_description: String,
    service_context: String,
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
                service_context,
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
                service_context,
                task_description,
                completed_count,
                total_tasks,
                e
            );
            Err(e)
        }
    }
}

/// Service-specific feature metadata
#[derive(Debug, Clone)]
pub struct ServiceFeatureMetadata {
    pub name: String,
    pub description: String,
    pub min_value: f64,
    pub max_value: f64,
}

/// Extracts and stores contextual features for all services in PARALLEL.
/// This function should be called from the RL phase of the service pipeline.
pub async fn extract_and_store_all_service_contextual_features(
    pool: &PgPool,
    _feature_cache: &SharedServiceFeatureCache,
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    info!("Starting PARALLEL extraction and storage of contextual features for all services...");
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
        pb.set_message("Setting up service feature extraction...");
        Some(pb)
    } else {
        None
    };

    // Get a connection to fetch the list of all service IDs
    let conn_for_list = pool
        .get()
        .await
        .context("Failed to get DB connection for service list")?;
    let service_rows = conn_for_list
        .query("SELECT id FROM public.service", &[])
        .await
        .context("Failed to query service IDs for contextual feature extraction")?;
    drop(conn_for_list);

    if service_rows.is_empty() {
        info!("No services found to process for contextual features.");
        if let Some(pb) = &main_pb {
            pb.finish_with_message("No services to process");
        }
        return Ok(0);
    }

    let service_ids: Vec<String> = service_rows
        .into_iter()
        .map(|row| row.get(0))
        .collect();

    let total_services_to_process = service_ids.len();
    info!(
        "Found {} services to process for contextual features.",
        total_services_to_process
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message(format!(
            "Processing {} services...",
            total_services_to_process
        ));
    }

    // Create detailed progress bar for service processing
    let service_progress_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(total_services_to_process as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("      {spinner:.blue} [{elapsed_precise}] {bar:20.yellow/red} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Processing services...");
        Some(pb)
    } else {
        None
    };

    // --- Parallel Processing Logic ---
    let desired_concurrency = 30;

    // Atomic counters for tracking progress in a thread-safe manner
    let succeeded_count = Arc::new(AtomicUsize::new(0));
    let processed_for_log_count = Arc::new(AtomicUsize::new(0));

    // Clone progress bar for use in async tasks
    let service_pb_clone = service_progress_pb.clone();

    stream::iter(service_ids)
        .map(|service_id| {
            let pool_clone = pool.clone();
            let succeeded_clone = Arc::clone(&succeeded_count);
            let processed_log_clone = Arc::clone(&processed_for_log_count);
            let pb_clone = service_pb_clone.clone();
            let total_services = total_services_to_process;

            tokio::spawn(async move {
                match pool_clone.get().await {
                    Ok(conn_guard) => {
                        match get_stored_service_features(&*conn_guard, &service_id).await {
                            Ok(_features) => {
                                succeeded_clone.fetch_add(1, Ordering::Relaxed);
                                debug!("Contextual features ensured for service {}", service_id);
                            }
                            Err(e) => {
                                warn!("Failed to ensure contextual features for service {}: {}", service_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get DB connection for service {}: {}", service_id, e);
                    }
                }
                
                // Update progress bars
                if let Some(ref pb) = pb_clone {
                    pb.inc(1);
                }
                
                // Log progress periodically
                let current_processed = processed_log_clone.fetch_add(1, Ordering::Relaxed) + 1;
                if current_processed % 100 == 0 || current_processed == total_services {
                    info!(
                        "Service contextual feature extraction progress: {}/{} service processing tasks dispatched/completed.",
                        current_processed, total_services
                    );
                    
                    if let Some(ref pb) = pb_clone {
                        if current_processed % 100 == 0 {
                            pb.set_message(format!("Processing services... ({}/{})", current_processed, total_services));
                        }
                    }
                }
            })
        })
        .buffer_unordered(desired_concurrency)
        .for_each(|result| async {
            if let Err(e) = result {
                warn!("A service contextual feature extraction task panicked or failed to join: {:?}", e);
            }
        })
        .await;

    let final_succeeded_count = succeeded_count.load(Ordering::Relaxed);

    // Finish service progress bar
    if let Some(pb) = &service_progress_pb {
        pb.finish_with_message(format!("Processed {} services", total_services_to_process));
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
        "Parallel service contextual feature extraction and storage complete in {:.2?}. Ensured features for {} out of {} services.",
        elapsed, final_succeeded_count, total_services_to_process
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.finish_with_message(format!(
            "Service feature extraction complete: {}/{} services",
            final_succeeded_count, total_services_to_process
        ));
    }

    Ok(final_succeeded_count)
}

/// Get feature metadata for services (12 individual + 7 pairwise features)
pub fn get_service_feature_metadata() -> Vec<ServiceFeatureMetadata> {
    vec![
        // --- Individual Service Features (Indices 0-11) ---
        ServiceFeatureMetadata {
            name: "service_name_complexity".to_string(),
            description: "Complexity of the service name.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "service_description_completeness".to_string(),
            description: "Completeness of service description.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "has_email".to_string(),
            description: "Boolean indicating if service has email.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "has_url".to_string(),
            description: "Boolean indicating if service has URL.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "has_phone".to_string(),
            description: "Boolean indicating if service has phone.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "has_location".to_string(),
            description: "Boolean indicating if service has location.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "taxonomy_count".to_string(),
            description: "Normalized count of service taxonomies.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "schedule_completeness".to_string(),
            description: "Completeness of service schedule information.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "eligibility_completeness".to_string(),
            description: "Completeness of service eligibility information.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "service_embedding_quality".to_string(),
            description: "Quality score of the service's embedding.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "service_embedding_distance_to_org_embedding".to_string(),
            description: "Distance from service embedding to organization embedding.".to_string(),
            min_value: 0.0,
            max_value: 2.0,
        },
        ServiceFeatureMetadata {
            name: "related_program_exists".to_string(),
            description: "Boolean indicating if service has related program.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        // --- Pairwise Features (Indices 12-18 when combined) ---
        ServiceFeatureMetadata {
            name: "service_name_similarity".to_string(),
            description: "Similarity between two service names.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "service_description_similarity".to_string(),
            description: "Semantic similarity between service descriptions.".to_string(),
            min_value: -1.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "service_embedding_similarity".to_string(),
            description: "Cosine similarity between service embeddings.".to_string(),
            min_value: -1.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "shared_organization".to_string(),
            description: "Boolean if services share the same organization.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "shared_taxonomy_count".to_string(),
            description: "Number of shared taxonomies between services.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "geographic_proximity".to_string(),
            description: "Geographic distance between service locations.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        ServiceFeatureMetadata {
            name: "shared_program".to_string(),
            description: "Boolean if services share the same program.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
    ]
}

#[derive(Debug)]
struct RawServiceData {
    service_name: Option<String>,
    service_description: Option<String>,
    service_email: Option<String>,
    service_url: Option<String>,
    organization_id: Option<String>,
    program_id: Option<String>,
    has_phone_flag: bool,
    has_location_flag: bool,
    taxonomy_count_val: i64,
    schedule_count_val: i64,
    eligibility_description: Option<String>,
}

/// Extracts the 12 individual features for a single service.
/// If features are already stored, they are retrieved; otherwise, they are calculated and stored.
pub async fn get_stored_service_features(
    conn: &PgConnection,
    service_id: &str,
) -> Result<Vec<f64>> {
    let service_context = format!("Service {}", service_id);
    debug!(
        "{} Checking for stored contextual features...",
        service_context
    );
    let rows = conn
        .query(
            "SELECT feature_name, feature_value
         FROM clustering_metadata.service_context_features
         WHERE service_id = $1
         ORDER BY feature_name",
            &[&service_id],
        )
        .await
        .context(format!(
            "Failed to query stored contextual features for service {}",
            service_id
        ))?;

    let metadata: Vec<ServiceFeatureMetadata> = get_service_feature_metadata();
    const INDIVIDUAL_FEATURE_COUNT: usize = 12;

    if !rows.is_empty() {
        debug!(
            "{} Found {} stored contextual feature rows, reconstructing vector...",
            service_context,
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
                    service_context, metadata[i].name
                );
                all_present = false;
                break;
            }
        }

        if all_present && features_vec.len() == INDIVIDUAL_FEATURE_COUNT {
            debug!(
                "{} Successfully reconstructed all {} stored contextual features.",
                service_context, INDIVIDUAL_FEATURE_COUNT
            );
            return Ok(features_vec);
        } else {
            warn!(
                "{}: Stored contextual features incomplete (found {}, expected {}) or mismatched. Re-extracting all.",
                service_context, features_vec.len(), INDIVIDUAL_FEATURE_COUNT
            );
        }
    } else {
        info!(
            "{}: No stored contextual features found, extracting now.",
            service_context
        );
    }

    extract_and_store_service_features(conn, service_id, &metadata.as_slice()).await
}

/// Extracts, stores, and returns the 12 individual features for a single service.
async fn extract_and_store_service_features(
    conn: &PgConnection,
    service_id: &str,
    all_feature_metadata: &[ServiceFeatureMetadata],
) -> Result<Vec<f64>> {
    let service_context = format!("Service {}", service_id);
    const INDIVIDUAL_FEATURE_COUNT: usize = 12;

    info!(
        "{} Extracting 9 basic contextual features (consolidated query)...",
        service_context
    );
    let basic_features_tasks_completed = Arc::new(AtomicUsize::new(0));
    let total_basic_feature_tasks = 1;

    let consolidated_query_future = async {
        let query = "
            SELECT
                s.name AS service_name, s.description AS service_description, 
                s.email AS service_email, s.url AS service_url,
                s.organization_id, s.program_id, s.eligibility_description,
                EXISTS (SELECT 1 FROM public.phone p WHERE p.service_id = s.id AND p.number IS NOT NULL AND p.number <> '') AS has_phone_flag,
                EXISTS (SELECT 1 FROM public.service_at_location sal JOIN public.location l ON sal.location_id = l.id WHERE sal.service_id = s.id AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL) AS has_location_flag,
                (SELECT COUNT(*) FROM public.service_taxonomy st WHERE st.service_id = s.id) AS taxonomy_count_val,
                (SELECT COUNT(*) FROM public.schedule sch WHERE sch.service_id = s.id) AS schedule_count_val
            FROM public.service s
            WHERE s.id = $1";

        conn.query_one(query, &[&service_id])
            .await
            .map_err(anyhow::Error::from)
            .context(format!(
                "Consolidated query for basic contextual features failed for service {}",
                service_id
            ))
    };

    let row = wrap_with_progress(
        consolidated_query_future,
        basic_features_tasks_completed.clone(),
        total_basic_feature_tasks,
        "Consolidated Basic Service Features Query".to_string(),
        service_context.clone(),
        LogLevel::Info,
    )
    .await?;

    let raw_data = RawServiceData {
        service_name: row.try_get("service_name").ok(),
        service_description: row.try_get("service_description").ok(),
        service_email: row.try_get("service_email").ok(),
        service_url: row.try_get("service_url").ok(),
        organization_id: row.try_get("organization_id").ok(),
        program_id: row.try_get("program_id").ok(),
        has_phone_flag: row.try_get("has_phone_flag").unwrap_or(false),
        has_location_flag: row.try_get("has_location_flag").unwrap_or(false),
        taxonomy_count_val: row.try_get("taxonomy_count_val").unwrap_or(0),
        schedule_count_val: row.try_get("schedule_count_val").unwrap_or(0),
        eligibility_description: row.try_get("eligibility_description").ok(),
    };

    let basic_features_vec = vec![
        calculate_service_name_complexity_from_data(&raw_data),
        calculate_service_description_completeness_from_data(&raw_data),
        calculate_has_email_from_data(&raw_data),
        calculate_has_url_from_data(&raw_data),
        calculate_has_phone_from_data(&raw_data),
        calculate_has_location_from_data(&raw_data),
        calculate_taxonomy_count_from_data(&raw_data),
        calculate_schedule_completeness_from_data(&raw_data),
        calculate_eligibility_completeness_from_data(&raw_data),
    ];

    info!(
        "{} Completed calculation of 9 basic contextual features.",
        service_context
    );

    let candle_device =
        get_candle_device().map_err(|e| anyhow::anyhow!("Failed to get candle device: {}", e))?;

    info!(
        "{} Starting extraction of 3 enhanced contextual features (using Candle)...",
        service_context
    );
    let enhanced_tasks_completed = Arc::new(AtomicUsize::new(0));
    let num_enhanced_feature_tasks = 3;

    let enhanced_futures: [SingleFeatureFuture<'_>; 3] = [
        Box::pin(wrap_with_progress(
            extract_service_embedding_quality(conn, service_id),
            enhanced_tasks_completed.clone(),
            num_enhanced_feature_tasks,
            all_feature_metadata[9].name.clone(),
            service_context.clone(),
            LogLevel::Debug,
        )),
        Box::pin(wrap_with_progress(
            extract_service_embedding_distance_to_org(conn, service_id, candle_device.clone()),
            enhanced_tasks_completed.clone(),
            num_enhanced_feature_tasks,
            all_feature_metadata[10].name.clone(),
            service_context.clone(),
            LogLevel::Debug,
        )),
        Box::pin(wrap_with_progress(
            extract_related_program_exists(conn, service_id),
            enhanced_tasks_completed.clone(),
            num_enhanced_feature_tasks,
            all_feature_metadata[11].name.clone(),
            service_context.clone(),
            LogLevel::Debug,
        )),
    ];
    let enhanced_results = future::try_join_all(Vec::from(enhanced_futures)).await?;
    info!(
        "{} Completed extraction of 3 enhanced contextual features.",
        service_context
    );

    let mut features = Vec::with_capacity(INDIVIDUAL_FEATURE_COUNT);
    features.extend(basic_features_vec);
    features.extend(enhanced_results);

    debug!(
        "{} Storing {} extracted individual contextual features...",
        service_context,
        features.len()
    );
    store_individual_service_features(conn, service_id, &features, all_feature_metadata).await?;
    debug!(
        "{} Successfully stored individual contextual features.",
        service_context
    );

    Ok(features)
}

/// Extracts the full feature vector for a PAIR of services.
pub async fn extract_service_context_for_pair(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
) -> Result<Vec<f64>> {
    let pair_context = format!("ServicePair ({}, {})", service1_id, service2_id);

    info!(
        "{} Starting service contextual feature extraction...",
        pair_context
    );
    
    debug!("{} Getting feature metadata...", pair_context);
    let all_feature_metadata = get_service_feature_metadata();

    debug!("{} Initializing Candle device...", pair_context);
    let candle_device = get_candle_device()
        .map_err(|e| anyhow::anyhow!("Failed to initialize Candle device: {}", e))?;

    info!(
        "{} Extracting individual contextual features for both services...",
        pair_context
    );

    // Get individual features (these should be cached from Phase 3)
    let service1_features = get_stored_service_features(conn, service1_id)
        .await
        .context(format!("Failed to get features for service1 ({})", service1_id))?;
        
    let service2_features = get_stored_service_features(conn, service2_id)
        .await
        .context(format!("Failed to get features for service2 ({})", service2_id))?;

    // Get all pairwise features in a single query
    info!("{} Extracting pairwise features with consolidated query...", pair_context);
    let pair_features = extract_pairwise_features_consolidated(conn, service1_id, service2_id, candle_device)
        .await
        .context(format!("Failed to extract pairwise features for ({}, {})", service1_id, service2_id))?;

    // Assemble final context vector
    let mut final_context_vector = Vec::with_capacity(31);
    final_context_vector.extend(service1_features);
    final_context_vector.extend(service2_features);
    final_context_vector.extend(pair_features);

    // Validate final vector
    if final_context_vector.len() != 31 {
        warn!(
            "{} Final context vector length is {}, expected 31",
            pair_context,
            final_context_vector.len()
        );
    }

    info!(
        "{} Successfully extracted complete context vector (length: {}) with consolidated query",
        pair_context,
        final_context_vector.len()
    );

    Ok(final_context_vector)
}

/// Extracts all pairwise features in a single consolidated database query
async fn extract_pairwise_features_consolidated(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
    candle_device: Device,
) -> Result<Vec<f64>> {
    let pair_context = format!("ServicePair ({}, {})", service1_id, service2_id);
    debug!("{} Extracting all pairwise features in single query...", pair_context);
    
    // Single consolidated query to get all the data we need
    let row = conn.query_one(
        r#"
        WITH service_data AS (
            SELECT 
                s1.id as s1_id,
                s2.id as s2_id,
                -- Names for similarity
                s1.name as s1_name,
                s2.name as s2_name,
                -- Embeddings for similarity calculations
                s1.embedding_v2 as s1_embedding,
                s2.embedding_v2 as s2_embedding,
                -- Organization comparison
                s1.organization_id as s1_org_id,
                s2.organization_id as s2_org_id,
                -- Program comparison
                s1.program_id as s1_program_id,
                s2.program_id as s2_program_id
            FROM public.service s1, public.service s2
            WHERE s1.id = $1 AND s2.id = $2
        ),
        taxonomy_data AS (
            SELECT COUNT(*)::DOUBLE PRECISION as shared_taxonomy_count
            FROM public.service_taxonomy st1 
            JOIN public.service_taxonomy st2 ON st1.taxonomy_term_id = st2.taxonomy_term_id
            WHERE st1.service_id = $1 AND st2.service_id = $2
        ),
        location_data AS (
            SELECT 
                CASE 
                    WHEN COUNT(*) = 0 THEN 0.0 
                    WHEN MIN(ST_Distance(l1.geom, l2.geom)) > 10000 THEN 0.0 
                    ELSE 1.0 - (MIN(ST_Distance(l1.geom, l2.geom)) / 10000.0) 
                END::DOUBLE PRECISION as geographic_proximity
            FROM public.service_at_location sal1 
            JOIN public.location l1 ON sal1.location_id = l1.id,
                 public.service_at_location sal2 
            JOIN public.location l2 ON sal2.location_id = l2.id
            WHERE sal1.service_id = $1 AND sal2.service_id = $2 
              AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL
        )
        SELECT 
            -- Name similarity (if both names exist)
            CASE 
                WHEN sd.s1_name IS NOT NULL AND sd.s2_name IS NOT NULL 
                THEN similarity(LOWER(sd.s1_name), LOWER(sd.s2_name))::DOUBLE PRECISION
                ELSE 0.0
            END as name_similarity,
            -- Embeddings for client-side similarity calculation
            sd.s1_embedding,
            sd.s2_embedding,
            -- Shared organization
            CASE 
                WHEN sd.s1_org_id IS NOT NULL AND sd.s1_org_id = sd.s2_org_id 
                THEN 1.0 
                ELSE 0.0 
            END::DOUBLE PRECISION as shared_organization,
            -- Shared program  
            CASE 
                WHEN sd.s1_program_id IS NOT NULL AND sd.s1_program_id = sd.s2_program_id 
                THEN 1.0 
                ELSE 0.0 
            END::DOUBLE PRECISION as shared_program,
            -- Taxonomy count (normalized)
            COALESCE((td.shared_taxonomy_count / 5.0), 0.0) as shared_taxonomy_normalized,
            -- Geographic proximity
            COALESCE(ld.geographic_proximity, 0.0) as geographic_proximity
        FROM service_data sd
        CROSS JOIN taxonomy_data td
        CROSS JOIN location_data ld
        "#,
        &[&service1_id, &service2_id]
    ).await.context(format!("Consolidated pairwise feature query failed for pair ({}, {})", service1_id, service2_id))?;
    
    // Extract values from the row
    let name_similarity: f64 = row.try_get("name_similarity").unwrap_or(0.0);
    let shared_organization: f64 = row.try_get("shared_organization").unwrap_or(0.0);
    let shared_program: f64 = row.try_get("shared_program").unwrap_or(0.0);
    let shared_taxonomy_normalized: f64 = row.try_get("shared_taxonomy_normalized").unwrap_or(0.0).min(1.0);
    let geographic_proximity: f64 = row.try_get("geographic_proximity").unwrap_or(0.0);
    
    // Calculate embedding similarities using Candle
    let mut description_similarity = 0.0;
    let mut embedding_similarity = 0.0;
    
    let s1_emb_opt: Option<PgVector> = row.try_get("s1_embedding").ok();
    let s2_emb_opt: Option<PgVector> = row.try_get("s2_embedding").ok();
    
    if let (Some(emb1), Some(emb2)) = (s1_emb_opt, s2_emb_opt) {
        let v1 = emb1.to_vec();
        let v2 = emb2.to_vec();
        
        if !v1.is_empty() && !v2.is_empty() && v1.len() == v2.len() {
            match cosine_similarity_candle(&v1, &v2) {
                Ok(sim) => {
                    description_similarity = sim;
                    embedding_similarity = sim; // Same as description for services
                }
                Err(e) => {
                    warn!("{} Failed to calculate embedding similarity: {}", pair_context, e);
                }
            }
        }
    }
    
    // Return features in the expected order
    let features = vec![
        name_similarity,
        description_similarity,
        embedding_similarity,
        shared_organization,
        shared_taxonomy_normalized,
        geographic_proximity,
        shared_program,
    ];
    
    debug!("{} Successfully extracted all {} pairwise features in single query", pair_context, features.len());
    Ok(features)
}

/// Stores the 12 individual service features into `clustering_metadata.service_context_features`.
async fn store_individual_service_features(
    conn: &PgConnection,
    service_id: &str,
    features: &[f64],
    all_feature_metadata: &[ServiceFeatureMetadata],
) -> Result<()> {
    if features.len() != 12 {
        warn!(
            "Attempting to store {} contextual features for service_id: {}, but expected 12. Skipping store.",
            features.len(),
            service_id
        );
        return Ok(());
    }

    let mut query = String::from(
        "INSERT INTO clustering_metadata.service_context_features (id, service_id, feature_name, feature_value, created_at) VALUES ",
    );
    let mut params_data: Vec<(String, String, String, f64, chrono::NaiveDateTime)> =
        Vec::with_capacity(12);
    let now = chrono::Utc::now().naive_utc();

    for i in 0..12 {
        let feature_meta = &all_feature_metadata[i];
        let feature_value = features[i];
        let feature_id_str = Uuid::new_v4().to_string();
        params_data.push((
            feature_id_str,
            service_id.to_string(),
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
    query.push_str(" ON CONFLICT (service_id, feature_name) DO UPDATE SET feature_value = EXCLUDED.feature_value, created_at = EXCLUDED.created_at");

    let mut sql_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
        Vec::with_capacity(12 * 5);
    for (id_val, service_id_val, name_val, value_val, created_at_val) in &params_data {
        sql_params.push(id_val);
        sql_params.push(service_id_val);
        sql_params.push(name_val);
        sql_params.push(value_val);
        sql_params.push(created_at_val);
    }

    match conn.execute(query.as_str(), &sql_params[..]).await {
        Ok(rows_affected) => {
            debug!(
                "Stored/updated 12 individual contextual features for service_id: {} ({} rows affected).",
                service_id, rows_affected
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                "Error storing individual contextual features for service_id: {}. Query: [{}], Error: {}",
                service_id, query, e
            );
            Err(anyhow::Error::from(e)
                .context("Storing individual service contextual features failed"))
        }
    }
}

// --- Basic Feature Calculation Functions ---
fn calculate_service_name_complexity_from_data(data: &RawServiceData) -> f64 {
    if let Some(name) = &data.service_name {
        let length = name.len() as f64;
        let word_count = name.split_whitespace().count() as f64;
        (length / 100.0).min(1.0) * 0.5 + (word_count / 10.0).min(1.0) * 0.5
    } else {
        0.0
    }
}

fn calculate_service_description_completeness_from_data(data: &RawServiceData) -> f64 {
    if let Some(desc) = &data.service_description {
        if desc.len() > 100 {
            1.0
        } else if desc.len() > 50 {
            0.7
        } else if desc.len() > 20 {
            0.4
        } else if desc.len() > 5 {
            0.2
        } else {
            0.0
        }
    } else {
        0.0
    }
}

fn calculate_has_email_from_data(data: &RawServiceData) -> f64 {
    if data.service_email.is_some() {
        1.0
    } else {
        0.0
    }
}

fn calculate_has_url_from_data(data: &RawServiceData) -> f64 {
    if data.service_url.is_some() {
        1.0
    } else {
        0.0
    }
}

fn calculate_has_phone_from_data(data: &RawServiceData) -> f64 {
    if data.has_phone_flag {
        1.0
    } else {
        0.0
    }
}

fn calculate_has_location_from_data(data: &RawServiceData) -> f64 {
    if data.has_location_flag {
        1.0
    } else {
        0.0
    }
}

fn calculate_taxonomy_count_from_data(data: &RawServiceData) -> f64 {
    (data.taxonomy_count_val as f64 / 5.0).min(1.0)
}

fn calculate_schedule_completeness_from_data(data: &RawServiceData) -> f64 {
    if data.schedule_count_val > 0 {
        1.0
    } else {
        0.0
    }
}

fn calculate_eligibility_completeness_from_data(data: &RawServiceData) -> f64 {
    if let Some(eligibility) = &data.eligibility_description {
        if eligibility.len() > 20 {
            1.0
        } else if eligibility.len() > 5 {
            0.5
        } else {
            0.1
        }
    } else {
        0.0
    }
}

// --- Enhanced Feature Extraction Functions ---
async fn extract_service_embedding_quality(
    conn: &PgConnection,
    service_id: &str,
) -> Result<f64> {
    let row_opt = conn.query_opt(
        "SELECT CASE 
            WHEN s.embedding_v2 IS NULL THEN 0.0 
            WHEN s.description IS NULL OR LENGTH(s.description) = 0 THEN 0.1 
            WHEN LENGTH(s.description) < 20 THEN 0.3 
            WHEN LENGTH(s.description) < 100 THEN 0.6 
            ELSE 0.9 
         END::DOUBLE PRECISION as embedding_quality 
         FROM public.service s 
         WHERE s.id = $1",
        &[&service_id]
    ).await.context(format!("DB query failed for service_embedding_quality for service {}", service_id))?;
    
    Ok(row_opt.map_or(0.0, |row| {
        get_guaranteed_f64_from_row(&row, 0, "service_embedding_quality", service_id)
    }))
}

async fn extract_service_embedding_distance_to_org(
    conn: &PgConnection,
    service_id: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!("Extracting service embedding distance to org for service {} using Candle", service_id);
    
    let row_opt = conn.query_opt(
        "SELECT s.embedding_v2 as service_emb, o.embedding as org_emb 
         FROM public.service s 
         JOIN public.organization o ON s.organization_id = o.id 
         WHERE s.id = $1 AND s.embedding_v2 IS NOT NULL AND o.embedding IS NOT NULL",
        &[&service_id]
    ).await.context("DB query for service and org embeddings failed")?;

    if let Some(row) = row_opt {
        let service_emb_pg: Option<PgVector> = row.try_get("service_emb").ok();
        let org_emb_pg: Option<PgVector> = row.try_get("org_emb").ok();
        if let (Some(s_emb), Some(o_emb)) = (service_emb_pg, org_emb_pg) {
            let service_vec_f32 = s_emb.to_vec();
            let org_vec_f32 = o_emb.to_vec();
            if service_vec_f32.is_empty() || org_vec_f32.is_empty() || service_vec_f32.len() != org_vec_f32.len() {
                warn!("Embedding issues for service {}. Max distance.", service_id);
                return Ok(2.0);
            }
            let similarity = cosine_similarity_candle(&service_vec_f32, &org_vec_f32)
                .map_err(|e| anyhow::anyhow!("Candle similarity failed: {}", e))?;
            Ok((1.0 - similarity).max(0.0).min(2.0))
        } else {
            Ok(2.0)
        }
    } else {
        Ok(2.0)
    }
}

async fn extract_related_program_exists(
    conn: &PgConnection,
    service_id: &str,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT CASE WHEN s.program_id IS NOT NULL THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION 
         FROM public.service s 
         WHERE s.id = $1",
        &[&service_id]
    ).await.context("DB query for related_program_exists failed")?;
    
    Ok(get_guaranteed_f64_from_row(&row, 0, "related_program_exists", service_id))
}

// --- Pairwise Feature Functions ---
async fn calculate_service_name_similarity(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
) -> Result<f64> {
    let row_opt = conn.query_opt(
        "SELECT similarity(LOWER(s1.name), LOWER(s2.name))::DOUBLE PRECISION as name_similarity 
         FROM public.service s1, public.service s2 
         WHERE s1.id = $1 AND s2.id = $2 AND s1.name IS NOT NULL AND s2.name IS NOT NULL",
        &[&service1_id, &service2_id]
    ).await.context("DB query for service_name_similarity failed")?;
    
    Ok(row_opt.map_or(0.0, |row| {
        get_guaranteed_f64_from_row(&row, 0, "service_name_similarity", &format!("{},{}", service1_id, service2_id))
    }))
}

async fn calculate_service_description_similarity(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!("Calculating service description similarity for pair ({}, {}) using Candle", service1_id, service2_id);
    
    let row_opt = conn.query_opt(
        "SELECT s1.embedding_v2 as emb1, s2.embedding_v2 as emb2 
         FROM public.service s1, public.service s2 
         WHERE s1.id = $1 AND s2.id = $2 AND s1.embedding_v2 IS NOT NULL AND s2.embedding_v2 IS NOT NULL",
        &[&service1_id, &service2_id]
    ).await.context("DB query for service embeddings failed")?;

    if let Some(row) = row_opt {
        let emb1_pg: Option<PgVector> = row.try_get("emb1").ok();
        let emb2_pg: Option<PgVector> = row.try_get("emb2").ok();
        if let (Some(e1), Some(e2)) = (emb1_pg, emb2_pg) {
            let v1_f32 = e1.to_vec();
            let v2_f32 = e2.to_vec();
            if v1_f32.is_empty() || v2_f32.is_empty() || v1_f32.len() != v2_f32.len() {
                warn!("Service embedding issues for pair ({}, {}). Sim 0.0.", service1_id, service2_id);
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

async fn calculate_service_embedding_similarity(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
    _candle_device: Device,
) -> Result<f64> {
    debug!("Calculating service embedding similarity for pair ({}, {}) using Candle", service1_id, service2_id);
    
    // This is the same as description similarity for services since we use embedding_v2
    calculate_service_description_similarity(conn, service1_id, service2_id, _candle_device).await
}

async fn check_shared_organization(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT CASE WHEN s1.organization_id = s2.organization_id THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION
         FROM public.service s1, public.service s2
         WHERE s1.id = $1 AND s2.id = $2 AND s1.organization_id IS NOT NULL",
        &[&service1_id, &service2_id]
    ).await.context("DB query for shared_organization failed")?;
    Ok(get_guaranteed_f64_from_row(&row, 0, "shared_organization", &format!("{},{}", service1_id, service2_id)))
}

async fn calculate_shared_taxonomy_count(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT COUNT(*)::DOUBLE PRECISION / 5.0 as shared_taxonomy_normalized
         FROM public.service_taxonomy st1 
         JOIN public.service_taxonomy st2 ON st1.taxonomy_term_id = st2.taxonomy_term_id
         WHERE st1.service_id = $1 AND st2.service_id = $2",
        &[&service1_id, &service2_id]
    ).await.context("DB query for shared_taxonomy_count failed")?;
    Ok(get_guaranteed_f64_from_row(&row, 0, "shared_taxonomy_count", &format!("{},{}", service1_id, service2_id)).min(1.0))
}

async fn calculate_service_geographic_proximity(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
) -> Result<f64> {
    let row = conn.query_one(
        "WITH service_locations AS (
            SELECT ST_Distance(l1.geom, l2.geom) as distance 
            FROM public.service_at_location sal1 
            JOIN public.location l1 ON sal1.location_id = l1.id,
                 public.service_at_location sal2 
            JOIN public.location l2 ON sal2.location_id = l2.id
            WHERE sal1.service_id = $1 AND sal2.service_id = $2 
              AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL
         ) 
         SELECT CASE 
            WHEN COUNT(distance) = 0 THEN 0.0 
            WHEN MIN(distance) > 10000 THEN 0.0 
            ELSE 1.0 - (MIN(distance) / 10000.0) 
         END::DOUBLE PRECISION FROM service_locations",
        &[&service1_id, &service2_id]
    ).await.context("DB query for service_geographic_proximity failed")?;
    Ok(get_guaranteed_f64_from_row(&row, 0, "service_geographic_proximity", &format!("{},{}", service1_id, service2_id)))
}

async fn check_shared_program(
    conn: &PgConnection,
    service1_id: &str,
    service2_id: &str,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT CASE 
            WHEN s1.program_id IS NOT NULL AND s1.program_id = s2.program_id THEN 1.0 
            ELSE 0.0 
         END::DOUBLE PRECISION
         FROM public.service s1, public.service s2
         WHERE s1.id = $1 AND s2.id = $2",
        &[&service1_id, &service2_id]
    ).await.context("DB query for shared_program failed")?;
    Ok(get_guaranteed_f64_from_row(&row, 0, "shared_program", &format!("{},{}", service1_id, service2_id)))
}

// --- Helper functions ---
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