// src/reinforcement/service_feature_extraction.rs

use anyhow::{Context, Result};
use futures::future;
use log::{debug, error, info, warn, Level as LogLevel};
use pgvector::Vector as PgVector;
use serde_json;
use tokio_postgres::{Client as PgConnection, GenericClient, Row as PgRow};
use uuid::Uuid;

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::db::PgPool;
use crate::models::ServiceId;
use crate::reinforcement::types::FeatureMetadata;
use crate::utils::{cosine_similarity_candle, extract_domain}; // Assuming extract_domain is in utils

use super::service_feature_cache_service::SharedServiceFeatureCache;

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

// Feature metadata for service matching
// (10 individual + 7 original pairwise + 2 new pairwise = 19 total pairwise if all are used,
// so 10 + 10 + 19 = 39 features in the full pair vector)
// For now, we will add the new pairwise features, and the calling code (orchestrator)
// will decide which ones to use for the RL model.
// The full vector sent to RL might be a subset of these 39.
// The `extract_context_for_service_pair` will now produce a 10 (s1) + 10 (s2) + 9 (pair) = 29 feature vector.
pub fn get_service_feature_metadata() -> Vec<FeatureMetadata> {
    vec![
        // --- Individual Service Features (Indices 0-9) ---
        FeatureMetadata {
            name: "s_name_length".to_string(),
            description: "Normalized length of the service name.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_desc_length".to_string(),
            description: "Normalized length of the service description.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_has_email".to_string(),
            description: "Boolean indicating if a service email exists.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_has_url".to_string(),
            description: "Boolean indicating if a service URL exists.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_status_active".to_string(),
            description: "Boolean indicating if service status is active.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_taxonomy_count".to_string(),
            description: "Normalized count of service taxonomies.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_location_count".to_string(),
            description: "Normalized count of service locations.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_embedding_v2_present".to_string(),
            description: "Boolean indicating if service has an embedding_v2.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_organization_name_length".to_string(),
            description: "Normalized length of the organization name associated with the service."
                .to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_organization_embedding_present".to_string(),
            description: "Boolean indicating if the service's organization has an embedding."
                .to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        // --- Pairwise Service Features (Indices 10-18 when combined for a 29-feature vector) ---
        FeatureMetadata {
            name: "pair_s_name_jaro_winkler".to_string(),
            description: "Jaro-Winkler similarity between two service names.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        }, // Index 10 (pairwise index 0)
        FeatureMetadata {
            name: "pair_s_embedding_v2_cosine_similarity".to_string(),
            description: "Cosine similarity between two service embeddings.".to_string(),
            min_value: -1.0, // Cosine similarity can be negative
            max_value: 1.0,
        }, // Index 11 (pairwise index 1)
        FeatureMetadata {
            name: "pair_s_taxonomy_jaccard".to_string(),
            description: "Jaccard similarity between service taxonomies.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        }, // Index 12 (pairwise index 2) - Original
        FeatureMetadata {
            name: "pair_s_location_jaccard".to_string(),
            description: "Jaccard similarity between service locations.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        }, // Index 13 (pairwise index 3) - Original
        FeatureMetadata {
            name: "pair_s_same_organization".to_string(),
            description: "Boolean indicating if services belong to the same organization."
                .to_string(),
            min_value: 0.0,
            max_value: 1.0,
        }, // Index 14 (pairwise index 4)
        FeatureMetadata {
            name: "pair_s_email_exact_match".to_string(),
            description: "Boolean indicating if services have the same email.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        }, // Index 15 (pairwise index 5)
        FeatureMetadata {
            name: "pair_s_url_domain_match".to_string(),
            description: "Boolean indicating if services share the same URL domain.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        }, // Index 16 (pairwise index 6)
           // NEW PAIRWISE FEATURES (as per III.A)
           // The original taxonomy_jaccard and location_jaccard are already present.
           // We'll ensure their calculation logic is robust.
           // The plan mentions "Service Name & Description : Utilize advanced text similarity".
           // Jaro-Winkler is already there for name. For description, a new feature could be added.
           // For now, let's ensure the existing ones are correctly placed and add placeholders if needed.
           // The vector will be 10 (s1) + 10 (s2) + 7 (original pairwise) = 27.
           // If we add more, this needs to be updated.
           // The document mentions "Service Taxonomy Overlap" and "Service Location Overlap"
           // which are covered by pair_s_taxonomy_jaccard and pair_s_location_jaccard.
           // Let's stick to the 7 original pairwise features for now to maintain the 27-feature vector length
           // and ensure the existing Jaccard calculations are robust.
           // If new *distinct* features are required, they would be added here and increase the vector size.
    ]
}

const INDIVIDUAL_FEATURE_COUNT: usize = 10;
const PAIRWISE_FEATURE_COUNT: usize = 7; // Number of pairwise features we calculate
const TOTAL_PAIR_FEATURES_IN_CONTEXT: usize = PAIRWISE_FEATURE_COUNT; // For clarity in context vector assembly
pub const SERVICE_CONTEXT_FEATURE_VECTOR_PAIR_SIZE: usize =
    INDIVIDUAL_FEATURE_COUNT * 2 + TOTAL_PAIR_FEATURES_IN_CONTEXT;

#[derive(Debug, Hash)] // Added Hash for signature calculation
pub struct RawServiceData {
    service_id: String, // Added for signature
    service_name: Option<String>,
    service_description: Option<String>,
    service_email: Option<String>,
    service_url: Option<String>,
    service_status: Option<String>,
    service_embedding_v2_present: bool,
    organization_id: Option<String>, // Added for signature and same_organization check
    organization_name: Option<String>,
    organization_embedding_present: bool,
    taxonomy_ids: Vec<String>, // Changed from count to actual IDs for Jaccard and signature
    location_ids: Vec<String>, // Changed from count to actual IDs for Jaccard and signature
    // Fields for signature that might not be direct features but define the service's state for comparison
    updated_at: Option<chrono::NaiveDateTime>, // From service table
}

/// Generates a signature for a service based on its relevant attributes.
/// This signature is used by the Service Comparison Cache.
fn calculate_service_signature(raw_data: &RawServiceData) -> String {
    let mut hasher = DefaultHasher::new();
    raw_data.hash(&mut hasher);
    hasher.finish().to_string()
}

/// Fetches raw data for a service and calculates its signature.
pub async fn get_service_raw_data_and_signature(
    conn: &PgConnection,
    service_id: &ServiceId,
) -> Result<(RawServiceData, String)> {
    let service_context = format!("Service {}", service_id.0);
    debug!("{} Fetching raw data for signature...", service_context);

    let query = "
        SELECT
            s.id as service_id,
            s.name AS service_name,
            s.description AS service_description,
            s.email AS service_email,
            s.url AS service_url,
            s.status AS service_status,
            s.embedding_v2 IS NOT NULL AS service_embedding_v2_present,
            s.organization_id,
            o.name AS organization_name,
            o.embedding IS NOT NULL AS organization_embedding_present,
            COALESCE(array_agg(DISTINCT st.taxonomy_term_id::TEXT) FILTER (WHERE st.taxonomy_term_id IS NOT NULL), '{}') AS taxonomy_ids,
            COALESCE(array_agg(DISTINCT sal.location_id::TEXT) FILTER (WHERE sal.location_id IS NOT NULL), '{}') AS location_ids,
            s.updated_at -- or last_modified if that's more relevant
        FROM public.service s
        LEFT JOIN public.organization o ON s.organization_id = o.id
        LEFT JOIN public.service_taxonomy st ON s.id = st.service_id
        LEFT JOIN public.service_at_location sal ON s.id = sal.service_id
        WHERE s.id = $1
        GROUP BY s.id, s.name, s.description, s.email, s.url, s.status, o.name, o.embedding, s.organization_id;
    ";

    let row = conn
        .query_one(query, &[&service_id.0])
        .await
        .map_err(anyhow::Error::from)
        .context(format!(
            "Consolidated query for service raw data failed for service {}",
            service_id.0
        ))?;

    let raw_data = RawServiceData {
        service_id: row.try_get("service_id").context("service_id missing")?,
        service_name: row.try_get("service_name").ok(),
        service_description: row.try_get("service_description").ok(),
        service_email: row.try_get("service_email").ok(),
        service_url: row.try_get("service_url").ok(),
        service_status: row.try_get("service_status").ok(),
        service_embedding_v2_present: row.try_get("service_embedding_v2_present").unwrap_or(false),
        organization_id: row.try_get("organization_id").ok(),
        organization_name: row.try_get("organization_name").ok(),
        organization_embedding_present: row
            .try_get("organization_embedding_present")
            .unwrap_or(false),
        taxonomy_ids: row.try_get("taxonomy_ids").unwrap_or_else(|_| Vec::new()),
        location_ids: row.try_get("location_ids").unwrap_or_else(|_| Vec::new()),
        updated_at: row.try_get("updated_at").ok(),
    };

    let signature = calculate_service_signature(&raw_data);
    debug!("{} Calculated signature: {}", service_context, signature);
    Ok((raw_data, signature))
}

/// Extracts the individual features for a single service.
/// If features are already stored, they are retrieved; otherwise, they are calculated and stored.
pub async fn get_stored_service_features(
    conn: &PgConnection,
    service_id: &ServiceId,
) -> Result<Vec<f64>> {
    let service_context = format!("Service {}", service_id.0);
    debug!("{} Checking for stored features...", service_context);

    let rows = conn
        .query(
            "SELECT feature_name, feature_value
             FROM clustering_metadata.service_context_features
             WHERE service_id = $1
             ORDER BY feature_name", // Ensure consistent order for reconstruction
            &[&service_id.0.to_string()], // Ensure it's passed as text if DB expects text
        )
        .await
        .context(format!(
            "Failed to query stored features for service {}",
            service_id.0
        ))?;

    let metadata = get_service_feature_metadata(); // Full metadata

    if !rows.is_empty() {
        debug!(
            "{} Found {} stored feature rows, reconstructing vector...",
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

        // Iterate through the *individual* feature metadata names
        for i in 0..INDIVIDUAL_FEATURE_COUNT {
            if let Some(value) = feature_map.get(&metadata[i].name) {
                features_vec.push(*value);
            } else {
                warn!(
                    "{}: Missing stored feature '{}', will need to re-extract all.",
                    service_context, metadata[i].name
                );
                all_present = false;
                break;
            }
        }

        if all_present && features_vec.len() == INDIVIDUAL_FEATURE_COUNT {
            debug!(
                "{} Successfully reconstructed all {} stored individual features.",
                service_context, INDIVIDUAL_FEATURE_COUNT
            );
            return Ok(features_vec);
        } else {
            warn!(
                "{}: Stored features incomplete (found {}, expected {}) or mismatched. Re-extracting all.",
                service_context, features_vec.len(), INDIVIDUAL_FEATURE_COUNT
            );
        }
    } else {
        info!(
            "{}: No stored features found, extracting now.",
            service_context
        );
    }

    // If not all features were present or rows were empty, extract and store them.
    // We need the raw data for this.
    let (raw_data, _signature) = get_service_raw_data_and_signature(conn, service_id)
        .await
        .context(format!(
            "Failed to get raw_data for feature extraction for service {}",
            service_id.0
        ))?;

    extract_and_store_service_features(conn, service_id, &raw_data, &metadata).await
}

/// Extracts, stores, and returns the individual features for a single service using pre-fetched RawServiceData.
pub async fn extract_and_store_service_features(
    conn: &PgConnection,
    service_id: &ServiceId,
    raw_data: &RawServiceData, // Takes RawServiceData as input
    all_feature_metadata: &[FeatureMetadata],
) -> Result<Vec<f64>> {
    let service_context = format!("Service {}", service_id.0);

    info!(
        "{} Calculating {} individual service features from raw data...",
        service_context, INDIVIDUAL_FEATURE_COUNT
    );

    let features_vec = vec![
        calculate_name_length_from_data(&raw_data),    // feature 0
        calculate_desc_length_from_data(&raw_data),    // feature 1
        calculate_has_email_from_data(&raw_data),      // feature 2
        calculate_has_url_from_data(&raw_data),        // feature 3
        calculate_status_active_from_data(&raw_data),  // feature 4
        calculate_taxonomy_count_from_data(&raw_data), // feature 5
        calculate_location_count_from_data(&raw_data), // feature 6
        calculate_embedding_v2_present_from_data(&raw_data), // feature 7
        calculate_organization_name_length_from_data(&raw_data), // feature 8
        calculate_organization_embedding_present_from_data(&raw_data), // feature 9
    ];

    info!(
        "{} Completed calculation of {} service features.",
        service_context,
        features_vec.len()
    );

    debug!(
        "{} Storing {} extracted individual features...",
        service_context,
        features_vec.len()
    );

    store_individual_service_features(conn, service_id, &features_vec, all_feature_metadata)
        .await?;

    debug!(
        "{} Successfully stored individual features.",
        service_context
    );

    Ok(features_vec)
}

/// Extracts the full feature vector for a pair of services.
/// The vector will have 10 (s1) + 10 (s2) + 7 (pair) = 27 features.
pub async fn extract_context_for_service_pair(
    pool: &PgPool,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<Vec<f64>> {
    let pair_context = format!("Service Pair ({}, {})", service1_id.0, service2_id.0);

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
        "{} Starting context extraction ({} features)...",
        pair_context, SERVICE_CONTEXT_FEATURE_VECTOR_PAIR_SIZE
    );
    let all_feature_metadata = get_service_feature_metadata();

    let context_steps_completed = Arc::new(AtomicUsize::new(0));
    let total_context_steps = 3;

    // --- Get Raw Data for both services (needed for some pairwise features too) ---
    let (raw_data1_res, raw_data2_res) = tokio::join!(
        get_service_raw_data_and_signature(conn, service1_id),
        get_service_raw_data_and_signature(conn, service2_id)
    );

    let (raw_data1, _sig1) = raw_data1_res.context(format!(
        "Failed to get raw_data for service1 ({})",
        service1_id.0
    ))?;
    let (raw_data2, _sig2) = raw_data2_res.context(format!(
        "Failed to get raw_data for service2 ({})",
        service2_id.0
    ))?;

    // --- TASK 1: Get Service 1 Features ---
    let service1_features_task = wrap_with_progress(
        get_stored_service_features(conn, service1_id), // This will use its internal raw data fetch if needed or can be adapted
        context_steps_completed.clone(),
        total_context_steps,
        format!("Features for service1 ({})", service1_id.0),
        pair_context.clone(),
        LogLevel::Info,
    );

    // --- TASK 2: Get Service 2 Features ---
    let service2_features_task = wrap_with_progress(
        get_stored_service_features(conn, service2_id),
        context_steps_completed.clone(),
        total_context_steps,
        format!("Features for service2 ({})", service2_id.0),
        pair_context.clone(),
        LogLevel::Info,
    );

    // --- TASK 3: Calculate Pairwise Features ---
    let pair_context_for_inner = pair_context.clone();
    // Clone raw_data for the async block
    let rd1_clone = Arc::new(raw_data1); // Use Arc for shared ownership if raw_data is large, or clone if small
    let rd2_clone = Arc::new(raw_data2);

    let pair_features_calculation_task_inner = async move {
        let pair_calc_tasks_completed_inner = Arc::new(AtomicUsize::new(0));
        const PAIRWISE_METADATA_OFFSET: usize = INDIVIDUAL_FEATURE_COUNT; // Pairwise features start after individual ones in metadata
                                                                          // NUM_PAIRWISE_FEATURES is defined globally

        let mut feature_outcomes = HashMap::with_capacity(PAIRWISE_FEATURE_COUNT);

        // Pass references to raw_data1 and raw_data2 into the async block
        let raw_data1_ref = rd1_clone.as_ref();
        let raw_data2_ref = rd2_clone.as_ref();

        let feature_tasks: Vec<(
            String,
            Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>>,
        )> = vec![
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET].name.clone(), // pair_s_name_jaro_winkler
                Box::pin(wrap_with_progress(
                    calculate_name_jaro_winkler_from_data(raw_data1_ref, raw_data2_ref),
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET].name.clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 1]
                    .name
                    .clone(), // pair_s_embedding_v2_cosine_similarity
                Box::pin(wrap_with_progress(
                    calculate_embedding_cosine_similarity(conn, service1_id, service2_id), // Still needs DB for embeddings
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 1]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 2]
                    .name
                    .clone(), // pair_s_taxonomy_jaccard
                Box::pin(wrap_with_progress(
                    calculate_taxonomy_jaccard_from_data(raw_data1_ref, raw_data2_ref),
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 2]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 3]
                    .name
                    .clone(), // pair_s_location_jaccard
                Box::pin(wrap_with_progress(
                    calculate_location_jaccard_from_data(raw_data1_ref, raw_data2_ref),
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 3]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 4]
                    .name
                    .clone(), // pair_s_same_organization
                Box::pin(wrap_with_progress(
                    check_same_organization_from_data(raw_data1_ref, raw_data2_ref),
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 4]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 5]
                    .name
                    .clone(), // pair_s_email_exact_match
                Box::pin(wrap_with_progress(
                    check_email_exact_match_from_data(raw_data1_ref, raw_data2_ref),
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 5]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 6]
                    .name
                    .clone(), // pair_s_url_domain_match
                Box::pin(wrap_with_progress(
                    check_url_domain_match_from_data(raw_data1_ref, raw_data2_ref),
                    pair_calc_tasks_completed_inner.clone(),
                    PAIRWISE_FEATURE_COUNT,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 6]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
        ];

        let mut feature_results = Vec::with_capacity(PAIRWISE_FEATURE_COUNT);
        let mut success_count = 0;
        let mut failure_count = 0;

        for (feature_name, task) in feature_tasks {
            match task.await {
                Ok(value) => {
                    feature_results.push(value);
                    feature_outcomes.insert(feature_name.clone(), "success".to_string());
                    success_count += 1;
                }
                Err(e) => {
                    warn!(
                        "{} Failed to extract feature '{}': {}",
                        pair_context_for_inner, feature_name, e
                    );
                    feature_outcomes.insert(feature_name.clone(), format!("failed: {}", e));
                    feature_results.push(0.0); // Default value
                    failure_count += 1;
                }
            }
        }

        if failure_count > 0 {
            warn!(
                "{} Completed pairwise feature extraction with {} successes and {} failures: {:?}",
                pair_context_for_inner, success_count, failure_count, feature_outcomes
            );
            if failure_count > PAIRWISE_FEATURE_COUNT / 2 {
                // If more than half failed
                return Err(anyhow::anyhow!(
                    "Too many pairwise feature failures ({}/{}): {:?}",
                    failure_count,
                    PAIRWISE_FEATURE_COUNT,
                    feature_outcomes
                ));
            }
        } else {
            info!(
                "{} Successfully extracted all {} pairwise features",
                pair_context_for_inner, PAIRWISE_FEATURE_COUNT
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

    let (service1_features_res, service2_features_res, pair_features_res) = tokio::join!(
        service1_features_task,
        service2_features_task,
        pair_features_task_logged
    );

    let service1_features = service1_features_res.context(format!(
        "Feature extraction failed for service1 ({})",
        service1_id.0
    ))?;
    let service2_features = service2_features_res.context(format!(
        "Feature extraction failed for service2 ({})",
        service2_id.0
    ))?;
    let pair_features = pair_features_res.context(format!(
        "Pairwise feature calculation failed for pair ({}, {})",
        service1_id.0, service2_id.0
    ))?;

    let mut final_context_vector = Vec::with_capacity(SERVICE_CONTEXT_FEATURE_VECTOR_PAIR_SIZE);
    final_context_vector.extend(service1_features.clone());
    final_context_vector.extend(service2_features.clone());
    final_context_vector.extend(pair_features.clone());

    if final_context_vector.len() != SERVICE_CONTEXT_FEATURE_VECTOR_PAIR_SIZE {
        warn!(
            "{} Final context vector length is {}, expected {}. Service1: {}, Service2: {}, Pairwise: {}",
            pair_context,
            final_context_vector.len(),
            SERVICE_CONTEXT_FEATURE_VECTOR_PAIR_SIZE,
            service1_features.len(),
            service2_features.len(),
            pair_features.len()
        );
        // Potentially return error if length mismatch is critical
    }

    info!(
        "{} Successfully extracted complete context vector (length: {})",
        pair_context,
        final_context_vector.len()
    );

    Ok(final_context_vector)
}

async fn store_individual_service_features(
    conn: &PgConnection,
    service_id: &ServiceId,
    features: &[f64],
    all_feature_metadata: &[FeatureMetadata],
) -> Result<()> {
    if features.len() != INDIVIDUAL_FEATURE_COUNT {
        warn!(
            "Attempting to store {} features for service_id: {}, but expected {}. Skipping store.",
            features.len(),
            service_id.0,
            INDIVIDUAL_FEATURE_COUNT
        );
        return Ok(());
    }

    let mut query = String::from(
        "INSERT INTO clustering_metadata.service_context_features (id, service_id, feature_name, feature_value, created_at) VALUES ",
    );
    let mut params_data: Vec<(String, String, String, f64, chrono::NaiveDateTime)> =
        Vec::with_capacity(INDIVIDUAL_FEATURE_COUNT);
    let now = chrono::Utc::now().naive_utc();

    for i in 0..INDIVIDUAL_FEATURE_COUNT {
        let feature_meta = &all_feature_metadata[i];
        let feature_value = features[i];
        let feature_id_str = Uuid::new_v4().to_string();
        params_data.push((
            feature_id_str,
            service_id.0.clone(),
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
        Vec::with_capacity(INDIVIDUAL_FEATURE_COUNT * 5);
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
                "Stored/updated {} individual features for service_id: {} ({} rows affected).",
                INDIVIDUAL_FEATURE_COUNT, service_id.0, rows_affected
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                "Error storing individual features for service_id: {}. Error: {}",
                service_id.0, e
            );
            Err(anyhow::Error::from(e).context("Storing individual service features failed"))
        }
    }
}

// --- Individual Feature Calculation Helpers (using RawServiceData) ---
fn calculate_name_length_from_data(data: &RawServiceData) -> f64 {
    data.service_name
        .as_ref()
        .map_or(0.0, |name| (name.len() as f64 / 100.0).min(1.0))
}
fn calculate_desc_length_from_data(data: &RawServiceData) -> f64 {
    data.service_description
        .as_ref()
        .map_or(0.0, |desc| (desc.len() as f64 / 1000.0).min(1.0))
}
fn calculate_has_email_from_data(data: &RawServiceData) -> f64 {
    if data.service_email.as_ref().map_or(false, |e| !e.is_empty()) {
        1.0
    } else {
        0.0
    }
}
fn calculate_has_url_from_data(data: &RawServiceData) -> f64 {
    if data.service_url.as_ref().map_or(false, |u| !u.is_empty()) {
        1.0
    } else {
        0.0
    }
}
fn calculate_status_active_from_data(data: &RawServiceData) -> f64 {
    if data
        .service_status
        .as_ref()
        .map_or(false, |s| s.to_lowercase() == "active")
    {
        1.0
    } else {
        0.0
    }
}
fn calculate_taxonomy_count_from_data(data: &RawServiceData) -> f64 {
    // Uses actual IDs now
    (data.taxonomy_ids.len() as f64 / 20.0).min(1.0)
}
fn calculate_location_count_from_data(data: &RawServiceData) -> f64 {
    // Uses actual IDs now
    (data.location_ids.len() as f64 / 10.0).min(1.0)
}
fn calculate_embedding_v2_present_from_data(data: &RawServiceData) -> f64 {
    if data.service_embedding_v2_present {
        1.0
    } else {
        0.0
    }
}
fn calculate_organization_name_length_from_data(data: &RawServiceData) -> f64 {
    data.organization_name
        .as_ref()
        .map_or(0.0, |name| (name.len() as f64 / 100.0).min(1.0))
}
fn calculate_organization_embedding_present_from_data(data: &RawServiceData) -> f64 {
    if data.organization_embedding_present {
        1.0
    } else {
        0.0
    }
}

// --- Pairwise Feature Calculation Functions (some using RawServiceData) ---

// Jaro-Winkler from RawServiceData
async fn calculate_name_jaro_winkler_from_data(
    data1: &RawServiceData,
    data2: &RawServiceData,
) -> Result<f64> {
    match (&data1.service_name, &data2.service_name) {
        (Some(n1), Some(n2)) => Ok(strsim::jaro_winkler(&n1.to_lowercase(), &n2.to_lowercase())),
        _ => Ok(0.0), // Or handle error if names are expected
    }
}

// Embedding cosine similarity still needs DB access for embeddings
async fn calculate_embedding_cosine_similarity(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row_opt = conn.query_opt(
        "SELECT s1.embedding_v2 as emb1, s2.embedding_v2 as emb2
         FROM public.service s1, public.service s2
         WHERE s1.id = $1 AND s2.id = $2 AND s1.embedding_v2 IS NOT NULL AND s2.embedding_v2 IS NOT NULL",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for embeddings failed")?;

    if let Some(row) = row_opt {
        let emb1_pg: Option<PgVector> = row.try_get("emb1").ok();
        let emb2_pg: Option<PgVector> = row.try_get("emb2").ok();

        if let (Some(e1), Some(e2)) = (emb1_pg, emb2_pg) {
            let v1_f32 = e1.to_vec();
            let v2_f32 = e2.to_vec();
            if v1_f32.is_empty() || v2_f32.is_empty() {
                return Ok(0.0);
            }
            if v1_f32.len() != v2_f32.len() {
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

fn jaccard_similarity_str(set1_ids: &[String], set2_ids: &[String]) -> f64 {
    let set1: HashSet<_> = set1_ids.iter().collect();
    let set2: HashSet<_> = set2_ids.iter().collect();

    let intersection = set1.intersection(&set2).count();
    let union = set1.union(&set2).count();

    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

async fn calculate_taxonomy_jaccard_from_data(
    data1: &RawServiceData,
    data2: &RawServiceData,
) -> Result<f64> {
    Ok(jaccard_similarity_str(
        &data1.taxonomy_ids,
        &data2.taxonomy_ids,
    ))
}

async fn calculate_location_jaccard_from_data(
    data1: &RawServiceData,
    data2: &RawServiceData,
) -> Result<f64> {
    Ok(jaccard_similarity_str(
        &data1.location_ids,
        &data2.location_ids,
    ))
}

async fn check_same_organization_from_data(
    data1: &RawServiceData,
    data2: &RawServiceData,
) -> Result<f64> {
    match (&data1.organization_id, &data2.organization_id) {
        (Some(org1_id), Some(org2_id)) if org1_id == org2_id => Ok(1.0),
        _ => Ok(0.0),
    }
}

async fn check_email_exact_match_from_data(
    data1: &RawServiceData,
    data2: &RawServiceData,
) -> Result<f64> {
    match (&data1.service_email, &data2.service_email) {
        (Some(e1), Some(e2)) if !e1.is_empty() && e1.to_lowercase() == e2.to_lowercase() => Ok(1.0),
        _ => Ok(0.0),
    }
}

async fn check_url_domain_match_from_data(
    data1: &RawServiceData,
    data2: &RawServiceData,
) -> Result<f64> {
    let domain1 = data1.service_url.as_ref().and_then(|u| extract_domain(u));
    let domain2 = data2.service_url.as_ref().and_then(|u| extract_domain(u));
    match (domain1, domain2) {
        (Some(d1), Some(d2)) if !d1.is_empty() && d1 == d2 => Ok(1.0),
        _ => Ok(0.0),
    }
}
