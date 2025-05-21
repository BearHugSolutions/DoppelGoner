// src/reinforcement/service_feature_extraction.rs

use anyhow::{Context, Result};
use futures::future;
use log::{debug, error, info, warn, Level as LogLevel};
use pgvector::Vector as PgVector;
use tokio_postgres::{Client as PgConnection, GenericClient, Row as PgRow};
use uuid::Uuid;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::db::PgPool;
use crate::models::ServiceId;
use crate::reinforcement::types::FeatureMetadata;
use crate::utils::cosine_similarity_candle;

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

// Feature metadata for service matching (10 individual + 7 pairwise features)
pub fn get_service_feature_metadata() -> Vec<FeatureMetadata> {
    vec![
        // --- Individual Service Features (Indices 0-9) ---
        FeatureMetadata {
            name: "s_name_length".to_string(),
            description: "Length of the service name.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_desc_length".to_string(),
            description: "Length of the service description.".to_string(),
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
            description: "Length of the organization name associated with the service.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "s_organization_embedding_present".to_string(),
            description: "Boolean indicating if the service's organization has an embedding.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        // --- Pairwise Service Features (Indices 10-16 when combined) ---
        FeatureMetadata {
            name: "pair_s_name_jaro_winkler".to_string(),
            description: "Jaro-Winkler similarity between two service names.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "pair_s_embedding_v2_cosine_similarity".to_string(),
            description: "Cosine similarity between two service embeddings.".to_string(),
            min_value: -1.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "pair_s_taxonomy_jaccard".to_string(),
            description: "Jaccard similarity between service taxonomies.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "pair_s_location_jaccard".to_string(),
            description: "Jaccard similarity between service locations.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "pair_s_same_organization".to_string(),
            description: "Boolean indicating if services belong to the same organization.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "pair_s_email_exact_match".to_string(),
            description: "Boolean indicating if services have the same email.".to_string(),
            min_value: 0.0,
            max_value: 1.0,
        },
        FeatureMetadata {
            name: "pair_s_url_domain_match".to_string(),
            description: "Boolean indicating if services share the same URL domain.".to_string(),
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
    service_status: Option<String>,
    service_embedding_v2_present: bool,
    organization_name: Option<String>,
    organization_embedding_present: bool,
    taxonomy_count: i64,
    location_count: i64,
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
             ORDER BY feature_name",
            &[&service_id.0],
        )
        .await
        .context(format!(
            "Failed to query stored features for service {}",
            service_id.0
        ))?;

    let metadata = get_service_feature_metadata();
    const INDIVIDUAL_FEATURE_COUNT: usize = 10; // First 10 are individual service features

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
                "{} Successfully reconstructed all {} stored features.",
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
    extract_and_store_service_features(conn, service_id, &metadata).await
}

/// Extracts, stores, and returns the individual features for a single service.
pub async fn extract_and_store_service_features(
    conn: &PgConnection,
    service_id: &ServiceId,
    all_feature_metadata: &[FeatureMetadata],
) -> Result<Vec<f64>> {
    let service_context = format!("Service {}", service_id.0);
    const INDIVIDUAL_FEATURE_COUNT: usize = 10;

    info!(
        "{} Extracting service features (consolidated query)...",
        service_context
    );
    
    let basic_features_tasks_completed = Arc::new(AtomicUsize::new(0));
    let total_basic_feature_tasks = 1;

    let consolidated_query_future = async {
        let query = "
            SELECT
                s.name AS service_name, 
                s.description AS service_description, 
                s.email AS service_email,
                s.url AS service_url, 
                s.status AS service_status,
                s.embedding_v2 IS NOT NULL AS service_embedding_v2_present,
                o.name AS organization_name,
                o.embedding IS NOT NULL AS organization_embedding_present,
                (SELECT COUNT(*) FROM public.service_taxonomy st WHERE st.service_id = s.id) AS taxonomy_count,
                (SELECT COUNT(*) FROM public.service_at_location sal WHERE sal.service_id = s.id) AS location_count
            FROM public.service s
            LEFT JOIN public.organization o ON s.organization_id = o.id
            WHERE s.id = $1";

        conn.query_one(query, &[&service_id.0])
            .await
            .map_err(anyhow::Error::from)
            .context(format!(
                "Consolidated query for service features failed for service {}",
                service_id.0
            ))
    };

    let row = wrap_with_progress(
        consolidated_query_future,
        basic_features_tasks_completed.clone(),
        total_basic_feature_tasks,
        "Consolidated Service Features Query".to_string(),
        service_context.clone(),
        LogLevel::Info,
    )
    .await?;

    let raw_data = RawServiceData {
        service_name: row.try_get("service_name").ok(),
        service_description: row.try_get("service_description").ok(),
        service_email: row.try_get("service_email").ok(),
        service_url: row.try_get("service_url").ok(),
        service_status: row.try_get("service_status").ok(),
        service_embedding_v2_present: row.try_get("service_embedding_v2_present").unwrap_or(false),
        organization_name: row.try_get("organization_name").ok(),
        organization_embedding_present: row.try_get("organization_embedding_present").unwrap_or(false),
        taxonomy_count: row.try_get("taxonomy_count").unwrap_or(0),
        location_count: row.try_get("location_count").unwrap_or(0),
    };

    let features_vec = vec![
        calculate_name_length_from_data(&raw_data),                    // feature 0
        calculate_desc_length_from_data(&raw_data),                    // feature 1
        calculate_has_email_from_data(&raw_data),                      // feature 2
        calculate_has_url_from_data(&raw_data),                        // feature 3
        calculate_status_active_from_data(&raw_data),                  // feature 4
        calculate_taxonomy_count_from_data(&raw_data),                 // feature 5
        calculate_location_count_from_data(&raw_data),                 // feature 6
        calculate_embedding_v2_present_from_data(&raw_data),           // feature 7
        calculate_organization_name_length_from_data(&raw_data),       // feature 8
        calculate_organization_embedding_present_from_data(&raw_data), // feature 9
    ];
    
    info!(
        "{} Completed calculation of {} service features.",
        service_context,
        features_vec.len()
    );

    // Store these features
    debug!(
        "{} Storing {} extracted individual features...",
        service_context,
        features_vec.len()
    );
    
    store_individual_service_features(conn, service_id, &features_vec, all_feature_metadata).await?;
    
    debug!(
        "{} Successfully stored individual features.",
        service_context
    );

    Ok(features_vec)
}

/// Extracts the full 27-element feature vector for a pair of services.
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
        "{} Starting context extraction (27 features)...",
        pair_context
    );
    debug!("{} Getting feature metadata...", pair_context);
    let all_feature_metadata = get_service_feature_metadata();

    // Setup progress tracking
    let context_steps_completed = Arc::new(AtomicUsize::new(0));
    let total_context_steps = 3; // Service1 features, Service2 features, Pairwise features

    info!(
        "{} Extracting individual features for both services...",
        pair_context
    );

    // --- TASK 1: Get Service 1 Features ---
    debug!(
        "{} Starting extraction for service1 ({})",
        pair_context, service1_id.0
    );
    let service1_features_task = wrap_with_progress(
        get_stored_service_features(conn, service1_id),
        context_steps_completed.clone(),
        total_context_steps,
        format!("Features for service1 ({})", service1_id.0),
        pair_context.clone(),
        LogLevel::Info,
    );

    // --- TASK 2: Get Service 2 Features ---
    debug!(
        "{} Starting extraction for service2 ({})",
        pair_context, service2_id.0
    );
    let service2_features_task = wrap_with_progress(
        get_stored_service_features(conn, service2_id),
        context_steps_completed.clone(),
        total_context_steps,
        format!("Features for service2 ({})", service2_id.0),
        pair_context.clone(),
        LogLevel::Info,
    );

    // --- TASK 3: Calculate Pairwise Features ---
    debug!("{} Starting pairwise feature calculations...", pair_context);
    let pair_context_for_inner = pair_context.clone();

    let pair_features_calculation_task_inner = async move {
        let pair_calc_tasks_completed_inner = Arc::new(AtomicUsize::new(0));
        const PAIRWISE_METADATA_OFFSET: usize = 10;
        const NUM_PAIRWISE_FEATURES: usize = 7;

        // Individual feature tracking map for better error reporting
        let mut feature_outcomes = HashMap::with_capacity(NUM_PAIRWISE_FEATURES);

        let feature_tasks: Vec<(
            String,
            Pin<Box<dyn Future<Output = Result<f64, anyhow::Error>> + Send + '_>>,
        )> = vec![
            // Name Jaro-Winkler similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET].name.clone(),
                Box::pin(wrap_with_progress(
                    // Added Box::pin()
                    calculate_name_jaro_winkler(conn, service1_id, service2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET].name.clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Embedding cosine similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 1]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    calculate_embedding_cosine_similarity(conn, service1_id, service2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 1]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Taxonomy Jaccard similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 2]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    calculate_taxonomy_jaccard(conn, service1_id, service2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 2]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Location Jaccard similarity
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 3]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    calculate_location_jaccard(conn, service1_id, service2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 3]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Same organization
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 4]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    check_same_organization(conn, service1_id, service2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 4]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // Email exact match
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 5]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    check_email_exact_match(conn, service1_id, service2_id),
                    pair_calc_tasks_completed_inner.clone(),
                    NUM_PAIRWISE_FEATURES,
                    all_feature_metadata[PAIRWISE_METADATA_OFFSET + 5]
                        .name
                        .clone(),
                    pair_context_for_inner.clone(),
                    LogLevel::Debug,
                )),
            ),
            // URL domain match
            (
                all_feature_metadata[PAIRWISE_METADATA_OFFSET + 6]
                    .name
                    .clone(),
                Box::pin(wrap_with_progress(
                    check_url_domain_match(conn, service1_id, service2_id),
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
    let (service1_features_res, service2_features_res, pair_features_res) = tokio::join!(
        service1_features_task,
        service2_features_task,
        pair_features_task_logged
    );

    // --- PROCESS RESULTS WITH DETAILED ERROR HANDLING ---

    // Service 1 features
    let service1_features = match service1_features_res {
        Ok(features) => {
            debug!(
                "{} Successfully extracted {} features for service1",
                pair_context,
                features.len()
            );
            features
        }
        Err(e) => {
            error!(
                "{} Failed to extract features for service1: {}",
                pair_context, e
            );
            return Err(anyhow::anyhow!(
                "Feature extraction failed for service1 ({}): {}",
                service1_id.0,
                e
            ));
        }
    };

    // Service 2 features
    let service2_features = match service2_features_res {
        Ok(features) => {
            debug!(
                "{} Successfully extracted {} features for service2",
                pair_context,
                features.len()
            );
            features
        }
        Err(e) => {
            error!(
                "{} Failed to extract features for service2: {}",
                pair_context, e
            );
            return Err(anyhow::anyhow!(
                "Feature extraction failed for service2 ({}): {}",
                service2_id.0,
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
                service1_id.0,
                service2_id.0,
                e
            ));
        }
    };

    // Assemble final context vector
    let mut final_context_vector = Vec::with_capacity(10 + 10 + 7);
    final_context_vector.extend(service1_features.clone());
    final_context_vector.extend(service2_features.clone());
    final_context_vector.extend(pair_features.clone());

    // Validate final vector
    if final_context_vector.len() != 27 {
        warn!(
            "{} Final context vector length is {}, expected 27. Service1: {}, Service2: {}, Pairwise: {}",
            pair_context,
            final_context_vector.len(),
            service1_features.len(),
            service2_features.len(),
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

// Store individual service features
async fn store_individual_service_features(
    conn: &PgConnection,
    service_id: &ServiceId,
    features: &[f64],                         // Should be the 10 features
    all_feature_metadata: &[FeatureMetadata], // Used for names
) -> Result<()> {
    if features.len() != 10 {
        warn!(
            "Attempting to store {} features for service_id: {}, but expected 10. Skipping store.",
            features.len(),
            service_id.0
        );
        return Ok(()); // Or return an error
    }

    let mut query = String::from(
        "INSERT INTO clustering_metadata.service_context_features (id, service_id, feature_name, feature_value, created_at) VALUES ",
    );
    let mut params_data: Vec<(String, String, String, f64, chrono::NaiveDateTime)> =
        Vec::with_capacity(10);
    let now = chrono::Utc::now().naive_utc();

    for i in 0..10 {
        // Iterate only for the first 10 individual features
        let feature_meta = &all_feature_metadata[i]; // Safe: metadata has at least 10 elements
        let feature_value = features[i]; // Safe: features has 10 elements
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
        Vec::with_capacity(10 * 5);
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
                "Stored/updated 10 individual features for service_id: {} ({} rows affected).",
                service_id.0, rows_affected
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

// --- Individual Feature Calculation Helpers ---
fn calculate_name_length_from_data(data: &RawServiceData) -> f64 {
    if let Some(name) = &data.service_name {
        let length = name.len();
        // Normalize to [0, 1] range, capping at 100 chars
        (length as f64 / 100.0).min(1.0)
    } else {
        0.0
    }
}

fn calculate_desc_length_from_data(data: &RawServiceData) -> f64 {
    if let Some(desc) = &data.service_description {
        let length = desc.len();
        // Normalize to [0, 1] range, capping at 1000 chars
        (length as f64 / 1000.0).min(1.0)
    } else {
        0.0
    }
}

fn calculate_has_email_from_data(data: &RawServiceData) -> f64 {
    if data.service_email.is_some() && !data.service_email.as_ref().unwrap().is_empty() {
        1.0
    } else {
        0.0
    }
}

fn calculate_has_url_from_data(data: &RawServiceData) -> f64 {
    if data.service_url.is_some() && !data.service_url.as_ref().unwrap().is_empty() {
        1.0
    } else {
        0.0
    }
}

fn calculate_status_active_from_data(data: &RawServiceData) -> f64 {
    if let Some(status) = &data.service_status {
        if status.to_lowercase() == "active" {
            1.0
        } else {
            0.0
        }
    } else {
        0.0
    }
}

fn calculate_taxonomy_count_from_data(data: &RawServiceData) -> f64 {
    // Normalize to [0, 1] range, capping at 20 taxonomies
    (data.taxonomy_count as f64 / 20.0).min(1.0)
}

fn calculate_location_count_from_data(data: &RawServiceData) -> f64 {
    // Normalize to [0, 1] range, capping at 10 locations
    (data.location_count as f64 / 10.0).min(1.0)
}

fn calculate_embedding_v2_present_from_data(data: &RawServiceData) -> f64 {
    if data.service_embedding_v2_present {
        1.0
    } else {
        0.0
    }
}

fn calculate_organization_name_length_from_data(data: &RawServiceData) -> f64 {
    if let Some(name) = &data.organization_name {
        let length = name.len();
        // Normalize to [0, 1] range, capping at 100 chars
        (length as f64 / 100.0).min(1.0)
    } else {
        0.0
    }
}

fn calculate_organization_embedding_present_from_data(data: &RawServiceData) -> f64 {
    if data.organization_embedding_present {
        1.0
    } else {
        0.0
    }
}

// --- Pairwise Feature Calculation Functions ---
async fn calculate_name_jaro_winkler(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT similarity(LOWER(s1.name), LOWER(s2.name))::DOUBLE PRECISION as similarity
         FROM public.service s1, public.service s2
         WHERE s1.id = $1 AND s2.id = $2 AND s1.name IS NOT NULL AND s2.name IS NOT NULL",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for name_jaro_winkler failed")?;
    
    Ok(row.get::<_, f64>("similarity"))
}

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
                warn!("Empty embedding(s) for service pair ({}, {})", service1_id.0, service2_id.0);
                return Ok(0.0);
            }
            
            if v1_f32.len() != v2_f32.len() {
                warn!(
                    "Embedding dimension mismatch for service pair ({}, {}). v1: {}, v2: {}",
                    service1_id.0, service2_id.0, v1_f32.len(), v2_f32.len()
                );
                return Ok(0.0);
            }
            
            cosine_similarity_candle(&v1_f32, &v2_f32)
                .map_err(|e| anyhow::anyhow!("Candle similarity failed: {}", e))
        } else {
            Ok(0.0)
        }
    } else {
        Ok(0.0) // No embeddings available
    }
}

async fn calculate_taxonomy_jaccard(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row = conn.query_one(
        "WITH
         taxonomies1 AS (
            SELECT taxonomy_term_id FROM public.service_taxonomy WHERE service_id = $1
         ),
         taxonomies2 AS (
            SELECT taxonomy_term_id FROM public.service_taxonomy WHERE service_id = $2
         ),
         intersection_count AS (
            SELECT COUNT(*) as count FROM taxonomies1 INNER JOIN taxonomies2 USING (taxonomy_term_id)
         ),
         union_count AS (
            SELECT COUNT(*) as count FROM (
                SELECT taxonomy_term_id FROM taxonomies1
                UNION
                SELECT taxonomy_term_id FROM taxonomies2
            ) as union_taxonomies
         )
         SELECT
            CASE
                WHEN (SELECT count FROM union_count) = 0 THEN 0.0
                ELSE (SELECT count FROM intersection_count)::FLOAT / (SELECT count FROM union_count)
            END as jaccard",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for taxonomy_jaccard failed")?;
    
    Ok(row.get::<_, f64>("jaccard"))
}

async fn calculate_location_jaccard(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row = conn.query_one(
        "WITH
         locations1 AS (
            SELECT location_id FROM public.service_at_location WHERE service_id = $1
         ),
         locations2 AS (
            SELECT location_id FROM public.service_at_location WHERE service_id = $2
         ),
         intersection_count AS (
            SELECT COUNT(*) as count FROM locations1 INNER JOIN locations2 USING (location_id)
         ),
         union_count AS (
            SELECT COUNT(*) as count FROM (
                SELECT location_id FROM locations1
                UNION
                SELECT location_id FROM locations2
            ) as union_locations
         )
         SELECT
            CASE
                WHEN (SELECT count FROM union_count) = 0 THEN 0.0
                ELSE (SELECT count FROM intersection_count)::FLOAT / (SELECT count FROM union_count)
            END as jaccard",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for location_jaccard failed")?;
    
    Ok(row.get::<_, f64>("jaccard"))
}

async fn check_same_organization(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT
            CASE
                WHEN s1.organization_id = s2.organization_id AND s1.organization_id IS NOT NULL THEN 1.0
                ELSE 0.0
            END as same_organization
         FROM public.service s1, public.service s2
         WHERE s1.id = $1 AND s2.id = $2",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for same_organization failed")?;
    
    Ok(row.get::<_, f64>("same_organization"))
}

async fn check_email_exact_match(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row = conn.query_one(
        "SELECT
            CASE
                WHEN s1.email = s2.email AND s1.email IS NOT NULL AND s1.email <> '' THEN 1.0
                ELSE 0.0
            END as email_match
         FROM public.service s1, public.service s2
         WHERE s1.id = $1 AND s2.id = $2",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for email_exact_match failed")?;
    
    Ok(row.get::<_, f64>("email_match"))
}

async fn check_url_domain_match(
    conn: &PgConnection,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<f64> {
    let row = conn.query_one(
        "WITH domain_extract AS (
            SELECT
                regexp_replace(LOWER(s1.url), '^https?://(www\\.)?|/.*$', '', 'g') as domain1,
                regexp_replace(LOWER(s2.url), '^https?://(www\\.)?|/.*$', '', 'g') as domain2
            FROM public.service s1, public.service s2
            WHERE s1.id = $1 AND s2.id = $2 AND s1.url IS NOT NULL AND s1.url <> '' 
            AND s2.url IS NOT NULL AND s2.url <> ''
        )
        SELECT
            CASE
                WHEN (SELECT domain1 FROM domain_extract) = (SELECT domain2 FROM domain_extract) 
                AND (SELECT domain1 FROM domain_extract) <> '' THEN 1.0
                ELSE 0.0
            END as domain_match",
        &[&service1_id.0, &service2_id.0],
    ).await.context("DB query for url_domain_match failed")?;
    
    Ok(row.get::<_, f64>("domain_match"))
}

pub async fn extract_and_store_all_service_context_features(
    pool: &PgPool,
    feature_cache: &SharedServiceFeatureCache
) -> Result<usize> {
    let conn = pool.get().await?;
    
    // Get all service IDs
    let rows = conn.query("SELECT id FROM public.service", &[]).await?;
    let service_ids: Vec<ServiceId> = rows.iter().map(|row| ServiceId(row.get(0))).collect();
    
    info!("Found {} services for feature extraction", service_ids.len());
    
    // Process in batches
    let batch_size = 100;
    let mut processed_count = 0;
    
    for chunk in service_ids.chunks(batch_size) {
        // Create a new connection for this batch
        let batch_conn = pool.get().await?;
        
        // Process each service in this batch
        for service_id in chunk {
            // Get or extract features - this will store them in the DB if needed
            match get_stored_service_features(&*batch_conn, service_id).await {
                Ok(features) => {
                    // Also populate the in-memory cache with these features
                    let mut cache_guard = feature_cache.lock().await;
                    let cache_key = service_id.0.clone();
                    cache_guard.individual_cache.put(cache_key, features);
                    processed_count += 1;
                },
                Err(e) => {
                    warn!("Failed to extract features for service {}: {}", service_id.0, e);
                }
            }
        }
        
        info!("Processed {}/{} services", processed_count, service_ids.len());
    }
    
    // Cache statistics for observability
    {
        let cache_guard = feature_cache.lock().await;
        let (_, _, ind_hits, ind_misses) = cache_guard.get_stats();
        info!(
            "Individual service features cache stats after pre-extraction: hits={}, misses={}", 
            ind_hits, ind_misses
        );
    }
    
    Ok(processed_count)
}