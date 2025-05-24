// src/service_cluster_visualization.rs
use anyhow::{Context, Result};
use futures::stream::{self, StreamExt}; // For concurrent processing
use indicatif::{ProgressBar, ProgressStyle}; // For progress tracking
use log::{debug, info, warn};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc; // For shared counter
use std::time::Instant;
use tokio::sync::Mutex; // For shared counter
use tokio_postgres::GenericClient; // No longer need Transaction directly in main flow
use uuid::Uuid;

// Local imports
use crate::db::PgPool;
use crate::models::{ServiceGroupClusterId, ServiceId};

// Batch size for inserting edges
const BATCH_SIZE: usize = 1000;
// Number of clusters to process concurrently
const CONCURRENT_CLUSTERS: usize = 10; // Adjust based on system resources

/// Simple struct to hold service_group record details
#[derive(Debug, Clone)]
struct ServiceGroupRecord {
    id: String,
    method_type: String,
    confidence_score: Option<f64>,
    pre_rl_confidence_score: Option<f64>,
    match_values: Option<serde_json::Value>,
}

/// Simple struct to hold service_group_cluster record details
struct ServiceGroupClusterRecord {
    id: String,
    name: Option<String>,
    service_count: Option<i32>, // In service_group_cluster, this is service_count
    average_coherence_score: Option<f64>,
}

/// Struct to hold edge data before insertion
#[derive(Debug, Clone)]
struct ServiceEdgeData {
    service_group_cluster_id: String,
    service_id_1: String,
    service_id_2: String,
    edge_weight: f64,
    details: serde_json::Value,
    pipeline_run_id: String,
}

/// Ensures the tables needed for service visualization edge weights exist
pub async fn ensure_visualization_tables_exist(pool: &PgPool) -> Result<()> {
    let client = pool.get().await.context("Failed to get DB connection")?;

    // Create indices for performance
    // Note: public-schema.md shows some individual UNIQUE constraints on service_edge_visualization
    // which might be too strict. The DELETE + re-insert pattern per run_id is common.
    // These indices are for query performance.
    let index_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_service_edge_visualization_cluster_id_run_id
         ON public.service_edge_visualization(service_group_cluster_id, pipeline_run_id)", // Combined for better selectivity
        "CREATE INDEX IF NOT EXISTS idx_service_edge_visualization_service_pairs
         ON public.service_edge_visualization(service_id_1, service_id_2)",
        "CREATE INDEX IF NOT EXISTS idx_service_edge_visualization_pipeline_run
         ON public.service_edge_visualization(pipeline_run_id)", // For cleanup
    ];

    for sql in &index_sqls {
        client
            .execute(*sql, &[])
            .await
            .context(format!("Failed to create index with SQL: {}", sql))?;
    }
    info!("Ensured service_edge_visualization table and indices exist.");
    Ok(())
}

/// Main function to calculate edge weights between services within each cluster
/// These edge weights will be used for frontend visualization
pub async fn calculate_visualization_edges(pool: &PgPool, pipeline_run_id: &str) -> Result<usize> {
    info!(
        "Starting service edge weight calculation for cluster visualization (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    // Clean up existing edges for this run
    let client_cleanup = pool
        .get()
        .await
        .context("Failed to get DB connection for initial cleanup")?;
    client_cleanup
        .execute(
            "DELETE FROM public.service_edge_visualization WHERE pipeline_run_id = $1",
            &[&pipeline_run_id],
        )
        .await
        .context("Failed to clean up existing service edges")?;
    drop(client_cleanup); // Release connection
    debug!(
        "Cleaned up existing service edges for pipeline_run_id: {}",
        pipeline_run_id
    );

    // Fetch all service clusters
    let clusters = {
        let client = pool
            .get()
            .await
            .context("Failed to get DB connection for fetching service clusters")?;
        fetch_service_clusters(&*client).await?
    };

    if clusters.is_empty() {
        info!("No service clusters found. Skipping visualization edge calculation.");
        return Ok(0);
    }
    info!(
        "Processing {} service clusters for visualization edge calculation",
        clusters.len()
    );

    // Get RL weight based on human feedback once
    let rl_weight = {
        let client = pool
            .get()
            .await
            .context("Failed to get DB connection for RL weight")?;
        get_service_rl_weight_from_feedback(&*client).await?
    };
    debug!(
        "Using RL confidence weight of {:.2} based on human feedback for services",
        rl_weight
    );

    // Set up progress tracking
    let progress = ProgressBar::new(clusters.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} service clusters ({eta})")
            .unwrap()
            .progress_chars("##-"),
    );

    let total_edges_created = Arc::new(Mutex::new(0usize));
    let mut handles = Vec::new();

    // Process clusters concurrently
    for cluster_record in clusters {
        let pool_clone = pool.clone();
        let cluster_id = ServiceGroupClusterId(cluster_record.id.clone());
        let pipeline_run_id_clone = pipeline_run_id.to_string();
        let rl_weight_clone = rl_weight;
        let progress_clone = progress.clone();
        let total_edges_clone = Arc::clone(&total_edges_created);

        let handle = tokio::spawn(async move {
            let result = process_service_cluster_optimized(
                &pool_clone,
                &cluster_id,
                &pipeline_run_id_clone,
                rl_weight_clone,
            )
            .await;

            progress_clone.inc(1);

            match result {
                Ok(edges_data) => {
                    let edge_count = edges_data.len();
                    if edge_count > 0 {
                        if let Err(e) = bulk_insert_service_edges(&pool_clone, &edges_data).await {
                            warn!(
                                "Failed to insert service edges for cluster {}: {}",
                                cluster_id.0, e
                            );
                            // Optionally, decide if this error should halt everything or just be logged.
                            // For now, we log and continue.
                            return Err(e); // Propagate error for the handle
                        }
                    }
                    let mut total_edges_guard = total_edges_clone.lock().await;
                    *total_edges_guard += edge_count;
                    Ok(edge_count)
                }
                Err(e) => {
                    warn!("Failed to process service cluster {}: {}", cluster_id.0, e);
                    Err(e) // Propagate error for the handle
                }
            }
        });
        handles.push(handle);

        // Limit concurrent tasks
        if handles.len() >= CONCURRENT_CLUSTERS {
            // Wait for about half of them to complete to manage resources
            let num_to_await = handles.len() / 2 + 1;
            let completed_handles = handles.drain(..num_to_await).collect::<Vec<_>>();
            for h in completed_handles {
                if let Err(e) = h.await? {
                    // Using `?` to propagate panics from spawned tasks
                    warn!("A service cluster processing task failed: {}", e);
                }
            }
        }
    }

    // Wait for any remaining tasks
    for handle in handles {
        if let Err(e) = handle.await? {
            warn!("A service cluster processing task failed: {}", e);
        }
    }

    progress.finish_with_message("Service cluster processing complete");

    let final_total_edges = *total_edges_created.lock().await;
    info!(
        "Service cluster visualization edge calculation finished in {:.2?}. {} edges created.",
        start_time.elapsed(),
        final_total_edges
    );

    Ok(final_total_edges)
}

/// Process a single service cluster to calculate visualization edges (optimized version)
async fn process_service_cluster_optimized(
    pool: &PgPool,
    cluster_id: &ServiceGroupClusterId,
    pipeline_run_id: &str,
    rl_weight: f64,
) -> Result<Vec<ServiceEdgeData>> {
    debug!(
        "Processing service cluster {} for visualization edges",
        cluster_id.0
    );

    let client = pool.get().await.context(format!(
        "Failed to get DB connection for service cluster {}",
        cluster_id.0
    ))?;

    // 1. Get all unique services in this cluster
    let services = fetch_services_in_cluster(&*client, cluster_id).await?;
    if services.len() < 2 {
        debug!(
            "Service cluster {} has {} services (fewer than 2), skipping visualization edge calculation",
            cluster_id.0,
            services.len()
        );
        return Ok(vec![]);
    }
    debug!(
        "Found {} services in cluster {}",
        services.len(),
        cluster_id.0
    );

    // 2. Bulk fetch all matching methods for all service pairs in this cluster
    let all_methods_map =
        fetch_all_matching_methods_for_service_cluster(&*client, &services).await?;
    if all_methods_map.is_empty() {
        debug!("No matching methods found between any service pairs in cluster {}. No edges will be created.", cluster_id.0);
        return Ok(vec![]);
    }

    // 3. For each service pair, calculate edge weight
    let mut edges_to_insert = Vec::new();
    for i in 0..services.len() {
        for j in (i + 1)..services.len() {
            let service1_id = &services[i];
            let service2_id = &services[j];

            // Ensure consistent ordering (service_id_1 < service_id_2) for map key and insertion
            let (source_id, target_id) = if service1_id.0 < service2_id.0 {
                (service1_id, service2_id)
            } else {
                (service2_id, service1_id)
            };

            let pair_key = (source_id.0.clone(), target_id.0.clone());

            if let Some(matching_methods) = all_methods_map.get(&pair_key) {
                if !matching_methods.is_empty() {
                    let (edge_weight, details_json) =
                        calculate_service_edge_details(matching_methods, rl_weight)?;

                    if edge_weight > 0.0 {
                        // Only create edges with some weight
                        edges_to_insert.push(ServiceEdgeData {
                            service_group_cluster_id: cluster_id.0.clone(),
                            service_id_1: source_id.0.clone(),
                            service_id_2: target_id.0.clone(),
                            edge_weight,
                            details: details_json,
                            pipeline_run_id: pipeline_run_id.to_string(),
                        });
                    }
                }
            }
        }
    }

    debug!(
        "Calculated {} visualization edges for service cluster {}",
        edges_to_insert.len(),
        cluster_id.0
    );
    Ok(edges_to_insert)
}

/// Fetch all service clusters from the database
async fn fetch_service_clusters(
    client: &impl GenericClient,
) -> Result<Vec<ServiceGroupClusterRecord>> {
    // Order by service_count descending to potentially process larger clusters first
    // This can sometimes help with load balancing if larger clusters take longer.
    let query = "SELECT id, name, service_count, average_coherence_score 
         FROM public.service_group_cluster
         ORDER BY service_count DESC NULLS LAST"; // Ensure consistent ordering

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to fetch service clusters")?;

    let mut clusters = Vec::with_capacity(rows.len());
    for row in rows {
        clusters.push(ServiceGroupClusterRecord {
            id: row.get("id"),
            name: row.get("name"),
            service_count: row.get("service_count"),
            average_coherence_score: row.get("average_coherence_score"),
        });
    }
    Ok(clusters)
}

/// Fetch all unique services that belong to a specific cluster
async fn fetch_services_in_cluster(
    client: &impl GenericClient,
    cluster_id: &ServiceGroupClusterId,
) -> Result<Vec<ServiceId>> {
    // Query to get all unique services in a cluster from service_group records
    // Ensure services are ordered for consistent processing if needed later, though pair iteration handles combinations.
    let query = "
        SELECT service_id_1 as service_id FROM public.service_group
        WHERE group_cluster_id = $1
        UNION
        SELECT service_id_2 as service_id FROM public.service_group
        WHERE group_cluster_id = $1
        ORDER BY service_id ASC
    "; // Added UNION to ensure distinctness and ORDER BY for consistency

    let rows = client
        .query(query, &[&cluster_id.0])
        .await
        .context(format!(
            "Failed to fetch services in cluster {}",
            cluster_id.0
        ))?;

    let mut services = Vec::with_capacity(rows.len());
    for row in rows {
        let service_id_str: String = row.get("service_id");
        // Basic validation: ensure service_id is not empty
        if service_id_str.trim().is_empty() {
            warn!(
                "Fetched empty service_id string for cluster {}, skipping.",
                cluster_id.0
            );
            continue;
        }
        services.push(ServiceId(service_id_str));
    }
    Ok(services)
}

/// Bulk fetch all matching methods for all service pairs in a cluster
async fn fetch_all_matching_methods_for_service_cluster(
    client: &impl GenericClient,
    service_ids: &[ServiceId],
) -> Result<HashMap<(String, String), Vec<ServiceGroupRecord>>> {
    if service_ids.len() < 2 {
        return Ok(HashMap::new()); // No pairs to fetch methods for
    }
    // Convert ServiceIds to strings for the query
    let service_id_strings: Vec<String> = service_ids.iter().map(|s| s.0.clone()).collect();

    // Query all service_group records where both services are in our list
    // This fetches all relevant service_group entries for the given set of services.
    let query = "
        SELECT id, service_id_1, service_id_2, method_type, confidence_score,
               pre_rl_confidence_score, match_values
        FROM public.service_group
        WHERE service_id_1 = ANY($1) AND service_id_2 = ANY($1)
          AND service_id_1 < service_id_2 -- Ensure we only get canonical pairs if they are stored that way
                                        -- If not, the grouping logic below handles it.
        ORDER BY service_id_1, service_id_2 -- Good for debugging, not strictly necessary for logic
    ";

    let rows = client
        .query(query, &[&service_id_strings])
        .await
        .context("Failed to fetch service matching methods in bulk")?;

    // Group methods by service pair
    let mut methods_map: HashMap<(String, String), Vec<ServiceGroupRecord>> = HashMap::new();

    for row in rows {
        let s_id_1: String = row.get("service_id_1");
        let s_id_2: String = row.get("service_id_2");

        // Ensure consistent ordering for the key
        let key = if s_id_1 < s_id_2 {
            (s_id_1, s_id_2)
        } else {
            (s_id_2.clone(), s_id_1.clone()) // Should not happen if query enforces s_id_1 < s_id_2
        };

        let record = ServiceGroupRecord {
            id: row.get("id"),
            method_type: row.get("method_type"),
            confidence_score: row.get("confidence_score"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
            match_values: row.get("match_values"),
        };

        methods_map.entry(key).or_default().push(record);
    }
    Ok(methods_map)
}

/// Determine the proper weighting between RL and pre-RL confidence scores
/// based on historical human feedback for services
async fn get_service_rl_weight_from_feedback(client: &impl GenericClient) -> Result<f64> {
    let query = "
        SELECT
            SUM(CASE WHEN is_match_correct = true THEN 1 ELSE 0 END)::bigint as correct_count,
            SUM(CASE WHEN is_match_correct = false THEN 1 ELSE 0 END)::bigint as incorrect_count
        FROM clustering_metadata.service_match_human_feedback
        WHERE reviewer_id != 'ml_system' OR reviewer_id IS NULL -- Consider entries not made by the system
    ";

    let result = client.query_opt(query, &[]).await;
    match result {
        Ok(Some(row)) => {
            let correct_count: i64 = row.try_get("correct_count").unwrap_or(0);
            let incorrect_count: i64 = row.try_get("incorrect_count").unwrap_or(0);

            if correct_count + incorrect_count > 0 {
                let accuracy = correct_count as f64 / (correct_count + incorrect_count) as f64;
                // Scale from 0.4 (poor accuracy) to 0.8 (excellent accuracy)
                // This weighting favors RL more as its accuracy increases.
                let rl_weight = 0.4 + (0.4 * accuracy.max(0.0).min(1.0)); // Clamp accuracy
                Ok(rl_weight)
            } else {
                // No feedback yet or only system feedback
                debug!("No human feedback for services found, using default RL weight 0.6");
                Ok(0.6) // Balanced default
            }
        }
        Ok(None) => {
            // Query returned no rows (e.g. table is empty)
            debug!(
                "No human feedback for services found (empty table), using default RL weight 0.6"
            );
            Ok(0.6)
        }
        Err(e) => {
            // Table might not exist or query had an issue
            warn!(
                "Could not query service human feedback (error: {}), using default RL weight 0.6",
                e
            );
            Ok(0.6) // Balanced default, but log the error
        }
    }
}

/// Calculate the edge weight and details JSON for a service visualization edge
fn calculate_service_edge_details(
    matching_methods: &[ServiceGroupRecord],
    rl_weight: f64,
) -> Result<(f64, serde_json::Value)> {
    // Ensure rl_weight is within a sensible range [0,1]
    let clamped_rl_weight = rl_weight.max(0.0).min(1.0);
    let pre_rl_weight = 1.0 - clamped_rl_weight;

    let mut method_details_vec = Vec::with_capacity(matching_methods.len());
    let mut method_confidences = Vec::with_capacity(matching_methods.len());

    for method in matching_methods {
        // Default to the RL confidence if pre_rl is missing, and vice-versa.
        // If both are missing, default to 0.0.
        let pre_rl_conf = method
            .pre_rl_confidence_score
            .unwrap_or_else(|| method.confidence_score.unwrap_or(0.0));
        let rl_conf = method
            .confidence_score
            .unwrap_or_else(|| method.pre_rl_confidence_score.unwrap_or(0.0));

        // Combined confidence using the weighted average
        let combined_confidence = (pre_rl_weight * pre_rl_conf) + (clamped_rl_weight * rl_conf);
        method_confidences.push(combined_confidence.max(0.0).min(1.0)); // Clamp individual combined conf

        method_details_vec.push(json!({
            "method_type": method.method_type,
            "pre_rl_confidence": pre_rl_conf,
            "rl_confidence": rl_conf,
            "combined_confidence": combined_confidence // Store original for inspection
        }));
    }

    // Calculate aggregate edge weight using probabilistic combination (Noisy-OR)
    // This assumes confidences are probabilities of a match.
    // Edge weight = 1 - product of (1 - confidence_i) for each method.
    let edge_weight = if method_confidences.is_empty() {
        0.0
    } else {
        1.0 - method_confidences
            .iter()
            .fold(1.0, |acc, &conf| acc * (1.0 - conf)) // conf is already clamped
    };

    let details_json = json!({
        "methods": method_details_vec,
        "rl_weight_factor": clamped_rl_weight, // Store the clamped weight used
        "method_count": matching_methods.len()
    });

    // Clamp final edge_weight to ensure it's between 0.0 and 1.0
    Ok((edge_weight.max(0.0).min(1.0), details_json))
}

/// Bulk insert service edges using efficient batch operations
async fn bulk_insert_service_edges(pool: &PgPool, edges: &[ServiceEdgeData]) -> Result<()> {
    if edges.is_empty() {
        return Ok(());
    }

    let client = pool
        .get()
        .await
        .context("Failed to get DB connection for bulk service edge insert")?;

    // Process in chunks to avoid overly large UNNEST operations
    for chunk in edges.chunks(BATCH_SIZE) {
        insert_service_edge_chunk(&*client, chunk).await?;
    }
    Ok(())
}

/// Insert a chunk of service edges
async fn insert_service_edge_chunk(
    client: &impl GenericClient,
    chunk: &[ServiceEdgeData],
) -> Result<()> {
    let mut id_vec = Vec::with_capacity(chunk.len());
    let mut cluster_id_vec = Vec::with_capacity(chunk.len());
    let mut service_id_1_vec = Vec::with_capacity(chunk.len());
    let mut service_id_2_vec = Vec::with_capacity(chunk.len());
    let mut edge_weight_vec = Vec::with_capacity(chunk.len()); // Store as f64, DB will handle numeric(10,5)
    let mut details_vec = Vec::with_capacity(chunk.len());
    let mut pipeline_run_id_vec = Vec::with_capacity(chunk.len());

    for edge in chunk {
        id_vec.push(Uuid::new_v4().to_string());
        cluster_id_vec.push(edge.service_group_cluster_id.clone());
        service_id_1_vec.push(edge.service_id_1.clone());
        service_id_2_vec.push(edge.service_id_2.clone());
        edge_weight_vec.push(edge.edge_weight); // f64
        details_vec.push(edge.details.clone());
        pipeline_run_id_vec.push(edge.pipeline_run_id.clone());
    }

    // Using UNNEST for bulk insert
    // Note: edge_weight is f64, public.service_edge_visualization.edge_weight is numeric(10,5)
    // tokio-postgres handles this conversion.
    client.execute(
        "INSERT INTO public.service_edge_visualization
         (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id)
         SELECT * FROM UNNEST(
             $1::text[], $2::text[], $3::text[], $4::text[], 
             $5::float8[], $6::jsonb[], $7::text[]
         )",
        &[
            &id_vec,
            &cluster_id_vec,
            &service_id_1_vec,
            &service_id_2_vec,
            &edge_weight_vec, // Pass as f64 (float8 in PG)
            &details_vec,
            &pipeline_run_id_vec,
        ],
    )
    .await
    .context("Failed to insert visualization service edges in batch")?;

    Ok(())
}

/// Checks if service visualization edges already exist for a given pipeline run ID.
pub async fn service_visualization_edges_exist(
    pool: &PgPool,
    pipeline_run_id: &str,
) -> Result<bool> {
    let client = pool.get().await.context("Failed to get DB connection")?;
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM public.service_edge_visualization WHERE pipeline_run_id = $1",
            &[&pipeline_run_id],
        )
        .await
        .context("Failed to query existing service visualization edges")?;

    let count: i64 = row.get(0);
    Ok(count > 0)
}
