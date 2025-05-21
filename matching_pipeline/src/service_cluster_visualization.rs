// src/service_cluster_visualization.rs
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use serde_json::json;
use std::time::Instant;
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

// Local imports
use crate::db::PgPool;
use crate::models::{ServiceId, ServiceGroupClusterId};

/// Simple struct to hold service_group record details
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
    service_count: Option<i32>,
    average_coherence_score: Option<f64>,
}

/// Ensures the tables needed for service visualization edge weights exist
pub async fn ensure_visualization_tables_exist(pool: &PgPool) -> Result<()> {
    let client = pool.get().await.context("Failed to get DB connection")?;

    // Create indices for performance
    let index_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_service_edge_visualization_cluster_id 
         ON public.service_edge_visualization(service_group_cluster_id)",
        "CREATE INDEX IF NOT EXISTS idx_service_edge_visualization_service_pairs 
         ON public.service_edge_visualization(service_id_1, service_id_2)",
    ];

    for sql in &index_sqls {
        client
            .execute(*sql, &[])
            .await
            .context(format!("Failed to create index with SQL: {}", sql))?;
    }

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

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for calculate_service_visualization_edges")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for calculate_service_visualization_edges")?;

    // Clean up existing edges for this run
    transaction
        .execute(
            "DELETE FROM public.service_edge_visualization WHERE pipeline_run_id = $1",
            &[&pipeline_run_id],
        )
        .await
        .context("Failed to clean up existing service edges")?;

    // Fetch all service clusters
    let clusters = fetch_service_clusters(&transaction).await?;
    info!(
        "Processing {} service clusters for visualization edge calculation",
        clusters.len()
    );

    let mut total_edges = 0;

    for cluster in &clusters {
        let edges_count = process_service_cluster_for_visualization(
            &transaction,
            &ServiceGroupClusterId(cluster.id.clone()),
            pipeline_run_id,
        )
        .await?;

        total_edges += edges_count;
    }

    transaction
        .commit()
        .await
        .context("Failed to commit service cluster visualization edge transaction")?;

    info!(
        "Service cluster visualization edge calculation finished in {:.2?}. {} edges created.",
        start_time.elapsed(),
        total_edges
    );

    Ok(total_edges)
}

/// Process a single service cluster to calculate visualization edges between its services
async fn process_service_cluster_for_visualization(
    transaction: &Transaction<'_>,
    cluster_id: &ServiceGroupClusterId,
    pipeline_run_id: &str,
) -> Result<usize> {
    debug!(
        "Processing service cluster {} for visualization edges",
        cluster_id.0
    );

    // 1. Get all unique services in this cluster
    let services = fetch_services_in_cluster(transaction, cluster_id).await?;
    if services.len() < 2 {
        debug!(
            "Service cluster {} has fewer than 2 services, skipping visualization edge calculation",
            cluster_id.0
        );
        return Ok(0);
    }

    debug!(
        "Found {} services in cluster {}",
        services.len(),
        cluster_id.0
    );

    // 2. Get RL weight based on human feedback
    let rl_weight = get_service_rl_weight_from_feedback(transaction).await?;
    debug!(
        "Using RL confidence weight of {:.2} based on human feedback for services",
        rl_weight
    );

    // 3. For each service pair, calculate edge weight
    let mut edges_created = 0;
    for i in 0..services.len() {
        for j in (i + 1)..services.len() {
            let service1_id = &services[i];
            let service2_id = &services[j];

            // Ensure consistent ordering (service_id_1 < service_id_2)
            let (source_id, target_id) = if service1_id.0 < service2_id.0 {
                (service1_id, service2_id)
            } else {
                (service2_id, service1_id)
            };

            // Find all matching methods between these services
            let matching_methods =
                fetch_service_matching_methods(transaction, source_id, target_id).await?;

            if !matching_methods.is_empty() {
                let edge_id = create_service_visualization_edge(
                    transaction,
                    cluster_id,
                    source_id,
                    target_id,
                    &matching_methods,
                    rl_weight,
                    pipeline_run_id,
                )
                .await?;

                if !edge_id.is_empty() {
                    edges_created += 1;
                }
            }
        }
    }

    debug!(
        "Created {} visualization edges for service cluster {}",
        edges_created, cluster_id.0
    );
    Ok(edges_created)
}

/// Fetch all service clusters from the database
async fn fetch_service_clusters(transaction: &Transaction<'_>) -> Result<Vec<ServiceGroupClusterRecord>> {
    let query = "SELECT id, name, service_count, average_coherence_score FROM public.service_group_cluster";

    let rows = transaction
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
    transaction: &Transaction<'_>,
    cluster_id: &ServiceGroupClusterId,
) -> Result<Vec<ServiceId>> {
    // Query to get all unique services in a cluster from service_group records
    let query = "
        SELECT DISTINCT service_id_1 as service_id FROM public.service_group 
        WHERE group_cluster_id = $1
        UNION
        SELECT DISTINCT service_id_2 as service_id FROM public.service_group 
        WHERE group_cluster_id = $1
    ";

    let rows = transaction
        .query(query, &[&cluster_id.0])
        .await
        .context("Failed to fetch services in cluster")?;

    let mut services = Vec::with_capacity(rows.len());
    for row in rows {
        let service_id: String = row.get("service_id");
        services.push(ServiceId(service_id));
    }

    Ok(services)
}

/// Fetch all matching methods that connect two specific services
async fn fetch_service_matching_methods(
    transaction: &Transaction<'_>,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
) -> Result<Vec<ServiceGroupRecord>> {
    // We already ensure service1_id < service2_id before calling this
    let query = "
        SELECT id, method_type, confidence_score, pre_rl_confidence_score, match_values
        FROM public.service_group
        WHERE service_id_1 = $1 AND service_id_2 = $2
    ";

    let rows = transaction
        .query(query, &[&service1_id.0, &service2_id.0])
        .await
        .context("Failed to fetch service matching methods")?;

    let mut methods = Vec::with_capacity(rows.len());
    for row in rows {
        methods.push(ServiceGroupRecord {
            id: row.get("id"),
            method_type: row.get("method_type"),
            confidence_score: row.get("confidence_score"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
            match_values: row.get("match_values"),
        });
    }

    Ok(methods)
}

/// Determine the proper weighting between RL and pre-RL confidence scores
/// based on historical human feedback for services
async fn get_service_rl_weight_from_feedback(transaction: &Transaction<'_>) -> Result<f64> {
    // Query human review feedback to see how often the RL model has been correct
    let query = "
        SELECT 
            COUNT(CASE WHEN is_match_correct = true THEN 1 END) as correct_count,
            COUNT(CASE WHEN is_match_correct = false THEN 1 END) as incorrect_count
        FROM clustering_metadata.service_match_human_feedback
        WHERE reviewer_id != 'ml_system'
    ";

    // This might fail if the table doesn't exist yet, so we need to handle that
    let result = transaction.query_opt(query, &[]).await;
    match result {
        Ok(Some(row)) => {
            let correct_count: i64 = row.get("correct_count");
            let incorrect_count: i64 = row.get("incorrect_count");

            // Calculate weight based on ML system accuracy
            if correct_count + incorrect_count > 0 {
                let accuracy = correct_count as f64 / (correct_count + incorrect_count) as f64;
                // Scale from 0.4 (poor accuracy) to 0.8 (excellent accuracy)
                let rl_weight = 0.4 + (0.4 * accuracy);
                Ok(rl_weight)
            } else {
                // No feedback yet
                Ok(0.6) // Balanced default
            }
        }
        _ => {
            // Table might not exist or query had an issue
            debug!("Could not query service human feedback, using default RL weight");
            Ok(0.6) // Balanced default
        }
    }
}

/// Calculate and store an edge between two services for visualization
async fn create_service_visualization_edge(
    transaction: &Transaction<'_>,
    cluster_id: &ServiceGroupClusterId,
    service1_id: &ServiceId,
    service2_id: &ServiceId,
    matching_methods: &[ServiceGroupRecord],
    rl_weight: f64,
    pipeline_run_id: &str,
) -> Result<String> {
    // Calculate edge weight
    let pre_rl_weight = 1.0 - rl_weight;

    // For each matching method, calculate its contribution
    let mut method_details = Vec::new();
    let mut method_confidences = Vec::new();

    for method in matching_methods {
        // Default to the RL confidence if pre_rl is missing
        let pre_rl_conf = method
            .pre_rl_confidence_score
            .unwrap_or_else(|| method.confidence_score.unwrap_or(0.0));

        // Default to pre_rl if RL is missing
        let rl_conf = method
            .confidence_score
            .unwrap_or_else(|| method.pre_rl_confidence_score.unwrap_or(0.0));

        // Combined confidence
        let combined_confidence = (pre_rl_weight * pre_rl_conf) + (rl_weight * rl_conf);
        method_confidences.push(combined_confidence);

        // Store details for JSONB
        method_details.push(json!({
            "method_type": method.method_type,
            "pre_rl_confidence": pre_rl_conf,
            "rl_confidence": rl_conf,
            "combined_confidence": combined_confidence
        }));
    }

    // Calculate aggregate edge weight using probabilistic combination
    let edge_weight = if method_confidences.is_empty() {
        0.0
    } else {
        // 1 - product of (1 - confidence)
        1.0 - method_confidences
            .iter()
            .fold(1.0, |acc, &conf| acc * (1.0 - conf.max(0.0).min(1.0)))
    };

    // Store the edge
    let edge_id = Uuid::new_v4().to_string();
    let details_json = json!({
        "methods": method_details,
        "rl_weight_factor": rl_weight,
        "method_count": matching_methods.len()
    });

    transaction
        .execute(
            "INSERT INTO public.service_edge_visualization
             (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &edge_id,
                &cluster_id.0,
                &service1_id.0,
                &service2_id.0,
                &edge_weight,
                &details_json,
                &pipeline_run_id,
            ],
        )
        .await
        .context("Failed to insert visualization service edge")?;

    Ok(edge_id)
}