// src/clustering/db.rs

use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use tokio_postgres::types::ToSql;
use uuid::Uuid;

use crate::utils::db_connect::PgPool;
use crate::clustering::create_clusters::{RawServiceGroup, ServiceClusterOutput, ServiceEdgeVisualizationData};

// Configuration
const BATCH_SIZE_DB_OPS: usize = 500; // Define a batch size for database operations

/// Fetches all service groups from the public.service_group table.
/// This function queries the database for service group records, filtering for those
/// with a non-null confidence score or pre-RL confidence score, and maps them
/// into `RawServiceGroup` structs.
///
/// Arguments:
/// * `pool` - A reference to the PostgreSQL connection pool.
/// * `multi_progress` - An optional `MultiProgress` instance for progress tracking.
///
/// Returns:
/// A `Result` containing a `Vec<RawServiceGroup>` on success, or an `anyhow::Error` on failure.
pub async fn fetch_all_service_groups(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
) -> Result<Vec<RawServiceGroup>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Create spinner for database fetch progress bar
    let fetch_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("    {spinner:.blue} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.set_message("Querying public.service_group table...");
        Some(pb)
    } else {
        None
    };

    // SQL query to select relevant fields from the service_group table
    let query = "
        SELECT id, service_id_1, service_id_2, confidence_score, pre_rl_confidence_score, method_type
        FROM public.service_group
        WHERE (confidence_score IS NOT NULL OR pre_rl_confidence_score IS NOT NULL)
        ORDER BY id
    ";

    // Execute the query
    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to query public.service_group table")?;

    // Map fetched rows into RawServiceGroup structs
    let mut service_groups = Vec::with_capacity(rows.len());
    for row in rows {
        service_groups.push(RawServiceGroup {
            id: row.get("id"),
            service_id_1: row.get("service_id_1"),
            service_id_2: row.get("service_id_2"),
            confidence_score: row.get("confidence_score"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
            method_type: row.get("method_type"),
        });
    }

    // Finish the progress bar
    if let Some(pb) = &fetch_pb {
        pb.finish_with_message(format!("Fetched {} service groups", service_groups.len()));
    }

    Ok(service_groups)
}

/// Stores service cluster data into the database.
/// This function performs batch insertions for `service_group_cluster` records,
/// updates `service_group` records with their new cluster IDs, and inserts
/// `service_edge_visualization` records for graph visualization.
///
/// Arguments:
/// * `pool` - A reference to the PostgreSQL connection pool.
/// * `clusters` - A slice of `ServiceClusterOutput` structs to be stored.
/// * `pipeline_run_id` - The ID of the current pipeline run.
/// * `multi_progress` - An optional `MultiProgress` instance for progress tracking.
///
/// Returns:
/// A `Result` indicating success or an `anyhow::Error` on failure.
pub async fn store_service_cluster_data(
    pool: &PgPool,
    clusters: &[ServiceClusterOutput],
    pipeline_run_id: &str,
    multi_progress: Option<MultiProgress>,
) -> Result<()> {
    if clusters.is_empty() {
        info!("No service clusters to store.");
        return Ok(());
    }

    // Create a main progress bar for storage operations
    let storage_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(3)); // 3 main storage operations: insert clusters, update groups, insert visualization edges
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.red} [{elapsed_precise}] {bar:25.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Storing service cluster data...");
        Some(pb)
    } else {
        None
    };

    let mut client = pool
        .get()
        .await
        .context("Failed to get DB client for storing service cluster data")?;

    // --- 1. Insert service_group_cluster records ---
    if let Some(pb) = &storage_pb {
        pb.set_message("Inserting service cluster records...");
    }
    info!("Batch inserting public.service_group_cluster records...");

    let mut cluster_insert_params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut cluster_insert_values_str = Vec::new();
    let mut i = 1; // Start parameter index from 1

    for cluster in clusters {
        // SQL values string for each cluster. Note that CURRENT_TIMESTAMP does not consume a $ parameter.
        cluster_insert_values_str.push(format!(
            "(${}, ${}, ${}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ${}, ${}, ${})",
            i,     // id
            i + 1, // name
            i + 2, // description
            i + 3, // service_count
            i + 4, // service_group_count
            i + 5  // average_coherence_score
        ));
        // Parameters for the SQL query, aligned with the placeholders above
        cluster_insert_params.push(Box::new(cluster.cluster_id_str.clone()));
        cluster_insert_params.push(Box::new(cluster.name.clone()));
        cluster_insert_params.push(Box::new(cluster.description.clone()));
        cluster_insert_params.push(Box::new(cluster.service_count));
        cluster_insert_params.push(Box::new(cluster.service_group_count));
        cluster_insert_params.push(Box::new(cluster.coherence_score.overall_score));
        
        // Increment 'i' by the number of actual dynamic parameters used in each set (which is 6)
        i += 6; 
    }

    if !cluster_insert_params.is_empty() {
        let cluster_insert_query = format!(
            "INSERT INTO public.service_group_cluster (id, name, description, created_at, updated_at, service_count, service_group_count, average_coherence_score) VALUES {}
            ON CONFLICT (id) DO NOTHING",
            cluster_insert_values_str.join(", ")
        );

        let params_slice: Vec<&(dyn ToSql + Sync)> = cluster_insert_params
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect();

        client
            .execute(&cluster_insert_query, &params_slice[..])
            .await
            .context("Failed to batch insert public.service_group_cluster records")?;
        info!(
            "Successfully inserted/updated {} public.service_group_cluster records.",
            clusters.len()
        );
    }

    if let Some(pb) = &storage_pb {
        pb.inc(1);
        pb.set_message("Updating service group records...");
    }

    // --- 2. Update service_group records with cluster IDs ---
    info!("Batch updating public.service_group records with cluster IDs...");
    let transaction = client
        .transaction()
        .await
        .context("Failed to start transaction for service_group updates")?;

    for cluster in clusters {
        if cluster.contributing_service_group_data.is_empty() {
            continue;
        }
        let service_group_ids_to_update: Vec<String> = cluster
            .contributing_service_group_data
            .iter()
            .map(|(id, _, _)| id.clone())
            .collect();

        // Process in chunks to avoid overwhelming the database with too many parameters
        for chunk in service_group_ids_to_update.chunks(BATCH_SIZE_DB_OPS) {
            if chunk.is_empty() {
                continue;
            }
            let update_query = "
                UPDATE public.service_group
                SET group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ANY($2)
            ";
            transaction
                .execute(update_query, &[&cluster.cluster_id_str, &chunk])
                .await
                .context(format!(
                    "Failed to update service_group records for cluster {}",
                    cluster.cluster_id_str
                ))?;
        }
    }

    transaction
        .commit()
        .await
        .context("Failed to commit service_group updates")?;
    info!("Successfully updated service_group records with cluster IDs.");

    if let Some(pb) = &storage_pb {
        pb.inc(1);
        pb.set_message("Inserting visualization edges...");
    }

    // --- 3. Insert service_edge_visualization records ---
    info!("Batch inserting public.service_edge_visualization records...");
    let viz_client = pool
        .get()
        .await
        .context("Failed to get new DB client for viz edges")?;

    // Create a separate progress bar for visualization edge insertion (can be very numerous)
    let viz_pb = if let Some(mp) = &multi_progress {
        let total_viz_edges: usize = clusters
            .iter()
            .map(|c| c.internal_edges_for_visualization.len())
            .sum();
        let pb = mp.add(ProgressBar::new(total_viz_edges as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("      {spinner:.yellow} [{elapsed_precise}] {bar:20.cyan/red} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Inserting service visualization edges...");
        Some(pb)
    } else {
        None
    };

    let mut viz_edge_insert_params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut viz_edge_insert_values_str = Vec::new();
    let mut current_val_idx = 1;
    let mut total_viz_edges_count = 0;

    for cluster in clusters {
        for viz_edge in &cluster.internal_edges_for_visualization {
            total_viz_edges_count += 1;
            if let Some(pb) = &viz_pb {
                pb.inc(1);
                if total_viz_edges_count % 1000 == 0 { // Update message periodically for large numbers
                    pb.set_message(format!("Inserting visualization edges... ({}/{})", total_viz_edges_count, pb.length().unwrap_or(0)));
                }
            }

            let edge_id = Uuid::new_v4().to_string(); // Generate unique ID for each edge
            viz_edge_insert_values_str.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                current_val_idx,
                current_val_idx + 1,
                current_val_idx + 2,
                current_val_idx + 3,
                current_val_idx + 4,
                current_val_idx + 5,
                current_val_idx + 6
            ));
            // Parameters in order: id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id
            viz_edge_insert_params.push(Box::new(edge_id));
            viz_edge_insert_params.push(Box::new(cluster.cluster_id_str.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.service_id_1.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.service_id_2.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.edge_weight));
            viz_edge_insert_params.push(Box::new(viz_edge.details.clone()));
            viz_edge_insert_params.push(Box::new(pipeline_run_id.to_string()));
            current_val_idx += 7;

            // Execute batch insert when sufficient parameters are collected
            if viz_edge_insert_params.len() >= BATCH_SIZE_DB_OPS * 7 {
                let query = format!(
                    "INSERT INTO public.service_edge_visualization (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id, created_at, updated_at) VALUES {}
                     ON CONFLICT (service_group_cluster_id, service_id_1, service_id_2, pipeline_run_id) DO NOTHING",
                    viz_edge_insert_values_str.join(", ")
                );
                let params_slice_viz: Vec<&(dyn ToSql + Sync)> =
                    viz_edge_insert_params
                        .iter()
                        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                        .collect();

                viz_client
                    .execute(&query, &params_slice_viz[..])
                    .await
                    .context(
                        "Failed to batch insert public.service_edge_visualization records (mid-batch)",
                    )?;

                viz_edge_insert_params.clear();
                viz_edge_insert_values_str.clear();
                current_val_idx = 1;
            }
        }
    }

    // Insert any remaining visualization edges
    if !viz_edge_insert_params.is_empty() {
        let query = format!(
            "INSERT INTO public.service_edge_visualization (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id, created_at, updated_at) VALUES {}
             ON CONFLICT (service_group_cluster_id, service_id_1, service_id_2, pipeline_run_id) DO NOTHING",
            viz_edge_insert_values_str.join(", ")
        );
        let params_slice_viz: Vec<&(dyn ToSql + Sync)> =
            viz_edge_insert_params
                .iter()
                .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                .collect();

        viz_client
            .execute(&query, &params_slice_viz[..])
            .await
            .context("Failed to batch insert remaining public.service_edge_visualization records")?;
    }

    if let Some(pb) = &viz_pb {
        pb.finish_with_message(format!("Inserted {} service visualization edges", total_viz_edges_count));
    }

    info!(
        "Successfully inserted/updated {} public.service_edge_visualization records.",
        total_viz_edges_count
    );

    if let Some(pb) = &storage_pb {
        pb.inc(1);
        pb.finish_with_message("Service cluster storage complete");
    }

    Ok(())
}
