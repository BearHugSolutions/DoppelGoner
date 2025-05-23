// src/cluster_visualization.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

// Local imports
use crate::db::PgPool;
use crate::models::{EntityId, GroupClusterId};

// Increased batch size for better performance
const BATCH_SIZE: usize = 1000;
// Number of clusters to process concurrently
const CONCURRENT_CLUSTERS: usize = 10;

/// Simple struct to hold entity_group record details
#[derive(Debug, Clone)]
struct EntityGroupRecord {
    id: String,
    method_type: String,
    confidence_score: Option<f64>,
    pre_rl_confidence_score: Option<f64>,
    match_values: Option<serde_json::Value>,
}

/// Simple struct to hold group_cluster record details
struct GroupClusterRecord {
    id: String,
    name: Option<String>,
    entity_count: Option<i32>,
    average_coherence_score: Option<f64>,
}

/// Struct to hold edge data before insertion
#[derive(Debug, Clone)]
struct EdgeData {
    cluster_id: String,
    entity_id_1: String,
    entity_id_2: String,
    edge_weight: f64,
    details: serde_json::Value,
    pipeline_run_id: String,
}

/// Ensures the tables needed for visualization edge weights exist
pub async fn ensure_visualization_tables_exist(pool: &PgPool) -> Result<()> {
    let client = pool.get().await.context("Failed to get DB connection")?;

    // Create indices for performance
    let index_sqls = [
        "CREATE INDEX IF NOT EXISTS idx_entity_edge_visualization_cluster_id
         ON public.entity_edge_visualization(cluster_id)",
        "CREATE INDEX IF NOT EXISTS idx_entity_edge_visualization_entity_pairs
         ON public.entity_edge_visualization(entity_id_1, entity_id_2)",
        "CREATE INDEX IF NOT EXISTS idx_entity_edge_visualization_run_cluster
         ON public.entity_edge_visualization(pipeline_run_id, cluster_id)",
    ];

    for sql in &index_sqls {
        client
            .execute(*sql, &[])
            .await
            .context(format!("Failed to create index with SQL: {}", sql))?;
    }

    Ok(())
}

/// Main function to calculate edge weights between entities within each cluster
/// These edge weights will be used for frontend visualization
pub async fn calculate_visualization_edges(pool: &PgPool, pipeline_run_id: &str) -> Result<usize> {
    info!(
        "Starting entity edge weight calculation for cluster visualization (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    // Clean up existing edges for this run
    let client = pool
        .get()
        .await
        .context("Failed to get DB connection for initial cleanup")?;
    client
        .execute(
            "DELETE FROM public.entity_edge_visualization WHERE pipeline_run_id = $1",
            &[&pipeline_run_id],
        )
        .await
        .context("Failed to clean up existing edges")?;
    drop(client);

    // Fetch all clusters
    let clusters = {
        let client = pool
            .get()
            .await
            .context("Failed to get DB connection for fetching clusters")?;
        fetch_clusters(&*client).await?
    };

    info!(
        "Processing {} clusters for visualization edge calculation",
        clusters.len()
    );

    // Get RL weight based on human feedback once
    let rl_weight = {
        let client = pool
            .get()
            .await
            .context("Failed to get DB connection for RL weight")?;
        get_rl_weight_from_feedback(&*client).await?
    };
    debug!(
        "Using RL confidence weight of {:.2} based on human feedback",
        rl_weight
    );

    // Set up progress tracking
    let progress = ProgressBar::new(clusters.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} clusters ({eta})")
            .unwrap()
            .progress_chars("##-"),
    );

    // Shared counter for total edges
    let total_edges = Arc::new(Mutex::new(0usize));
    
    // Process clusters in parallel batches
    let mut cluster_stream = stream::iter(clusters.into_iter());
    let mut handles = Vec::new();

    while let Some(cluster) = cluster_stream.next().await {
        let pool_clone = pool.clone();
        let cluster_id = GroupClusterId(cluster.id.clone());
        let pipeline_run_id = pipeline_run_id.to_string();
        let rl_weight_copy = rl_weight;
        let progress_clone = progress.clone();
        let total_edges_clone = Arc::clone(&total_edges);

        let handle = tokio::spawn(async move {
            let result = process_cluster_optimized(
                &pool_clone,
                &cluster_id,
                &pipeline_run_id,
                rl_weight_copy,
            )
            .await;
            
            progress_clone.inc(1);
            
            match result {
                Ok(edges) => {
                    let edge_count = edges.len();
                    if edge_count > 0 {
                        // Insert edges for this cluster
                        if let Err(e) = bulk_insert_edges(&pool_clone, &edges).await {
                            warn!("Failed to insert edges for cluster {}: {}", cluster_id.0, e);
                            return Err(e);
                        }
                    }
                    let mut total = total_edges_clone.lock().await;
                    *total += edge_count;
                    Ok(edge_count)
                }
                Err(e) => {
                    warn!("Failed to process cluster {}: {}", cluster_id.0, e);
                    Err(e)
                }
            }
        });

        handles.push(handle);

        // Limit concurrent tasks
        if handles.len() >= CONCURRENT_CLUSTERS {
            // Wait for half of them to complete
            let completed = handles.drain(..CONCURRENT_CLUSTERS / 2).collect::<Vec<_>>();
            for handle in completed {
                if let Err(e) = handle.await? {
                    warn!("Cluster processing task failed: {}", e);
                }
            }
        }
    }

    // Wait for remaining tasks
    for handle in handles {
        if let Err(e) = handle.await? {
            warn!("Cluster processing task failed: {}", e);
        }
    }

    progress.finish();

    let final_total = *total_edges.lock().await;
    info!(
        "Cluster visualization edge calculation finished in {:.2?}. {} edges created.",
        start_time.elapsed(),
        final_total
    );

    Ok(final_total)
}

/// Process a single cluster to calculate visualization edges between its entities
async fn process_cluster_optimized(
    pool: &PgPool,
    cluster_id: &GroupClusterId,
    pipeline_run_id: &str,
    rl_weight: f64,
) -> Result<Vec<EdgeData>> {
    debug!(
        "Processing cluster {} for visualization edges",
        cluster_id.0
    );

    // Get a connection and hold it for the entire cluster processing
    let client = pool.get().await.context("Failed to get DB connection")?;

    // Get all unique entities in this cluster
    let entities = fetch_entities_in_cluster(&*client, cluster_id).await?;
    if entities.len() < 2 {
        debug!(
            "Cluster {} has fewer than 2 entities, skipping visualization edge calculation",
            cluster_id.0
        );
        return Ok(vec![]);
    }

    debug!(
        "Found {} entities in cluster {}",
        entities.len(),
        cluster_id.0
    );

    // Bulk fetch all matching methods for all entity pairs in this cluster
    let all_methods = fetch_all_matching_methods_for_cluster(&*client, &entities).await?;

    // Pre-allocate edge vector with expected capacity
    let expected_edges = entities.len() * (entities.len() - 1) / 2;
    let mut edges = Vec::with_capacity(expected_edges);

    // Process entity pairs in memory
    for i in 0..entities.len() {
        for j in (i + 1)..entities.len() {
            let entity1_id = &entities[i];
            let entity2_id = &entities[j];

            // Ensure consistent ordering (entity_id_1 < entity_id_2)
            let (source_id, target_id) = if entity1_id.0 < entity2_id.0 {
                (entity1_id, entity2_id)
            } else {
                (entity2_id, entity1_id)
            };

            let key = (source_id.0.clone(), target_id.0.clone());
            
            // Check if we have matching methods for this pair
            if let Some(methods) = all_methods.get(&key) {
                if !methods.is_empty() {
                    let (edge_weight, details_json) = calculate_edge_details(methods, rl_weight)?;
                    
                    edges.push(EdgeData {
                        cluster_id: cluster_id.0.clone(),
                        entity_id_1: source_id.0.clone(),
                        entity_id_2: target_id.0.clone(),
                        edge_weight,
                        details: details_json,
                        pipeline_run_id: pipeline_run_id.to_string(),
                    });
                }
            }
        }
    }

    debug!(
        "Calculated {} visualization edges for cluster {}",
        edges.len(),
        cluster_id.0
    );

    Ok(edges)
}

/// Bulk fetch all matching methods for all entity pairs in a cluster
async fn fetch_all_matching_methods_for_cluster(
    client: &impl GenericClient,
    entity_ids: &[EntityId],
) -> Result<HashMap<(String, String), Vec<EntityGroupRecord>>> {
    // Convert EntityIds to strings for the query
    let entity_id_strings: Vec<String> = entity_ids.iter().map(|e| e.0.clone()).collect();
    
    // Query all entity_group records where both entities are in our list
    let query = "
        SELECT id, entity_id_1, entity_id_2, method_type, confidence_score, 
               pre_rl_confidence_score, match_values
        FROM public.entity_group
        WHERE entity_id_1 = ANY($1) AND entity_id_2 = ANY($1)
        ORDER BY entity_id_1, entity_id_2
    ";
    
    let rows = client
        .query(query, &[&entity_id_strings])
        .await
        .context("Failed to fetch entity matching methods in bulk")?;
    
    // Group methods by entity pair
    let mut methods_map: HashMap<(String, String), Vec<EntityGroupRecord>> = HashMap::new();
    
    for row in rows {
        let entity_id_1: String = row.get("entity_id_1");
        let entity_id_2: String = row.get("entity_id_2");
        
        // Ensure consistent ordering for the key
        let key = if entity_id_1 < entity_id_2 {
            (entity_id_1, entity_id_2)
        } else {
            (entity_id_2.clone(), entity_id_1.clone())
        };
        
        let record = EntityGroupRecord {
            id: row.get("id"),
            method_type: row.get("method_type"),
            confidence_score: row.get("confidence_score"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
            match_values: row.get("match_values"),
        };
        
        methods_map.entry(key).or_insert_with(Vec::new).push(record);
    }
    
    Ok(methods_map)
}

/// Bulk insert edges using efficient batch operations
async fn bulk_insert_edges(pool: &PgPool, edges: &[EdgeData]) -> Result<()> {
    if edges.is_empty() {
        return Ok(());
    }

    let client = pool
        .get()
        .await
        .context("Failed to get DB connection for bulk insert")?;

    // Process in chunks
    for chunk in edges.chunks(BATCH_SIZE) {
        insert_edge_chunk(&*client, chunk).await?;
    }

    Ok(())
}

/// Insert a chunk of edges
async fn insert_edge_chunk(
    client: &impl GenericClient,
    chunk: &[EdgeData],
) -> Result<()> {
    let mut id_vec = Vec::with_capacity(chunk.len());
    let mut cluster_id_vec = Vec::with_capacity(chunk.len());
    let mut entity_id_1_vec = Vec::with_capacity(chunk.len());
    let mut entity_id_2_vec = Vec::with_capacity(chunk.len());
    let mut edge_weight_vec = Vec::with_capacity(chunk.len());
    let mut details_vec = Vec::with_capacity(chunk.len());
    let mut pipeline_run_id_vec = Vec::with_capacity(chunk.len());

    for edge in chunk {
        id_vec.push(Uuid::new_v4().to_string());
        cluster_id_vec.push(edge.cluster_id.clone());
        entity_id_1_vec.push(edge.entity_id_1.clone());
        entity_id_2_vec.push(edge.entity_id_2.clone());
        edge_weight_vec.push(edge.edge_weight);
        details_vec.push(edge.details.clone());
        pipeline_run_id_vec.push(edge.pipeline_run_id.clone());
    }

    client.execute(
        "INSERT INTO public.entity_edge_visualization
         (id, cluster_id, entity_id_1, entity_id_2, edge_weight, details, pipeline_run_id)
         SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[], $6::jsonb[], $7::text[])",
        &[
            &id_vec,
            &cluster_id_vec,
            &entity_id_1_vec,
            &entity_id_2_vec,
            &edge_weight_vec,
            &details_vec,
            &pipeline_run_id_vec,
        ],
    )
    .await
    .context("Failed to insert visualization entity edges in batch")?;

    Ok(())
}

/// Fetch all clusters from the database
async fn fetch_clusters(client: &impl GenericClient) -> Result<Vec<GroupClusterRecord>> {
    let query =
        "SELECT id, name, entity_count, average_coherence_score 
         FROM public.entity_group_cluster
         ORDER BY entity_count DESC"; // Process larger clusters first for better load balancing

    let rows = client
        .query(query, &[])
        .await
        .context("Failed to fetch clusters")?;

    let mut clusters = Vec::with_capacity(rows.len());
    for row in rows {
        clusters.push(GroupClusterRecord {
            id: row.get("id"),
            name: row.get("name"),
            entity_count: row.get("entity_count"),
            average_coherence_score: row.get("average_coherence_score"),
        });
    }

    Ok(clusters)
}

/// Fetch all unique entities that belong to a specific cluster
async fn fetch_entities_in_cluster(
    client: &impl GenericClient,
    cluster_id: &GroupClusterId,
) -> Result<Vec<EntityId>> {
    // Query to get all unique entities in a cluster from entity_group records
    let query = "
        SELECT DISTINCT entity_id_1 as entity_id FROM public.entity_group
        WHERE group_cluster_id = $1
        UNION
        SELECT DISTINCT entity_id_2 as entity_id FROM public.entity_group
        WHERE group_cluster_id = $1
        ORDER BY entity_id
    ";

    let rows = client
        .query(query, &[&cluster_id.0])
        .await
        .context("Failed to fetch entities in cluster")?;

    let mut entities = Vec::with_capacity(rows.len());
    for row in rows {
        let entity_id: String = row.get("entity_id");
        entities.push(EntityId(entity_id));
    }

    Ok(entities)
}

/// Determine the proper weighting between RL and pre-RL confidence scores
/// based on historical human feedback
async fn get_rl_weight_from_feedback(client: &impl GenericClient) -> Result<f64> {
    // Query human review feedback to see how often the RL model has been correct
    let query = "
        SELECT
            COUNT(CASE WHEN is_match_correct = true THEN 1 END) as correct_count,
            COUNT(CASE WHEN is_match_correct = false THEN 1 END) as incorrect_count
        FROM clustering_metadata.human_feedback
        WHERE reviewer_id != 'ml_system'
    ";

    let result = client.query_opt(query, &[]).await;
    match result {
        Ok(Some(row)) => {
            let correct_count: i64 = row.get("correct_count");
            let incorrect_count: i64 = row.get("incorrect_count");

            if correct_count + incorrect_count > 0 {
                let accuracy = correct_count as f64 / (correct_count + incorrect_count) as f64;
                let rl_weight = 0.4 + (0.4 * accuracy);
                Ok(rl_weight)
            } else {
                Ok(0.6)
            }
        }
        _ => {
            debug!("Could not query human feedback, using default RL weight");
            Ok(0.6)
        }
    }
}

/// Calculate the edge weight and details JSON for a visualization edge
fn calculate_edge_details(
    matching_methods: &[EntityGroupRecord],
    rl_weight: f64,
) -> Result<(f64, serde_json::Value)> {
    let pre_rl_weight = 1.0 - rl_weight;

    let mut method_details = Vec::new();
    let mut method_confidences = Vec::new();

    for method in matching_methods {
        let pre_rl_conf = method
            .pre_rl_confidence_score
            .unwrap_or_else(|| method.confidence_score.unwrap_or(0.0));

        let rl_conf = method
            .confidence_score
            .unwrap_or_else(|| method.pre_rl_confidence_score.unwrap_or(0.0));

        let combined_confidence = (pre_rl_weight * pre_rl_conf) + (rl_weight * rl_conf);
        method_confidences.push(combined_confidence);

        method_details.push(json!({
            "method_type": method.method_type,
            "pre_rl_confidence": pre_rl_conf,
            "rl_confidence": rl_conf,
            "combined_confidence": combined_confidence
        }));
    }

    let edge_weight = if method_confidences.is_empty() {
        0.0
    } else {
        1.0 - method_confidences
            .iter()
            .fold(1.0, |acc, &conf| acc * (1.0 - conf.max(0.0).min(1.0)))
    };

    let details_json = json!({
        "methods": method_details,
        "rl_weight_factor": rl_weight,
        "method_count": matching_methods.len()
    });

    Ok((edge_weight, details_json))
}

/// Checks if visualization edges already exist for a given pipeline run ID.
pub async fn visualization_edges_exist(pool: &PgPool, pipeline_run_id: &str) -> Result<bool> {
    let client = pool.get().await.context("Failed to get DB connection")?;
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM public.entity_edge_visualization WHERE pipeline_run_id = $1",
            &[&pipeline_run_id],
        )
        .await
        .context("Failed to query existing visualization edges")?;

    let count: i64 = row.get(0);
    Ok(count > 0)
}