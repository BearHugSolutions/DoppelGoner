// src/consolidate_clusters.rs
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use rand::seq::SliceRandom;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio_postgres::GenericClient;
use uuid::Uuid;

// xgraph imports
use xgraph::leiden_clustering::{CommunityConfig, CommunityDetection};
use xgraph::Graph;

// Local imports
use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType, EntityGroupRecord, EntityId, GroupClusterId, NewSuggestedAction, NodeMapper, SuggestionStatus, VisualizationEdgeData
};
use crate::utils::{cosine_similarity_candle, cosine_similarity_manual};

/// Main entry point for the cluster consolidation process.
/// This function now orchestrates building an entity-based graph, running community detection,
/// and saving the resulting clusters and their visualization edges.
pub async fn process_clusters(pool: &PgPool, pipeline_run_id: &str) -> Result<usize> {
    info!(
        "Starting unified cluster consolidation and visualization edge calculation (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    // 1. Build the graph with entities as nodes and consolidated weights as edges.
    // This function is the new core of the process.
    let (graph, node_mapper, visualization_edges) =
        build_entity_graph(pool, pipeline_run_id)
            .await
            .context("Failed to build the entity graph")?;

    if graph.nodes.is_empty() {
        info!("No unclustered entities found to build graph. Skipping consolidation.");
        return Ok(0);
    }

    info!(
        "Graph built with {} entity nodes and {} consolidated edges. Starting Leiden clustering...",
        node_mapper.get_node_count(),
        graph.edges.len()
    );

    // 2. Run Leiden community detection on the entity graph.
    let leiden_config = CommunityConfig {
        resolution: config::LEIDEN_RESOLUTION_PARAMETER,
        deterministic: true,
        seed: Some(42),
        iterations: 10,
        gamma: 1.0,
    };
    let leiden_start = Instant::now();
    let communities_result = graph
        .detect_communities_with_config(leiden_config)
        .context("Leiden community detection failed")?;
    info!(
        "Leiden algorithm finished in {:.2?}, found {} communities.",
        leiden_start.elapsed(),
        communities_result.len()
    );

    let mut clusters_created = 0;
    let mut new_cluster_ids_for_verification = Vec::new();

    // 3. Process each community into a final cluster.
    for node_indices_in_community in communities_result {
        // FIX: The community result is a tuple (usize, Vec<usize>), we need the vector at index 1.
        let entity_ids_in_cluster: HashSet<EntityId> = node_indices_in_community
            .1 // Access the vector of node indices from the tuple
            .iter()
            .filter_map(|&node_idx| node_mapper.get_id(node_idx).cloned())
            .collect();

        if entity_ids_in_cluster.len() < 2 {
            debug!(
                "Skipping community with < 2 entities (found {}).",
                entity_ids_in_cluster.len()
            );
            continue;
        }

        // 4. Save the new cluster to the database.
        let new_cluster_id = GroupClusterId(Uuid::new_v4().to_string());
        create_cluster_and_update_groups(pool, &new_cluster_id, &entity_ids_in_cluster)
            .await?;
        
        new_cluster_ids_for_verification.push(new_cluster_id);
        clusters_created += 1;
    }
    
    info!("Completed processing Leiden communities. {} new clusters created.", clusters_created);

    // 5. Save the pre-calculated visualization edges to the database.
    if !visualization_edges.is_empty() {
        info!("Inserting {} visualization edges...", visualization_edges.len());
        // We need to update the cluster_id for each edge now that we've formed clusters.
        let updated_edges = update_edge_cluster_ids(pool, visualization_edges).await?;
        db::bulk_insert_visualization_edges(pool, &updated_edges).await?;
    }

    // 6. Run verification on the newly created clusters.
    if !new_cluster_ids_for_verification.is_empty() {
        info!("Starting cluster verification...");
        // This function should adapt well as it already works with entities.
        verify_clusters_optimized(pool, &new_cluster_ids_for_verification, pipeline_run_id).await;
    }

    info!(
        "Cluster consolidation process finished in {:.2?}. Total {} clusters created.",
        start_time.elapsed(),
        clusters_created
    );
    Ok(clusters_created)
}

/// Builds a graph where nodes are entities and edges are weighted by consolidating all
/// matching methods between any two entities.
async fn build_entity_graph(
    pool: &PgPool,
    pipeline_run_id: &str,
) -> Result<(
    Graph<f64, EntityId, ()>,
    NodeMapper<EntityId>,
    Vec<VisualizationEdgeData>,
)> {
    let mut conn = pool.get().await.context("Failed to get DB connection")?;
    let transaction = conn.transaction().await.context("Failed to start transaction")?;

    info!("Fetching all unclustered entity_group records...");
    let group_records = fetch_unclustered_group_records(&transaction).await?;
    info!("Found {} unclustered group records.", group_records.len());

    if group_records.is_empty() {
        return Ok((
            Graph::new(false),
            NodeMapper::new(),
            Vec::new(),
        ));
    }
    
    // Aggregate all matching methods by the pair of entities they connect.
    let mut entity_pairs_to_methods: HashMap<(EntityId, EntityId), Vec<EntityGroupRecord>> = HashMap::new();
    let mut all_entities: HashSet<EntityId> = HashSet::new();

    for record in group_records {
        let entity_id_1: EntityId = transaction.query_one("SELECT entity_id_1 FROM public.entity_group WHERE id = $1", &[&record.id]).await?.get(0);
        let entity_id_2: EntityId = transaction.query_one("SELECT entity_id_2 FROM public.entity_group WHERE id = $1", &[&record.id]).await?.get(0);

        all_entities.insert(entity_id_1.clone());
        all_entities.insert(entity_id_2.clone());

        let key = if entity_id_1.0 < entity_id_2.0 {
            (entity_id_1, entity_id_2)
        } else {
            (entity_id_2, entity_id_1)
        };
        entity_pairs_to_methods.entry(key).or_default().push(record);
    }
    
    transaction.commit().await?;
    
    let mut graph = Graph::new(false); // Undirected graph
    let mut node_mapper = NodeMapper::new();
    let mut visualization_edges = Vec::new();

    // Add all unique entities as nodes to the graph.
    for entity_id in all_entities {
        node_mapper.get_or_add_node(&mut graph, entity_id);
    }
    
    // Get the reinforcement learning weight once for all calculations.
    let rl_weight = db::get_rl_weight_from_feedback(pool).await?;

    info!("Calculating consolidated edge weights for {} entity pairs...", entity_pairs_to_methods.len());
    for ((entity1_id, entity2_id), methods) in entity_pairs_to_methods {
        if methods.is_empty() {
            continue;
        }

        // Calculate a single, consolidated weight for the edge.
        let (edge_weight, details_json) =
            calculate_consolidated_edge(&methods, rl_weight)?;

        // Add edge to the graph for clustering if it meets a certain threshold.
        if edge_weight > config::WEAK_LINK_THRESHOLD {
            let node1_idx = node_mapper.get_or_add_node(&mut graph, entity1_id.clone());
            let node2_idx = node_mapper.get_or_add_node(&mut graph, entity2_id.clone());
            if let Err(e) = graph.add_edge(node1_idx, node2_idx, edge_weight, ()) {
                error!("Failed to add edge to xgraph between {} and {}: {}", entity1_id.0, entity2_id.0, e);
            }
        }
        
        // Always create a visualization edge, regardless of the clustering threshold.
        // The cluster_id will be populated later.
        visualization_edges.push(VisualizationEdgeData {
            cluster_id: "unclustered".to_string(), // Placeholder
            entity_id_1: entity1_id.0,
            entity_id_2: entity2_id.0,
            edge_weight,
            details: details_json,
            pipeline_run_id: pipeline_run_id.to_string(),
        });
    }

    Ok((graph, node_mapper, visualization_edges))
}


/// Fetches all entity_group records that have not yet been assigned to a cluster.
async fn fetch_unclustered_group_records(
    client: &impl GenericClient,
) -> Result<Vec<EntityGroupRecord>> {
    let rows = client.query(
        "SELECT id, method_type, confidence_score, pre_rl_confidence_score 
         FROM public.entity_group WHERE group_cluster_id IS NULL",
        &[],
    ).await.context("Failed to fetch unclustered entity_group records")?;

    Ok(rows
        .into_iter()
        .map(|row| EntityGroupRecord {
            id: row.get("id"),
            method_type: row.get("method_type"),
            confidence_score: row.get("confidence_score"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
        })
        .collect())
}

/// This logic is moved from `cluster_visualization.rs` to serve as the single
/// source of truth for edge weight calculation.
fn calculate_consolidated_edge(
    matching_methods: &[EntityGroupRecord],
    rl_weight: f64,
) -> Result<(f64, serde_json::Value)> {
    let pre_rl_weight = 1.0 - rl_weight;
    let mut method_details = Vec::new();
    let mut method_confidences = Vec::new();

    for method in matching_methods {
        let pre_rl_conf = method.pre_rl_confidence_score.unwrap_or(0.0);
        let rl_conf = method.confidence_score.unwrap_or(pre_rl_conf);
        let combined_confidence = (pre_rl_weight * pre_rl_conf) + (rl_weight * rl_conf);
        method_confidences.push(combined_confidence);

        method_details.push(json!({
            "method_type": method.method_type,
            "pre_rl_confidence": pre_rl_conf,
            "rl_confidence": rl_conf,
            "combined_confidence": combined_confidence
        }));
    }

    // Probabilistic union of confidences: 1 - Π(1 - ci)
    let edge_weight = 1.0 - method_confidences
        .iter()
        .fold(1.0, |acc, &conf| acc * (1.0 - conf.max(0.0).min(1.0)));

    let details_json = json!({
        "methods": method_details,
        "rl_weight_factor": rl_weight,
        "method_count": matching_methods.len()
    });

    Ok((edge_weight, details_json))
}

/// Creates a new cluster record and updates all fully contained entity_group records.
async fn create_cluster_and_update_groups(
    pool: &PgPool,
    new_cluster_id: &GroupClusterId,
    entity_ids_in_cluster: &HashSet<EntityId>,
) -> Result<()> {
    let mut conn = pool.get().await.context("DB connection failed")?;
    let transaction = conn.transaction().await.context("Transaction start failed")?;

    let entity_id_strings: Vec<String> = entity_ids_in_cluster.iter().map(|e| e.0.clone()).collect();

    // Find all entity_group records that are fully contained within this set of entities.
    let groups_in_cluster_rows = transaction.query(
        "SELECT id FROM public.entity_group WHERE entity_id_1 = ANY($1) AND entity_id_2 = ANY($1)",
        &[&entity_id_strings]
    ).await.context("Failed to find groups within new cluster")?;

    let group_ids_in_cluster: Vec<String> = groups_in_cluster_rows.iter().map(|row| row.get("id")).collect();

    let entity_count = entity_ids_in_cluster.len() as i32;
    let group_count = group_ids_in_cluster.len() as i32;

    info!(
        "Creating cluster {} with {} entities and {} groups.",
        new_cluster_id.0, entity_count, group_count
    );
    
    // Create the new cluster record.
    let now = Utc::now().naive_utc();
    let cluster_name = format!("LeidenCluster-{}", &new_cluster_id.0[..8]);
    transaction.execute(
        "INSERT INTO public.entity_group_cluster (id, name, created_at, updated_at, entity_count, group_count)
         VALUES ($1, $2, $3, $4, $5, $6)",
        &[&new_cluster_id.0, &cluster_name, &now, &now, &entity_count, &group_count]
    ).await.context("Failed to insert new cluster record")?;

    // Bulk-update the contained entity_group records.
    if !group_ids_in_cluster.is_empty() {
        transaction.execute(
            "UPDATE public.entity_group SET group_cluster_id = $1, updated_at = $2 WHERE id = ANY($3)",
            &[&new_cluster_id.0, &now, &group_ids_in_cluster]
        ).await.context("Failed to update entity_group with new cluster_id")?;
    }

    transaction.commit().await.context("Failed to commit cluster creation")?;
    Ok(())
}

/// After clusters are formed, this function updates the `cluster_id` field for the
/// visualization edges that were calculated earlier.
async fn update_edge_cluster_ids(
    pool: &PgPool,
    mut edges: Vec<VisualizationEdgeData>,
) -> Result<Vec<VisualizationEdgeData>> {
    info!("Mapping visualization edges to their final cluster IDs...");
    let conn = pool.get().await?;
    let mut entity_to_cluster_map: HashMap<String, String> = HashMap::new();

    // This could be optimized, but for clarity, we'll fetch all clustered entities.
    let rows = conn.query(
        "SELECT entity_id_1, group_cluster_id FROM public.entity_group WHERE group_cluster_id IS NOT NULL
         UNION
         SELECT entity_id_2, group_cluster_id FROM public.entity_group WHERE group_cluster_id IS NOT NULL",
        &[]
    ).await?;

    for row in rows {
        let entity_id: String = row.get(0);
        let cluster_id: String = row.get(1);
        entity_to_cluster_map.insert(entity_id, cluster_id);
    }

    for edge in &mut edges {
        if let Some(cluster_id) = entity_to_cluster_map.get(&edge.entity_id_1) {
            // We assume if one entity is in a cluster, the other is too, since the edge connects them.
            edge.cluster_id = cluster_id.clone();
        }
    }
    info!("Finished mapping edges to clusters.");
    Ok(edges)
}


// The verification logic can now be called on the new clusters.
// This function needs to be adapted to fetch entities from the newly formed clusters.
async fn verify_clusters_optimized(
    // FIX: Add underscores to unused variables to resolve warnings.
    _pool: &PgPool,
    new_cluster_ids: &[GroupClusterId],
    _pipeline_run_id: &str,
) {
    info!(
        "Verifying quality of {} newly created clusters...",
        new_cluster_ids.len()
    );
    // The existing verification logic that samples pairs of entities within a cluster
    // and calculates a coherence score should still be valid.
    // The main change is how we get the list of entities for a cluster.
    
    // (Implementation of verification logic would go here, it's complex and
    // is assumed to be mostly correct from the original file, needing only minor
    // adjustments to how it queries for entities within a cluster.)
    warn!("Cluster verification logic is complex and has been stubbed out in this refactoring. Please reintegrate your existing verification logic here.");

}