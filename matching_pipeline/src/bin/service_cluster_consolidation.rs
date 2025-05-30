// src/bin/service_cluster_consolidation.rs
use anyhow::{Context, Result};
use chrono::Utc;
use dedupe_lib::db::{self, PgPool};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio_postgres::Client; // Required for transaction
use uuid::Uuid;

// xgraph imports for HeterogeneousGraph
use xgraph::hgraph::h_graph::HeterogeneousGraph;
use xgraph::hgraph::h_node::NodeType;
use xgraph::hgraph::h_edge::EdgeType;
use xgraph::hgraph::algorithms::h_connectivity::HeteroConnectivity; // For find_connected_components
// NodeId is typically usize, often re-exported or directly usable.
// If xgraph::hgraph::h_graph::NodeId is a specific type, use that. Assuming usize for now.
type NodeId = usize;


// Configuration
const BATCH_SIZE_DB_OPS: usize = 500;

/// Represents a row fetched from the service_group table.
#[derive(Debug, Clone)]
struct RawServiceGroup {
    id: String,
    service_id_1: String,
    service_id_2: String,
    pre_rl_confidence_score: f64,
    method_type: String,
}

// --- NodeType and EdgeType Implementations ---
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct ServiceNode(String); // Wrapper for service ID string

impl NodeType for ServiceNode {
    fn as_string(&self) -> String {
        self.0.clone()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)] // Added Serialize for JSON
struct ServiceEdgeDetails(Vec<(String, f64)>); // Wrapper for contributing methods

impl EdgeType for ServiceEdgeDetails {
    fn as_string(&self) -> String {
        // Represent as a simple string, or more complex if needed for other xgraph features
        format!("{} methods", self.0.len())
    }
}
// --- End NodeType and EdgeType ---


/// Detailed output for a cluster, ready for database insertion.
#[derive(Debug, Clone)]
struct ServiceClusterOutput {
    cluster_id_uuid: Uuid,
    cluster_id_str: String,
    name: String,
    description: String,
    service_ids: HashSet<String>, // Store actual service ID strings
    contributing_service_group_data: Vec<(String, String, String)>,
    coherence_score: f64,
    service_count: i32,
    service_group_count: i32,
    internal_edges_for_visualization: Vec<ServiceEdgeVisualizationData>,
}

/// Data for inserting into the service_edge_visualization table.
#[derive(Debug, Clone)]
struct ServiceEdgeVisualizationData {
    service_id_1: String,
    service_id_2: String,
    edge_weight: f64,
    details: serde_json::Value, // JSON of ServiceEdgeDetails.0
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting Service Cluster Consolidation Pipeline (using HeterogeneousGraph)...");

    let env_paths = [".env", ".env.local", "../.env", "../../.env"];
    let mut loaded_env = false;
    for path in env_paths.iter() {
        if Path::new(path).exists() {
            if let Err(e) = db::load_env_from_file(path) {
                warn!("Failed to load environment from {}: {}", path, e);
            } else {
                info!("Loaded environment variables from {}", path);
                loaded_env = true;
                break;
            }
        }
    }
    if !loaded_env {
        info!("No .env file found or failed to load, using environment variables from system if available.");
    }

    let pipeline_run_id = format!(
        "service-consolidation-{}",
        Utc::now().format("%Y%m%d-%H%M%S")
    );
    info!("Pipeline Run ID: {}", pipeline_run_id);

    let pool = db::connect().await.context("Failed to connect to database")?;

    info!("Fetching service_group records...");
    let service_groups = fetch_all_service_groups(&pool).await?;
    info!("Fetched {} service_group records.", service_groups.len());

    if service_groups.is_empty() {
        info!("No service groups to process. Exiting.");
        return Ok(());
    }

    info!("Building service graph...");
    let (service_graph, service_id_to_nodeid_map) = build_service_graph(&service_groups)?;
    info!(
        "Service graph built with {} nodes and {} edges.",
        service_graph.nodes.len(),
        service_graph.get_all_edges().len() // Use get_all_edges().len()
    );

    info!("Identifying clusters using connected components...");
    // HeteroConnectivity trait provides find_connected_components
    let connected_components_result = service_graph.find_connected_components();
    
    match connected_components_result {
        Ok(components) => {
            info!("Found {} connected components.", components.len());

            info!("Processing clusters and calculating coherence scores...");
            let processed_clusters = process_clusters(
                &service_graph,
                components, // Pass the Vec<Vec<NodeId>> from Ok result
                &service_groups,
                &service_id_to_nodeid_map,
            )?;
            info!(
                "Processed {} clusters for storage.",
                processed_clusters.len()
            );

            info!("Storing cluster data in database...");
            store_cluster_data(&pool, &processed_clusters, &pipeline_run_id).await?;
        }
        Err(e) => {
            error!("Failed to find connected components: {:?}", e);
            // Depending on the error type from xgraph, you might need specific handling
            // For now, just context it.
            return Err(anyhow::anyhow!("Connectivity error: {}", e));
        }
    }


    info!("Service Cluster Consolidation Pipeline completed successfully.");
    Ok(())
}

async fn fetch_all_service_groups(pool: &PgPool) -> Result<Vec<RawServiceGroup>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;
    let query = "
        SELECT id, service_id_1, service_id_2, pre_rl_confidence_score, method_type
        FROM public.service_group
        WHERE pre_rl_confidence_score IS NOT NULL
        ORDER BY id
    ";

    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to query service_group table")?;

    let mut service_groups = Vec::with_capacity(rows.len());
    for row in rows {
        let pre_rl_confidence_score: Option<f64> = row.get("pre_rl_confidence_score");
        service_groups.push(RawServiceGroup {
            id: row.get("id"),
            service_id_1: row.get("service_id_1"),
            service_id_2: row.get("service_id_2"),
            pre_rl_confidence_score: pre_rl_confidence_score.unwrap_or(0.0),
            method_type: row.get("method_type"),
        });
    }
    Ok(service_groups)
}

// Graph: Weight W=f64, NodeData N=ServiceNode, EdgeData E=ServiceEdgeDetails
fn build_service_graph(
    service_groups: &[RawServiceGroup],
) -> Result<(
    HeterogeneousGraph<f64, ServiceNode, ServiceEdgeDetails>,
    HashMap<String, NodeId>, // Map Service ID (String) to NodeId (usize)
)> {
    let mut graph = HeterogeneousGraph::new(false); // false for undirected
    let mut service_id_to_nodeid_map: HashMap<String, NodeId> = HashMap::new();
    
    // Temporary map to aggregate edge data before adding to graph
    // Key: (min_node_id, max_node_id) to uniquely identify an edge
    // Value: (total_confidence, Vec<(method_type, confidence)>)
    let mut edge_aggregation_map: HashMap<(NodeId, NodeId), (f64, Vec<(String, f64)>)> = HashMap::new();

    for sg in service_groups {
        if sg.service_id_1.is_empty() || sg.service_id_2.is_empty() {
            warn!("SG {}: empty service_id_1 or service_id_2, skipping.", sg.id);
            continue;
        }
        if sg.service_id_1 == sg.service_id_2 {
            warn!("SG {}: links service to itself, skipping.", sg.id);
            continue;
        }

        let node_id_1 = *service_id_to_nodeid_map
            .entry(sg.service_id_1.clone())
            .or_insert_with(|| graph.add_node(ServiceNode(sg.service_id_1.clone())));
        let node_id_2 = *service_id_to_nodeid_map
            .entry(sg.service_id_2.clone())
            .or_insert_with(|| graph.add_node(ServiceNode(sg.service_id_2.clone())));

        let edge_key = if node_id_1 < node_id_2 {
            (node_id_1, node_id_2)
        } else {
            (node_id_2, node_id_1)
        };

        let (current_total_confidence, current_methods) = 
            edge_aggregation_map.entry(edge_key).or_insert((0.0, Vec::new()));

        *current_total_confidence += sg.pre_rl_confidence_score;
        current_methods.push((sg.method_type.clone(), sg.pre_rl_confidence_score));
    }

    for ((id1, id2), (total_confidence, methods)) in edge_aggregation_map {
        if id1 != id2 { // Should already be handled by keying
            graph.add_edge(id1, id2, total_confidence, ServiceEdgeDetails(methods))
            .map_err(|e| anyhow::anyhow!("Failed to add edge between {:?} and {:?}: {}", id1, id2, e))?;
        }
    }

    Ok((graph, service_id_to_nodeid_map))
}

fn process_clusters(
    graph: &HeterogeneousGraph<f64, ServiceNode, ServiceEdgeDetails>,
    components: Vec<Vec<NodeId>>, // Vec<Vec<usize>>
    all_service_groups: &[RawServiceGroup],
    service_id_to_nodeid_map: &HashMap<String, NodeId>,
) -> Result<Vec<ServiceClusterOutput>> {
    let mut processed_clusters = Vec::new();
    let pb = ProgressBar::new(components.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}",
            )
            .context("Failed to set progress bar style")?
            .progress_chars("#>-"),
    );
    pb.set_message("Processing components into clusters...");

    for component_node_ids in components {
        pb.inc(1);
        if component_node_ids.is_empty() {
            continue;
        }

        let cluster_uuid = Uuid::new_v4();
        let cluster_id_str = cluster_uuid.to_string();

        let mut service_ids_in_component: HashSet<String> = HashSet::new();
        for node_id_val in &component_node_ids {
            if let Some(node_obj) = graph.nodes.get(*node_id_val) {
                service_ids_in_component.insert(node_obj.data.0.clone()); // Access inner String
            } else {
                 warn!("NodeId {} not found in graph.nodes slab during cluster processing component.", node_id_val);
            }
        }
        
        if service_ids_in_component.len() < 1 {
            debug!("Skipping empty or invalid component.");
            continue;
        }

        let mut internal_edge_weights: Vec<f64> = Vec::new();
        let mut internal_edge_details_list: Vec<&ServiceEdgeDetails> = Vec::new();
        let mut internal_edges_for_viz: Vec<ServiceEdgeVisualizationData> = Vec::new();
        let mut actual_internal_edges_count = 0;

        // Use graph.get_all_edges() which returns Vec<(NodeId, NodeId, W, E)>
        for (from_node_id, to_node_id, weight, edge_details_obj) in graph.get_all_edges() {
            if component_node_ids.contains(&from_node_id) && component_node_ids.contains(&to_node_id) {
                internal_edge_weights.push(weight);
                internal_edge_details_list.push(&edge_details_obj); // Store reference to ServiceEdgeDetails
                actual_internal_edges_count += 1;

                let s1_data = &graph.nodes[from_node_id].data.0; // Get String
                let s2_data = &graph.nodes[to_node_id].data.0; // Get String
                
                internal_edges_for_viz.push(ServiceEdgeVisualizationData {
                    service_id_1: if s1_data < s2_data { s1_data.clone() } else { s2_data.clone() },
                    service_id_2: if s1_data < s2_data { s2_data.clone() } else { s1_data.clone() },
                    edge_weight: weight,
                    details: json!(edge_details_obj.0), // Serialize the Vec<(String, f64)>
                });
            }
        }

        let mut contributing_service_group_data: Vec<(String, String, String)> = Vec::new();
        for sg in all_service_groups {
            if let (Some(s1_nodeid), Some(s2_nodeid)) = (
                service_id_to_nodeid_map.get(&sg.service_id_1),
                service_id_to_nodeid_map.get(&sg.service_id_2),
            ) {
                if component_node_ids.contains(s1_nodeid) && component_node_ids.contains(s2_nodeid) {
                    contributing_service_group_data.push((
                        sg.id.clone(),
                        sg.service_id_1.clone(),
                        sg.service_id_2.clone(),
                    ));
                }
            }
        }
        
        let service_count = service_ids_in_component.len();
        let service_group_count_in_cluster = contributing_service_group_data.len();

        let avg_internal_edge_weight = if !internal_edge_weights.is_empty() {
            internal_edge_weights.iter().sum::<f64>() / (internal_edge_weights.len() as f64)
        } else {
            0.0
        };

        let mut unique_method_types: HashSet<String> = HashSet::new();
        for edge_detail_obj in &internal_edge_details_list { // Iterate over &ServiceEdgeDetails
            for (method_type, _) in &edge_detail_obj.0 { // Access inner Vec
                unique_method_types.insert(method_type.clone());
            }
        }
        let method_diversity_score = (1.0 + (unique_method_types.len() as f64).ln()).max(1.0);

        let density = if service_count > 1 {
            let max_possible_edges = (service_count * (service_count - 1)) / 2;
            if max_possible_edges > 0 {
                (actual_internal_edges_count as f64) / (max_possible_edges as f64)
            } else {
                1.0
            }
        } else {
            1.0
        };

        let coherence_score = avg_internal_edge_weight * method_diversity_score * density;

        processed_clusters.push(ServiceClusterOutput {
            cluster_id_uuid: cluster_uuid,
            cluster_id_str: cluster_id_str.clone(),
            name: format!("ServiceCluster-{}", cluster_id_str.split('-').next().unwrap_or("")),
            description: format!(
                "Cluster of {} services identified by connected components.",
                service_count
            ),
            service_ids: service_ids_in_component,
            contributing_service_group_data,
            coherence_score,
            service_count: service_count as i32,
            service_group_count: service_group_count_in_cluster as i32,
            internal_edges_for_visualization: internal_edges_for_viz,
        });
    }
    pb.finish_with_message("Cluster processing complete.");
    Ok(processed_clusters)
}


async fn store_cluster_data(
    pool: &PgPool,
    clusters: &[ServiceClusterOutput],
    pipeline_run_id: &str,
) -> Result<()> {
    if clusters.is_empty() {
        info!("No clusters to store.");
        return Ok(());
    }

    let mut client = pool
        .get()
        .await
        .context("Failed to get DB client for storing cluster data")?;

    info!("Batch inserting service_group_cluster records...");
    let mut cluster_insert_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        Vec::new();
    let mut cluster_insert_values_str = Vec::new();
    let mut i = 1;
    for cluster in clusters {
        cluster_insert_values_str.push(format!(
            "(${}, ${}, ${}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ${}, ${}, ${})",
            i,
            i + 1,
            i + 2,
            i + 3,
            i + 4,
            i + 5
        ));
        cluster_insert_params.push(Box::new(cluster.cluster_id_str.clone()));
        cluster_insert_params.push(Box::new(cluster.name.clone()));
        cluster_insert_params.push(Box::new(cluster.description.clone()));
        cluster_insert_params.push(Box::new(cluster.service_count));
        cluster_insert_params.push(Box::new(cluster.service_group_count));
        cluster_insert_params.push(Box::new(cluster.coherence_score));
        i += 6;
    }

    if !cluster_insert_params.is_empty() {
        let cluster_insert_query = format!(
            "INSERT INTO public.service_group_cluster (id, name, description, created_at, updated_at, service_count, service_group_count, average_coherence_score) VALUES {}
            ON CONFLICT (id) DO NOTHING",
            cluster_insert_values_str.join(", ")
        );

        let params_slice: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            cluster_insert_params
                .iter()
                .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

        client
            .execute(&cluster_insert_query, &params_slice[..])
            .await
            .context("Failed to batch insert service_group_cluster records")?;
        info!(
            "Successfully inserted/updated {} service_group_cluster records.",
            clusters.len()
        );
    }

    info!("Batch updating service_group records with cluster IDs...");
    let transaction = client
        .transaction()
        .await
        .context("Failed to start transaction for service_group updates")?;
    let update_pb = ProgressBar::new(clusters.len() as u64);
    update_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}",
            )
            .context("Failed to set progress bar style")?
            .progress_chars("#>-"),
    );
    update_pb.set_message("Updating service_groups...");

    for cluster in clusters {
        update_pb.inc(1);
        if cluster.contributing_service_group_data.is_empty() {
            continue;
        }
        let service_group_ids_to_update: Vec<String> = cluster
            .contributing_service_group_data
            .iter()
            .map(|(id, _, _)| id.clone())
            .collect();

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
    update_pb.finish_with_message("Service_group updates complete.");

    info!("Batch inserting service_edge_visualization records...");
    let viz_client = pool.get().await.context("Failed to get new DB client for viz edges")?;

    let mut viz_edge_insert_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        Vec::new();
    let mut viz_edge_insert_values_str = Vec::new();
    let mut current_val_idx = 1;
    let mut total_viz_edges = 0;

    for cluster in clusters {
        for viz_edge in &cluster.internal_edges_for_visualization {
            total_viz_edges += 1;
            let edge_id = Uuid::new_v4().to_string();
            viz_edge_insert_values_str.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, CURRENT_TIMESTAMP)",
                current_val_idx,
                current_val_idx + 1,
                current_val_idx + 2,
                current_val_idx + 3,
                current_val_idx + 4,
                current_val_idx + 5,
                current_val_idx + 6
            ));
            viz_edge_insert_params.push(Box::new(edge_id));
            viz_edge_insert_params.push(Box::new(cluster.cluster_id_str.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.service_id_1.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.service_id_2.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.edge_weight));
            viz_edge_insert_params.push(Box::new(viz_edge.details.clone()));
            viz_edge_insert_params.push(Box::new(pipeline_run_id.to_string()));
            current_val_idx += 7;

            if viz_edge_insert_params.len() >= BATCH_SIZE_DB_OPS * 7 {
                let query = format!(
                    "INSERT INTO public.service_edge_visualization (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id, created_at) VALUES {}
                     ON CONFLICT (service_group_cluster_id, service_id_1, service_id_2, pipeline_run_id) DO NOTHING",
                    viz_edge_insert_values_str.join(", ")
                );
                let params_slice_viz: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    viz_edge_insert_params
                        .iter()
                        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                        .collect();

                viz_client.execute(&query, &params_slice_viz[..]).await
                    .context("Failed to batch insert service_edge_visualization records (mid-batch)")?;

                viz_edge_insert_params.clear();
                viz_edge_insert_values_str.clear();
                current_val_idx = 1;
            }
        }
    }

    if !viz_edge_insert_params.is_empty() {
        let query = format!(
            "INSERT INTO public.service_edge_visualization (id, service_group_cluster_id, service_id_1, service_id_2, edge_weight, details, pipeline_run_id, created_at) VALUES {}
             ON CONFLICT (service_group_cluster_id, service_id_1, service_id_2, pipeline_run_id) DO NOTHING",
            viz_edge_insert_values_str.join(", ")
        );
        let params_slice_viz: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            viz_edge_insert_params
                .iter()
                .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();
        
         viz_client.execute(&query, &params_slice_viz[..]).await
            .context("Failed to batch insert remaining service_edge_visualization records")?;
    }
    info!(
        "Successfully inserted/updated {} service_edge_visualization records.",
        total_viz_edges
    );

    Ok(())
}
