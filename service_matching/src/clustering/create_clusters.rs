// src/clustering/create_clusters.rs

use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

// petgraph imports
use petgraph::algo::connected_components;
use petgraph::graph::{NodeIndex, UnGraph};

use crate::utils::db_connect::PgPool;
use crate::clustering::db::{fetch_all_service_groups, store_service_cluster_data}; // Import database functions

/// Represents a row fetched from the public.service_group table.
/// This struct holds the raw data for a matched pair of services.
#[derive(Debug, Clone)]
pub struct RawServiceGroup {
    pub id: String,
    pub service_id_1: String,
    pub service_id_2: String,
    pub confidence_score: Option<f64>,
    pub pre_rl_confidence_score: Option<f64>,
    pub method_type: String,
}

/// Node data for the service graph.
/// Each node in the graph represents a unique service.
#[derive(Debug, Clone)]
pub struct ServiceNode {
    pub service_id: String,
}

/// Edge data storing multiple matching methods and their confidences between two services.
/// This struct aggregates information about how two services are linked.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceEdgeDetails {
    pub contributing_methods: Vec<(String, f64)>, // (method_type, confidence)
    pub total_confidence: f64,          // Original sum of confidence, kept for context
    pub pre_rl_total_confidence: f64,   // Original sum of pre-RL confidence, kept for context
    pub calculated_edge_weight: f64,    // The new, sophisticated edge weight
}

/// Detailed output for a service cluster, ready for database insertion.
/// This struct encapsulates all information about a discovered cluster of services.
#[derive(Debug, Clone)]
pub struct ServiceClusterOutput {
    pub cluster_id_uuid: Uuid,
    pub cluster_id_str: String,
    pub name: String,
    pub description: String,
    pub service_ids: HashSet<String>,
    pub contributing_service_group_data: Vec<(String, String, String)>, // (group_id, service_id_1, service_id_2)
    pub coherence_score: ServiceCoherenceScore,
    pub service_count: i32,
    pub service_group_count: i32,
    pub internal_edges_for_visualization: Vec<ServiceEdgeVisualizationData>,
}

/// Data for inserting into the public.service_edge_visualization table.
/// This is used to visualize the relationships within a cluster.
#[derive(Debug, Clone)]
pub struct ServiceEdgeVisualizationData {
    pub service_id_1: String,
    pub service_id_2: String,
    pub edge_weight: f64, // This will now use calculated_edge_weight
    pub details: serde_json::Value,
}

/// Sophisticated coherence score calculation for service clusters.
/// This score evaluates the quality and consistency of a service cluster.
#[derive(Debug, Clone)]
pub struct ServiceCoherenceScore {
    /// Ratio of actual edges to possible edges in the cluster (0.0 to 1.0)
    pub density: f64,
    /// Average confidence of all edges in the cluster (0.0 to 1.0) - now uses calculated_edge_weight
    pub average_confidence: f64,
    /// Variance in confidence scores (lower is better) - now uses calculated_edge_weight
    pub confidence_variance: f64,
    /// Consistency of matching methods across services (0.0 to 1.0)
    pub method_consistency: f64,
    /// Number of different types of evidence
    pub evidence_diversity: f64,
    /// Overall weighted coherence score (0.0 to 1.0)
    pub overall_score: f64,
    /// Total number of edges in the cluster
    pub total_edges: usize,
    /// Number of services in the cluster
    pub cluster_size: usize,
    /// Standard deviation of confidence scores
    pub confidence_std_dev: f64,
}

impl Default for ServiceCoherenceScore {
    fn default() -> Self {
        Self {
            density: 0.0,
            average_confidence: 0.0,
            confidence_variance: 0.0,
            method_consistency: 0.0,
            evidence_diversity: 0.0,
            overall_score: 0.0,
            total_edges: 0,
            cluster_size: 0,
            confidence_std_dev: 0.0,
        }
    }
}

/// Orchestrates the entire service clustering process.
/// This function fetches service groups, builds a graph, identifies connected components
/// (clusters), processes them, calculates coherence scores, and stores the results
/// in the database.
///
/// Arguments:
/// * `pool` - A reference to the PostgreSQL connection pool.
/// * `pipeline_run_id` - The ID of the current pipeline run.
/// * `multi_progress` - An optional `MultiProgress` instance for progress tracking.
///
/// Returns:
/// A `Result` containing the total number of processed clusters on success, or an `anyhow::Error` on failure.
pub async fn run_service_clustering(
    pool: &PgPool,
    pipeline_run_id: &str,
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    // Create a main progress bar for the service clustering phase
    let main_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(5)); // 5 main steps for clustering
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  {spinner:.cyan} [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Starting service clustering...");
        Some(pb)
    } else {
        None
    };

    // Step 1: Fetch all service groups from the database
    info!("Fetching service groups from database...");
    if let Some(pb) = &main_pb {
        pb.set_message("Fetching service groups...");
    }
    let service_groups = fetch_all_service_groups(pool, multi_progress.clone()).await?;
    info!(
        "Fetched {} service groups from database.",
        service_groups.len()
    );
    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Building service graph...");
    }

    // Step 2: Build the service graph from the fetched service groups
    info!("Building service graph...");
    let (service_graph, service_id_to_nodeidx_map) =
        build_service_graph(&service_groups, multi_progress.clone())?;
    info!(
        "Service graph built with {} nodes and {} edges.",
        service_graph.node_count(),
        service_graph.edge_count()
    );
    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Identifying connected components...");
    }

    // Step 3: Identify connected components (clusters) in the graph
    info!("Identifying service clusters using connected components...");
    let num_components = connected_components(&service_graph);
    info!("Found {} connected components (potential clusters).", num_components);
    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Processing service clusters...");
    }

    // Step 4: Process the identified clusters, calculate coherence scores, and prepare output
    info!("Processing service clusters and calculating coherence scores...");
    let processed_clusters = process_service_clusters(
        &service_graph,
        &service_groups,
        &service_id_to_nodeidx_map,
        multi_progress.clone(),
    )?;
    info!(
        "Processed {} service clusters for storage.",
        processed_clusters.len()
    );
    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Storing service cluster data...");
    }

    // Step 5: Store the processed cluster data in the database
    info!("Storing service cluster data in database...");
    store_service_cluster_data(
        &pool,
        &processed_clusters,
        pipeline_run_id,
        multi_progress.clone(),
    )
    .await?;
    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.finish_with_message("Service clustering complete");
    }

    info!("Service Cluster Consolidation Pipeline completed successfully.");

    // Log coherence statistics for the generated clusters
    log_coherence_statistics(&processed_clusters);

    Ok(processed_clusters.len())
}

/// Builds an undirected graph of services based on `RawServiceGroup` records.
/// Services are nodes, and service groups represent edges. Multiple service groups
/// between the same two services are aggregated into a single edge with combined details.
///
/// Arguments:
/// * `service_groups` - A slice of `RawServiceGroup` records.
/// * `multi_progress` - An optional `MultiProgress` instance for progress tracking.
///
/// Returns:
/// A `Result` containing a tuple of `(UnGraph<ServiceNode, ServiceEdgeDetails>, HashMap<String, NodeIndex>)`
/// on success, or an `anyhow::Error` on failure.
fn build_service_graph(
    service_groups: &[RawServiceGroup],
    multi_progress: Option<MultiProgress>,
) -> Result<(
    UnGraph<ServiceNode, ServiceEdgeDetails>,
    HashMap<String, NodeIndex>,
)> {
    // Create progress bar for graph building
    let graph_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(service_groups.len() as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.green} [{elapsed_precise}] {bar:25.cyan/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Building graph...");
        Some(pb)
    } else {
        None
    };

    let mut graph = UnGraph::new_undirected();
    let mut service_id_to_nodeidx_map: HashMap<String, NodeIndex> = HashMap::new();

    // Temporary map to aggregate edge data before adding to graph and calculating final weight
    // Key: (min_node_idx, max_node_idx) to uniquely identify an edge between two services
    // Value: ServiceEdgeDetails
    let mut edge_aggregation_map: HashMap<(NodeIndex, NodeIndex), ServiceEdgeDetails> =
        HashMap::new();

    for (i, sg) in service_groups.iter().enumerate() {
        if let Some(pb) = &graph_pb {
            pb.inc(1);
            if i % 1000 == 0 { // Update message periodically for large datasets
                pb.set_message(format!("Building graph... ({}/{})", i, service_groups.len()));
            }
        }

        // Validate service IDs
        if sg.service_id_1.is_empty() || sg.service_id_2.is_empty() {
            warn!("SG {}: empty service_id_1 or service_id_2, skipping.", sg.id);
            continue;
        }
        if sg.service_id_1 == sg.service_id_2 {
            warn!("SG {}: links service to itself, skipping.", sg.id);
            continue;
        }

        // Get or create node indices for the two services
        let node_idx_1 = *service_id_to_nodeidx_map
            .entry(sg.service_id_1.clone())
            .or_insert_with(|| {
                graph.add_node(ServiceNode {
                    service_id: sg.service_id_1.clone(),
                })
            });
        let node_idx_2 = *service_id_to_nodeidx_map
            .entry(sg.service_id_2.clone())
            .or_insert_with(|| {
                graph.add_node(ServiceNode {
                    service_id: sg.service_id_2.clone(),
                })
            });

        // Determine a canonical key for the edge to aggregate multiple links between the same two services
        let edge_key = if node_idx_1 < node_idx_2 {
            (node_idx_1, node_idx_2)
        } else {
            (node_idx_2, node_idx_1)
        };

        let confidence = sg.confidence_score.unwrap_or(0.0);
        let pre_rl_confidence = sg.pre_rl_confidence_score.unwrap_or(0.0);

        // Aggregate edge details
        edge_aggregation_map
            .entry(edge_key)
            .and_modify(|details| {
                details.contributing_methods.push((sg.method_type.clone(), confidence));
                details.total_confidence += confidence;
                details.pre_rl_total_confidence += pre_rl_confidence;
            })
            .or_insert(ServiceEdgeDetails {
                contributing_methods: vec![(sg.method_type.clone(), confidence)],
                total_confidence: confidence,
                pre_rl_total_confidence: pre_rl_confidence,
                calculated_edge_weight: 0.0, // This will be calculated in the next step
            });
    }

    // Now, iterate through the aggregated edge details to calculate the sophisticated edge weight
    for ((idx1, idx2), mut edge_details) in edge_aggregation_map.drain() {
        edge_details.calculated_edge_weight =
            calculate_sophisticated_edge_weight(&edge_details.contributing_methods);
        graph.add_edge(idx1, idx2, edge_details);
    }

    // Finish the graph building progress bar
    if let Some(pb) = &graph_pb {
        pb.finish_with_message(format!(
            "Graph built: {} nodes, {} edges",
            graph.node_count(),
            graph.edge_count()
        ));
    }

    Ok((graph, service_id_to_nodeidx_map))
}

/// Processes connected components in the service graph into `ServiceClusterOutput` structs.
/// For each component, it extracts service IDs, collects internal edges, calculates
/// a coherence score, and prepares the data for database insertion.
///
/// Arguments:
/// * `graph` - The constructed `UnGraph` of services.
/// * `all_service_groups` - A slice of all `RawServiceGroup` records.
/// * `service_id_to_nodeidx_map` - A map from service ID to NodeIndex.
/// * `multi_progress` - An optional `MultiProgress` instance for progress tracking.
///
/// Returns:
/// A `Result` containing a `Vec<ServiceClusterOutput>` on success, or an `anyhow::Error` on failure.
fn process_service_clusters(
    graph: &UnGraph<ServiceNode, ServiceEdgeDetails>,
    all_service_groups: &[RawServiceGroup],
    service_id_to_nodeidx_map: &HashMap<String, NodeIndex>,
    multi_progress: Option<MultiProgress>,
) -> Result<Vec<ServiceClusterOutput>> {
    let mut processed_clusters = Vec::new();

    // Find all connected components using a manual DFS/BFS to get node indices for each component
    let mut visited = vec![false; graph.node_count()];
    let mut components = Vec::new();

    for node_idx in graph.node_indices() {
        if !visited[node_idx.index()] {
            let mut component = Vec::new();
            let mut stack = vec![node_idx]; // Use a stack for DFS

            while let Some(current) = stack.pop() {
                if visited[current.index()] {
                    continue;
                }
                visited[current.index()] = true;
                component.push(current);

                // Add neighbors to stack
                for neighbor in graph.neighbors(current) {
                    if !visited[neighbor.index()] {
                        stack.push(neighbor);
                    }
                }
            }

            if !component.is_empty() {
                components.push(component);
            }
        }
    }

    // Create a progress bar for processing individual components
    let pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(components.len() as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("    {spinner:.magenta} [{elapsed_precise}] {bar:25.yellow/red} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Processing components into clusters...");
        Some(pb)
    } else {
        None
    };

    // Iterate through each connected component and process it into a cluster
    for (i, component_node_indices) in components.iter().enumerate() {
        if let Some(pb) = &pb {
            pb.inc(1);
            if i % 100 == 0 { // Update message periodically
                pb.set_message(format!(
                    "Processing clusters... ({}/{})",
                    i,
                    components.len()
                ));
            }
        }

        if component_node_indices.is_empty() {
            continue;
        }

        let cluster_uuid = Uuid::new_v4();
        let cluster_id_str = cluster_uuid.to_string();

        // Extract service IDs from the current component's nodes
        let mut service_ids_in_component: HashSet<String> = HashSet::new();
        for &node_idx in component_node_indices {
            if let Some(node_data) = graph.node_weight(node_idx) {
                service_ids_in_component.insert(node_data.service_id.clone());
            }
        }

        if service_ids_in_component.len() < 1 {
            debug!("Skipping empty or invalid component.");
            continue;
        }

        // Collect internal edges data and prepare for visualization
        let mut internal_edges_data = Vec::new();
        let mut internal_edges_for_viz = Vec::new();

        // Iterate through all pairs of nodes within the component to find edges
        for i in 0..component_node_indices.len() {
            for j in (i + 1)..component_node_indices.len() {
                let node1 = component_node_indices[i];
                let node2 = component_node_indices[j];

                // Check if an edge exists between the two nodes
                if let Some(edge_idx) = graph.find_edge(node1, node2) {
                    if let Some(edge_data) = graph.edge_weight(edge_idx) {
                        internal_edges_data.push(edge_data); // For coherence calculation

                        let service1 = &graph[node1].service_id;
                        let service2 = &graph[node2].service_id;

                        // Store data for service_edge_visualization table
                        internal_edges_for_viz.push(ServiceEdgeVisualizationData {
                            service_id_1: if service1 < service2 {
                                service1.clone()
                            } else {
                                service2.clone()
                            },
                            service_id_2: if service1 < service2 {
                                service2.clone()
                            } else {
                                service1.clone()
                            },
                            edge_weight: edge_data.calculated_edge_weight, // Use the new calculated weight here
                            details: json!(edge_data), // Serialize edge details to JSONB
                        });
                    }
                }
            }
        }

        // Find all RawServiceGroup records that contributed to this cluster
        let mut contributing_service_group_data: Vec<(String, String, String)> = Vec::new();
        for sg in all_service_groups {
            if let (Some(&s1_nodeidx), Some(&s2_nodeidx)) = (
                service_id_to_nodeidx_map.get(&sg.service_id_1),
                service_id_to_nodeidx_map.get(&sg.service_id_2),
            ) {
                // Check if both services of the group are within the current component
                if component_node_indices.contains(&s1_nodeidx)
                    && component_node_indices.contains(&s2_nodeidx)
                {
                    contributing_service_group_data.push((
                        sg.id.clone(),
                        sg.service_id_1.clone(),
                        sg.service_id_2.clone(),
                    ));
                }
            }
        }

        // Calculate the sophisticated coherence score for the current cluster
        let coherence_score =
            calculate_service_coherence_score(component_node_indices, &internal_edges_data);

        let service_count = service_ids_in_component.len();
        let service_group_count_in_cluster = contributing_service_group_data.len();

        // Create the ServiceClusterOutput struct
        processed_clusters.push(ServiceClusterOutput {
            cluster_id_uuid: cluster_uuid,
            cluster_id_str: cluster_id_str.clone(),
            name: format!("ServiceCluster-{}", cluster_id_str.split('-').next().unwrap_or("")),
            description: format!(
                "Cluster of {} services identified by connected components with coherence score {:.3}.",
                service_count, coherence_score.overall_score
            ),
            service_ids: service_ids_in_component,
            contributing_service_group_data,
            coherence_score,
            service_count: service_count as i32,
            service_group_count: service_group_count_in_cluster as i32,
            internal_edges_for_visualization: internal_edges_for_viz,
        });
    }

    if let Some(pb) = &pb {
        pb.finish_with_message("Service cluster processing complete.");
    }

    Ok(processed_clusters)
}

/// Calculates the sophisticated edge weight based on contributing method confidences.
/// This function implements the logic for:
/// - One weak connection as a very poor edge weight.
/// - A single moderate to strong connection as an above average edge weight.
/// - Multiple strong connections as very close to 1.
///
/// The approach uses a transformation of individual confidences, aggressively penalizing
/// low scores and slightly boosting higher scores. These transformed scores are then
/// combined using a formula similar to combining probabilities (1 - product of disbeliefs),
/// which naturally exhibits saturating behavior towards 1.
fn calculate_sophisticated_edge_weight(
    contributing_methods: &[(String, f64)],
) -> f64 {
    if contributing_methods.is_empty() {
        return 0.0;
    }

    let mut product_of_disbeliefs = 1.0;
    const LOW_CONF_THRESHOLD: f64 = 0.5;
    const LOW_CONF_SCALING_FACTOR: f64 = 0.2; // E.g., 0.3 (weak) becomes 0.06
    const HIGH_CONF_POWER: f64 = 1.5;         // E.g., 0.7 (moderate) becomes 0.7^1.5 ~ 0.58; 0.9 (strong) becomes 0.9^1.5 ~ 0.85
    const EPSILON: f64 = 1e-9;                // Small number to prevent (1.0 - 1.0) leading to 0 in product

    for &(_, confidence) in contributing_methods {
        let clamped_confidence = confidence.max(0.0).min(1.0); // Ensure confidence is within [0, 1]

        // Transform individual confidence based on its value
        let transformed_c = if clamped_confidence < LOW_CONF_THRESHOLD {
            clamped_confidence * LOW_CONF_SCALING_FACTOR
        } else {
            clamped_confidence.powf(HIGH_CONF_POWER)
        };

        // Calculate (1 - transformed_c) ensuring it's not exactly 0 to avoid issues with product
        let disbelief = (1.0 - transformed_c).max(EPSILON);
        product_of_disbeliefs *= disbelief;
    }

    // Final edge weight: 1 - product of transformed disbeliefs.
    // This formula naturally pushes the combined score towards 1 as more
    // strong contributions reduce `product_of_disbeliefs` towards 0.
    let final_weight = (1.0 - product_of_disbeliefs).max(0.0).min(1.0);

    final_weight
}

/// Calculates a sophisticated coherence score for a given service cluster.
/// The score considers density, average confidence, confidence variance,
/// method consistency, and evidence diversity.
///
/// Arguments:
/// * `cluster_nodes` - A slice of `NodeIndex` representing services within the cluster.
/// * `internal_edges_data` - A slice of `&ServiceEdgeDetails` for edges within the cluster.
///
/// Returns:
/// A `ServiceCoherenceScore` struct.
fn calculate_service_coherence_score(
    cluster_nodes: &[NodeIndex],
    internal_edges_data: &[&ServiceEdgeDetails],
) -> ServiceCoherenceScore {
    if cluster_nodes.len() <= 1 {
        return ServiceCoherenceScore::default();
    }

    let cluster_size = cluster_nodes.len();
    let total_edges = internal_edges_data.len();
    // Maximum possible edges in an undirected graph (n * (n - 1) / 2)
    let possible_edges = (cluster_size * (cluster_size - 1)) / 2;

    // 1. Calculate density: How connected the cluster is
    let density = if possible_edges > 0 {
        total_edges as f64 / possible_edges as f64
    } else {
        0.0
    };

    // 2. Calculate confidence statistics: Average, variance, and standard deviation
    let confidences: Vec<f64> = internal_edges_data
        .iter()
        .map(|edge| edge.calculated_edge_weight) // Use the new calculated weight here
        .collect();

    let average_confidence = if !confidences.is_empty() {
        confidences.iter().sum::<f64>() / confidences.len() as f64
    } else {
        0.0
    };

    let confidence_variance = if confidences.len() > 1 {
        let mean = average_confidence;
        confidences.iter().map(|&c| (c - mean).powi(2)).sum::<f64>() / confidences.len() as f64
    } else {
        0.0
    };

    let confidence_std_dev = confidence_variance.sqrt();

    // 3. Calculate method diversity and consistency
    let mut method_types: HashSet<String> = HashSet::new(); // Unique method types
    let mut method_counts: HashMap<String, usize> = HashMap::new(); // Count of each method type
    let mut total_method_instances = 0; // Total occurrences of all methods across edges

    for edge in internal_edges_data {
        for (method_type, _confidence) in &edge.contributing_methods {
            method_types.insert(method_type.clone());
            *method_counts.entry(method_type.clone()).or_insert(0) += 1;
            total_method_instances += 1;
        }
    }

    let evidence_diversity = method_types.len() as f64; // Number of distinct method types

    // Calculate method consistency using entropy (higher entropy means more consistent/diverse usage)
    let method_consistency = if method_types.len() > 1 && total_method_instances > 0 {
        let entropy: f64 = method_counts
            .values()
            .map(|&count| {
                let p = count as f64 / total_method_instances as f64;
                if p > 0.0 {
                    -p * p.log2() // Shannon entropy formula
                } else {
                    0.0
                }
            })
            .sum();

        let max_entropy = (method_types.len() as f64).log2();
        if max_entropy > 0.0 {
            entropy / max_entropy // Normalize entropy to get a consistency score (0 to 1)
        } else {
            1.0 // If only one method type, it's perfectly consistent
        }
    } else {
        1.0 // Perfect consistency for single method or no methods (no diversity to measure inconsistency)
    };

    // Calculate overall coherence score (weighted combination of metrics)
    // Weights can be adjusted based on desired importance of each factor
    let overall_score = (density * 0.25) +           // How connected the cluster is (higher is better)
                       (average_confidence * 0.35) +  // How confident the matches are (higher is better)
                       (method_consistency * 0.20) +   // How consistent the evidence is (higher is better)
                       ((1.0 - (confidence_variance / (average_confidence + 0.1))) * 0.15) + // Stability of confidence (lower variance is better, scaled to 0-1)
                       ((evidence_diversity.ln() + 1.0).min(2.0) / 2.0 * 0.05); // Evidence diversity bonus (logarithmic scale, capped and normalized)

    ServiceCoherenceScore {
        density,
        average_confidence,
        confidence_variance,
        method_consistency,
        evidence_diversity,
        overall_score: overall_score.min(1.0).max(0.0), // Ensure score is within 0.0 and 1.0
        total_edges,
        cluster_size,
        confidence_std_dev,
    }
}

/// Logs summary statistics about the coherence scores of the generated clusters.
/// This helps in understanding the quality distribution of the clusters.
///
/// Arguments:
/// * `clusters` - A slice of `ServiceClusterOutput` structs.
fn log_coherence_statistics(clusters: &[ServiceClusterOutput]) {
    let coherence_scores: Vec<f64> = clusters
        .iter()
        .map(|c| c.coherence_score.overall_score)
        .collect();

    if !coherence_scores.is_empty() {
        let avg_coherence = coherence_scores.iter().sum::<f64>() / coherence_scores.len() as f64;
        let max_coherence = coherence_scores.iter().fold(0.0f64, |a, &b| a.max(b));
        let min_coherence = coherence_scores.iter().fold(1.0f64, |a, &b| a.min(b));

        info!("Service Cluster Coherence Statistics:");
        info!("  Average: {:.3}", avg_coherence);
        info!("  Maximum: {:.3}", max_coherence);
        info!("  Minimum: {:.3}", min_coherence);

        let high_coherence_clusters = clusters
            .iter()
            .filter(|c| c.coherence_score.overall_score >= 0.8)
            .count();
        let medium_coherence_clusters = clusters
            .iter()
            .filter(|c| {
                c.coherence_score.overall_score >= 0.6 && c.coherence_score.overall_score < 0.8
            })
            .count();
        let low_coherence_clusters = clusters
            .iter()
            .filter(|c| c.coherence_score.overall_score < 0.6)
            .count();

        info!(
            "  High coherence (≥0.8): {} clusters",
            high_coherence_clusters
        );
        info!(
            "  Medium coherence (0.6-0.8): {} clusters",
            medium_coherence_clusters
        );
        info!(
            "  Low coherence (<0.6): {} clusters",
            low_coherence_clusters
        );
    } else {
        info!("No service clusters to report coherence statistics for.");
    }
}
