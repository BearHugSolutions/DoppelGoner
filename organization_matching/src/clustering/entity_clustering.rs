// src/clustering/entity_clustering.rs - Enhanced with address-only connection filtering

use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use strsim::jaro_winkler;
use regex::Regex;

// petgraph imports
use petgraph::algo::connected_components;
use petgraph::graph::{NodeIndex, UnGraph};

use crate::utils::db_connect::PgPool;
use crate::utils::candle::cosine_similarity_candle;

// Configuration
const BATCH_SIZE_DB_OPS: usize = 500;
const LARGE_CLUSTER_THRESHOLD: usize = 10; // Clusters larger than this will be analyzed
const MIN_ADDRESS_ONLY_SIMILARITY: f32 = 0.75; // Minimum combined similarity for address-only edges
const NAME_SIMILARITY_WEIGHT: f32 = 0.4;
const EMBEDDING_SIMILARITY_WEIGHT: f32 = 0.4;
const ORGANIZATION_TYPE_WEIGHT: f32 = 0.2;

/// Represents a row fetched from the entity_group table.
#[derive(Debug, Clone)]
pub struct RawEntityGroup {
    id: String,
    entity_id_1: String,
    entity_id_2: String,
    confidence_score: Option<f64>,
    pre_rl_confidence_score: Option<f64>,
    method_type: String,
}

/// Node data for the entity graph
#[derive(Debug, Clone)]
pub struct EntityNode {
    entity_id: String,
}

/// Edge data storing multiple matching methods and their confidences
#[derive(Debug, Clone, Serialize)]
pub struct EntityEdgeDetails {
    pub contributing_methods: Vec<(String, f64)>, // (method_type, confidence)
    pub total_confidence: f64,          // Original sum of confidence, kept for context
    pub pre_rl_total_confidence: f64,   // Original sum of pre-RL confidence, kept for context
    pub calculated_edge_weight: f64,    // The new, sophisticated edge weight
}

/// Enhanced cluster processing configuration
#[derive(Debug, Clone)]
pub struct ClusterFilterConfig {
    pub large_cluster_threshold: usize,
    pub min_address_only_similarity: f32,
    pub name_weight: f32,
    pub embedding_weight: f32,
    pub org_type_weight: f32,
}

impl Default for ClusterFilterConfig {
    fn default() -> Self {
        Self {
            large_cluster_threshold: LARGE_CLUSTER_THRESHOLD,
            min_address_only_similarity: MIN_ADDRESS_ONLY_SIMILARITY,
            name_weight: NAME_SIMILARITY_WEIGHT,
            embedding_weight: EMBEDDING_SIMILARITY_WEIGHT,
            org_type_weight: ORGANIZATION_TYPE_WEIGHT,
        }
    }
}

/// Represents additional entity data needed for similarity calculations
#[derive(Debug, Clone)]
pub struct EntitySimilarityData {
    pub entity_id: String,
    pub name: Option<String>,
    pub normalized_name: Option<String>,
    pub organization_id: String,
    pub embedding: Option<Vec<f32>>,
    pub organization_type: Option<String>,
}

/// Information about an edge that needs evaluation
#[derive(Debug, Clone)]
pub struct EdgeEvaluationData {
    pub entity_id_1: String,
    pub entity_id_2: String,
    pub methods: Vec<String>,
    pub calculated_edge_weight: f64,
    pub is_address_only: bool,
}

/// Result of similarity evaluation for an address-only edge
#[derive(Debug, Clone)]
pub struct AddressOnlySimilarityResult {
    pub name_similarity: f32,
    pub embedding_similarity: f32,
    pub org_type_compatibility: f32,
    pub combined_score: f32,
    pub should_keep: bool,
}

/// Statistics from the filtering process
#[derive(Debug, Clone)]
pub struct FilterStats {
    pub clusters_analyzed: usize,
    pub edges_removed: usize,
    pub edges_kept: usize,
    pub avg_name_similarity: f32,
    pub avg_embedding_similarity: f32,
    pub avg_combined_score: f32,
}

/// Detailed output for a cluster, ready for database insertion.
#[derive(Debug, Clone)]
struct EntityClusterOutput {
    cluster_id_uuid: Uuid,
    cluster_id_str: String,
    name: String,
    description: String,
    entity_ids: HashSet<String>,
    contributing_entity_group_data: Vec<(String, String, String)>, // (group_id, entity_id_1, entity_id_2)
    coherence_score: EntityCoherenceScore,
    entity_count: i32,
    group_count: i32,
    internal_edges_for_visualization: Vec<EntityEdgeVisualizationData>,
}

/// Data for inserting into the entity_edge_visualization table.
#[derive(Debug, Clone)]
pub struct EntityEdgeVisualizationData {
    entity_id_1: String,
    entity_id_2: String,
    edge_weight: f64, // This will now use calculated_edge_weight
    details: serde_json::Value,
}

/// Sophisticated coherence score calculation
#[derive(Debug, Clone)]
pub struct EntityCoherenceScore {
    /// Ratio of actual edges to possible edges in the cluster (0.0 to 1.0)
    density: f64,
    /// Average confidence of all edges in the cluster (0.0 to 1.0) - now uses calculated_edge_weight
    average_confidence: f64,
    /// Variance in confidence scores (lower is better) - now uses calculated_edge_weight
    confidence_variance: f64,
    /// Consistency of matching methods across entities (0.0 to 1.0)
    method_consistency: f64,
    /// Number of different types of evidence
    evidence_diversity: f64,
    /// Overall weighted coherence score (0.0 to 1.0)
    overall_score: f64,
    /// Total number of edges in the cluster
    total_edges: usize,
    /// Number of entities in the cluster
    cluster_size: usize,
    /// Standard deviation of confidence scores
    confidence_std_dev: f64,
}

impl Default for EntityCoherenceScore {
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

/// Main clustering function with optional address-only filtering
pub async fn run_entity_clustering(
    pool: &PgPool,
    pipeline_run_id: &str,
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    run_entity_clustering_with_filtering(pool, pipeline_run_id, multi_progress, None).await
}

/// Enhanced clustering function with configurable address-only filtering
pub async fn run_entity_clustering_with_filtering(
    pool: &PgPool,
    pipeline_run_id: &str,
    multi_progress: Option<MultiProgress>,
    filter_config: Option<ClusterFilterConfig>,
) -> Result<usize> {
    let config = filter_config.unwrap_or_default();
    
    // Create main clustering progress bar
    let main_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(6)); // 6 main steps now (added filtering)
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  {spinner:.cyan} [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Starting clustering...");
        Some(pb)
    } else {
        None
    };

    info!("Fetching entity groups from database...");
    if let Some(pb) = &main_pb {
        pb.set_message("Fetching entity groups...");
    }

    let entity_groups = fetch_all_entity_groups(pool, multi_progress.clone()).await?;
    info!(
        "Fetched {} entity groups from database.",
        entity_groups.len()
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Building entity graph...");
    }

    info!("Building entity graph...");
    let (mut entity_graph, entity_id_to_nodeidx_map) =
        build_entity_graph(&entity_groups, multi_progress.clone())?;
    info!(
        "Entity graph built with {} nodes and {} edges.",
        entity_graph.node_count(),
        entity_graph.edge_count()
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Identifying connected components...");
    }

    info!("Identifying clusters using connected components...");
    let initial_num_components = connected_components(&entity_graph);
    info!("Found {} initial connected components.", initial_num_components);

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Filtering large clusters...");
    }

    // NEW: Filter large clusters by breaking weak address-only connections
    info!("Filtering large clusters by analyzing address-only connections...");
    let (filtered_graph, filter_stats) = filter_large_clusters_by_address_similarity(
        entity_graph,
        &entity_id_to_nodeidx_map,
        pool,
        &config,
        multi_progress.clone(),
    ).await?;
    
    entity_graph = filtered_graph;
    let final_num_components = connected_components(&entity_graph);
    info!(
        "After filtering: {} components (was {}). Removed {} address-only edges, kept {}.",
        final_num_components,
        initial_num_components,
        filter_stats.edges_removed,
        filter_stats.edges_kept
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Processing filtered clusters...");
    }

    info!("Processing clusters and calculating coherence scores...");
    let processed_clusters = process_clusters(
        &entity_graph,
        &entity_groups,
        &entity_id_to_nodeidx_map,
        multi_progress.clone(),
    )?;
    info!(
        "Processed {} clusters for storage.",
        processed_clusters.len()
    );

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.set_message("Storing cluster data...");
    }

    info!("Storing cluster data in database...");
    store_cluster_data(
        &pool,
        &processed_clusters,
        &pipeline_run_id,
        multi_progress.clone(),
    )
    .await?;

    if let Some(pb) = &main_pb {
        pb.inc(1);
        pb.finish_with_message("Entity clustering with filtering complete");
    }

    info!("Entity Cluster Consolidation Pipeline completed successfully.");
    Ok(processed_clusters.len())
}

/// Filter large clusters by analyzing address-only connections
async fn filter_large_clusters_by_address_similarity(
    mut graph: UnGraph<EntityNode, EntityEdgeDetails>,
    entity_id_to_nodeidx_map: &HashMap<String, NodeIndex>,
    pool: &PgPool,
    config: &ClusterFilterConfig,
    multi_progress: Option<MultiProgress>,
) -> Result<(UnGraph<EntityNode, EntityEdgeDetails>, FilterStats)> {
    let filter_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("    {spinner:.blue} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.set_message("Analyzing large clusters...");
        Some(pb)
    } else {
        None
    };

    // Find all connected components and identify large ones
    let mut visited = vec![false; graph.node_count()];
    let mut large_components = Vec::new();

    for node_idx in graph.node_indices() {
        if !visited[node_idx.index()] {
            let mut component = Vec::new();
            let mut stack = vec![node_idx];

            while let Some(current) = stack.pop() {
                if visited[current.index()] {
                    continue;
                }
                visited[current.index()] = true;
                component.push(current);

                for neighbor in graph.neighbors(current) {
                    if !visited[neighbor.index()] {
                        stack.push(neighbor);
                    }
                }
            }

            if component.len() > config.large_cluster_threshold {
                large_components.push(component);
            }
        }
    }

    info!(
        "Found {} large clusters (>{} entities) to analyze",
        large_components.len(),
        config.large_cluster_threshold
    );

    let mut filter_stats = FilterStats {
        clusters_analyzed: large_components.len(),
        edges_removed: 0,
        edges_kept: 0,
        avg_name_similarity: 0.0,
        avg_embedding_similarity: 0.0,
        avg_combined_score: 0.0,
    };

    if large_components.is_empty() {
        if let Some(pb) = &filter_pb {
            pb.finish_with_message("No large clusters found");
        }
        return Ok((graph, filter_stats));
    }

    // Get entity similarity data for all entities in large clusters
    let mut all_entity_ids = HashSet::new();
    for component in &large_components {
        for &node_idx in component {
            if let Some(node_data) = graph.node_weight(node_idx) {
                all_entity_ids.insert(node_data.entity_id.clone());
            }
        }
    }

    if let Some(pb) = &filter_pb {
        pb.set_message("Loading entity similarity data...");
    }

    let entity_ids_vec: Vec<String> = all_entity_ids.into_iter().collect();
    let entity_similarity_map = load_entity_similarity_data(pool, &entity_ids_vec).await?;

    // Process each large component
    let mut edges_to_remove = Vec::new();
    let mut similarity_scores = Vec::new();

    for (comp_idx, component) in large_components.iter().enumerate() {
        if let Some(pb) = &filter_pb {
            pb.set_message(format!(
                "Analyzing cluster {}/{} ({} entities)...",
                comp_idx + 1,
                large_components.len(),
                component.len()
            ));
        }

        // Analyze edges within this component
        let edge_evaluations = analyze_component_edges(&graph, component)?;
        
        for edge_eval in edge_evaluations {
            if edge_eval.is_address_only {
                let similarity_result = evaluate_address_only_edge_similarity(
                    &edge_eval,
                    &entity_similarity_map,
                    config,
                )?;

                similarity_scores.push(similarity_result.clone());

                if !similarity_result.should_keep {
                    // Find the edge in the graph and mark for removal
                    if let (Some(&node1_idx), Some(&node2_idx)) = (
                        entity_id_to_nodeidx_map.get(&edge_eval.entity_id_1),
                        entity_id_to_nodeidx_map.get(&edge_eval.entity_id_2),
                    ) {
                        if let Some(edge_idx) = graph.find_edge(node1_idx, node2_idx) {
                            edges_to_remove.push(edge_idx);
                            filter_stats.edges_removed += 1;
                        }
                    }
                } else {
                    filter_stats.edges_kept += 1;
                }
            }
        }
    }

    // Remove identified edges
    for edge_idx in edges_to_remove {
        graph.remove_edge(edge_idx);
    }

    // Calculate average similarity scores
    if !similarity_scores.is_empty() {
        filter_stats.avg_name_similarity = similarity_scores.iter()
            .map(|s| s.name_similarity)
            .sum::<f32>() / similarity_scores.len() as f32;
        filter_stats.avg_embedding_similarity = similarity_scores.iter()
            .map(|s| s.embedding_similarity)
            .sum::<f32>() / similarity_scores.len() as f32;
        filter_stats.avg_combined_score = similarity_scores.iter()
            .map(|s| s.combined_score)
            .sum::<f32>() / similarity_scores.len() as f32;
    }

    if let Some(pb) = &filter_pb {
        pb.finish_with_message(format!(
            "Filtered {} large clusters: removed {} edges, kept {}",
            filter_stats.clusters_analyzed,
            filter_stats.edges_removed,
            filter_stats.edges_kept
        ));
    }

    info!(
        "Large cluster filtering complete. Avg similarities - Name: {:.3}, Embedding: {:.3}, Combined: {:.3}",
        filter_stats.avg_name_similarity,
        filter_stats.avg_embedding_similarity,
        filter_stats.avg_combined_score
    );

    Ok((graph, filter_stats))
}

/// Analyze edges within a component to identify address-only connections
fn analyze_component_edges(
    graph: &UnGraph<EntityNode, EntityEdgeDetails>,
    component: &[NodeIndex],
) -> Result<Vec<EdgeEvaluationData>> {
    let mut edge_evaluations = Vec::new();

    for i in 0..component.len() {
        for j in (i + 1)..component.len() {
            let node1 = component[i];
            let node2 = component[j];

            if let Some(edge_idx) = graph.find_edge(node1, node2) {
                if let Some(edge_data) = graph.edge_weight(edge_idx) {
                    let entity_id_1 = &graph[node1].entity_id;
                    let entity_id_2 = &graph[node2].entity_id;

                    let methods: Vec<String> = edge_data.contributing_methods
                        .iter()
                        .map(|(method, _)| method.clone())
                        .collect();

                    let is_address_only = methods.len() == 1 && methods[0] == "address";

                    edge_evaluations.push(EdgeEvaluationData {
                        entity_id_1: if entity_id_1 < entity_id_2 {
                            entity_id_1.clone()
                        } else {
                            entity_id_2.clone()
                        },
                        entity_id_2: if entity_id_1 < entity_id_2 {
                            entity_id_2.clone()
                        } else {
                            entity_id_1.clone()
                        },
                        methods,
                        calculated_edge_weight: edge_data.calculated_edge_weight,
                        is_address_only,
                    });
                }
            }
        }
    }

    Ok(edge_evaluations)
}

/// Load entity similarity data for the given entity IDs
async fn load_entity_similarity_data(
    pool: &PgPool,
    entity_ids: &[String],
) -> Result<HashMap<String, EntitySimilarityData>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let query = "
        SELECT 
            e.id,
            e.name,
            e.organization_id,
            o.embedding,
            CASE 
                WHEN e.name ILIKE '%police%' OR e.name ILIKE '%sheriff%' THEN 'police'
                WHEN e.name ILIKE '%fire%' OR e.name ILIKE '%ems%' THEN 'fire'
                WHEN e.name ILIKE '%school%' OR e.name ILIKE '%education%' THEN 'education'
                WHEN e.name ILIKE '%hospital%' OR e.name ILIKE '%medical%' OR e.name ILIKE '%health%' THEN 'health'
                WHEN e.name ILIKE '%library%' THEN 'library'
                WHEN e.name ILIKE '%city%' OR e.name ILIKE '%county%' THEN 'government'
                ELSE 'general'
            END as organization_type
        FROM public.entity e
        LEFT JOIN public.organization o ON e.organization_id = o.id
        WHERE e.id = ANY($1)";

    let rows = conn
        .query(query, &[&entity_ids])
        .await
        .context("Failed to query entity similarity data")?;

    let mut entity_map = HashMap::new();

    for row in rows {
        let entity_id: String = row.get("id");
        let name: Option<String> = row.get("name");
        let normalized_name = name.as_ref().map(|n| normalize_name_for_similarity(n));
        let organization_id: String = row.get("organization_id");
        let embedding_pgvector: Option<pgvector::Vector> = row.get("embedding");
        let embedding = embedding_pgvector.map(|v| v.to_vec());
        let organization_type: Option<String> = row.get("organization_type");

        entity_map.insert(entity_id.clone(), EntitySimilarityData {
            entity_id,
            name,
            normalized_name,
            organization_id,
            embedding,
            organization_type,
        });
    }

    Ok(entity_map)
}

/// Evaluate similarity for an address-only edge
fn evaluate_address_only_edge_similarity(
    edge_eval: &EdgeEvaluationData,
    entity_map: &HashMap<String, EntitySimilarityData>,
    config: &ClusterFilterConfig,
) -> Result<AddressOnlySimilarityResult> {
    let entity1 = entity_map.get(&edge_eval.entity_id_1)
        .ok_or_else(|| anyhow::anyhow!("Entity {} not found in similarity data", edge_eval.entity_id_1))?;
    let entity2 = entity_map.get(&edge_eval.entity_id_2)
        .ok_or_else(|| anyhow::anyhow!("Entity {} not found in similarity data", edge_eval.entity_id_2))?;

    // Calculate name similarity
    let name_similarity = calculate_name_similarity(entity1, entity2);

    // Calculate embedding similarity
    let embedding_similarity = calculate_embedding_similarity(entity1, entity2);

    // Calculate organization type compatibility
    let org_type_compatibility = calculate_org_type_compatibility(entity1, entity2);

    // Calculate combined score
    let combined_score = (name_similarity * config.name_weight) +
                        (embedding_similarity * config.embedding_weight) +
                        (org_type_compatibility * config.org_type_weight);

    let should_keep = combined_score >= config.min_address_only_similarity;

    Ok(AddressOnlySimilarityResult {
        name_similarity,
        embedding_similarity,
        org_type_compatibility,
        combined_score,
        should_keep,
    })
}

/// Calculate name similarity between two entities
fn calculate_name_similarity(entity1: &EntitySimilarityData, entity2: &EntitySimilarityData) -> f32 {
    match (&entity1.normalized_name, &entity2.normalized_name) {
        (Some(name1), Some(name2)) => {
            if name1.is_empty() || name2.is_empty() {
                return 0.0;
            }
            
            // Use Jaro-Winkler similarity
            let jaro_score = jaro_winkler(name1, name2) as f32;
            
            // Boost score for exact matches or very high similarity
            if jaro_score > 0.95 {
                jaro_score * 1.1 // Slight boost for very similar names
            } else {
                jaro_score
            }.min(1.0)
        }
        _ => 0.0, // No names available
    }
}

/// Calculate embedding similarity between two entities
fn calculate_embedding_similarity(entity1: &EntitySimilarityData, entity2: &EntitySimilarityData) -> f32 {
    match (&entity1.embedding, &entity2.embedding) {
        (Some(emb1), Some(emb2)) => {
            cosine_similarity_candle(emb1, emb2).unwrap_or(0.0) as f32
        }
        _ => 0.0, // No embeddings available
    }
}

/// Calculate organization type compatibility
fn calculate_org_type_compatibility(entity1: &EntitySimilarityData, entity2: &EntitySimilarityData) -> f32 {
    match (&entity1.organization_type, &entity2.organization_type) {
        (Some(type1), Some(type2)) => {
            if type1 == type2 {
                1.0 // Same type
            } else if are_compatible_org_types(type1, type2) {
                0.7 // Compatible types
            } else if are_incompatible_org_types(type1, type2) {
                0.0 // Incompatible types
            } else {
                0.5 // Neutral/unknown compatibility
            }
        }
        _ => 0.5, // Unknown types - neutral score
    }
}

/// Check if organization types are compatible
fn are_compatible_org_types(type1: &str, type2: &str) -> bool {
    let compatible_pairs = [
        ("government", "police"),
        ("government", "fire"),
        ("health", "medical"),
        ("education", "library"),
    ];
    
    compatible_pairs.iter().any(|(t1, t2)| {
        (type1 == *t1 && type2 == *t2) || (type1 == *t2 && type2 == *t1)
    })
}

/// Check if organization types are incompatible
fn are_incompatible_org_types(type1: &str, type2: &str) -> bool {
    let incompatible_pairs = [
        ("police", "fire"),
        ("police", "education"),
        ("fire", "education"),
        ("health", "education"),
        ("library", "police"),
        ("library", "fire"),
    ];
    
    incompatible_pairs.iter().any(|(t1, t2)| {
        (type1 == *t1 && type2 == *t2) || (type1 == *t2 && type2 == *t1)
    })
}

/// Normalize name for similarity calculation (simplified version of name.rs logic)
fn normalize_name_for_similarity(name: &str) -> String {
    let mut normalized = name.to_lowercase();
    
    // Basic character substitutions
    let substitutions = [
        ("&", " and "),
        ("+", " plus "),
        ("/", " "),
        ("-", " "),
        (".", " "),
        ("'", ""),
        ("(", " "),
        (")", " "),
        (",", " "),
    ];
    
    for (pattern, replacement) in &substitutions {
        normalized = normalized.replace(pattern, replacement);
    }
    
    // Remove common prefixes and suffixes
    let prefixes = ["the ", "a ", "an "];
    for prefix in prefixes {
        if normalized.starts_with(prefix) {
            normalized = normalized[prefix.len()..].to_string();
        }
    }
    
    let suffixes = [
        " inc", " corp", " llc", " ltd", " company", " co", " org", " organization"
    ];
    for suffix in suffixes {
        if normalized.ends_with(suffix) {
            normalized = normalized[..normalized.len() - suffix.len()].trim_end().to_string();
        }
    }
    
    // Normalize whitespace
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    normalized.trim().to_string()
}

async fn fetch_all_entity_groups(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
) -> Result<Vec<RawEntityGroup>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Create spinner for database fetch
    let fetch_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("    {spinner:.blue} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.set_message("Querying entity_group table...");
        Some(pb)
    } else {
        None
    };

    let query = "
        SELECT id, entity_id_1, entity_id_2, confidence_score, pre_rl_confidence_score, method_type
        FROM public.entity_group
        WHERE (confidence_score IS NOT NULL OR pre_rl_confidence_score IS NOT NULL)
        ORDER BY id
    ";

    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to query entity_group table")?;

    let mut entity_groups = Vec::with_capacity(rows.len());
    for row in rows {
        entity_groups.push(RawEntityGroup {
            id: row.get("id"),
            entity_id_1: row.get("entity_id_1"),
            entity_id_2: row.get("entity_id_2"),
            confidence_score: row.get("confidence_score"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
            method_type: row.get("method_type"),
        });
    }

    if let Some(pb) = &fetch_pb {
        pb.finish_with_message(format!("Fetched {} entity groups", entity_groups.len()));
    }

    Ok(entity_groups)
}

fn build_entity_graph(
    entity_groups: &[RawEntityGroup],
    multi_progress: Option<MultiProgress>,
) -> Result<(
    UnGraph<EntityNode, EntityEdgeDetails>,
    HashMap<String, NodeIndex>,
)> {
    // Create progress bar for graph building
    let graph_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(entity_groups.len() as u64));
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
    let mut entity_id_to_nodeidx_map: HashMap<String, NodeIndex> = HashMap::new();

    // Temporary map to aggregate edge data before adding to graph and calculating final weight
    let mut edge_aggregation_map: HashMap<(NodeIndex, NodeIndex), EntityEdgeDetails> =
        HashMap::new();

    for (i, eg) in entity_groups.iter().enumerate() {
        if let Some(pb) = &graph_pb {
            pb.inc(1);
            if i % 1000 == 0 {
                pb.set_message(format!("Building graph... ({}/{})", i, entity_groups.len()));
            }
        }

        if eg.entity_id_1.is_empty() || eg.entity_id_2.is_empty() {
            warn!("EG {}: empty entity_id_1 or entity_id_2, skipping.", eg.id);
            continue;
        }
        if eg.entity_id_1 == eg.entity_id_2 {
            warn!("EG {}: links entity to itself, skipping.", eg.id);
            continue;
        }

        let node_idx_1 = *entity_id_to_nodeidx_map
            .entry(eg.entity_id_1.clone())
            .or_insert_with(|| {
                graph.add_node(EntityNode {
                    entity_id: eg.entity_id_1.clone(),
                })
            });
        let node_idx_2 = *entity_id_to_nodeidx_map
            .entry(eg.entity_id_2.clone())
            .or_insert_with(|| {
                graph.add_node(EntityNode {
                    entity_id: eg.entity_id_2.clone(),
                })
            });

        let edge_key = if node_idx_1 < node_idx_2 {
            (node_idx_1, node_idx_2)
        } else {
            (node_idx_2, node_idx_1)
        };

        let confidence = eg.confidence_score.unwrap_or(0.0);
        let pre_rl_confidence = eg.pre_rl_confidence_score.unwrap_or(0.0);

        // Aggregate contributing methods and basic sums
        edge_aggregation_map
            .entry(edge_key)
            .and_modify(|details| {
                details.contributing_methods.push((eg.method_type.clone(), confidence));
                details.total_confidence += confidence;
                details.pre_rl_total_confidence += pre_rl_confidence;
            })
            .or_insert(EntityEdgeDetails {
                contributing_methods: vec![(eg.method_type.clone(), confidence)],
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

    if let Some(pb) = &graph_pb {
        pb.finish_with_message(format!(
            "Graph built: {} nodes, {} edges",
            graph.node_count(),
            graph.edge_count()
        ));
    }

    Ok((graph, entity_id_to_nodeidx_map))
}

fn process_clusters(
    graph: &UnGraph<EntityNode, EntityEdgeDetails>,
    all_entity_groups: &[RawEntityGroup],
    entity_id_to_nodeidx_map: &HashMap<String, NodeIndex>,
    multi_progress: Option<MultiProgress>,
) -> Result<Vec<EntityClusterOutput>> {
    let mut processed_clusters = Vec::new();

    // Find all connected components
    let mut visited = vec![false; graph.node_count()];
    let mut components = Vec::new();

    for node_idx in graph.node_indices() {
        if !visited[node_idx.index()] {
            let mut component = Vec::new();
            let mut stack = vec![node_idx];

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

    for (i, component_node_indices) in components.iter().enumerate() {
        if let Some(pb) = &pb {
            pb.inc(1);
            if i % 100 == 0 {
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

        // Extract entity IDs from component
        let mut entity_ids_in_component: HashSet<String> = HashSet::new();
        for &node_idx in component_node_indices {
            if let Some(node_data) = graph.node_weight(node_idx) {
                entity_ids_in_component.insert(node_data.entity_id.clone());
            }
        }

        if entity_ids_in_component.len() < 1 {
            debug!("Skipping empty or invalid component.");
            continue;
        }

        // Collect internal edges and their data
        let mut internal_edges_data = Vec::new();
        let mut internal_edges_for_viz = Vec::new();

        for i in 0..component_node_indices.len() {
            for j in (i + 1)..component_node_indices.len() {
                let node1 = component_node_indices[i];
                let node2 = component_node_indices[j];

                if let Some(edge_idx) = graph.find_edge(node1, node2) {
                    if let Some(edge_data) = graph.edge_weight(edge_idx) {
                        internal_edges_data.push(edge_data);

                        let entity1 = &graph[node1].entity_id;
                        let entity2 = &graph[node2].entity_id;

                        internal_edges_for_viz.push(EntityEdgeVisualizationData {
                            entity_id_1: if entity1 < entity2 {
                                entity1.clone()
                            } else {
                                entity2.clone()
                            },
                            entity_id_2: if entity1 < entity2 {
                                entity2.clone()
                            } else {
                                entity1.clone()
                            },
                            edge_weight: edge_data.calculated_edge_weight, // Use the new calculated weight
                            details: json!(edge_data),
                        });
                    }
                }
            }
        }

        // Find contributing entity group data
        let mut contributing_entity_group_data: Vec<(String, String, String)> = Vec::new();
        for eg in all_entity_groups {
            if let (Some(&e1_nodeidx), Some(&e2_nodeidx)) = (
                entity_id_to_nodeidx_map.get(&eg.entity_id_1),
                entity_id_to_nodeidx_map.get(&eg.entity_id_2),
            ) {
                if component_node_indices.contains(&e1_nodeidx)
                    && component_node_indices.contains(&e2_nodeidx)
                {
                    contributing_entity_group_data.push((
                        eg.id.clone(),
                        eg.entity_id_1.clone(),
                        eg.entity_id_2.clone(),
                    ));
                }
            }
        }

        // Calculate sophisticated coherence score
        let coherence_score =
            calculate_coherence_score(component_node_indices, &internal_edges_data);

        let entity_count = entity_ids_in_component.len();
        let group_count_in_cluster = contributing_entity_group_data.len();

        processed_clusters.push(EntityClusterOutput {
            cluster_id_uuid: cluster_uuid,
            cluster_id_str: cluster_id_str.clone(),
            name: format!("EntityCluster-{}", cluster_id_str.split('-').next().unwrap_or("")),
            description: format!(
                "Cluster of {} entities identified by connected components with coherence score {:.3}.",
                entity_count, coherence_score.overall_score
            ),
            entity_ids: entity_ids_in_component,
            contributing_entity_group_data,
            coherence_score,
            entity_count: entity_count as i32,
            group_count: group_count_in_cluster as i32,
            internal_edges_for_visualization: internal_edges_for_viz,
        });
    }

    if let Some(pb) = &pb {
        pb.finish_with_message("Cluster processing complete.");
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

fn calculate_coherence_score(
    cluster_nodes: &[NodeIndex],
    internal_edges_data: &[&EntityEdgeDetails],
) -> EntityCoherenceScore {
    if cluster_nodes.len() <= 1 {
        return EntityCoherenceScore::default();
    }

    let cluster_size = cluster_nodes.len();
    let total_edges = internal_edges_data.len();
    let possible_edges = (cluster_size * (cluster_size - 1)) / 2;

    // Calculate density
    let density = if possible_edges > 0 {
        total_edges as f64 / possible_edges as f64
    } else {
        0.0
    };

    // Calculate confidence statistics using the new calculated_edge_weight
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

    // Calculate method diversity and consistency
    let mut method_types: HashSet<String> = HashSet::new();
    let mut method_counts: HashMap<String, usize> = HashMap::new();
    let mut total_method_instances = 0;

    for edge in internal_edges_data {
        for (method_type, _confidence) in &edge.contributing_methods {
            method_types.insert(method_type.clone());
            *method_counts.entry(method_type.clone()).or_insert(0) += 1;
            total_method_instances += 1;
        }
    }

    let evidence_diversity = method_types.len() as f64;

    // Calculate method consistency (how evenly distributed the methods are)
    let method_consistency = if method_types.len() > 1 && total_method_instances > 0 {
        // Calculate entropy-based consistency score
        let entropy: f64 = method_counts
            .values()
            .map(|&count| {
                let p = count as f64 / total_method_instances as f64;
                if p > 0.0 {
                    -p * p.log2()
                } else {
                    0.0
                }
            })
            .sum();

        let max_entropy = (method_types.len() as f64).log2();
        if max_entropy > 0.0 {
            entropy / max_entropy
        } else {
            1.0
        }
    } else {
        1.0 // Perfect consistency for single method or no methods
    };

    // Calculate overall coherence score (weighted combination)
    let overall_score = (density * 0.25) +           // How connected the cluster is
                       (average_confidence * 0.35) +  // How confident the matches are
                       (method_consistency * 0.20) +   // How consistent the evidence is
                       ((1.0 - (confidence_variance / (average_confidence + 0.1))) * 0.15) + // Stability of confidence
                       ((evidence_diversity.ln() + 1.0).min(2.0) / 2.0 * 0.05); // Evidence diversity bonus

    EntityCoherenceScore {
        density,
        average_confidence,
        confidence_variance,
        method_consistency,
        evidence_diversity,
        overall_score: overall_score.min(1.0).max(0.0),
        total_edges,
        cluster_size,
        confidence_std_dev,
    }
}

async fn store_cluster_data(
    pool: &PgPool,
    clusters: &[EntityClusterOutput],
    pipeline_run_id: &str,
    multi_progress: Option<MultiProgress>,
) -> Result<()> {
    if clusters.is_empty() {
        info!("No clusters to store.");
        return Ok(());
    }

    // Create progress tracking for storage operations
    let storage_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(3)); // 3 main storage operations
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.red} [{elapsed_precise}] {bar:25.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Storing cluster data...");
        Some(pb)
    } else {
        None
    };

    let mut client = pool
        .get()
        .await
        .context("Failed to get DB client for storing cluster data")?;

    if let Some(pb) = &storage_pb {
        pb.set_message("Inserting cluster records...");
    }

    info!("Batch inserting entity_group_cluster records...");
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
        cluster_insert_params.push(Box::new(cluster.entity_count));
        cluster_insert_params.push(Box::new(cluster.group_count));
        cluster_insert_params.push(Box::new(cluster.coherence_score.overall_score));
        i += 6;
    }

    if !cluster_insert_params.is_empty() {
        let cluster_insert_query = format!(
            "INSERT INTO public.entity_group_cluster (id, name, description, created_at, updated_at, entity_count, group_count, average_coherence_score) VALUES {}
            ON CONFLICT (id) DO NOTHING",
            cluster_insert_values_str.join(", ")
        );

        let params_slice: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = cluster_insert_params
            .iter()
            .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();

        client
            .execute(&cluster_insert_query, &params_slice[..])
            .await
            .context("Failed to batch insert entity_group_cluster records")?;
        info!(
            "Successfully inserted/updated {} entity_group_cluster records.",
            clusters.len()
        );
    }

    if let Some(pb) = &storage_pb {
        pb.inc(1);
        pb.set_message("Updating entity group records...");
    }

    info!("Batch updating entity_group records with cluster IDs...");
    let transaction = client
        .transaction()
        .await
        .context("Failed to start transaction for entity_group updates")?;

    for cluster in clusters {
        if cluster.contributing_entity_group_data.is_empty() {
            continue;
        }
        let entity_group_ids_to_update: Vec<String> = cluster
            .contributing_entity_group_data
            .iter()
            .map(|(id, _, _)| id.clone())
            .collect();

        for chunk in entity_group_ids_to_update.chunks(BATCH_SIZE_DB_OPS) {
            if chunk.is_empty() {
                continue;
            }
            let update_query = "
                UPDATE public.entity_group
                SET group_cluster_id = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ANY($2)
            ";
            transaction
                .execute(update_query, &[&cluster.cluster_id_str, &chunk])
                .await
                .context(format!(
                    "Failed to update entity_group records for cluster {}",
                    cluster.cluster_id_str
                ))?;
        }
    }

    transaction
        .commit()
        .await
        .context("Failed to commit entity_group updates")?;

    if let Some(pb) = &storage_pb {
        pb.inc(1);
        pb.set_message("Inserting visualization edges...");
    }

    info!("Batch inserting entity_edge_visualization records...");
    let viz_client = pool
        .get()
        .await
        .context("Failed to get new DB client for viz edges")?;

    // Create separate progress bar for visualization edge insertion
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
        pb.set_message("Inserting visualization edges...");
        Some(pb)
    } else {
        None
    };

    let mut viz_edge_insert_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
        Vec::new();
    let mut viz_edge_insert_values_str = Vec::new();
    let mut current_val_idx = 1;
    let mut total_viz_edges = 0;

    for cluster in clusters {
        for viz_edge in &cluster.internal_edges_for_visualization {
            total_viz_edges += 1;
            if let Some(pb) = &viz_pb {
                pb.inc(1);
            }

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
            viz_edge_insert_params.push(Box::new(viz_edge.entity_id_1.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.entity_id_2.clone()));
            viz_edge_insert_params.push(Box::new(viz_edge.edge_weight));
            viz_edge_insert_params.push(Box::new(viz_edge.details.clone()));
            viz_edge_insert_params.push(Box::new(pipeline_run_id.to_string()));
            current_val_idx += 7;

            if viz_edge_insert_params.len() >= BATCH_SIZE_DB_OPS * 7 {
                let query = format!(
                    "INSERT INTO public.entity_edge_visualization (id, cluster_id, entity_id_1, entity_id_2, edge_weight, details, pipeline_run_id, created_at) VALUES {}
                     ON CONFLICT (cluster_id, entity_id_1, entity_id_2, pipeline_run_id) DO NOTHING",
                    viz_edge_insert_values_str.join(", ")
                );
                let params_slice_viz: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
                    viz_edge_insert_params
                        .iter()
                        .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                        .collect();

                viz_client
                    .execute(&query, &params_slice_viz[..])
                    .await
                    .context(
                        "Failed to batch insert entity_edge_visualization records (mid-batch)",
                    )?;

                viz_edge_insert_params.clear();
                viz_edge_insert_values_str.clear();
                current_val_idx = 1;
            }
        }
    }

    if !viz_edge_insert_params.is_empty() {
        let query = format!(
            "INSERT INTO public.entity_edge_visualization (id, cluster_id, entity_id_1, entity_id_2, edge_weight, details, pipeline_run_id, created_at) VALUES {}
             ON CONFLICT (cluster_id, entity_id_1, entity_id_2, pipeline_run_id) DO NOTHING",
            viz_edge_insert_values_str.join(", ")
        );
        let params_slice_viz: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
            viz_edge_insert_params
                .iter()
                .map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

        viz_client
            .execute(&query, &params_slice_viz[..])
            .await
            .context("Failed to batch insert remaining entity_edge_visualization records")?;
    }

    if let Some(pb) = &viz_pb {
        pb.finish_with_message(format!("Inserted {} visualization edges", total_viz_edges));
    }

    info!(
        "Successfully inserted/updated {} entity_edge_visualization records.",
        total_viz_edges
    );

    if let Some(pb) = &storage_pb {
        pb.inc(1);
        pb.finish_with_message("Cluster storage complete");
    }

    // Log coherence statistics
    let coherence_scores: Vec<f64> = clusters
        .iter()
        .map(|c| c.coherence_score.overall_score)
        .collect();
    if !coherence_scores.is_empty() {
        let avg_coherence = coherence_scores.iter().sum::<f64>() / coherence_scores.len() as f64;
        let max_coherence = coherence_scores.iter().fold(0.0f64, |a, &b| a.max(b));
        let min_coherence = coherence_scores.iter().fold(1.0f64, |a, &b| a.min(b));

        info!("Coherence Statistics:");
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
    }

    Ok(())
}