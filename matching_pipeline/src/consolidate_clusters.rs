// src/consolidate_clusters.rs
use rand::seq::SliceRandom;
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet}; 
use std::time::Instant;
use tokio::sync::Mutex;
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;
use xgraph::graph::graph::NodeId;

// xgraph imports
use xgraph::leiden_clustering::{CommunityConfig, CommunityDetection};
use xgraph::Graph;

// Local imports
use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType,
    ContributingSharedEntityDetail,
    EntityGroupId,
    EntityId,
    GroupClusterId,
    NewClusterFormationEdge,
    NewSuggestedAction,
    SuggestionStatus,
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;

#[derive(Debug)]
struct NodeMapper {
    group_to_idx: HashMap<EntityGroupId, NodeId>,
    idx_to_group: HashMap<NodeId, EntityGroupId>,
}

impl NodeMapper {
    fn new() -> Self {
        NodeMapper {
            group_to_idx: HashMap::new(),
            idx_to_group: HashMap::new(),
        }
    }

    fn get_or_add_node(
        &mut self,
        graph: &mut Graph<f64, EntityGroupId, ()>,
        group_id: &EntityGroupId,
    ) -> NodeId {
        *self
            .group_to_idx
            .entry(group_id.clone())
            .or_insert_with(|| {
                let node_idx = graph.add_node(group_id.clone());
                self.idx_to_group.insert(node_idx, group_id.clone());
                node_idx
            })
    }

    fn get_group_id(&self, idx: NodeId) -> Option<&EntityGroupId> {
        self.idx_to_group.get(&idx)
    }

    fn get_node_count(&self) -> usize {
        self.idx_to_group.len()
    }
}

#[derive(Debug)]
struct ClusteringState {
    unclustered_groups: Vec<EntityGroupId>,
    existing_edges: HashMap<(String, String), f64>,
    last_clustering_run: Option<String>,
    groups_added_since_last_run: Vec<EntityGroupId>,
}

/// Check the current state and determine what work needs to be done
async fn analyze_clustering_state(
    transaction: &Transaction<'_>,
    current_pipeline_run_id: &str,
) -> Result<ClusteringState> {
    info!("Analyzing clustering state to optimize processing...");
    
    // 1. Get unclustered groups
    let unclustered_query = "SELECT id FROM public.entity_group WHERE group_cluster_id IS NULL";
    let unclustered_rows = transaction
        .query(unclustered_query, &[])
        .await
        .context("Failed to fetch unclustered groups")?;
    
    let unclustered_groups: Vec<EntityGroupId> = unclustered_rows
        .into_iter()
        .map(|row| EntityGroupId(row.get("id")))
        .collect();
    
    info!("Found {} unclustered groups", unclustered_groups.len());
    
    // 2. Find the most recent successful clustering run
    let last_run_query = "
        SELECT id, completed_at 
        FROM clustering_metadata.pipeline_run 
        WHERE id != $1 
          AND total_clusters > 0 
          AND completed_at IS NOT NULL
        ORDER BY completed_at DESC 
        LIMIT 1";
    
    let last_run_result = transaction
        .query_opt(last_run_query, &[&current_pipeline_run_id])
        .await
        .context("Failed to query last clustering run")?;
    
    let last_clustering_run = last_run_result.map(|row| row.get::<_, String>("id"));
    
    if let Some(ref last_run_id) = last_clustering_run {
        info!("Found previous clustering run: {}", last_run_id);
    } else {
        info!("No previous clustering run found - will perform full clustering");
    }
    
    // 3. Get existing cluster formation edges to potentially reuse
    let existing_edges_query = if let Some(ref last_run_id) = last_clustering_run {
        "SELECT source_group_id, target_group_id, calculated_edge_weight 
         FROM clustering_metadata.cluster_formation_edges 
         WHERE pipeline_run_id = $1"
    } else {
        "SELECT source_group_id, target_group_id, calculated_edge_weight 
         FROM clustering_metadata.cluster_formation_edges 
         WHERE 1=0" // No edges to reuse if no previous run
    };
    
    let existing_edges_rows = if let Some(ref last_run_id) = last_clustering_run {
        transaction
            .query(existing_edges_query, &[last_run_id])
            .await
            .context("Failed to fetch existing cluster formation edges")?
    } else {
        Vec::new()
    };
    
    let mut existing_edges = HashMap::new();
    for row in existing_edges_rows {
        let source: String = row.get("source_group_id");
        let target: String = row.get("target_group_id");
        let weight: f64 = row.get("calculated_edge_weight");
        
        // Store both directions for easy lookup
        let key1 = if source < target { (source.clone(), target.clone()) } else { (target.clone(), source.clone()) };
        existing_edges.insert(key1, weight);
    }
    
    info!("Found {} existing cluster formation edges that could be reused", existing_edges.len());
    
    // 4. Identify groups added since the last clustering run
    let groups_added_since_last_run = if let Some(ref last_run_id) = last_clustering_run {
        // Get the completion time of the last run
        let last_run_time_query = "
            SELECT completed_at 
            FROM clustering_metadata.pipeline_run 
            WHERE id = $1";
        
        let last_run_time = transaction
            .query_opt(last_run_time_query, &[last_run_id])
            .await
            .context("Failed to get last run completion time")?
            .and_then(|row| row.get::<_, Option<chrono::NaiveDateTime>>("completed_at"));
        
        if let Some(last_completion_time) = last_run_time {
            let new_groups_query = "
                SELECT id FROM public.entity_group 
                WHERE created_at > $1 AND group_cluster_id IS NULL";
            
            let new_groups_rows = transaction
                .query(new_groups_query, &[&last_completion_time])
                .await
                .context("Failed to fetch groups added since last run")?;
            
            let new_groups: Vec<EntityGroupId> = new_groups_rows
                .into_iter()
                .map(|row| EntityGroupId(row.get("id")))
                .collect();
            
            info!("Found {} groups added since last clustering run", new_groups.len());
            new_groups
        } else {
            info!("Last run completion time not available - treating all unclustered groups as new");
            unclustered_groups.clone()
        }
    } else {
        unclustered_groups.clone()
    };
    
    Ok(ClusteringState {
        unclustered_groups,
        existing_edges,
        last_clustering_run,
        groups_added_since_last_run,
    })
}

/// Check if we can reuse an existing edge calculation
fn can_reuse_edge(
    state: &ClusteringState,
    group_a_id: &str,
    group_b_id: &str,
) -> Option<f64> {
    let key = if group_a_id < group_b_id {
        (group_a_id.to_string(), group_b_id.to_string())
    } else {
        (group_b_id.to_string(), group_a_id.to_string())
    };
    
    state.existing_edges.get(&key).copied()
}

async fn build_weighted_xgraph_optimized(
    pool: &PgPool,
    transaction: &Transaction<'_>,
    pipeline_run_id: &str,
    state: &ClusteringState,
) -> Result<(Graph<f64, EntityGroupId, ()>, NodeMapper)> {
    info!(
        "Building optimized weighted xgraph for cluster consolidation (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    if state.unclustered_groups.is_empty() {
        info!("No unclustered entity groups found to build graph.");
        return Ok((Graph::new(false), NodeMapper::new()));
    }

    // Prepare group IDs for batch queries
    let group_id_strings: Vec<String> = state.unclustered_groups
        .iter()
        .map(|g| g.0.clone())
        .collect();

    // Fetch group details in batch
    let groups_query = "
        SELECT id, entity_id_1, entity_id_2, confidence_score 
        FROM public.entity_group 
        WHERE id = ANY($1)";
    
    let group_rows = transaction
        .query(groups_query, &[&group_id_strings])
        .await
        .context("Failed to fetch unclustered pairwise entity groups")?;

    let mut node_mapper = NodeMapper::new();
    let mut xgraph_instance: Graph<f64, EntityGroupId, ()> = Graph::new(false);
    let mut entity_to_pairwise_groups: HashMap<EntityId, Vec<EntityGroupId>> = HashMap::new();
    let mut group_confidences: HashMap<EntityGroupId, f64> = HashMap::new();

    info!("Loading {} pairwise entity groups...", group_rows.len());
    
    for row in &group_rows {
        let group_id_str: String = row.get("id");
        let entity_group_id = EntityGroupId(group_id_str);
        let entity_id_1_str: String = row.get("entity_id_1");
        let entity_id_2_str: String = row.get("entity_id_2");
        let confidence_score: Option<f64> = row.get("confidence_score");

        let _node_id = node_mapper.get_or_add_node(&mut xgraph_instance, &entity_group_id);

        if let Some(conf) = confidence_score {
            group_confidences.insert(entity_group_id.clone(), conf);
        } else {
            warn!(
                "Pairwise group {} has NULL confidence_score. Defaulting to 0.0.",
                entity_group_id.0
            );
            group_confidences.insert(entity_group_id.clone(), 0.0);
        }

        entity_to_pairwise_groups
            .entry(EntityId(entity_id_1_str))
            .or_default()
            .push(entity_group_id.clone());
        entity_to_pairwise_groups
            .entry(EntityId(entity_id_2_str))
            .or_default()
            .push(entity_group_id.clone());
    }

    info!(
        "Loaded {} pairwise entity groups as nodes.",
        node_mapper.get_node_count()
    );

    // Edge creation with optimization for reusing existing calculations
    let mut edges_reused = 0;
    let mut edges_calculated = 0;
    let mut batch_edges = Vec::new();
    let mut batch_suggestions = Vec::new();
    
    const EDGE_BATCH_SIZE: usize = 1000;

    for (entity_z, groups_sharing_entity_z) in entity_to_pairwise_groups {
        if groups_sharing_entity_z.len() < 2 {
            continue;
        }

        for i in 0..groups_sharing_entity_z.len() {
            for j in (i + 1)..groups_sharing_entity_z.len() {
                let group_a_id = &groups_sharing_entity_z[i];
                let group_b_id = &groups_sharing_entity_z[j];

                // Check if we can reuse existing edge calculation
                if let Some(existing_weight) = can_reuse_edge(state, &group_a_id.0, &group_b_id.0) {
                    edges_reused += 1;
                    
                    // Add edge to graph using existing weight
                    let node_a_idx = node_mapper.get_or_add_node(&mut xgraph_instance, group_a_id);
                    let node_b_idx = node_mapper.get_or_add_node(&mut xgraph_instance, group_b_id);
                    
                    if let Err(e) = xgraph_instance.add_edge(node_a_idx, node_b_idx, existing_weight, ()) {
                        warn!(
                            "Failed to add reused edge to xgraph between {} and {}: {}",
                            group_a_id.0, group_b_id.0, e
                        );
                        continue;
                    }
                    
                    debug!(
                        "Reused edge: {} --({:.4})-- {} (from previous run)",
                        group_a_id.0, existing_weight, group_b_id.0
                    );
                    continue;
                }

                // Calculate new edge weight
                edges_calculated += 1;
                
                let c_a = group_confidences
                    .get(group_a_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!("Confidence not found for group_a_id {}. Defaulting to 0.0", group_a_id.0);
                        0.0
                    });

                let c_b = group_confidences
                    .get(group_b_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!("Confidence not found for group_b_id {}. Defaulting to 0.0", group_b_id.0);
                        0.0
                    });

                let w_z = (c_a + c_b) / 2.0;
                let edge_weight = w_z; // Simplified for this entity pair

                if edge_weight <= 0.0 {
                    debug!(
                        "Skipping edge between {} and {} due to non-positive weight {:.4}",
                        group_a_id.0, group_b_id.0, edge_weight
                    );
                    continue;
                }

                let node_a_idx = node_mapper.get_or_add_node(&mut xgraph_instance, group_a_id);
                let node_b_idx = node_mapper.get_or_add_node(&mut xgraph_instance, group_b_id);

                if let Err(e) = xgraph_instance.add_edge(node_a_idx, node_b_idx, edge_weight, ()) {
                    warn!(
                        "Failed to add calculated edge to xgraph between {} and {}: {}",
                        group_a_id.0, group_b_id.0, e
                    );
                    continue;
                }

                debug!(
                    "Calculated new edge: {} --({:.4})-- {}",
                    group_a_id.0, edge_weight, group_b_id.0
                );

                // Store edge for future reuse
                let contributing_entities = vec![ContributingSharedEntityDetail {
                    entity_id: entity_z.0.clone(),
                    conf_entity_in_source_group: c_a,
                    conf_entity_in_target_group: c_b,
                    w_z,
                }];
                
                let contributing_entities_json = serde_json::to_value(&contributing_entities).ok();
                let cfe = NewClusterFormationEdge {
                    pipeline_run_id: pipeline_run_id.to_string(),
                    source_group_id: group_a_id.0.clone(),
                    target_group_id: group_b_id.0.clone(),
                    calculated_edge_weight: edge_weight,
                    contributing_shared_entities: contributing_entities_json,
                };
                batch_edges.push(cfe);

                // Handle weak edge suggestions
                if edge_weight > 0.0 && edge_weight < config::WEAK_LINK_THRESHOLD {
                    let details_json = serde_json::json!({
                        "calculated_edge_weight": edge_weight,
                        "contributing_links_count": 1,
                        "shared_entity_details": contributing_entities,
                    });
                    let reason_message = format!(
                        "Inter-pairwise-group link between PairwiseGroup {} and PairwiseGroup {} has a weak calculated weight of {:.4}",
                        group_a_id.0, group_b_id.0, edge_weight
                    );
                    let suggestion = NewSuggestedAction {
                        pipeline_run_id: Some(pipeline_run_id.to_string()),
                        action_type: ActionType::ReviewInterGroupLink.as_str().to_string(),
                        entity_id: None,
                        group_id_1: Some(group_a_id.0.clone()),
                        group_id_2: Some(group_b_id.0.clone()),
                        cluster_id: None,
                        triggering_confidence: Some(edge_weight),
                        details: Some(details_json),
                        reason_code: Some("WEAK_INTER_PAIRWISE_GROUP_EDGE".to_string()),
                        reason_message: Some(reason_message),
                        priority: 0,
                        status: SuggestionStatus::PendingReview.as_str().to_string(),
                        reviewer_id: None,
                        reviewed_at: None,
                        review_notes: None,
                    };
                    batch_suggestions.push(suggestion);
                }

                // Process batches when full
                if batch_edges.len() >= EDGE_BATCH_SIZE {
                    if let Err(e) = db::insert_cluster_formation_edges_batch(transaction, &batch_edges).await {
                        warn!("Failed to insert batch of cluster formation edges: {}", e);
                    }
                    batch_edges.clear();

                    if !batch_suggestions.is_empty() {
                        if let Err(e) = db::insert_suggestions_batch(transaction, &batch_suggestions).await {
                            warn!("Failed to insert batch of suggestions: {}", e);
                        }
                        batch_suggestions.clear();
                    }
                }
            }
        }
    }

    // Process remaining batches
    if !batch_edges.is_empty() {
        if let Err(e) = db::insert_cluster_formation_edges_batch(transaction, &batch_edges).await {
            warn!("Failed to insert final batch of cluster formation edges: {}", e);
        }
    }

    if !batch_suggestions.is_empty() {
        if let Err(e) = db::insert_suggestions_batch(transaction, &batch_suggestions).await {
            warn!("Failed to insert final batch of suggestions: {}", e);
        }
    }

    let total_elapsed = start_time.elapsed();
    
    info!(
        "Optimized weighted xgraph built in {:.2?} with {} nodes and {} edges.",
        total_elapsed,
        node_mapper.get_node_count(),
        xgraph_instance.edges.len()
    );
    
    info!(
        "Edge optimization: {} edges reused from previous run, {} edges calculated new",
        edges_reused,
        edges_calculated
    );

    Ok((xgraph_instance, node_mapper))
}

/// Optimized cluster verification that skips unchanged clusters
async fn verify_clusters_optimized(
    transaction: &Transaction<'_>,
    new_cluster_ids: &[GroupClusterId],
    pipeline_run_id: &str,
    state: &ClusteringState,
) -> Result<()> {
    info!(
        "Optimized cluster verification for {} clusters (run ID: {})...",
        new_cluster_ids.len(),
        pipeline_run_id
    );

    // Only verify clusters that contain groups added since the last run
    let mut clusters_to_verify = Vec::new();
    
    for cluster_id in new_cluster_ids {
        // Check if this cluster contains any groups that were added since the last run
        let cluster_has_new_groups_query = "
            SELECT EXISTS(
                SELECT 1 FROM public.entity_group 
                WHERE group_cluster_id = $1 
                AND id = ANY($2)
            )";
        
        let new_group_ids: Vec<String> = state.groups_added_since_last_run
            .iter()
            .map(|g| g.0.clone())
            .collect();
        
        if new_group_ids.is_empty() {
            debug!("No new groups to check for cluster {}", cluster_id.0);
            continue;
        }
        
        let has_new_groups = transaction
            .query_one(cluster_has_new_groups_query, &[&cluster_id.0, &new_group_ids])
            .await
            .context("Failed to check if cluster has new groups")?
            .get::<_, bool>(0);
        
        if has_new_groups {
            clusters_to_verify.push(cluster_id.clone());
            debug!("Cluster {} contains new groups and will be verified", cluster_id.0);
        } else {
            debug!("Cluster {} has no new groups, skipping verification", cluster_id.0);
        }
    }
    
    if clusters_to_verify.is_empty() {
        info!("No clusters need verification - all are unchanged from previous run");
        return Ok(());
    }
    
    info!("Verifying {} clusters that contain new groups", clusters_to_verify.len());
    
    // Use the existing lightweight verification for clusters that need it
    verify_clusters_lightweight(transaction, &clusters_to_verify, pipeline_run_id).await
}

pub async fn process_clusters(
    pool: &PgPool,
    pipeline_run_id: &str,
) -> Result<usize> {
    info!(
        "Starting optimized cluster consolidation process (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for process_clusters")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for process_clusters")?;

    // Analyze current state to optimize processing
    let state = analyze_clustering_state(&transaction, pipeline_run_id).await?;
    
    if state.unclustered_groups.is_empty() {
        info!("No unclustered groups found. Skipping consolidation.");
        transaction.commit().await.context("Failed to commit empty cluster transaction")?;
        return Ok(0);
    }
    
    // Early exit if minimal changes since last run
    if state.groups_added_since_last_run.len() < 10 && state.last_clustering_run.is_some() {
        info!(
            "Only {} new groups since last run - considering incremental processing",
            state.groups_added_since_last_run.len()
        );
    }

    info!("Building optimized weighted entity group graph...");
    let build_start = Instant::now();
    let (graph, node_mapper) = build_weighted_xgraph_optimized(pool, &transaction, pipeline_run_id, &state).await?;
    info!("Optimized graph built in {:.2?}", build_start.elapsed());

    if graph.nodes.is_empty() {
        info!("No nodes in the graph. Skipping Leiden clustering.");
        transaction.commit().await.context("Failed to commit empty cluster transaction")?;
        return Ok(0);
    }

    info!(
        "Graph built with {} nodes and {} edges. Starting Leiden clustering...",
        node_mapper.get_node_count(),
        graph.edges.len()
    );

    let leiden_config = CommunityConfig {
        resolution: config::LEIDEN_RESOLUTION_PARAMETER,
        deterministic: true,
        seed: Some(42),
        iterations: 10,
        gamma: 1.0,
    };

    let leiden_start = Instant::now();
    let communities_result = match graph.detect_communities_with_config(leiden_config) {
        Ok(communities) => communities,
        Err(e) => {
            let _ = transaction.rollback().await.map_err(|rb_err| warn!("Rollback failed: {}", rb_err));
            return Err(anyhow::Error::from(e).context("Leiden community detection failed"));
        }
    };
    info!(
        "Leiden algorithm finished in {:.2?}, found {} raw communities.",
        leiden_start.elapsed(),
        communities_result.len()
    );

    let mut clusters_created = 0;
    let mut new_cluster_ids_for_verification = Vec::new();

    for (_community_key, node_indices_in_community) in communities_result.iter() {
        let pairwise_group_ids_in_community: Vec<EntityGroupId> = node_indices_in_community
            .iter()
            .filter_map(|&node_idx| node_mapper.get_group_id(node_idx).cloned())
            .collect();

        if pairwise_group_ids_in_community.is_empty() {
            debug!("Skipping community with 0 mapped pairwise groups.");
            continue;
        }

        // Fetch all unique entity IDs for the cluster
        let mut unique_entities_in_cluster = HashSet::new();
        let group_id_strings_for_query: Vec<String> = pairwise_group_ids_in_community
            .iter()
            .map(|g| g.0.clone())
            .collect();

        let entities_query = "
            SELECT entity_id_1, entity_id_2
            FROM public.entity_group
            WHERE id = ANY($1)";

        let entity_rows = match transaction.query(entities_query, &[&group_id_strings_for_query]).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!("Failed to fetch entities for community {:?}: {}", group_id_strings_for_query, e);
                continue;
            }
        };

        for row in entity_rows {
            unique_entities_in_cluster.insert(EntityId(row.get("entity_id_1")));
            unique_entities_in_cluster.insert(EntityId(row.get("entity_id_2")));
        }
        let unique_entity_count = unique_entities_in_cluster.len() as i32;

        if unique_entity_count < 2 {
            debug!(
                "Skipping community with < 2 unique entities: {:?} ({} entities)",
                group_id_strings_for_query, unique_entity_count
            );
            continue;
        }

        debug!(
            "Processing community of {} pairwise groups, forming a cluster with {} unique entities",
            pairwise_group_ids_in_community.len(),
            unique_entity_count
        );

        let new_cluster_id = GroupClusterId(Uuid::new_v4().to_string());
        if let Err(e) = create_cluster_record(
            &transaction,
            &new_cluster_id,
            pairwise_group_ids_in_community.len() as i32,
            unique_entity_count,
        ).await {
            warn!("Failed to create cluster record for cluster {}: {}", new_cluster_id.0, e);
            continue;
        }

        if let Err(e) = update_entity_groups_with_cluster_id(
            &transaction,
            &new_cluster_id,
            &pairwise_group_ids_in_community,
        ).await {
            warn!("Failed to update pairwise groups for cluster {}: {}", new_cluster_id.0, e);
            continue;
        }

        new_cluster_ids_for_verification.push(new_cluster_id.clone());
        clusters_created += 1;
        info!(
            "Created GroupCluster {} (ID: {}) for {} pairwise groups and {} unique entities.",
            clusters_created,
            new_cluster_id.0,
            pairwise_group_ids_in_community.len(),
            unique_entity_count
        );
    }

    info!("Completed processing Leiden communities. {} new clusters created.", clusters_created);

    if !new_cluster_ids_for_verification.is_empty() {
        info!("Starting optimized cluster verification...");
        let verify_start = Instant::now();
        match verify_clusters_optimized(
            &transaction,
            &new_cluster_ids_for_verification,
            pipeline_run_id,
            &state,
        ).await {
            Ok(_) => info!("Optimized cluster verification completed in {:.2?}.", verify_start.elapsed()),
            Err(e) => warn!("Optimized cluster verification failed: {}", e),
        }
    }

    transaction.commit().await.context("Failed to commit cluster processing transaction")?;
    info!(
        "Optimized cluster consolidation finished in {:.2?}. {} clusters created.",
        start_time.elapsed(),
        clusters_created
    );
    Ok(clusters_created)
}

async fn create_cluster_record(
    transaction: &Transaction<'_>,
    cluster_id: &GroupClusterId,
    pair_count: i32,
    entity_count: i32,
) -> Result<()> {
    let now = Utc::now().naive_utc();
    let cluster_name = format!("LeidenCluster-{}", &cluster_id.0[..8]);
    let description = format!(
        "Leiden-generated cluster from {} pairwise entity groups, {} unique entities.",
        pair_count, entity_count
    );

    transaction.execute(
        "INSERT INTO public.entity_group_cluster (id, name, description, created_at, updated_at, entity_count, group_count, average_coherence_score)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)",
        &[&cluster_id.0, &cluster_name, &description, &now, &now, &entity_count, &pair_count],
    ).await.context("Failed to insert entity_group_cluster")?;
    Ok(())
}

async fn update_entity_groups_with_cluster_id(
    transaction: &Transaction<'_>,
    cluster_id: &GroupClusterId,
    pairwise_group_ids: &[EntityGroupId],
) -> Result<()> {
    if pairwise_group_ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now().naive_utc();
    let group_id_strings: Vec<String> = pairwise_group_ids.iter().map(|g| g.0.clone()).collect();
    transaction.execute(
        "UPDATE public.entity_group SET group_cluster_id = $1, updated_at = $2 WHERE id = ANY($3)",
        &[&cluster_id.0, &now, &group_id_strings],
    ).await.context("Failed to update entity_group with cluster_id for pairwise groups")?;
    Ok(())
}

/// Lightweight coherence calculation using direct PostgreSQL queries
async fn calculate_lightweight_coherence_score(
    conn: &impl tokio_postgres::GenericClient,
    entity1_id: &EntityId,
    entity2_id: &EntityId,
) -> Result<f64> {
    let query = "
        SELECT 
            -- Name similarity
            COALESCE(similarity(LOWER(o1.name), LOWER(o2.name)), 0.0) as name_sim,
            
            -- Embedding similarity (using PostgreSQL cosine similarity)
            CASE 
                WHEN o1.embedding IS NOT NULL AND o2.embedding IS NOT NULL 
                THEN 1.0 - (o1.embedding <=> o2.embedding)
                ELSE 0.0 
            END as embedding_sim,
            
            -- Geographic proximity
            CASE 
                WHEN EXISTS(
                    SELECT 1 FROM public.location l1 
                    JOIN public.entity_feature ef1 ON ef1.table_id = l1.id 
                    JOIN public.location l2 
                    JOIN public.entity_feature ef2 ON ef2.table_id = l2.id 
                    WHERE ef1.entity_id = e1.id AND ef1.table_name = 'location'
                    AND ef2.entity_id = e2.id AND ef2.table_name = 'location'
                    AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL
                ) THEN (
                    SELECT 
                        CASE 
                            WHEN MIN(ST_Distance(l1.geom, l2.geom)) > 10000 THEN 0.0
                            ELSE 1.0 - (MIN(ST_Distance(l1.geom, l2.geom)) / 10000.0)
                        END
                    FROM public.location l1 
                    JOIN public.entity_feature ef1 ON ef1.table_id = l1.id,
                    public.location l2 
                    JOIN public.entity_feature ef2 ON ef2.table_id = l2.id 
                    WHERE ef1.entity_id = e1.id AND ef1.table_name = 'location'
                    AND ef2.entity_id = e2.id AND ef2.table_name = 'location'
                    AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL
                )
                ELSE 0.0
            END as geo_proximity,
            
            -- Max service similarity (using PostgreSQL vector ops)
            COALESCE((
                SELECT MAX(1.0 - (s1.embedding_v2 <=> s2.embedding_v2))
                FROM public.service s1 
                JOIN public.entity_feature ef1 ON ef1.table_id = s1.id,
                public.service s2 
                JOIN public.entity_feature ef2 ON ef2.table_id = s2.id
                WHERE ef1.entity_id = e1.id AND ef1.table_name = 'service'
                AND ef2.entity_id = e2.id AND ef2.table_name = 'service'
                AND s1.embedding_v2 IS NOT NULL AND s2.embedding_v2 IS NOT NULL
            ), 0.0) as max_service_sim
            
        FROM public.entity e1 
        JOIN public.organization o1 ON e1.organization_id = o1.id,
        public.entity e2 
        JOIN public.organization o2 ON e2.organization_id = o2.id
        WHERE e1.id = $1 AND e2.id = $2
    ";

    let row = conn.query_one(query, &[&entity1_id.0, &entity2_id.0]).await
        .context("Failed to calculate lightweight coherence metrics")?;

    let name_sim: f64 = row.get::<_, Option<f32>>("name_sim").unwrap_or(0.0) as f64;
    let embedding_sim: f64 = row.get::<_, Option<f64>>("embedding_sim").unwrap_or(0.0);
    let geo_proximity: f64 = row.get::<_, Option<f64>>("geo_proximity").unwrap_or(0.0);
    let max_service_sim: f64 = row.get::<_, Option<f64>>("max_service_sim").unwrap_or(0.0);

    let coherence_score = (embedding_sim * 0.4) + 
                         (name_sim * 0.3) + 
                         (max_service_sim * 0.2) + 
                         (geo_proximity * 0.1);
    
    let normalized_coherence = coherence_score.max(0.0).min(1.0);
    
    Ok(normalized_coherence)
}

/// Lightweight cluster verification that doesn't require the ML orchestrator
async fn verify_clusters_lightweight(
    transaction: &Transaction<'_>,
    new_cluster_ids: &[GroupClusterId],
    pipeline_run_id: &str,
) -> Result<()> {
    info!(
        "Verifying quality of {} newly formed clusters using lightweight verification (run ID: {})...",
        new_cluster_ids.len(),
        pipeline_run_id
    );

    for cluster_id in new_cluster_ids {
        debug!("Verifying cluster: {}", cluster_id.0);

        let mut entities_in_cluster = HashSet::new();
        let entity_query = "
            SELECT entity_id_1, entity_id_2
            FROM public.entity_group
            WHERE group_cluster_id = $1";

        let entity_rows = match transaction.query(entity_query, &[&cluster_id.0]).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    "Failed to fetch entities for cluster verification {}: {}. Skipping.",
                    cluster_id.0, e
                );
                continue;
            }
        };

        if entity_rows.is_empty() {
            debug!(
                "No pairwise groups found for cluster {} during verification. Skipping.",
                cluster_id.0
            );
            continue;
        }

        for row in entity_rows {
            entities_in_cluster.insert(EntityId(row.get("entity_id_1")));
            entities_in_cluster.insert(EntityId(row.get("entity_id_2")));
        }

        let entities: Vec<EntityId> = entities_in_cluster.into_iter().collect();

        if entities.len() < 2 {
            debug!(
                "Skipping verification for cluster {} as it has < 2 unique entities ({} found).",
                cluster_id.0,
                entities.len()
            );
            continue;
        }

        // Sample pairs for verification
        let mut entity_pairs_to_check: Vec<(EntityId, EntityId)> = Vec::new();
        let max_entities_to_sample = 10;
        let max_pairs_to_check_per_cluster = 20;

        if entities.len() <= max_entities_to_sample {
            for i in 0..entities.len() {
                for j in (i + 1)..entities.len() {
                    if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                        break;
                    }
                    entity_pairs_to_check.push((entities[i].clone(), entities[j].clone()));
                }
                if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                    break;
                }
            }
        } else {
            let mut rng = rand::thread_rng();
            let sampled_entities: Vec<EntityId> = entities
                .choose_multiple(&mut rng, max_entities_to_sample)
                .cloned()
                .collect();
            for i in 0..sampled_entities.len() {
                for j in (i + 1)..sampled_entities.len() {
                    if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                        break;
                    }
                    entity_pairs_to_check
                        .push((sampled_entities[i].clone(), sampled_entities[j].clone()));
                }
                if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                    break;
                }
            }
        }

        debug!(
            "Verifying cluster {} with {} sampled entity pairs (out of {} total entities) using lightweight verification.",
            cluster_id.0,
            entity_pairs_to_check.len(),
            entities.len()
        );

        let mut verification_scores: Vec<f64> = Vec::new();

        for (entity1_id, entity2_id) in entity_pairs_to_check {
            match calculate_lightweight_coherence_score(transaction, &entity1_id, &entity2_id).await {
                Ok(coherence_score) => {
                    verification_scores.push(coherence_score);
                    debug!(
                        "Cluster {} lightweight coherence: pair ({}, {}) scored {:.4}",
                        cluster_id.0, entity1_id.0, entity2_id.0, coherence_score
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to calculate lightweight coherence for pair ({}, {}) in cluster {}: {}",
                        entity1_id.0, entity2_id.0, cluster_id.0, e
                    );
                }
            }
        }

        let avg_score_to_store: Option<f64> = if verification_scores.is_empty() {
            None
        } else {
            Some(verification_scores.iter().sum::<f64>() / verification_scores.len() as f64)
        };

        let update_score_query = "
            UPDATE public.entity_group_cluster
            SET average_coherence_score = $1
            WHERE id = $2";
        if let Err(e) = transaction
            .execute(update_score_query, &[&avg_score_to_store, &cluster_id.0])
            .await
        {
            warn!(
                "Failed to update average_coherence_score for cluster {}: {}",
                cluster_id.0, e
            );
        }

        if let Some(calculated_avg_score) = avg_score_to_store {
            if calculated_avg_score < config::VERIFICATION_THRESHOLD {
                let details_json = serde_json::json!({
                    "average_internal_pair_confidence": calculated_avg_score,
                    "entities_in_cluster_count": entities.len(),
                    "checked_pairs_count": verification_scores.len(),
                    "verification_threshold": config::VERIFICATION_THRESHOLD,
                    "verification_scores_sample": verification_scores.iter().take(5).copied().collect::<Vec<_>>(),
                    "verification_method": "lightweight_postgres_based"
                });
                let reason_message = format!(
                    "Cluster {} ({} entities, {} pairs checked) has low average internal entity-pair confidence: {:.4}, below threshold of {}. Verified using lightweight method.",
                    cluster_id.0, entities.len(), verification_scores.len(), calculated_avg_score, config::VERIFICATION_THRESHOLD
                );
                let suggestion = NewSuggestedAction {
                    pipeline_run_id: Some(pipeline_run_id.to_string()),
                    action_type: ActionType::SuggestSplitCluster.as_str().to_string(),
                    entity_id: None,
                    group_id_1: None,
                    group_id_2: None,
                    cluster_id: Some(cluster_id.0.clone()),
                    triggering_confidence: Some(calculated_avg_score),
                    details: Some(details_json),
                    reason_code: Some("LOW_INTERNAL_CLUSTER_COHERENCE_LIGHTWEIGHT".to_string()),
                    reason_message: Some(reason_message),
                    priority: 1,
                    status: SuggestionStatus::PendingReview.as_str().to_string(),
                    reviewer_id: None,
                    reviewed_at: None,
                    review_notes: None,
                };
                if let Err(e) = db::insert_suggestion(transaction, &suggestion).await {
                    warn!(
                        "Failed to log SUGGEST_SPLIT_CLUSTER for cluster {}: {}",
                        cluster_id.0, e
                    );
                }
            }
        }
    }
    info!("Finished lightweight verification of all new clusters.");
    Ok(())
}