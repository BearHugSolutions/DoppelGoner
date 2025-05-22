// src/consolidate_clusters.rs
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet}; // HashSet might be useful for unique entity collection
use std::time::Instant;
use tokio::sync::Mutex;
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;
use xgraph::graph::graph::NodeId; // Assuming xgraph::NodeId is usize or similar

// xgraph imports
use xgraph::leiden_clustering::{CommunityConfig, CommunityDetection};
use xgraph::Graph;

// Local imports
use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType,
    ContributingSharedEntityDetail, // For edge logging (ensure fields align with new pairwise confidence)
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

// Refactored db::get_confidence_for_entity_in_group (conceptual placement, actual might be in db.rs)
// This function's logic changes significantly. Given a *pairwise* entity_group_id,
// it should return its confidence_score if the entity_id is part of that pair.
async fn get_confidence_for_pairwise_group(
    transaction: &impl GenericClient, // Use GenericClient for flexibility with Transaction
    entity_group_id: &str,
    // entity_id: &str, // This parameter might become less relevant if we just fetch the group's confidence
) -> Result<Option<f64>> {
    let row = transaction
        .query_opt(
            "SELECT entity_id_1, entity_id_2, confidence_score FROM public.entity_group WHERE id = $1",
            &[&entity_group_id],
        )
        .await
        .context("Failed to fetch pairwise entity_group for confidence")?;

    if let Some(row) = row {
        // let entity_id_1: String = row.get("entity_id_1");
        // let entity_id_2: String = row.get("entity_id_2");
        // if entity_id_1 == entity_id || entity_id_2 == entity_id {
        // Directly return the confidence score of the pair
        Ok(row.get("confidence_score"))
        // } else {
        //     warn!(
        //         "Entity {} not found in pairwise group {}. This check might be redundant.",
        //         entity_id, entity_group_id
        //     );
        //     Ok(None)
        // }
    } else {
        Ok(None)
    }
}

async fn build_weighted_xgraph(
    pool: &PgPool, // Keep PgPool for other db operations if necessary outside transaction scope
    transaction: &Transaction<'_>,
    pipeline_run_id: &str,
) -> Result<(Graph<f64, EntityGroupId, ()>, NodeMapper)> {
    info!(
        "Building weighted xgraph for cluster consolidation (run ID: {}) using streaming approach...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    // 1. Nodes: Fetch pairwise entity_group records where group_cluster_id IS NULL
    // These records are now direct links (entity1, entity2, confidence)
    let groups_query = "SELECT id, entity_id_1, entity_id_2, confidence_score FROM public.entity_group WHERE group_cluster_id IS NULL";
    let group_rows = transaction
        .query(groups_query, &[])
        .await
        .context("Failed to fetch unclustered pairwise entity groups")?;

    if group_rows.is_empty() {
        info!("No unclustered entity groups found to build graph.");
        return Ok((Graph::new(false), NodeMapper::new()));
    }

    let mut node_mapper = NodeMapper::new();
    let mut xgraph_instance: Graph<f64, EntityGroupId, ()> = Graph::new(false); // Undirected
    let mut entity_to_pairwise_groups: HashMap<EntityId, Vec<EntityGroupId>> = HashMap::new();

    // Map to store confidence of each pairwise group (node)
    let mut group_confidences: HashMap<EntityGroupId, f64> = HashMap::new();

    info!("Loading {} pairwise entity groups...", group_rows.len());
    let loading_start = Instant::now();

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
        "Loaded {} pairwise entity groups as nodes in {:.2?}.",
        node_mapper.get_node_count(),
        loading_start.elapsed()
    );
    info!(
        "Processed entity memberships for {} unique entities in these pairs.",
        entity_to_pairwise_groups.len()
    );

    // 2. Edge Creation & Weighting: Stream processing with batching
    const ENTITY_BATCH_SIZE: usize = 100; // Process entities in batches
    const EDGE_BATCH_SIZE: usize = 1000; // Database batch size
    const PROGRESS_INTERVAL: usize = 10000; // Log progress every N edges

    let total_entities = entity_to_pairwise_groups.len();
    let mut processed_entities = 0;
    let mut total_edges_processed = 0;
    let mut valid_edges_added = 0;

    // Estimate total potential edges for progress tracking
    let estimated_total_edges: usize = entity_to_pairwise_groups
        .values()
        .map(|groups| if groups.len() >= 2 { groups.len() * (groups.len() - 1) / 2 } else { 0 })
        .sum();
    
    info!(
        "Estimated {} potential edges to process from {} entities.",
        estimated_total_edges, total_entities
    );

    let edge_processing_start = Instant::now();
    
    // Batch processing buffers
    let mut batch_edges = Vec::with_capacity(EDGE_BATCH_SIZE);
    let mut batch_suggestions = Vec::with_capacity(EDGE_BATCH_SIZE);
    
    // Process entities in chunks to limit memory usage
    let entities_vec: Vec<_> = entity_to_pairwise_groups.into_iter().collect();
    let entity_chunks = entities_vec.chunks(ENTITY_BATCH_SIZE);
    let total_chunks = entities_vec.len().div_ceil(ENTITY_BATCH_SIZE);

    for (chunk_idx, entity_chunk) in entity_chunks.enumerate() {
        // Accumulate edge contributions for this chunk
        let mut chunk_edge_contributions: HashMap<
            (EntityGroupId, EntityGroupId),
            Vec<ContributingSharedEntityDetail>,
        > = HashMap::new();

        // Process each entity in the chunk
        for (entity_z, groups_sharing_entity_z) in entity_chunk {
            processed_entities += 1;
            
            if groups_sharing_entity_z.len() < 2 {
                continue; // No edge can be formed by this shared entity
            }

            // Generate pairs of *pairwise groups* that share entity_z
            for i in 0..groups_sharing_entity_z.len() {
                for j in (i + 1)..groups_sharing_entity_z.len() {
                    let group_a_id = &groups_sharing_entity_z[i];
                    let group_b_id = &groups_sharing_entity_z[j];

                    // Confidence of GroupA (pair A) is its direct confidence_score
                    let c_a = group_confidences
                        .get(group_a_id)
                        .cloned()
                        .unwrap_or_else(|| {
                            warn!(
                                "Confidence not found for group_a_id {}. Defaulting to 0.0",
                                group_a_id.0
                            );
                            0.0
                        });

                    // Confidence of GroupB (pair B) is its direct confidence_score
                    let c_b = group_confidences
                        .get(group_b_id)
                        .cloned()
                        .unwrap_or_else(|| {
                            warn!(
                                "Confidence not found for group_b_id {}. Defaulting to 0.0",
                                group_b_id.0
                            );
                            0.0
                        });

                    // W_z is the average confidence of the two pairs linked by entity_z
                    let w_z = (c_a + c_b) / 2.0;

                    let key = if group_a_id.0 < group_b_id.0 {
                        (group_a_id.clone(), group_b_id.clone())
                    } else {
                        (group_b_id.clone(), group_a_id.clone())
                    };

                    chunk_edge_contributions
                        .entry(key)
                        .or_default()
                        .push(ContributingSharedEntityDetail {
                            entity_id: entity_z.0.clone(),
                            conf_entity_in_source_group: c_a,
                            conf_entity_in_target_group: c_b,
                            w_z,
                        });
                }
            }
        }

        // Process accumulated edges for this chunk
        info!(
            "Processing chunk {}/{} with {} potential edges...",
            chunk_idx + 1,
            total_chunks,
            chunk_edge_contributions.len()
        );

        for ((group_a_id, group_b_id), contributions) in chunk_edge_contributions {
            if contributions.is_empty() {
                total_edges_processed += 1;
                continue;
            }

            let product_of_negations = contributions.iter().fold(1.0, |acc, detail| {
                acc * (1.0 - detail.w_z.max(0.0).min(1.0)) // Clamp w_z
            });
            let edge_weight = 1.0 - product_of_negations;

            if edge_weight <= 0.0 {
                debug!(
                    "Skipping edge between {} and {} due to non-positive weight {:.4}",
                    group_a_id.0, group_b_id.0, edge_weight
                );
                total_edges_processed += 1;
                continue;
            }

            let node_a_idx = node_mapper.get_or_add_node(&mut xgraph_instance, &group_a_id);
            let node_b_idx = node_mapper.get_or_add_node(&mut xgraph_instance, &group_b_id);

            if let Err(e) = xgraph_instance.add_edge(node_a_idx, node_b_idx, edge_weight, ()) {
                warn!(
                    "Failed to add edge to xgraph between {} ({:?}) and {} ({:?}): {}",
                    group_a_id.0, node_a_idx, group_b_id.0, node_b_idx, e
                );
                total_edges_processed += 1;
                continue;
            }

            valid_edges_added += 1;
            debug!(
                "Added edge to xgraph: {} --({:.4})-- {}",
                group_a_id.0, edge_weight, group_b_id.0
            );

            // Prepare for batch database insertion
            let contributing_entities_json = serde_json::to_value(&contributions).ok();
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
                    "contributing_links_count": contributions.len(),
                    "shared_entity_details": contributions,
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

            total_edges_processed += 1;

            // Process database batches when they're full
            if batch_edges.len() >= EDGE_BATCH_SIZE {
                if let Err(e) = db::insert_cluster_formation_edges_batch(transaction, &batch_edges).await {
                    warn!("Failed to insert batch of cluster formation edges: {}", e);
                } else {
                    debug!("Inserted batch of {} cluster formation edges", batch_edges.len());
                }
                batch_edges.clear();

                if !batch_suggestions.is_empty() {
                    if let Err(e) = db::insert_suggestions_batch(transaction, &batch_suggestions).await {
                        warn!("Failed to insert batch of suggestions: {}", e);
                    } else {
                        debug!("Inserted batch of {} suggestions", batch_suggestions.len());
                    }
                    batch_suggestions.clear();
                }
            }

            // Progress logging
            if total_edges_processed % PROGRESS_INTERVAL == 0 && total_edges_processed > 0 {
                let elapsed = edge_processing_start.elapsed();
                let rate = total_edges_processed as f64 / elapsed.as_secs_f64();
                let eta_seconds = if rate > 0.0 {
                    (estimated_total_edges.saturating_sub(total_edges_processed)) as f64 / rate
                } else {
                    0.0
                };
                
                info!(
                    "Edge processing progress: {}/{} ({:.1}%) | Valid edges: {} | Rate: {:.0} edges/sec | ETA: {:.1}min",
                    total_edges_processed,
                    estimated_total_edges,
                    (total_edges_processed as f64 / estimated_total_edges.max(1) as f64) * 100.0,
                    valid_edges_added,
                    rate,
                    eta_seconds / 60.0
                );
            }
        }

        // Progress logging per chunk
        let chunk_elapsed = edge_processing_start.elapsed();
        let entity_rate = processed_entities as f64 / chunk_elapsed.as_secs_f64();
        let entities_eta = if entity_rate > 0.0 {
            (total_entities.saturating_sub(processed_entities)) as f64 / entity_rate
        } else {
            0.0
        };

        info!(
            "Chunk {}/{} completed. Processed {}/{} entities ({:.1}%) | Entity rate: {:.0}/sec | ETA: {:.1}min",
            chunk_idx + 1,
            total_chunks,
            processed_entities,
            total_entities,
            (processed_entities as f64 / total_entities as f64) * 100.0,
            entity_rate,
            entities_eta / 60.0
        );
    }

    // Process remaining batches
    if !batch_edges.is_empty() {
        if let Err(e) = db::insert_cluster_formation_edges_batch(transaction, &batch_edges).await {
            warn!("Failed to insert final batch of cluster formation edges: {}", e);
        } else {
            info!("Inserted final batch of {} cluster formation edges", batch_edges.len());
        }
    }

    if !batch_suggestions.is_empty() {
        if let Err(e) = db::insert_suggestions_batch(transaction, &batch_suggestions).await {
            warn!("Failed to insert final batch of suggestions: {}", e);
        } else {
            info!("Inserted final batch of {} suggestions", batch_suggestions.len());
        }
    }

    let total_elapsed = start_time.elapsed();
    let edge_processing_elapsed = edge_processing_start.elapsed();
    
    info!(
        "Weighted xgraph built in {:.2?} (edge processing: {:.2?}) with {} nodes and {} edges.",
        total_elapsed,
        edge_processing_elapsed,
        node_mapper.get_node_count(),
        xgraph_instance.edges.len()
    );
    
    info!(
        "Edge processing summary: {}/{} edges processed, {} valid edges added to graph",
        total_edges_processed,
        estimated_total_edges,
        valid_edges_added
    );

    Ok((xgraph_instance, node_mapper))
}

pub async fn process_clusters(
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Mutex<MatchingOrchestrator>>,
    pipeline_run_id: &str,
) -> Result<usize> {
    info!(
        "Starting cluster consolidation process (run ID: {})...",
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

    info!("Building weighted entity group graph (pairwise nodes)...");
    let build_start = Instant::now();
    let (graph, node_mapper) = build_weighted_xgraph(pool, &transaction, pipeline_run_id).await?;
    info!("Graph built in {:.2?}", build_start.elapsed());

    if graph.nodes.is_empty() {
        info!("No nodes in the graph. Skipping Leiden clustering.");
        transaction
            .commit()
            .await
            .context("Failed to commit empty cluster transaction")?;
        return Ok(0);
    }

    info!(
        "Graph built with {} nodes (pairwise groups) and {} edges. Starting Leiden clustering...",
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
            let _ = transaction
                .rollback()
                .await
                .map_err(|rb_err| warn!("Rollback failed: {}", rb_err));
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
        // These node_indices map to pairwise EntityGroupIds
        let pairwise_group_ids_in_community: Vec<EntityGroupId> = node_indices_in_community
            .iter()
            .filter_map(|&node_idx| node_mapper.get_group_id(node_idx).cloned())
            .collect();

        // A community of pairwise_group_ids forms a cluster.
        // We need more than one *pairwise group* to form a meaningful new cluster from multiple links.
        // Or, a single pairwise group that doesn't connect to others can also be its own "cluster" (of 2 entities).
        // The original logic was "if group_ids_in_community.len() < 2", which meant < 2 original multi-entity groups.
        // Now, if a community has just ONE pairwise_group_id, it means that pair didn't connect to any other pairs.
        // It's still a cluster of two entities.
        // The condition "if pairwise_group_ids_in_community.len() < 1" would be more appropriate if we are only interested in >1 pair.
        // Let's consider a cluster is valid even if formed by a single pair that couldn't merge further.
        if pairwise_group_ids_in_community.is_empty() {
            debug!("Skipping community with 0 mapped pairwise groups.");
            continue;
        }

        // Fetch all unique entity_id_1 and entity_id_2 from these pairwise_group_ids
        let mut unique_entities_in_cluster = HashSet::new();
        let group_id_strings_for_query: Vec<String> = pairwise_group_ids_in_community
            .iter()
            .map(|g| g.0.clone())
            .collect();

        if group_id_strings_for_query.is_empty() {
            continue;
        }

        // Query to get all entity IDs from the involved pairwise groups
        let entities_query = "
            SELECT entity_id_1, entity_id_2
            FROM public.entity_group
            WHERE id = ANY($1)";

        let entity_rows = match transaction
            .query(entities_query, &[&group_id_strings_for_query])
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    "Failed to fetch entities for community {:?}: {}",
                    group_id_strings_for_query, e
                );
                continue;
            }
        };

        for row in entity_rows {
            unique_entities_in_cluster.insert(EntityId(row.get("entity_id_1")));
            unique_entities_in_cluster.insert(EntityId(row.get("entity_id_2")));
        }
        let unique_entity_count = unique_entities_in_cluster.len() as i32;

        // A cluster makes sense if it has at least 2 unique entities.
        // A single pairwise group already guarantees this.
        if unique_entity_count < 2 {
            debug!(
                "Skipping community with < 2 unique entities: {:?} ({} entities)",
                group_id_strings_for_query, unique_entity_count
            );
            continue;
        }

        debug!(
            "Processing community of {} pairwise groups, forming a cluster with {} unique entities: {:?}",
            pairwise_group_ids_in_community.len(),
            unique_entity_count,
            pairwise_group_ids_in_community.iter().map(|g| &g.0).collect::<Vec<_>>()
        );

        let new_cluster_id = GroupClusterId(Uuid::new_v4().to_string());
        if let Err(e) = create_cluster_record(
            &transaction,
            &new_cluster_id,
            pairwise_group_ids_in_community.len() as i32, // This is now count of PAIRS
            unique_entity_count,
        )
        .await
        {
            warn!(
                "Failed to create cluster record for cluster {}: {}",
                new_cluster_id.0, e
            );
            continue;
        }

        // Update all the *pairwise* entity_group records with this new_cluster_id
        if let Err(e) = update_entity_groups_with_cluster_id(
            &transaction,
            &new_cluster_id,
            &pairwise_group_ids_in_community,
        )
        .await
        {
            warn!(
                "Failed to update pairwise groups for cluster {}: {}",
                new_cluster_id.0, e
            );
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

    info!(
        "Completed processing Leiden communities. {} new clusters created.",
        clusters_created
    );

    if let Some(orchestrator_ref) = reinforcement_orchestrator {
        if !new_cluster_ids_for_verification.is_empty() {
            info!(
                "Verifying {} newly formed clusters...",
                new_cluster_ids_for_verification.len()
            );
            let verify_start = Instant::now();
            match verify_clusters(
                &transaction, // Pass the transaction for logging suggestions AND UPDATING SCORE
                pool,         // Pass the pool for orchestrator's context extraction
                &new_cluster_ids_for_verification,
                orchestrator_ref,
                pipeline_run_id,
            )
            .await
            {
                Ok(_) => info!(
                    "Cluster verification step completed in {:.2?}.",
                    verify_start.elapsed()
                ),
                Err(e) => warn!("Cluster verification step failed: {}", e),
            }
        }
    }

    transaction
        .commit()
        .await
        .context("Failed to commit cluster processing transaction")?;
    info!(
        "Cluster consolidation finished in {:.2?}. {} clusters created.",
        start_time.elapsed(),
        clusters_created
    );
    Ok(clusters_created)
}

async fn create_cluster_record(
    transaction: &Transaction<'_>,
    cluster_id: &GroupClusterId,
    pair_count: i32, // Renamed from group_count to be more specific
    entity_count: i32,
) -> Result<()> {
    let now = Utc::now().naive_utc();
    let cluster_name = format!("LeidenCluster-{}", &cluster_id.0[..8]);
    let description = format!(
        "Leiden-generated cluster from {} pairwise entity groups, {} unique entities.",
        pair_count, entity_count
    );

    // The average_coherence_score column is nullable and will be populated by verify_clusters.
    // It will default to NULL upon insertion here.
    // Explicitly listing columns for clarity and future-proofing.
    transaction.execute(
        "INSERT INTO public.entity_group_cluster (id, name, description, created_at, updated_at, entity_count, group_count, average_coherence_score)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)", // group_count here refers to number of pairs, average_coherence_score is NULL initially
        &[&cluster_id.0, &cluster_name, &description, &now, &now, &entity_count, &pair_count],
    ).await.context("Failed to insert entity_group_cluster")?;
    Ok(())
}

async fn update_entity_groups_with_cluster_id(
    transaction: &Transaction<'_>,
    cluster_id: &GroupClusterId,
    pairwise_group_ids: &[EntityGroupId], // These are IDs of pairwise entity_group records
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

async fn verify_clusters(
    transaction: &Transaction<'_>, // For logging suggestions AND UPDATING SCORE
    pool: &PgPool,                 // For MatchingOrchestrator's DB needs
    new_cluster_ids: &[GroupClusterId],
    reinforcement_orchestrator_mutex: &Mutex<MatchingOrchestrator>,
    pipeline_run_id: &str,
) -> Result<()> {
    info!(
        "Verifying quality of {} newly formed clusters (run ID: {})...",
        new_cluster_ids.len(),
        pipeline_run_id
    );

    for cluster_id in new_cluster_ids {
        debug!("Verifying cluster: {}", cluster_id.0);

        // Fetch all unique entity_ids for the current cluster
        // by collecting entity_id_1 and entity_id_2 from all pairwise entity_group records
        // associated with this group_cluster_id.
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
            // If no entities, we can't calculate a score, so update with NULL.
            let update_null_score_query = "
                UPDATE public.entity_group_cluster
                SET average_coherence_score = NULL
                WHERE id = $1";
            if let Err(e) = transaction
                .execute(update_null_score_query, &[&cluster_id.0])
                .await
            {
                warn!(
                    "Failed to update average_coherence_score to NULL for empty cluster {}: {}",
                    cluster_id.0, e
                );
            }
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
            // If less than 2 entities, score is not meaningful. Update with NULL.
            let update_null_score_query = "
                UPDATE public.entity_group_cluster
                SET average_coherence_score = NULL
                WHERE id = $1";
            if let Err(e) = transaction
                .execute(update_null_score_query, &[&cluster_id.0])
                .await
            {
                warn!(
                    "Failed to update average_coherence_score to NULL for cluster {} with < 2 entities: {}",
                    cluster_id.0, e
                );
            }
            continue;
        }

        // Sample pairs of entities if the cluster is too large
        let mut entity_pairs_to_check: Vec<(EntityId, EntityId)> = Vec::new();
        let max_entities_to_sample = 10; // If more than 10 entities, sample pairs
        let max_pairs_to_check_per_cluster = 20; // Max pairs to check

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
            // Basic sampling: just take pairs from the first N entities
            // More sophisticated sampling might be needed for very large clusters
            use rand::seq::SliceRandom;
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

        if entity_pairs_to_check.is_empty() && entities.len() >= 2 {
            // Handle case where sampling yielded no pairs but should have
            // Fallback to first pair if possible
            if entities.len() >= 2 {
                entity_pairs_to_check.push((entities[0].clone(), entities[1].clone()));
            }
        }

        debug!(
            "Verifying cluster {} with {} sampled entity pairs (out of {} total entities).",
            cluster_id.0,
            entity_pairs_to_check.len(),
            entities.len()
        );

        let mut verification_scores: Vec<f64> = Vec::new();

        // Get the orchestrator to leverage its feature cache for efficient feature extraction
        let orchestrator = reinforcement_orchestrator_mutex.lock().await;

        for (entity1_id, entity2_id) in entity_pairs_to_check {
            debug!(
                "Verifying pair: ({}, {}) for cluster {}",
                entity1_id.0, entity2_id.0, cluster_id.0
            );

            // Use the orchestrator's cached feature extraction to get the 31-element context vector
            match orchestrator.get_pair_features(pool, &entity1_id, &entity2_id).await {
                Ok(context_features) => {
                    // Calculate cluster coherence score directly from context features
                    // We'll use key similarity features from the 31-element vector:
                    // - Index 13: embedding_similarity (pairwise feature)
                    // - Index 12: name_similarity (pairwise feature) 
                    // - Index 14: max_service_similarity (pairwise feature)
                    // - Index 15: geographic_distance (pairwise feature, inverted for similarity)
                    
                    if context_features.len() >= 19 { // Ensure we have all the features we need
                        let name_sim = context_features[12].max(0.0).min(1.0);
                        let embedding_sim = context_features[13].max(-1.0).min(1.0);
                        let service_sim = context_features[14].max(-1.0).min(1.0);
                        let geo_proximity = context_features[15].max(0.0).min(1.0); // Already normalized as proximity
                        
                        // Composite coherence score: weighted average of key similarity measures
                        // Embedding similarity gets highest weight as it's most comprehensive
                        let coherence_score = (embedding_sim * 0.4) + 
                                            (name_sim * 0.3) + 
                                            (service_sim * 0.2) + 
                                            (geo_proximity * 0.1);
                        
                        // Normalize to 0-1 range (embedding_sim can be negative)
                        let normalized_coherence = ((coherence_score + 1.0) / 2.0).max(0.0).min(1.0);
                        
                        verification_scores.push(normalized_coherence);
                        debug!(
                            "Cluster {} coherence: pair ({}, {}) scored {:.4} (name:{:.3}, emb:{:.3}, svc:{:.3}, geo:{:.3})",
                            cluster_id.0, entity1_id.0, entity2_id.0, normalized_coherence, 
                            name_sim, embedding_sim, service_sim, geo_proximity
                        );
                    } else {
                        warn!(
                            "Insufficient features ({}) for coherence calculation of pair ({}, {}) in cluster {}",
                            context_features.len(), entity1_id.0, entity2_id.0, cluster_id.0
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to extract features for pair ({}, {}) in cluster {}: {}",
                        entity1_id.0, entity2_id.0, cluster_id.0, e
                    );
                    // Continue with other pairs rather than failing completely
                }
            }
        }

        // Drop the orchestrator lock
        drop(orchestrator);

        let avg_score_to_store: Option<f64>; // Use Option for clarity in update

        if verification_scores.is_empty() {
            debug!("No verification scores obtained for cluster {}. Storing NULL for average_coherence_score.", cluster_id.0);
            avg_score_to_store = None;
        } else {
            let calculated_avg_score: f64 =
                verification_scores.iter().sum::<f64>() / verification_scores.len() as f64;
            avg_score_to_store = Some(calculated_avg_score);
            debug!(
                "Cluster {} - Avg internal pair confidence: {:.4} from {} scores. Storing this score.",
                cluster_id.0,
                calculated_avg_score,
                verification_scores.len()
            );
        }

        // Update the group_cluster table with the calculated average_coherence_score (or NULL)
        // Ensure this uses the `transaction` object.
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
            // Decide on error handling: continue or propagate?
            // Current pattern is to warn and continue for non-critical updates within loops.
        } else {
            if let Some(score_val) = avg_score_to_store {
                info!(
                    "Successfully stored average_coherence_score {:.4} for cluster {}.",
                    score_val, cluster_id.0
                );
            } else {
                info!(
                    "Successfully stored NULL average_coherence_score for cluster {} (no scores calculated).",
                    cluster_id.0
                );
            }
        }

        // Existing logic for checking threshold and logging suggestion (only if a score was calculated)
        if let Some(calculated_avg_score) = avg_score_to_store {
            // Check if a score was actually calculated
            if calculated_avg_score < config::VERIFICATION_THRESHOLD {
                info!("Cluster {} has low internal coherence (avg score: {:.4}, threshold: {}). Logging SUGGEST_SPLIT_CLUSTER.", cluster_id.0, calculated_avg_score, config::VERIFICATION_THRESHOLD);
                let details_json = serde_json::json!({
                    "average_internal_pair_confidence": calculated_avg_score, // Use the actual calculated score
                    "entities_in_cluster_count": entities.len(),
                    "checked_pairs_count": verification_scores.len(),
                    "verification_threshold": config::VERIFICATION_THRESHOLD,
                    "verification_scores_sample": verification_scores.iter().take(5).copied().collect::<Vec<_>>(),
                });
                let reason_message = format!(
                    "Cluster {} ({} entities, {} pairs checked) has low average internal entity-pair confidence: {:.4}, below threshold of {}.",
                    cluster_id.0, entities.len(), verification_scores.len(), calculated_avg_score, config::VERIFICATION_THRESHOLD
                );
                let suggestion = NewSuggestedAction {
                    pipeline_run_id: Some(pipeline_run_id.to_string()),
                    action_type: ActionType::SuggestSplitCluster.as_str().to_string(),
                    entity_id: None,
                    group_id_1: None,
                    group_id_2: None,
                    cluster_id: Some(cluster_id.0.clone()),
                    triggering_confidence: Some(calculated_avg_score), // Use the actual calculated score
                    details: Some(details_json),
                    reason_code: Some("LOW_INTERNAL_CLUSTER_COHERENCE".to_string()),
                    reason_message: Some(reason_message),
                    priority: 1,
                    status: SuggestionStatus::PendingReview.as_str().to_string(),
                    reviewer_id: None,
                    reviewed_at: None,
                    review_notes: None,
                };
                match db::insert_suggestion(transaction, &suggestion).await {
                    Ok(id) => info!(
                        "Logged SUGGEST_SPLIT_CLUSTER suggestion {} for cluster {}.",
                        id, cluster_id.0
                    ),
                    Err(e) => warn!(
                        "Failed to log SUGGEST_SPLIT_CLUSTER for cluster {}: {}",
                        cluster_id.0, e
                    ),
                }
            } else {
                info!(
                    "Cluster {} passed verification (avg score: {:.4}, threshold: {}).",
                    cluster_id.0,
                    calculated_avg_score,
                    config::VERIFICATION_THRESHOLD
                );
            }
        }
        // No "else" needed here if avg_score_to_store was None, as suggestion logic depends on a score.
    }
    info!("Finished verifying all new clusters.");
    Ok(())
}
