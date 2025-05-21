// src/service_consolidate_clusters.rs
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
    ContributingSharedServiceDetail, // New struct (will need to be added to models.rs)
    NewClusterFormationEdge,
    NewSuggestedAction,
    ServiceGroupClusterId,
    ServiceGroupId,
    ServiceId,
    SuggestionStatus,
};
use crate::reinforcement::service::service_orchestrator::ServiceMatchingOrchestrator;

#[derive(Debug)]
struct NodeMapper {
    group_to_idx: HashMap<ServiceGroupId, NodeId>,
    idx_to_group: HashMap<NodeId, ServiceGroupId>,
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
        graph: &mut Graph<f64, ServiceGroupId, ()>,
        group_id: &ServiceGroupId,
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

    fn get_group_id(&self, idx: NodeId) -> Option<&ServiceGroupId> {
        self.idx_to_group.get(&idx)
    }

    fn get_node_count(&self) -> usize {
        self.idx_to_group.len()
    }
}

// Get confidence score for a pairwise service group
async fn get_confidence_for_pairwise_group(
    transaction: &impl GenericClient,
    service_group_id: &str,
) -> Result<Option<f64>> {
    let row = transaction
        .query_opt(
            "SELECT service_id_1, service_id_2, confidence_score FROM public.service_group WHERE id = $1",
            &[&service_group_id],
        )
        .await
        .context("Failed to fetch pairwise service_group for confidence")?;

    if let Some(row) = row {
        Ok(row.get("confidence_score"))
    } else {
        Ok(None)
    }
}

async fn build_weighted_xgraph(
    pool: &PgPool,
    transaction: &Transaction<'_>,
    pipeline_run_id: &str,
) -> Result<(Graph<f64, ServiceGroupId, ()>, NodeMapper)> {
    info!(
        "Building weighted xgraph for service cluster consolidation (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    // 1. Nodes: Fetch pairwise service_group records where group_cluster_id IS NULL
    let groups_query = "SELECT id, service_id_1, service_id_2, confidence_score FROM public.service_group WHERE group_cluster_id IS NULL";
    let group_rows = transaction
        .query(groups_query, &[])
        .await
        .context("Failed to fetch unclustered pairwise service groups")?;

    if group_rows.is_empty() {
        info!("No unclustered service groups found to build graph.");
        return Ok((Graph::new(false), NodeMapper::new()));
    }

    let mut node_mapper = NodeMapper::new();
    let mut xgraph_instance: Graph<f64, ServiceGroupId, ()> = Graph::new(false); // Undirected
    let mut service_to_pairwise_groups: HashMap<ServiceId, Vec<ServiceGroupId>> = HashMap::new();

    // Map to store confidence of each pairwise group (node)
    let mut group_confidences: HashMap<ServiceGroupId, f64> = HashMap::new();

    for row in &group_rows {
        let group_id_str: String = row.get("id");
        let service_group_id = ServiceGroupId(group_id_str);
        let service_id_1_str: String = row.get("service_id_1");
        let service_id_2_str: String = row.get("service_id_2");
        let confidence_score: Option<f64> = row.get("confidence_score");

        let _node_id = node_mapper.get_or_add_node(&mut xgraph_instance, &service_group_id);

        if let Some(conf) = confidence_score {
            group_confidences.insert(service_group_id.clone(), conf);
        } else {
            // Default to 0.0 if confidence is NULL
            warn!(
                "Pairwise service group {} has NULL confidence_score. Defaulting to 0.0.",
                service_group_id.0
            );
            group_confidences.insert(service_group_id.clone(), 0.0);
        }

        service_to_pairwise_groups
            .entry(ServiceId(service_id_1_str))
            .or_default()
            .push(service_group_id.clone());
        service_to_pairwise_groups
            .entry(ServiceId(service_id_2_str))
            .or_default()
            .push(service_group_id.clone());
    }

    info!(
        "Loaded {} pairwise service groups as nodes.",
        node_mapper.get_node_count()
    );
    info!(
        "Processed service memberships for {} unique services in these pairs.",
        service_to_pairwise_groups.len()
    );

    // 2. Edge Creation & Weighting: Edges between PAIRS that share a service
    let mut edge_contributions: HashMap<
        (ServiceGroupId, ServiceGroupId), // Key: (PairwiseGroupId1, PairwiseGroupId2)
        Vec<ContributingSharedServiceDetail>, // Value: Details of the shared service
    > = HashMap::new();

    for (service_z, groups_sharing_service_z) in service_to_pairwise_groups {
        if groups_sharing_service_z.len() < 2 {
            continue; // No edge can be formed by this shared service
        }

        // Generate pairs of service groups that share service_z
        for i in 0..groups_sharing_service_z.len() {
            for j in (i + 1)..groups_sharing_service_z.len() {
                let group_a_id = &groups_sharing_service_z[i]; // This is a pairwise group
                let group_b_id = &groups_sharing_service_z[j]; // This is another pairwise group

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

                // W_z is the average confidence of the two pairs linked by service_z
                let w_z = (c_a + c_b) / 2.0;

                let key = if group_a_id.0 < group_b_id.0 {
                    (group_a_id.clone(), group_b_id.clone())
                } else {
                    (group_b_id.clone(), group_a_id.clone())
                };

                edge_contributions
                    .entry(key)
                    .or_default()
                    .push(ContributingSharedServiceDetail {
                        service_id: service_z.0.clone(),
                        conf_service_in_source_group: c_a,
                        conf_service_in_target_group: c_b,
                        w_z,
                    });
            }
        }
    }

    info!(
        "Calculated initial W_z contributions for {} potential inter-group edges.",
        edge_contributions.len()
    );

    for ((group_a_id, group_b_id), contributions) in edge_contributions {
        if contributions.is_empty() {
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
            continue;
        }

        let node_a_idx = node_mapper.get_or_add_node(&mut xgraph_instance, &group_a_id);
        let node_b_idx = node_mapper.get_or_add_node(&mut xgraph_instance, &group_b_id);

        if let Err(e) = xgraph_instance.add_edge(node_a_idx, node_b_idx, edge_weight, ()) {
            warn!(
                "Failed to add edge to xgraph between {} ({:?}) and {} ({:?}): {}",
                group_a_id.0, node_a_idx, group_b_id.0, node_b_idx, e
            );
            continue;
        }
        debug!(
            "Added edge to xgraph: {} --({:.4})-- {}",
            group_a_id.0, edge_weight, group_b_id.0
        );

        let contributing_services_json = serde_json::to_value(&contributions).ok();
        let cfe = NewClusterFormationEdge {
            pipeline_run_id: pipeline_run_id.to_string(),
            source_group_id: group_a_id.0.clone(),
            target_group_id: group_b_id.0.clone(),
            calculated_edge_weight: edge_weight,
            contributing_shared_entities: contributing_services_json,
        };
        if let Err(e) = db::insert_cluster_formation_edge(transaction, &cfe).await {
            warn!(
                "Failed to log cluster formation edge between {} and {}: {}",
                group_a_id.0, group_b_id.0, e
            );
        }

        // Using SERVICE_WEAK_LINK_THRESHOLD which should be defined in config.rs
        if edge_weight > 0.0 && edge_weight < config::SERVICE_WEAK_LINK_THRESHOLD {
            let details_json = serde_json::json!({
                "calculated_edge_weight": edge_weight,
                "contributing_links_count": contributions.len(),
                "shared_service_details": contributions, // Log full contribution details
            });
            let reason_message = format!(
                "Inter-pairwise-group link between ServiceGroup {} and ServiceGroup {} has a weak calculated weight of {:.4}",
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
                reason_code: Some("WEAK_INTER_SERVICE_GROUP_EDGE".to_string()),
                reason_message: Some(reason_message),
                priority: 0,
                status: SuggestionStatus::PendingReview.as_str().to_string(),
                reviewer_id: None,
                reviewed_at: None,
                review_notes: None,
            };
            if let Err(e) = db::insert_suggestion(transaction, &suggestion).await {
                warn!(
                    "Failed to log suggestion for weak inter-service-group link: {}",
                    e
                );
            }
        }
    }

    info!(
        "Weighted xgraph built in {:.2?} with {} nodes and {} edges.",
        start_time.elapsed(),
        node_mapper.get_node_count(),
        xgraph_instance.edges.len()
    );

    Ok((xgraph_instance, node_mapper))
}

pub async fn process_clusters(
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Mutex<ServiceMatchingOrchestrator>>,
    pipeline_run_id: &str,
) -> Result<usize> {
    info!(
        "Starting service cluster consolidation process (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for service_process_clusters")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for service_process_clusters")?;

    info!("Building weighted service group graph (pairwise nodes)...");
    let build_start = Instant::now();
    let (graph, node_mapper) = build_weighted_xgraph(pool, &transaction, pipeline_run_id).await?;
    info!("Graph built in {:.2?}", build_start.elapsed());

    if graph.nodes.is_empty() {
        info!("No nodes in the graph. Skipping Leiden clustering for services.");
        transaction
            .commit()
            .await
            .context("Failed to commit empty service cluster transaction")?;
        return Ok(0);
    }

    info!(
        "Graph built with {} nodes (pairwise service groups) and {} edges. Starting Leiden clustering...",
        node_mapper.get_node_count(),
        graph.edges.len()
    );

    // Use a different resolution parameter for services if needed
    let leiden_config = CommunityConfig {
        resolution: config::SERVICE_LEIDEN_RESOLUTION_PARAMETER,
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
            return Err(
                anyhow::Error::from(e).context("Leiden community detection failed for services")
            );
        }
    };
    info!(
        "Leiden algorithm finished in {:.2?}, found {} raw communities for services.",
        leiden_start.elapsed(),
        communities_result.len()
    );

    let mut clusters_created = 0;
    let mut new_cluster_ids_for_verification = Vec::new();

    for (_community_key, node_indices_in_community) in communities_result.iter() {
        // These node_indices map to pairwise ServiceGroupIds
        let pairwise_group_ids_in_community: Vec<ServiceGroupId> = node_indices_in_community
            .iter()
            .filter_map(|&node_idx| node_mapper.get_group_id(node_idx).cloned())
            .collect();

        if pairwise_group_ids_in_community.is_empty() {
            debug!("Skipping community with 0 mapped pairwise service groups.");
            continue;
        }

        // Fetch all unique service_id_1 and service_id_2 from these pairwise_group_ids
        let mut unique_services_in_cluster = HashSet::new();
        let group_id_strings_for_query: Vec<String> = pairwise_group_ids_in_community
            .iter()
            .map(|g| g.0.clone())
            .collect();

        if group_id_strings_for_query.is_empty() {
            continue;
        }

        // Query to get all service IDs from the involved pairwise groups
        let services_query = "
            SELECT service_id_1, service_id_2
            FROM public.service_group
            WHERE id = ANY($1)";

        let service_rows = match transaction
            .query(services_query, &[&group_id_strings_for_query])
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    "Failed to fetch services for community {:?}: {}",
                    group_id_strings_for_query, e
                );
                continue;
            }
        };

        for row in service_rows {
            unique_services_in_cluster.insert(ServiceId(row.get("service_id_1")));
            unique_services_in_cluster.insert(ServiceId(row.get("service_id_2")));
        }
        let unique_service_count = unique_services_in_cluster.len() as i32;

        // A cluster makes sense if it has at least 2 unique services.
        // A single pairwise group already guarantees this.
        if unique_service_count < 2 {
            debug!(
                "Skipping community with < 2 unique services: {:?} ({} services)",
                group_id_strings_for_query, unique_service_count
            );
            continue;
        }

        debug!(
            "Processing community of {} pairwise service groups, forming a cluster with {} unique services: {:?}",
            pairwise_group_ids_in_community.len(),
            unique_service_count,
            pairwise_group_ids_in_community.iter().map(|g| &g.0).collect::<Vec<_>>()
        );

        let new_cluster_id = ServiceGroupClusterId(Uuid::new_v4().to_string());
        if let Err(e) = create_service_cluster_record(
            &transaction,
            &new_cluster_id,
            pairwise_group_ids_in_community.len() as i32, // This is count of PAIRS
            unique_service_count,
        )
        .await
        {
            warn!(
                "Failed to create service cluster record for cluster {}: {}",
                new_cluster_id.0, e
            );
            continue;
        }

        // Update all the *pairwise* service_group records with this new_cluster_id
        if let Err(e) = update_service_groups_with_cluster_id(
            &transaction,
            &new_cluster_id,
            &pairwise_group_ids_in_community,
        )
        .await
        {
            warn!(
                "Failed to update pairwise service groups for cluster {}: {}",
                new_cluster_id.0, e
            );
            continue;
        }

        new_cluster_ids_for_verification.push(new_cluster_id.clone());
        clusters_created += 1;
        info!(
            "Created ServiceGroupCluster {} (ID: {}) for {} pairwise groups and {} unique services.",
            clusters_created,
            new_cluster_id.0,
            pairwise_group_ids_in_community.len(),
            unique_service_count
        );
    }

    info!(
        "Completed processing Leiden communities for services. {} new service clusters created.",
        clusters_created
    );

    if let Some(orchestrator_ref) = reinforcement_orchestrator {
        if !new_cluster_ids_for_verification.is_empty() {
            info!(
                "Verifying {} newly formed service clusters...",
                new_cluster_ids_for_verification.len()
            );
            let verify_start = Instant::now();
            match verify_service_clusters(
                &transaction,
                pool,
                &new_cluster_ids_for_verification,
                orchestrator_ref,
                pipeline_run_id,
            )
            .await
            {
                Ok(_) => info!(
                    "Service cluster verification step completed in {:.2?}.",
                    verify_start.elapsed()
                ),
                Err(e) => warn!("Service cluster verification step failed: {}", e),
            }
        }
    }

    transaction
        .commit()
        .await
        .context("Failed to commit service cluster processing transaction")?;
    info!(
        "Service cluster consolidation finished in {:.2?}. {} service clusters created.",
        start_time.elapsed(),
        clusters_created
    );
    Ok(clusters_created)
}

async fn create_service_cluster_record(
    transaction: &Transaction<'_>,
    cluster_id: &ServiceGroupClusterId,
    pair_count: i32,
    service_count: i32,
) -> Result<()> {
    let now = Utc::now().naive_utc();
    let cluster_name = format!("ServiceLeidenCluster-{}", &cluster_id.0[..8]);
    let description = format!(
        "Leiden-generated service cluster from {} pairwise service groups, {} unique services.",
        pair_count, service_count
    );

    transaction.execute(
        "INSERT INTO public.service_group_cluster (id, name, description, created_at, updated_at, service_count, service_group_count, average_coherence_score)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)", 
        &[&cluster_id.0, &cluster_name, &description, &now, &now, &service_count, &pair_count],
    ).await.context("Failed to insert service_group_cluster")?;
    Ok(())
}

async fn update_service_groups_with_cluster_id(
    transaction: &Transaction<'_>,
    cluster_id: &ServiceGroupClusterId,
    pairwise_group_ids: &[ServiceGroupId],
) -> Result<()> {
    if pairwise_group_ids.is_empty() {
        return Ok(());
    }
    let now = Utc::now().naive_utc();
    let group_id_strings: Vec<String> = pairwise_group_ids.iter().map(|g| g.0.clone()).collect();
    transaction.execute(
        "UPDATE public.service_group SET group_cluster_id = $1, updated_at = $2 WHERE id = ANY($3)",
        &[&cluster_id.0, &now, &group_id_strings],
    ).await.context("Failed to update service_group with cluster_id for pairwise groups")?;
    Ok(())
}

async fn verify_service_clusters(
    transaction: &Transaction<'_>,
    pool: &PgPool,
    new_cluster_ids: &[ServiceGroupClusterId],
    reinforcement_orchestrator_mutex: &Mutex<ServiceMatchingOrchestrator>,
    pipeline_run_id: &str,
) -> Result<()> {
    info!(
        "Verifying quality of {} newly formed service clusters (run ID: {})...",
        new_cluster_ids.len(),
        pipeline_run_id
    );

    for cluster_id in new_cluster_ids {
        debug!("Verifying service cluster: {}", cluster_id.0);

        // Fetch all unique service_ids for the current cluster
        let mut services_in_cluster = HashSet::new();
        let service_query = "
            SELECT service_id_1, service_id_2
            FROM public.service_group
            WHERE group_cluster_id = $1";

        let service_rows = match transaction.query(service_query, &[&cluster_id.0]).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    "Failed to fetch services for cluster verification {}: {}. Skipping.",
                    cluster_id.0, e
                );
                continue;
            }
        };

        if service_rows.is_empty() {
            debug!(
                "No pairwise groups found for service cluster {} during verification. Skipping.",
                cluster_id.0
            );
            // If no services, we can't calculate a score, so update with NULL
            let update_null_score_query = "
                UPDATE public.service_group_cluster
                SET average_coherence_score = NULL
                WHERE id = $1";
            if let Err(e) = transaction
                .execute(update_null_score_query, &[&cluster_id.0])
                .await
            {
                warn!(
                    "Failed to update average_coherence_score to NULL for empty service cluster {}: {}",
                    cluster_id.0, e
                );
            }
            continue;
        }

        for row in service_rows {
            services_in_cluster.insert(ServiceId(row.get("service_id_1")));
            services_in_cluster.insert(ServiceId(row.get("service_id_2")));
        }

        let services: Vec<ServiceId> = services_in_cluster.into_iter().collect();

        if services.len() < 2 {
            debug!(
                "Skipping verification for service cluster {} as it has < 2 unique services ({} found).",
                cluster_id.0,
                services.len()
            );
            // If less than 2 services, score is not meaningful. Update with NULL.
            let update_null_score_query = "
                UPDATE public.service_group_cluster
                SET average_coherence_score = NULL
                WHERE id = $1";
            if let Err(e) = transaction
                .execute(update_null_score_query, &[&cluster_id.0])
                .await
            {
                warn!(
                    "Failed to update average_coherence_score to NULL for service cluster {} with < 2 services: {}",
                    cluster_id.0, e
                );
            }
            continue;
        }

        // Sample pairs of services if the cluster is too large
        let mut service_pairs_to_check: Vec<(ServiceId, ServiceId)> = Vec::new();
        let max_services_to_sample = 10;
        let max_pairs_to_check_per_cluster = 20;

        if services.len() <= max_services_to_sample {
            for i in 0..services.len() {
                for j in (i + 1)..services.len() {
                    if service_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                        break;
                    }
                    service_pairs_to_check.push((services[i].clone(), services[j].clone()));
                }
                if service_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                    break;
                }
            }
        } else {
            // Basic sampling: just take pairs from the first N services
            use rand::seq::SliceRandom;
            let mut rng = rand::thread_rng();
            let sampled_services: Vec<ServiceId> = services
                .choose_multiple(&mut rng, max_services_to_sample)
                .cloned()
                .collect();
            for i in 0..sampled_services.len() {
                for j in (i + 1)..sampled_services.len() {
                    if service_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                        break;
                    }
                    service_pairs_to_check
                        .push((sampled_services[i].clone(), sampled_services[j].clone()));
                }
                if service_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                    break;
                }
            }
        }

        if service_pairs_to_check.is_empty() && services.len() >= 2 {
            // Handle case where sampling yielded no pairs but should have
            // Fallback to first pair if possible
            if services.len() >= 2 {
                service_pairs_to_check.push((services[0].clone(), services[1].clone()));
            }
        }

        debug!(
            "Verifying service cluster {} with {} sampled service pairs (out of {} total services).",
            cluster_id.0,
            service_pairs_to_check.len(),
            services.len()
        );

        let mut verification_scores: Vec<f64> = Vec::new();

        // Here you would get your orchestrator and extract features for each pair
        // For now, we'll just use a placeholder for the verification scores
        for (service1_id, service2_id) in service_pairs_to_check {
            debug!(
                "Verifying pair: ({}, {}) for service cluster {}",
                service1_id.0, service2_id.0, cluster_id.0
            );

            // This would be the actual feature extraction from the ServiceMatchingOrchestrator
            // For now, let's use a placeholder logic to get confidence between the services
            let confidence = match transaction
                .query_opt(
                    "SELECT confidence_score FROM public.service_group 
                     WHERE (service_id_1 = $1 AND service_id_2 = $2) 
                     OR (service_id_1 = $2 AND service_id_2 = $1)",
                    &[&service1_id.0, &service2_id.0],
                )
                .await
            {
                Ok(Some(row)) => row.get::<_, Option<f64>>("confidence_score").unwrap_or(0.5),
                Ok(None) => 0.5, // Default if no direct match exists
                Err(e) => {
                    warn!(
                        "Error querying service pair confidence ({}, {}): {}",
                        service1_id.0, service2_id.0, e
                    );
                    0.5 // Default on error
                }
            };

            verification_scores.push(confidence);
        }

        let avg_score_to_store: Option<f64>;

        if verification_scores.is_empty() {
            debug!("No verification scores obtained for service cluster {}. Storing NULL for average_coherence_score.", cluster_id.0);
            avg_score_to_store = None;
        } else {
            let calculated_avg_score: f64 =
                verification_scores.iter().sum::<f64>() / verification_scores.len() as f64;
            avg_score_to_store = Some(calculated_avg_score);
            debug!(
                "Service cluster {} - Avg internal pair confidence: {:.4} from {} scores. Storing this score.",
                cluster_id.0,
                calculated_avg_score,
                verification_scores.len()
            );
        }

        // Update the service_group_cluster table with the calculated average_coherence_score (or NULL)
        let update_score_query = "
            UPDATE public.service_group_cluster
            SET average_coherence_score = $1
            WHERE id = $2";
        if let Err(e) = transaction
            .execute(update_score_query, &[&avg_score_to_store, &cluster_id.0])
            .await
        {
            warn!(
                "Failed to update average_coherence_score for service cluster {}: {}",
                cluster_id.0, e
            );
        } else {
            if let Some(score_val) = avg_score_to_store {
                info!(
                    "Successfully stored average_coherence_score {:.4} for service cluster {}.",
                    score_val, cluster_id.0
                );
            } else {
                info!(
                    "Successfully stored NULL average_coherence_score for service cluster {} (no scores calculated).",
                    cluster_id.0
                );
            }
        }

        // Check if score is below threshold and log suggestion if needed
        if let Some(calculated_avg_score) = avg_score_to_store {
            // Use SERVICE_VERIFICATION_THRESHOLD from config
            if calculated_avg_score < config::SERVICE_VERIFICATION_THRESHOLD {
                info!("Service cluster {} has low internal coherence (avg score: {:.4}, threshold: {}). Logging SUGGEST_SPLIT_CLUSTER.", 
                      cluster_id.0, calculated_avg_score, config::SERVICE_VERIFICATION_THRESHOLD);
                let details_json = serde_json::json!({
                    "average_internal_pair_confidence": calculated_avg_score,
                    "services_in_cluster_count": services.len(),
                    "checked_pairs_count": verification_scores.len(),
                    "verification_threshold": config::SERVICE_VERIFICATION_THRESHOLD,
                    "verification_scores_sample": verification_scores.iter().take(5).copied().collect::<Vec<_>>(),
                });
                let reason_message = format!(
                    "Service cluster {} ({} services, {} pairs checked) has low average internal service-pair confidence: {:.4}, below threshold of {}.",
                    cluster_id.0, services.len(), verification_scores.len(), calculated_avg_score, config::SERVICE_VERIFICATION_THRESHOLD
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
                    reason_code: Some("LOW_INTERNAL_SERVICE_CLUSTER_COHERENCE".to_string()),
                    reason_message: Some(reason_message),
                    priority: 1,
                    status: SuggestionStatus::PendingReview.as_str().to_string(),
                    reviewer_id: None,
                    reviewed_at: None,
                    review_notes: None,
                };
                match db::insert_suggestion(transaction, &suggestion).await {
                    Ok(id) => info!(
                        "Logged SUGGEST_SPLIT_CLUSTER suggestion {} for service cluster {}.",
                        id, cluster_id.0
                    ),
                    Err(e) => warn!(
                        "Failed to log SUGGEST_SPLIT_CLUSTER for service cluster {}: {}",
                        cluster_id.0, e
                    ),
                }
            } else {
                info!(
                    "Service cluster {} passed verification (avg score: {:.4}, threshold: {}).",
                    cluster_id.0,
                    calculated_avg_score,
                    config::SERVICE_VERIFICATION_THRESHOLD
                );
            }
        }
    }
    info!("Finished verifying all new service clusters.");
    Ok(())
}
