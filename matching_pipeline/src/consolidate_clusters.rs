// src/consolidate_clusters.rs
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use rand::seq::SliceRandom;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
// Removed unused Mutex import
use serde_json::Value;
use tokio_postgres::{GenericClient, Transaction}; // Still here, but will be used differently
use uuid::Uuid;
use xgraph::graph::graph::NodeId;

// xgraph imports
use xgraph::leiden_clustering::{CommunityConfig, CommunityDetection};
use xgraph::Graph;

// Local imports
use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType, ContributingSharedEntityDetail, EntityGroupId, EntityId, GroupClusterId,
    NewClusterFormationEdge, NewSuggestedAction, SuggestionStatus,
};
// Not used in this snippet, but kept
use crate::utils::{cosine_similarity_candle, cosine_similarity_manual};

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

// Embedding cache for local similarity calculations
#[derive(Debug, Clone)]
struct CachedEmbedding {
    embedding: Vec<f32>,
    cached_at: std::time::Instant,
}

#[derive(Debug)]
struct EmbeddingCache {
    embeddings: HashMap<EntityId, CachedEmbedding>,
    cache_duration: std::time::Duration,
    max_cache_size: usize,
}

impl EmbeddingCache {
    fn new(cache_duration_secs: u64, max_cache_size: usize) -> Self {
        Self {
            embeddings: HashMap::new(),
            cache_duration: std::time::Duration::from_secs(cache_duration_secs),
            max_cache_size,
        }
    }

    fn get(&self, entity_id: &EntityId) -> Option<&Vec<f32>> {
        if let Some(cached) = self.embeddings.get(entity_id) {
            if cached.cached_at.elapsed() < self.cache_duration {
                debug!("Cache hit for entity_id: {}", entity_id.0);
                return Some(&cached.embedding);
            } else {
                debug!("Cache expired for entity_id: {}", entity_id.0);
            }
        }
        debug!("Cache miss for entity_id: {}", entity_id.0);
        None
    }

    fn insert(&mut self, entity_id: EntityId, embedding: Vec<f32>) {
        // Simple cache eviction: if we're at capacity, remove oldest entries
        if self.embeddings.len() >= self.max_cache_size {
            let mut oldest_key = None;
            let mut oldest_time = std::time::Instant::now();

            for (key, cached) in &self.embeddings {
                if cached.cached_at < oldest_time {
                    oldest_time = cached.cached_at;
                    oldest_key = Some(key.clone());
                }
            }

            if let Some(key) = oldest_key {
                debug!("Evicting oldest cache entry for entity_id: {}", key.0);
                self.embeddings.remove(&key);
            }
        }

        debug!("Inserting into cache for entity_id: {}", entity_id.0);
        self.embeddings.insert(
            entity_id,
            CachedEmbedding {
                embedding,
                cached_at: std::time::Instant::now(),
            },
        );
    }

    fn len(&self) -> usize {
        self.embeddings.len()
    }
}

// Batch embedding fetcher
struct EmbeddingBatch;

impl EmbeddingBatch {
    async fn fetch_embeddings_batch(
        conn: &impl tokio_postgres::GenericClient,
        entity_ids: &[EntityId],
        cache: &mut EmbeddingCache,
    ) -> Result<HashMap<EntityId, Vec<f32>>> {
        let mut result = HashMap::new();
        let mut entities_to_fetch = Vec::new();

        // Check cache first
        for entity_id in entity_ids {
            if let Some(embedding) = cache.get(entity_id) {
                result.insert(entity_id.clone(), embedding.clone());
            } else {
                entities_to_fetch.push(entity_id);
            }
        }

        if entities_to_fetch.is_empty() {
            debug!("All embeddings found in cache for batch.");
            return Ok(result);
        }

        // Batch fetch missing embeddings
        let entity_id_strings: Vec<String> =
            entities_to_fetch.iter().map(|e| e.0.clone()).collect();
        debug!(
            "Fetching {} embeddings from DB for batch.",
            entity_id_strings.len()
        );

        let query = "
            SELECT e.id, o.embedding
            FROM public.entity e
            JOIN public.organization o ON e.organization_id = o.id
            WHERE e.id = ANY($1) AND o.embedding IS NOT NULL
        ";

        let rows = conn
            .query(query, &[&entity_id_strings])
            .await
            .context("Failed to batch fetch embeddings")?;

        for row in rows {
            let entity_id_str: String = row.get("id");
            let entity_id = EntityId(entity_id_str);

            // FIX: Change the type from Option<Vec<f32>> to Option<pgvector::Vector>
            if let Some(embedding_pg) = row.get::<_, Option<pgvector::Vector>>("embedding") {
                let embedding_vec = embedding_pg.to_vec();
                result.insert(entity_id.clone(), embedding_vec.clone());
                cache.insert(entity_id, embedding_vec);
            } else {
                warn!("Embedding for entity {} was NULL.", entity_id.0);
            }
        }

        Ok(result)
    }
}

// Enhanced lightweight coherence calculation with local embedding similarity
async fn calculate_lightweight_coherence_score_local(
    conn: &impl tokio_postgres::GenericClient,
    entity1_id: &EntityId,
    entity2_id: &EntityId,
    cache: &mut EmbeddingCache,
) -> Result<f64> {
    debug!(
        "Calculating lightweight coherence score for ({}, {})",
        entity1_id.0, entity2_id.0
    );
    let start_time = Instant::now();

    // First, get embeddings using the batch fetcher (even for single pairs)
    let entity_ids = vec![entity1_id.clone(), entity2_id.clone()];
    let embeddings = EmbeddingBatch::fetch_embeddings_batch(conn, &entity_ids, cache).await?;

    // Calculate embedding similarity locally
    let embedding_sim = if let (Some(emb1), Some(emb2)) =
        (embeddings.get(entity1_id), embeddings.get(entity2_id))
    {
        match cosine_similarity_candle(emb1, emb2) {
            Ok(sim) => sim,
            Err(e) => {
                warn!("Failed to calculate embedding similarity with Candle for ({}, {}): {}. Falling back to manual calculation.", entity1_id.0, entity2_id.0, e);
                cosine_similarity_manual(emb1, emb2).unwrap_or_else(|| {
                    warn!(
                        "Manual cosine similarity also failed for ({}, {}). Defaulting to 0.0.",
                        entity1_id.0, entity2_id.0
                    );
                    0.0
                })
            }
        }
    } else {
        warn!(
            "One or both embeddings not found for ({}, {}). Embedding similarity set to 0.0.",
            entity1_id.0, entity2_id.0
        );
        0.0
    };
    debug!(
        "Embedding similarity for ({}, {}): {:.4}",
        entity1_id.0, entity2_id.0, embedding_sim
    );

    // Keep other calculations in SQL (they're lightweight)
    // Adjusted queries based on provided schema, especially for `location.geom`
    let query = "
        SELECT
            -- Name similarity
            COALESCE(similarity(LOWER(o1.name), LOWER(o2.name)), 0.0) as name_sim,

            -- Geographic proximity
            CASE
                WHEN EXISTS(
                    SELECT 1 FROM public.location l1
                    JOIN public.entity_feature ef1 ON ef1.table_id = l1.id AND ef1.table_name = 'location'
                    JOIN public.location l2
                    JOIN public.entity_feature ef2 ON ef2.table_id = l2.id AND ef2.table_name = 'location'
                    WHERE ef1.entity_id = $1 AND ef2.entity_id = $2
                    AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL
                ) THEN (
                    SELECT
                        CASE
                            WHEN MIN(ST_Distance(l1.geom, l2.geom)) > 10000 THEN 0.0
                            ELSE 1.0 - (MIN(ST_Distance(l1.geom, l2.geom)) / 10000.0)
                        END
                    FROM public.location l1
                    JOIN public.entity_feature ef1 ON ef1.table_id = l1.id AND ef1.table_name = 'location',
                    public.location l2
                    JOIN public.entity_feature ef2 ON ef2.table_id = l2.id AND ef2.table_name = 'location'
                    WHERE ef1.entity_id = $1 AND ef2.entity_id = $2
                    AND l1.geom IS NOT NULL AND l2.geom IS NOT NULL
                )
                ELSE 0.0
            END as geo_proximity

        FROM public.entity e1
        JOIN public.organization o1 ON e1.organization_id = o1.id,
        public.entity e2
        JOIN public.organization o2 ON e2.organization_id = o2.id
        WHERE e1.id = $1 AND e2.id = $2
    ";

    let row = conn
        .query_one(query, &[&entity1_id.0, &entity2_id.0])
        .await
        .context(format!(
            "Failed to calculate lightweight coherence metrics for ({}, {}) (excluding embeddings)",
            entity1_id.0, entity2_id.0
        ))?;

    let name_sim: f64 = row.get::<_, Option<f32>>("name_sim").unwrap_or(0.0) as f64;
    let geo_proximity: f64 = row.get::<_, Option<f64>>("geo_proximity").unwrap_or(0.0);
    debug!(
        "Name similarity for ({}, {}): {:.4}",
        entity1_id.0, entity2_id.0, name_sim
    );
    debug!(
        "Geographic proximity for ({}, {}): {:.4}",
        entity1_id.0, entity2_id.0, geo_proximity
    );

    // Calculate service similarity locally as well (fetch service embeddings)
    let max_service_sim =
        calculate_service_similarity_local(conn, entity1_id, entity2_id, cache).await?;
    debug!(
        "Max service similarity for ({}, {}): {:.4}",
        entity1_id.0, entity2_id.0, max_service_sim
    );

    let coherence_score =
        (embedding_sim * 0.4) + (name_sim * 0.3) + (max_service_sim * 0.2) + (geo_proximity * 0.1);

    let normalized_coherence = coherence_score.max(0.0).min(1.0);
    debug!(
        "Final coherence score for ({}, {}): {:.4} (took {:.2?})",
        entity1_id.0,
        entity2_id.0,
        normalized_coherence,
        start_time.elapsed()
    );

    Ok(normalized_coherence)
}

// Local service similarity calculation
async fn calculate_service_similarity_local(
    conn: &impl tokio_postgres::GenericClient,
    entity1_id: &EntityId,
    entity2_id: &EntityId,
    _cache: &mut EmbeddingCache, // Could extend to cache service embeddings too
) -> Result<f64> {
    debug!(
        "Calculating service similarity for ({}, {})",
        entity1_id.0, entity2_id.0
    );
    let query = "
        SELECT s1.embedding_v2 as emb1, s2.embedding_v2 as emb2
        FROM public.service s1
        JOIN public.entity_feature ef1 ON ef1.table_id = s1.id AND ef1.table_name = 'service'
        JOIN public.service s2
        JOIN public.entity_feature ef2 ON ef2.table_id = s2.id AND ef2.table_name = 'service'
        WHERE ef1.entity_id = $1 AND ef2.entity_id = $2
        AND s1.embedding_v2 IS NOT NULL AND s2.embedding_v2 IS NOT NULL
    ";

    let rows = conn
        .query(query, &[&entity1_id.0, &entity2_id.0])
        .await
        .context(format!(
            "Failed to fetch service embeddings for ({}, {})",
            entity1_id.0, entity2_id.0
        ))?;

    let mut max_similarity: f64 = 0.0;

    for row in rows {
        // FIX: Change the type from Option<Vec<f32>> to Option<pgvector::Vector>
        if let (Some(emb1_pg), Some(emb2_pg)) = (
            row.get::<_, Option<pgvector::Vector>>("emb1"),
            row.get::<_, Option<pgvector::Vector>>("emb2"),
        ) {
            let emb1 = emb1_pg.to_vec();
            let emb2 = emb2_pg.to_vec();

            let similarity = match cosine_similarity_candle(&emb1, &emb2) {
                Ok(sim) => sim,
                Err(e) => {
                    warn!("Failed to calculate service embedding similarity with Candle for ({}, {}): {}. Falling back to manual calculation.", entity1_id.0, entity2_id.0, e);
                    cosine_similarity_manual(&emb1, &emb2).unwrap_or_else(|| {
                        warn!("Manual service embedding similarity also failed for ({}, {}). Defaulting to 0.0.", entity1_id.0, entity2_id.0);
                        0.0
                    })
                }
            };
            max_similarity = max_similarity.max(similarity);
            debug!(
                "Service embedding pair similarity: {:.4}, max so far: {:.4}",
                similarity, max_similarity
            );
        } else {
            debug!("One or both service embeddings were NULL for a pair.");
        }
    }

    Ok(max_similarity)
}

// Batched coherence calculation for multiple entity pairs
async fn calculate_coherence_scores_batch(
    conn: &impl tokio_postgres::GenericClient,
    entity_pairs: &[(EntityId, EntityId)],
    cache: &mut EmbeddingCache,
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    const BATCH_SIZE: usize = 50; // Process pairs in batches
    let mut results = Vec::new();

    debug!(
        "Calculating coherence scores for {} entity pairs in batches of {}",
        entity_pairs.len(),
        BATCH_SIZE
    );

    for (i, chunk) in entity_pairs.chunks(BATCH_SIZE).enumerate() {
        debug!(
            "Processing coherence batch {}/{}",
            i + 1,
            (entity_pairs.len() + BATCH_SIZE - 1) / BATCH_SIZE
        );
        // Collect all unique entity IDs in this chunk
        let mut unique_entities = HashSet::new();
        for (e1, e2) in chunk {
            unique_entities.insert(e1.clone());
            unique_entities.insert(e2.clone());
        }
        let unique_entities: Vec<EntityId> = unique_entities.into_iter().collect();

        // Pre-fetch all embeddings for this batch. Errors here will be propagated by '?'
        let _embeddings =
            EmbeddingBatch::fetch_embeddings_batch(conn, &unique_entities, cache).await?;
        debug!("Batch embeddings pre-fetched for current chunk.");

        // Now calculate coherence for each pair in the batch
        for (entity1, entity2) in chunk {
            match calculate_lightweight_coherence_score_local(conn, entity1, entity2, cache).await {
                Ok(score) => results.push((entity1.clone(), entity2.clone(), score)),
                Err(e) => {
                    warn!(
                        "Failed to calculate coherence for pair ({}, {}): {}. Defaulting score to 0.0.",
                        entity1.0, entity2.0, e
                    );
                    results.push((entity1.clone(), entity2.clone(), 0.0));
                }
            }
        }
    }

    Ok(results)
}

/// Check the current state and determine what work needs to be done
async fn analyze_clustering_state(
    pool: &PgPool, // Changed to pool to create new transaction
    current_pipeline_run_id: &str,
) -> Result<ClusteringState> {
    info!("Analyzing clustering state to optimize processing...");
    let start_time = Instant::now();

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for analyze_clustering_state")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for analyze_clustering_state")?;

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
    // Adjusted query to reflect `run_timestamp` instead of `created_at`
    let last_run_query = "
        SELECT id, run_timestamp
        FROM clustering_metadata.pipeline_run
        WHERE id != $1
          AND total_clusters > 0
        ORDER BY run_timestamp DESC
        LIMIT 1";

    let last_run_result = transaction
        .query_opt(last_run_query, &[&current_pipeline_run_id])
        .await
        .context("Failed to query last clustering run")?;

    // Adjusted to use `run_timestamp`
    let last_clustering_run = last_run_result
        .clone()
        .map(|row| row.get::<_, String>("id"));
    let last_run_timestamp: Option<chrono::NaiveDateTime> = last_run_result
        .and_then(|row| row.get::<_, Option<chrono::NaiveDateTime>>("run_timestamp"));

    if let Some(ref last_run_id) = last_clustering_run {
        info!("Found previous clustering run: {}", last_run_id);
    } else {
        info!("No previous clustering run found - will perform full clustering");
    }

    // FIX: Always fetch existing cluster formation edges regardless of last_clustering_run
    let existing_edges_query = "
        SELECT source_group_id, target_group_id, calculated_edge_weight
        FROM clustering_metadata.cluster_formation_edges";

    let existing_edges_rows = transaction
        .query(existing_edges_query, &[]) // No filtering by pipeline_run_id
        .await
        .context("Failed to fetch existing cluster formation edges")?;

    let mut existing_edges = HashMap::new();
    for row in existing_edges_rows {
        let source: String = row.get("source_group_id");
        let target: String = row.get("target_group_id");
        let weight: f64 = row.get("calculated_edge_weight");

        // Store both directions for easy lookup, ensuring canonical key order
        let key1 = if source < target {
            (source.clone(), target.clone())
        } else {
            (target.clone(), source.clone())
        };
        existing_edges.insert(key1, weight);
    }

    info!(
        "Found {} existing cluster formation edges that could be reused",
        existing_edges.len()
    );

    // 4. Identify groups added since the last clustering run
    let groups_added_since_last_run = if let Some(last_creation_time) = last_run_timestamp {
        debug!("Last run run_timestamp: {}", last_creation_time);
        let new_groups_query = "
            SELECT id FROM public.entity_group
            WHERE created_at > $1 AND group_cluster_id IS NULL";

        let new_groups_rows = transaction
            .query(new_groups_query, &[&last_creation_time])
            .await
            .context("Failed to fetch groups added since last run")?;

        let new_groups: Vec<EntityGroupId> = new_groups_rows
            .into_iter()
            .map(|row| EntityGroupId(row.get("id")))
            .collect();

        info!(
            "Found {} groups added since last clustering run",
            new_groups.len()
        );
        new_groups
    } else {
        info!("No previous run timestamp, treating all unclustered groups as new.");
        unclustered_groups.clone()
    };

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for analyze_clustering_state")?;

    info!(
        "Clustering state analysis completed in {:.2?}.",
        start_time.elapsed()
    );
    Ok(ClusteringState {
        unclustered_groups,
        existing_edges,
        last_clustering_run, // This is still the ID, which is fine
        groups_added_since_last_run,
    })
}

/// Check if we can reuse an existing edge calculation
fn can_reuse_edge(state: &ClusteringState, group_a_id: &str, group_b_id: &str) -> Option<f64> {
    let key = if group_a_id < group_b_id {
        (group_a_id.to_string(), group_b_id.to_string())
    } else {
        (group_b_id.to_string(), group_a_id.to_string())
    };

    state.existing_edges.get(&key).copied()
}

async fn build_weighted_xgraph_optimized(
    pool: &PgPool, // Changed to pool to create new transaction
    pipeline_run_id: &str,
    state: &ClusteringState,
) -> Result<(Graph<f64, EntityGroupId, ()>, NodeMapper)> {
    info!(
        "Building optimized weighted xgraph for cluster consolidation (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    if state.unclustered_groups.is_empty() {
        info!("No unclustered entity groups found to build graph. Returning empty graph.");
        return Ok((Graph::new(false), NodeMapper::new()));
    }

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for build_weighted_xgraph_optimized")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for build_weighted_xgraph_optimized")?;

    // Prepare group IDs for batch queries
    let group_id_strings: Vec<String> = state
        .unclustered_groups
        .iter()
        .map(|g| g.0.clone())
        .collect();
    debug!(
        "Fetching details for {} unclustered groups.",
        group_id_strings.len()
    );

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

    info!(
        "Loading {} pairwise entity groups as graph nodes...",
        group_rows.len()
    );

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
        "Loaded {} pairwise entity groups as nodes. Now calculating/reusing edges.",
        node_mapper.get_node_count()
    );

    // Edge creation with optimization for reusing existing calculations
    let mut edges_reused = 0;
    let mut edges_calculated = 0;
    let mut batch_edges = Vec::new();
    let mut batch_suggestions = Vec::new();

    const EDGE_BATCH_SIZE: usize = 1000;

    // Use a cloned `transaction` for this loop, or pass `pool` and open new transactions.
    // For simplicity and avoiding lifetime issues with `Transaction`, we will pass the `pool`
    // and let `db::insert_cluster_formation_edges_batch` and `db::insert_suggestions_batch`
    // manage their own connections/transactions implicitly via the pool, or within their function.

    for (entity_z, groups_sharing_entity_z) in entity_to_pairwise_groups {
        if groups_sharing_entity_z.len() < 2 {
            debug!(
                "Skipping entity {} as it's shared by less than 2 groups ({})",
                entity_z.0,
                groups_sharing_entity_z.len()
            );
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

                    if let Err(e) =
                        xgraph_instance.add_edge(node_a_idx, node_b_idx, existing_weight, ())
                    {
                        error!(
                            "Failed to add reused edge to xgraph between {} and {}: {}. This indicates a graph inconsistency, but not a panic. Continue processing.",
                            group_a_id.0, group_b_id.0, e
                        );
                        // Do not panic, but log the error and skip this edge
                        continue;
                    }

                    debug!(
                        "Reused edge: {} --({:.4})-- {} (from previous run) via shared entity {}",
                        group_a_id.0, existing_weight, group_b_id.0, entity_z.0
                    );
                    continue;
                }

                // Calculate new edge weight
                edges_calculated += 1;

                let c_a = group_confidences
                    .get(group_a_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!(
                            "Confidence not found for group_a_id {}. Defaulting to 0.0.",
                            group_a_id.0
                        );
                        0.0
                    });

                let c_b = group_confidences
                    .get(group_b_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!(
                            "Confidence not found for group_b_id {}. Defaulting to 0.0.",
                            group_b_id.0
                        );
                        0.0
                    });

                // Simple average for demonstration; complex logic might go here.
                let w_z = (c_a + c_b) / 2.0;
                let edge_weight = w_z;

                if edge_weight <= 0.0 {
                    debug!(
                        "Skipping new edge between {} and {} due to non-positive weight {:.4} (shared entity {}).",
                        group_a_id.0, group_b_id.0, edge_weight, entity_z.0
                    );
                    continue;
                }

                let node_a_idx = node_mapper.get_or_add_node(&mut xgraph_instance, group_a_id);
                let node_b_idx = node_mapper.get_or_add_node(&mut xgraph_instance, group_b_id);

                if let Err(e) = xgraph_instance.add_edge(node_a_idx, node_b_idx, edge_weight, ()) {
                    error!(
                        "Failed to add newly calculated edge to xgraph between {} and {}: {}. This indicates a graph inconsistency, but not a panic. Continue processing.",
                        group_a_id.0, group_b_id.0, e
                    );
                    // Do not panic, but log the error and skip this edge
                    continue;
                }

                debug!(
                    "Calculated new edge: {} --({:.4})-- {} (via shared entity {})",
                    group_a_id.0, edge_weight, group_b_id.0, entity_z.0
                );

                // Store edge for future reuse
                let contributing_entities = vec![ContributingSharedEntityDetail {
                    entity_id: entity_z.0.clone(),
                    conf_entity_in_source_group: c_a,
                    conf_entity_in_target_group: c_b,
                    w_z,
                }];

                let contributing_entities_json = serde_json::to_value(&contributing_entities)
                    .unwrap_or_else(|e| {
                        warn!("Failed to serialize contributing entities for edge between {} and {}: {}. Storing as empty array JSON.", group_a_id.0, group_b_id.0, e);
                        serde_json::json!([])
                    });

                let cfe = NewClusterFormationEdge {
                    pipeline_run_id: pipeline_run_id.to_string(),
                    source_group_id: group_a_id.0.clone(),
                    target_group_id: group_b_id.0.clone(),
                    calculated_edge_weight: edge_weight,
                    contributing_shared_entities: Some(contributing_entities_json),
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
                    debug!(
                        "Generated weak link suggestion for ({}, {}) with weight {:.4}",
                        group_a_id.0, group_b_id.0, edge_weight
                    );
                }

                // Process batches when full
                if batch_edges.len() >= EDGE_BATCH_SIZE {
                    info!(
                        "Inserting batch of {} cluster formation edges...",
                        batch_edges.len()
                    );
                    // Use pool directly for batch inserts to manage their own transactions
                    if let Err(e) =
                        db::insert_cluster_formation_edges_batch(pool, &batch_edges).await
                    {
                        error!("Failed to insert batch of cluster formation edges: {}. Attempting to continue.", e);
                    }
                    batch_edges.clear();

                    if !batch_suggestions.is_empty() {
                        info!(
                            "Inserting batch of {} suggestions...",
                            batch_suggestions.len()
                        );
                        // Use pool directly for batch inserts
                        if let Err(e) = db::insert_suggestions_batch(pool, &batch_suggestions).await
                        {
                            error!("Failed to insert batch of suggestions: {}. Attempting to continue.", e);
                        }
                        batch_suggestions.clear();
                    }
                }
            }
        }
    }

    // Process remaining batches
    if !batch_edges.is_empty() {
        info!(
            "Inserting final batch of {} cluster formation edges...",
            batch_edges.len()
        );
        if let Err(e) = db::insert_cluster_formation_edges_batch(pool, &batch_edges).await {
            error!("Failed to insert final batch of cluster formation edges: {}. Attempting to continue.", e);
        }
    }

    if !batch_suggestions.is_empty() {
        info!(
            "Inserting final batch of {} suggestions...",
            batch_suggestions.len()
        );
        if let Err(e) = db::insert_suggestions_batch(pool, &batch_suggestions).await {
            error!(
                "Failed to insert final batch of suggestions: {}. Attempting to continue.",
                e
            );
        }
    }

    // Commit the transaction after fetching and processing group details
    transaction
        .commit()
        .await
        .context("Failed to commit transaction for build_weighted_xgraph_optimized")?;

    let total_elapsed = start_time.elapsed();

    info!(
        "Optimized weighted xgraph built in {:.2?} with {} nodes and {} edges.",
        total_elapsed,
        node_mapper.get_node_count(),
        xgraph_instance.edges.len()
    );

    info!(
        "Edge optimization: {} edges reused from previous run, {} edges calculated new",
        edges_reused, edges_calculated
    );

    Ok((xgraph_instance, node_mapper))
}

/// Optimized cluster verification with local embedding calculations and batching
/// This function now handles its own errors by logging and continuing, it does not return a Result.
async fn verify_clusters_optimized(
    pool: &PgPool, // Changed to pool to create new transactions
    new_cluster_ids: &[GroupClusterId],
    pipeline_run_id: &str,
    state: &ClusteringState,
) {
    info!(
        "Optimized cluster verification for {} clusters (run ID: {})...",
        new_cluster_ids.len(),
        pipeline_run_id
    );
    let start_time = Instant::now();

    // Initialize embedding cache
    let mut cache = EmbeddingCache::new(3600, 10000); // 1 hour cache, max 10k embeddings
    debug!(
        "Initialized embedding cache with duration {:?} and max size {}.",
        cache.cache_duration, cache.max_cache_size
    );

    let mut clusters_to_verify = Vec::new();

    // Only verify clusters that contain groups added since the last run
    for cluster_id in new_cluster_ids {
        let new_group_ids: Vec<String> = state
            .groups_added_since_last_run
            .iter()
            .map(|g| g.0.clone())
            .collect();

        if new_group_ids.is_empty() {
            debug!(
                "No new groups to check for cluster {}. Skipping check for this cluster.",
                cluster_id.0
            );
            continue;
        }

        let conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "Failed to get DB connection for cluster verification check: {}",
                    e
                );
                continue;
            }
        };

        let cluster_has_new_groups_query = "
            SELECT EXISTS(
                SELECT 1 FROM public.entity_group
                WHERE group_cluster_id = $1
                AND id = ANY($2)
            )";

        match conn
            .query_one(
                cluster_has_new_groups_query,
                &[&cluster_id.0, &new_group_ids],
            )
            .await
        {
            Ok(row) => {
                if row.get::<_, bool>(0) {
                    clusters_to_verify.push(cluster_id.clone());
                    debug!(
                        "Cluster {} contains new groups and will be verified.",
                        cluster_id.0
                    );
                } else {
                    debug!(
                        "Cluster {} has no new groups, skipping verification.",
                        cluster_id.0
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Failed to check if cluster {} has new groups: {}. Skipping verification for this cluster.",
                    cluster_id.0, e
                );
                continue;
            }
        }
    }

    if clusters_to_verify.is_empty() {
        info!("No clusters need verification - all are unchanged from previous run or contain no new groups.");
        return;
    }

    info!(
        "Proceeding to verify {} clusters that contain new groups.",
        clusters_to_verify.len()
    );

    // Use the enhanced verification with local calculations
    // This function will now manage its own connections from the pool
    verify_clusters_with_local_embeddings(
        pool, // Pass pool directly
        &clusters_to_verify,
        pipeline_run_id,
        &mut cache,
    )
    .await;

    info!(
        "Optimized cluster verification phase completed in {:.2?}.",
        start_time.elapsed()
    );
}

/// Enhanced cluster verification using local embedding calculations
/// This function now handles its own errors by logging and continuing, it does not return a Result.
async fn verify_clusters_with_local_embeddings(
    pool: &PgPool, // Changed to pool to create new transactions
    cluster_ids: &[GroupClusterId],
    pipeline_run_id: &str,
    cache: &mut EmbeddingCache,
) {
    info!(
        "Verifying quality of {} clusters using local embedding calculations (run ID: {})...",
        cluster_ids.len(),
        pipeline_run_id
    );
    let start_time = Instant::now();

    for cluster_id in cluster_ids {
        debug!("Starting verification for cluster: {}", cluster_id.0);

        let mut conn = match pool.get().await {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "Failed to get DB connection for cluster {}: {}",
                    cluster_id.0, e
                );
                continue;
            }
        };
        let transaction = match conn.transaction().await {
            Ok(tx) => tx,
            Err(e) => {
                warn!(
                    "Failed to start transaction for cluster {}: {}",
                    cluster_id.0, e
                );
                continue;
            }
        };

        let mut entities_in_cluster = HashSet::new();
        let entity_query = "
            SELECT entity_id_1, entity_id_2
            FROM public.entity_group
            WHERE group_cluster_id = $1";

        let entity_rows = match transaction.query(entity_query, &[&cluster_id.0]).await {
            Ok(rows) => rows,
            Err(e) => {
                warn!(
                    "Failed to fetch entities for cluster verification {}: {}. Skipping this cluster.",
                    cluster_id.0, e
                );
                let _ = transaction.rollback().await;
                continue;
            }
        };

        if entity_rows.is_empty() {
            debug!(
                "No pairwise groups found for cluster {} during verification. Skipping this cluster.",
                cluster_id.0
            );
            let _ = transaction.rollback().await;
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
            let _ = transaction.rollback().await;
            continue;
        }

        // Sample pairs for verification
        let entity_pairs_to_check = match sample_entity_pairs_for_verification(&entities) {
            Ok(pairs) => pairs,
            Err(e) => {
                warn!(
                    "Failed to sample entity pairs for cluster {}: {}. Skipping verification for this cluster.",
                    cluster_id.0, e
                );
                let _ = transaction.rollback().await;
                continue;
            }
        };

        debug!(
            "Verifying cluster {} with {} sampled entity pairs (out of {} total entities) using local embedding calculations.",
            cluster_id.0,
            entity_pairs_to_check.len(),
            entities.len()
        );

        // Use batched coherence calculation
        // Pass a reference to the transaction for these calculations
        let verification_results = match calculate_coherence_scores_batch(
            &transaction, // Use the current transaction for calculation
            &entity_pairs_to_check,
            cache,
        )
        .await
        {
            Ok(results) => results,
            Err(e) => {
                warn!(
                    "Failed to calculate coherence scores batch for cluster {}: {}. Coherence score will be NULL for this cluster.",
                    cluster_id.0, e
                );
                let _ = transaction.rollback().await;
                continue;
            }
        };

        let verification_scores: Vec<f64> = verification_results
            .into_iter()
            .map(|(_, _, score)| score)
            .collect();

        info!(
            "Cluster {} verification processed {} pairs. Cache size: {}",
            cluster_id.0,
            verification_scores.len(),
            cache.len()
        );

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
            let _ = transaction.rollback().await;
            continue;
        } else {
            debug!(
                "Updated average_coherence_score for cluster {}: {:?}",
                cluster_id.0, avg_score_to_store
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
                    "verification_method": "local_embedding_candle_based",
                    "cache_current_size": cache.len(),
                    "cache_max_size": cache.max_cache_size
                });
                let reason_message = format!(
                    "Cluster {} ({} entities, {} pairs checked) has low average internal entity-pair confidence: {:.4}, below threshold of {}. Verified using local embedding calculations.",
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
                    reason_code: Some(
                        "LOW_INTERNAL_CLUSTER_COHERENCE_LOCAL_EMBEDDINGS".to_string(),
                    ),
                    reason_message: Some(reason_message),
                    priority: 1, // High priority for review
                    status: SuggestionStatus::PendingReview.as_str().to_string(),
                    reviewer_id: None,
                    reviewed_at: None,
                    review_notes: None,
                };
                // Use pool directly for inserting suggestion
                if let Err(e) = db::insert_suggestion(pool, &suggestion).await {
                    // Pass pool
                    warn!(
                        "Failed to log SUGGEST_SPLIT_CLUSTER for cluster {}: {}",
                        cluster_id.0, e
                    );
                } else {
                    info!(
                        "Logged SUGGEST_SPLIT_CLUSTER suggestion for cluster {} due to low coherence.",
                        cluster_id.0
                    );
                }
            } else {
                debug!(
                    "Cluster {} coherence score {:.4} is above threshold {}.",
                    cluster_id.0,
                    calculated_avg_score,
                    config::VERIFICATION_THRESHOLD
                );
            }
        } else {
            warn!(
                "No average coherence score calculated for cluster {}.",
                cluster_id.0
            );
        }
        // Commit the transaction for this cluster's verification
        if let Err(e) = transaction.commit().await {
            error!(
                "Failed to commit transaction for cluster {}: {}",
                cluster_id.0, e
            );
        }
    }
    info!(
        "Finished enhanced verification of all relevant clusters with local embedding calculations (took {:.2?}).",
        start_time.elapsed()
    );
}

/// Sample entity pairs for verification to balance thoroughness with performance
fn sample_entity_pairs_for_verification(
    entities: &[EntityId],
) -> Result<Vec<(EntityId, EntityId)>> {
    let mut entity_pairs_to_check: Vec<(EntityId, EntityId)> = Vec::new();
    let max_entities_to_sample = 10;
    let max_pairs_to_check_per_cluster = 20; // Aim for a max of 20 pairs

    if entities.len() < 2 {
        debug!("Less than 2 entities to sample for verification. Returning empty.");
        return Ok(Vec::new());
    }

    if entities.len() <= max_entities_to_sample {
        debug!(
            "Entities count ({}) is <= max_entities_to_sample ({}), checking all possible pairs.",
            entities.len(),
            max_entities_to_sample
        );
        for i in 0..entities.len() {
            for j in (i + 1)..entities.len() {
                if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                    debug!(
                        "Reached max_pairs_to_check_per_cluster ({}).",
                        max_pairs_to_check_per_cluster
                    );
                    break;
                }
                entity_pairs_to_check.push((entities[i].clone(), entities[j].clone()));
            }
            if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                break;
            }
        }
    } else {
        debug!(
            "Entities count ({}) is > max_entities_to_sample ({}), sampling entities.
            ",
            entities.len(),
            max_entities_to_sample
        );
        let mut rng = rand::thread_rng();
        let sampled_entities: Vec<EntityId> = entities
            .choose_multiple(&mut rng, max_entities_to_sample)
            .cloned()
            .collect();
        for i in 0..sampled_entities.len() {
            for j in (i + 1)..sampled_entities.len() {
                if entity_pairs_to_check.len() >= max_pairs_to_check_per_cluster {
                    debug!(
                        "Reached max_pairs_to_check_per_cluster ({}).",
                        max_pairs_to_check_per_cluster
                    );
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
        "Sampled {} entity pairs for verification.",
        entity_pairs_to_check.len()
    );
    Ok(entity_pairs_to_check)
}

pub async fn process_clusters(pool: &PgPool, pipeline_run_id: &str) -> Result<usize> {
    info!(
        "Starting optimized cluster consolidation process with local embedding calculations (run ID: {})...",
        pipeline_run_id
    );
    let start_time = Instant::now();

    // Analyze current state to optimize processing - now uses its own transaction
    let state = analyze_clustering_state(pool, pipeline_run_id) // Pass pool
        .await
        .context("Failed to analyze clustering state.")?;

    if state.unclustered_groups.is_empty() {
        info!("No unclustered groups found. Skipping consolidation.");
        return Ok(0);
    }

    // Early exit if minimal changes since last run - This is a heuristic, keep it informative.
    if state.groups_added_since_last_run.len() < 10 && state.last_clustering_run.is_some() {
        info!(
            "Only {} new groups since last run - considering incremental processing strategies (current: full re-clustering with edge reuse).",
            state.groups_added_since_last_run.len()
        );
    }

    info!("Building optimized weighted entity group graph...");
    let build_start = Instant::now();
    // build_weighted_xgraph_optimized now manages its own transaction internally.
    let (graph, node_mapper) = build_weighted_xgraph_optimized(pool, pipeline_run_id, &state)
        .await
        .context("Failed to build weighted xgraph.")?;
    info!("Optimized graph built in {:.2?}", build_start.elapsed());

    if graph.nodes.is_empty() {
        info!("No nodes in the graph after building. Skipping Leiden clustering.");
        return Ok(0);
    }

    info!(
        "Graph built with {} nodes and {} edges. Starting Leiden clustering...",
        node_mapper.get_node_count(),
        graph.edges.len()
    );
    debug!(
        "Leiden resolution parameter: {}",
        config::LEIDEN_RESOLUTION_PARAMETER
    );

    let leiden_config = CommunityConfig {
        resolution: config::LEIDEN_RESOLUTION_PARAMETER,
        deterministic: true,
        seed: Some(42),
        iterations: 10, // Max iterations for Leiden
        gamma: 1.0,     // Typically 1.0 for standard modularity optimization
    };

    let leiden_start = Instant::now();
    let communities_result = graph
        .detect_communities_with_config(leiden_config)
        .context("Leiden community detection failed.")?;
    info!(
        "Leiden algorithm finished in {:.2?}, found {} raw communities.",
        leiden_start.elapsed(),
        communities_result.len()
    );

    let mut clusters_created = 0;
    let mut new_cluster_ids_for_verification = Vec::new();

    for (_community_idx, node_indices_in_community) in communities_result.into_iter() {
        debug!(
            "Processing raw community ({} nodes)...",
            node_indices_in_community.len()
        );
        let pairwise_group_ids_in_community: Vec<EntityGroupId> = node_indices_in_community
            .iter()
            .filter_map(|&node_idx| node_mapper.get_group_id(node_idx).cloned())
            .collect();

        if pairwise_group_ids_in_community.is_empty() {
            debug!("Skipping raw community with 0 mapped pairwise groups.",);
            continue;
        }

        // Fetch all unique entity IDs for the cluster - within a new, short-lived transaction
        let mut conn = pool
            .get()
            .await
            .context("Failed to get DB connection for fetching entities in community")?;
        let community_tx = conn
            .transaction()
            .await
            .context("Failed to start transaction for fetching entities in community")?;

        let mut unique_entities_in_cluster = HashSet::new();
        let group_id_strings_for_query: Vec<String> = pairwise_group_ids_in_community
            .iter()
            .map(|g| g.0.clone())
            .collect();
        debug!(
            "Fetching entities for {} pairwise groups in community.",
            group_id_strings_for_query.len(),
        );

        let entities_query = "
            SELECT entity_id_1, entity_id_2
            FROM public.entity_group
            WHERE id = ANY($1)";

        let entity_rows = community_tx
            .query(entities_query, &[&group_id_strings_for_query])
            .await
            .context(format!("Failed to fetch entities for community.",))?;

        for row in entity_rows {
            unique_entities_in_cluster.insert(EntityId(row.get("entity_id_1")));
            unique_entities_in_cluster.insert(EntityId(row.get("entity_id_2")));
        }
        let unique_entity_count = unique_entities_in_cluster.len() as i32;

        community_tx
            .commit()
            .await
            .context("Failed to commit transaction for fetching entities in community")?;

        if unique_entity_count < 2 {
            debug!(
                "Skipping community with < 2 unique entities ({} found).",
                unique_entity_count
            );
            continue;
        }

        info!(
            "Community will form a cluster with {} pairwise groups and {} unique entities.",
            pairwise_group_ids_in_community.len(),
            unique_entity_count
        );

        // Create cluster record and update entity groups - each in their own short-lived transactions
        let new_cluster_id = GroupClusterId(Uuid::new_v4().to_string());
        create_cluster_record(
            pool, // Pass pool
            &new_cluster_id,
            pairwise_group_ids_in_community.len() as i32,
            unique_entity_count,
        )
        .await
        .context(format!(
            "Failed to create cluster record for new cluster {}.",
            new_cluster_id.0
        ))?;
        debug!("Created cluster record for ID: {}", new_cluster_id.0);

        update_entity_groups_with_cluster_id(
            pool, // Pass pool
            &new_cluster_id,
            &pairwise_group_ids_in_community,
        )
        .await
        .context(format!(
            "Failed to update pairwise groups for cluster {}.",
            new_cluster_id.0
        ))?;
        debug!(
            "Updated {} pairwise groups with cluster ID: {}",
            pairwise_group_ids_in_community.len(),
            new_cluster_id.0
        );

        new_cluster_ids_for_verification.push(new_cluster_id.clone());
        clusters_created += 1;
        info!(
            "Successfully created GroupCluster {} (ID: {}) for {} pairwise groups and {} unique entities.",
            clusters_created,
            new_cluster_id.0,
            pairwise_group_ids_in_community.len(),
            unique_entity_count
        );
    }

    info!(
        "Completed processing Leiden communities. {} new clusters created in this run.",
        clusters_created
    );

    if !new_cluster_ids_for_verification.is_empty() {
        info!("Starting optimized cluster verification with local embedding calculations...");
        let verify_start = Instant::now();
        verify_clusters_optimized(
            pool, // Pass pool
            &new_cluster_ids_for_verification,
            pipeline_run_id,
            &state,
        )
        .await;
        info!(
            "Optimized cluster verification completed in {:.2?}.",
            verify_start.elapsed()
        );
    } else {
        info!("No new clusters created, skipping verification phase.");
    }

    info!(
        "Optimized cluster consolidation with local embedding calculations finished in {:.2?}. Total {} clusters created in this run.",
        start_time.elapsed(),
        clusters_created
    );
    Ok(clusters_created)
}

// Changed to accept pool directly instead of transaction
async fn create_cluster_record(
    pool: &PgPool,
    cluster_id: &GroupClusterId,
    pair_count: i32,
    entity_count: i32,
) -> Result<()> {
    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for create_cluster_record")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for create_cluster_record")?;

    let now = Utc::now().naive_utc();
    let cluster_name = format!("LeidenCluster-{}", &cluster_id.0[..8]);
    let description = format!(
        "Leiden-generated cluster from {} pairwise entity groups, {} unique entities.",
        pair_count, entity_count
    );

    debug!(
        "Inserting new cluster record: ID={}, Name={}, Description={}",
        cluster_id.0, cluster_name, description
    );
    transaction
        .execute(
            "INSERT INTO public.entity_group_cluster (id, name, description, created_at, updated_at, entity_count, group_count, average_coherence_score)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL)",
            &[
                &cluster_id.0,
                &cluster_name,
                &description,
                &now,
                &now,
                &entity_count,
                &pair_count,
            ],
        )
        .await
        .context(format!(
            "Failed to insert entity_group_cluster record for {}",
            cluster_id.0
        ))?;
    debug!(
        "Successfully inserted cluster record for ID: {}",
        cluster_id.0
    );

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for create_cluster_record")?;
    Ok(())
}

// Changed to accept pool directly instead of transaction
async fn update_entity_groups_with_cluster_id(
    pool: &PgPool,
    cluster_id: &GroupClusterId,
    pairwise_group_ids: &[EntityGroupId],
) -> Result<()> {
    if pairwise_group_ids.is_empty() {
        debug!(
            "No pairwise group IDs to update for cluster {}. Skipping update.",
            cluster_id.0
        );
        return Ok(());
    }

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for update_entity_groups_with_cluster_id")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for update_entity_groups_with_cluster_id")?;

    let now = Utc::now().naive_utc();
    let group_id_strings: Vec<String> = pairwise_group_ids.iter().map(|g| g.0.clone()).collect();
    debug!(
        "Updating {} entity_group records with cluster ID: {}",
        group_id_strings.len(),
        cluster_id.0
    );
    transaction
        .execute(
            "UPDATE public.entity_group SET group_cluster_id = $1, updated_at = $2 WHERE id = ANY($3)",
            &[&cluster_id.0, &now, &group_id_strings],
        )
        .await
        .context(format!(
            "Failed to update entity_group with cluster_id {} for {} pairwise groups",
            cluster_id.0,
            group_id_strings.len()
        ))?;
    debug!(
        "Successfully updated {} entity_group records for cluster ID: {}",
        group_id_strings.len(),
        cluster_id.0
    );

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for update_entity_groups_with_cluster_id")?;
    Ok(())
}
