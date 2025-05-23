// src/service_consolidate_clusters.rs
use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
// use serde_json::json; // Not directly used after changes, but kept if needed for future metadata
use std::collections::{HashMap, HashSet};
use std::time::{Duration as StdDuration, Instant}; // Renamed to avoid conflict with pgvector::Vector
use tokio_postgres::{GenericClient, Transaction};
use uuid::Uuid;

// pgvector for embedding type (assuming it's a crate dependency)
// If not using pgvector::Vector directly for cosine_similarity, Vec<f32> is fine.
// For this refactor, we'll fetch as pgvector::Vector and convert to Vec<f32> for use.
use pgvector::Vector;


// Local imports
use crate::db::PgPool;
use crate::models::{ServiceGroupClusterId, ServiceId};
// Assuming cosine_similarity_manual is available in utils
use crate::utils::{cosine_similarity_candle, cosine_similarity_manual};


/// Configuration for consolidation parameters
#[derive(Debug, Clone)]
pub struct ConsolidationConfig {
    pub similarity_threshold: f64,
    pub embedding_batch_size: usize, // For fetching from DB to cache
    pub db_batch_size: usize,        // For DB query LIMIT
    pub max_cache_size: usize,       // Max items in EmbeddingCache
    pub min_cluster_size: usize,     // Min services in a cluster to be considered for centroid calculation
    pub embedding_cache_duration_secs: u64, // Duration for embedding cache entries
}

impl Default for ConsolidationConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: 0.85, // Default similarity for merging clusters
            embedding_batch_size: 200,  // How many embeddings to process for similarity at once
            db_batch_size: 100,         // How many services to fetch from DB in one go
            max_cache_size: 10000,      // Max service embeddings in local cache
            min_cluster_size: 2,        // Clusters smaller than this won't have centroids calculated (or won't be processed)
            embedding_cache_duration_secs: 3600, // 1 hour
        }
    }
}

/// Represents a service with its embedding data (using Vec<f32> for processing)
#[derive(Debug, Clone)]
struct ServiceWithEmbedding {
    service_id: ServiceId,
    cluster_id: ServiceGroupClusterId, // Current cluster
    embedding: Vec<f32>, // Processed embedding
    // metadata: serde_json::Value, // If needed later
}

/// Represents a cluster with aggregated data for consolidation
#[derive(Debug, Clone)]
struct ClusterData {
    cluster_id: ServiceGroupClusterId,
    service_count: usize,
    average_embedding: Vec<f32>, // Centroid
    service_ids: HashSet<ServiceId>, // Services in this cluster
                                     // coherence_score: Option<f64>, // If needed later
}

/// Cache for storing service embeddings locally to reduce DB calls
#[derive(Debug)]
struct ServiceEmbeddingCache {
    embeddings: HashMap<ServiceId, CachedServiceEmbedding>,
    cache_duration: StdDuration,
    max_size: usize,
    hits: u64,
    misses: u64,
}

#[derive(Debug, Clone)]
struct CachedServiceEmbedding {
    embedding: Vec<f32>,
    cached_at: Instant,
}

impl ServiceEmbeddingCache {
    fn new(duration_secs: u64, max_size: usize) -> Self {
        Self {
            embeddings: HashMap::with_capacity(max_size / 2), // Pre-allocate a bit
            cache_duration: StdDuration::from_secs(duration_secs),
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, service_id: &ServiceId) -> Option<&Vec<f32>> {
        // First, check if the entry exists and if it's expired.
        // This initial check is purely immutable.
        let entry_is_expired = match self.embeddings.get(service_id) {
            Some(cached) => cached.cached_at.elapsed() >= self.cache_duration, // Check if expired
            None => {
                // Entry does not exist
                self.misses += 1;
                return None;
            }
        };

        // If we reach here, the entry exists. Now, handle based on expiration.
        if entry_is_expired {
            // Entry exists but is expired. Remove it. This is a mutable operation.
            // The previous immutable borrow from `self.embeddings.get()` in the match is now finished.
            self.embeddings.remove(service_id);
            self.misses += 1; // It was a miss because it was unusable.
            None
        } else {
            // Entry exists and is not expired. Increment hits and return the reference.
            // We need to get the reference again because the previous one from the match scope is gone.
            // This is safe because we've established it's not expired and haven't done a mutable op since.
            self.hits += 1;
            // This re-borrows `self.embeddings` immutably.
            self.embeddings.get(service_id).map(|c| &c.embedding)
        }
    }

    fn insert(&mut self, service_id: ServiceId, embedding: Vec<f32>) {
        if self.embeddings.len() >= self.max_size && self.max_size > 0 { // ensure max_size > 0 to prevent division by zero or unintended full clear
            // Simple eviction: remove a random key if full (can be improved with LRU)
            // To make it slightly more deterministic for testing or specific needs,
            // one might sort keys or use a more sophisticated eviction.
            // For now, `keys().next()` is simple but not truly random.
            // A more robust LRU would require more state (e.g., a linked list of keys).
            if let Some(key_to_remove) = self.embeddings.keys().next().cloned() { // .cloned() is important here
                self.embeddings.remove(&key_to_remove);
            }
        }
        self.embeddings.insert(
            service_id,
            CachedServiceEmbedding {
                embedding,
                cached_at: Instant::now(),
            },
        );
    }
    #[allow(dead_code)] // Potentially useful for debugging
    fn stats(&self) -> (u64, u64) {
        (self.hits, self.misses)
    }
}


/// Batch fetcher for service embeddings
struct ServiceEmbeddingBatch;

impl ServiceEmbeddingBatch {
    /// Fetches embeddings for a batch of service IDs, utilizing the cache.
    async fn fetch_embeddings(
        conn: &impl GenericClient,
        service_ids: &[ServiceId],
        cache: &mut ServiceEmbeddingCache,
    ) -> Result<HashMap<ServiceId, Vec<f32>>> {
        let mut results = HashMap::new();
        let mut ids_to_fetch_from_db = Vec::new();

        for service_id in service_ids {
            if let Some(embedding) = cache.get(service_id) {
                results.insert(service_id.clone(), embedding.clone());
            } else {
                ids_to_fetch_from_db.push(service_id.0.clone()); // Store String for query
            }
        }

        if !ids_to_fetch_from_db.is_empty() {
            debug!("Cache miss for {} service IDs, fetching from DB.", ids_to_fetch_from_db.len());
            // Corrected table name to public.service and column to embedding_v2
            let query = "
                SELECT id, embedding_v2
                FROM public.service
                WHERE id = ANY($1) AND embedding_v2 IS NOT NULL
            ";
            let rows = conn
                .query(query, &[&ids_to_fetch_from_db])
                .await
                .context("Failed to batch fetch service embeddings from DB")?;

            for row in rows {
                let id_str: String = row.get("id");
                let service_id = ServiceId(id_str);
                if let Some(pg_vector) = row.get::<_, Option<Vector>>("embedding_v2") {
                    let embedding_vec_f32 = pg_vector.to_vec(); // Convert pgvector::Vector to Vec<f32>
                    results.insert(service_id.clone(), embedding_vec_f32.clone());
                    cache.insert(service_id, embedding_vec_f32);
                } else {
                    warn!("Service {} found but its embedding_v2 is NULL.", service_id.0);
                }
            }
        }
        Ok(results)
    }
}


/// Main function to consolidate service clusters based on embedding similarity
pub async fn consolidate_service_clusters(
    pool: &PgPool,
    pipeline_run_id: &str,
    config: Option<ConsolidationConfig>,
) -> Result<usize> {
    let config = config.unwrap_or_default();
    info!(
        "Starting service cluster consolidation (run ID: {}, threshold: {:.3}, min_cluster_size: {})...",
        pipeline_run_id, config.similarity_threshold, config.min_cluster_size
    );
    let start_time = Instant::now();

    // Initialize cache
    let mut embedding_cache = ServiceEmbeddingCache::new(config.embedding_cache_duration_secs, config.max_cache_size);


    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for cluster consolidation")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for cluster consolidation")?;

    // Step 1: Load all services that are part of any cluster, along with their embeddings
    // This is slightly different from entity consolidation; we get services from service_group.
    let services_with_embeddings =
        load_clustered_services_with_embeddings(&transaction, &config, &mut embedding_cache).await?;
    info!(
        "Loaded {} services with embeddings for consolidation. Cache hits: {}, misses: {}",
        services_with_embeddings.len(), embedding_cache.hits, embedding_cache.misses
    );

    if services_with_embeddings.is_empty() {
        info!("No services with embeddings found in clusters, skipping consolidation.");
        transaction.commit().await.context("Failed to commit empty transaction")?;
        return Ok(0);
    }

    // Step 2: Group services by current clusters
    let mut clusters_map = group_services_into_cluster_data(services_with_embeddings);
    info!("Found {} initial service clusters from service_group records", clusters_map.len());

    // Step 3: Calculate cluster centroids and filter by minimum size
    calculate_and_filter_cluster_centroids(&mut clusters_map, config.min_cluster_size);
    let mut clusters_to_process: Vec<ClusterData> = clusters_map.into_values().collect();

    info!(
        "Processing {} service clusters meeting minimum size requirement ({} services)",
        clusters_to_process.len(), config.min_cluster_size
    );

    if clusters_to_process.len() < 2 {
        info!("Not enough service clusters ({} found) to consolidate further.", clusters_to_process.len());
        transaction.commit().await.context("Failed to commit transaction")?;
        return Ok(0);
    }
    
    // Sort clusters by ID for deterministic processing (optional, but good for reproducibility)
    clusters_to_process.sort_by(|a, b| a.cluster_id.0.cmp(&b.cluster_id.0));


    // Step 4: Find similar clusters using batched similarity calculations
    // SimilarityCache for cluster-pair similarities (not individual embeddings)
    let mut similarity_pair_cache = ClusterSimilarityPairCache::new(config.max_cache_size / 10); // Smaller cache for pairs

    let merge_candidates = find_merge_candidates_for_services(&clusters_to_process, &config, &mut similarity_pair_cache).await?;
    info!("Found {} potential service cluster merges. Similarity cache hits: {}, misses: {}", merge_candidates.len(), similarity_pair_cache.hits, similarity_pair_cache.misses);

    if merge_candidates.is_empty() {
        info!("No similar service clusters found for merging.");
        transaction.commit().await.context("Failed to commit transaction")?;
        return Ok(0);
    }

    // Step 5: Execute cluster merges in the database
    let merged_count =
        execute_service_cluster_merges(&transaction, merge_candidates, pipeline_run_id, pool).await?;

    transaction
        .commit()
        .await
        .context("Failed to commit service cluster consolidation transaction")?;

    let (cache_hits, cache_misses) = embedding_cache.stats();
    let total_lookups = cache_hits + cache_misses;
    let hit_ratio = if total_lookups > 0 { (cache_hits as f64 / total_lookups as f64) * 100.0 } else { 0.0 };

    info!(
        "Service cluster consolidation completed in {:.2?}. {} cluster pairs merged. Embedding Cache hit ratio: {:.2}% ({}/{} lookups)",
        start_time.elapsed(),
        merged_count,
        hit_ratio,
        cache_hits,
        total_lookups
    );

    Ok(merged_count)
}

/// Load services that are part of clusters, along with their embeddings.
async fn load_clustered_services_with_embeddings(
    transaction: &Transaction<'_>,
    config: &ConsolidationConfig,
    cache: &mut ServiceEmbeddingCache,
) -> Result<Vec<ServiceWithEmbedding>> {
    // Get all distinct service IDs present in service_group table that have a group_cluster_id
    let query_service_ids_in_clusters = "
        SELECT DISTINCT service_id, sg.group_cluster_id
        FROM (
            SELECT service_id_1 AS service_id, group_cluster_id FROM public.service_group WHERE group_cluster_id IS NOT NULL
            UNION
            SELECT service_id_2 AS service_id, group_cluster_id FROM public.service_group WHERE group_cluster_id IS NOT NULL
        ) sg
    ";
    let rows = transaction.query(query_service_ids_in_clusters, &[])
        .await.context("Failed to fetch service IDs from service_group")?;

    let mut service_cluster_map = HashMap::new();
    let mut distinct_service_ids_to_fetch_embeddings = HashSet::new();

    for row in rows {
        let service_id_str: String = row.get("service_id");
        let cluster_id_str: String = row.get("group_cluster_id");
        let service_id = ServiceId(service_id_str);
        
        service_cluster_map.insert(service_id.clone(), ServiceGroupClusterId(cluster_id_str));
        distinct_service_ids_to_fetch_embeddings.insert(service_id);
    }
    
    if distinct_service_ids_to_fetch_embeddings.is_empty() {
        return Ok(Vec::new());
    }

    info!("Found {} distinct services across all service clusters. Fetching embeddings...", distinct_service_ids_to_fetch_embeddings.len());

    let mut all_services_with_embeddings = Vec::new();
    let service_id_vec: Vec<ServiceId> = distinct_service_ids_to_fetch_embeddings.into_iter().collect();

    // Fetch embeddings in batches using ServiceEmbeddingBatch
    for chunk in service_id_vec.chunks(config.db_batch_size) { // db_batch_size for fetching from DB
        let fetched_embeddings_map = ServiceEmbeddingBatch::fetch_embeddings(transaction, chunk, cache).await?;
        for (service_id, embedding) in fetched_embeddings_map {
            if let Some(cluster_id) = service_cluster_map.get(&service_id) {
                 all_services_with_embeddings.push(ServiceWithEmbedding {
                    service_id,
                    cluster_id: cluster_id.clone(),
                    embedding,
                });
            }
        }
    }
    Ok(all_services_with_embeddings)
}


/// Group services into clusters and prepare cluster data structures
fn group_services_into_cluster_data(
    services_with_embeddings: Vec<ServiceWithEmbedding>,
) -> HashMap<ServiceGroupClusterId, ClusterData> {
    let mut cluster_map: HashMap<ServiceGroupClusterId, ClusterData> = HashMap::new();

    for svc_emb in services_with_embeddings {
        let cluster_id_key = svc_emb.cluster_id.clone();

        cluster_map
            .entry(cluster_id_key)
            .and_modify(|cluster_data| {
                cluster_data.service_count += 1;
                cluster_data.service_ids.insert(svc_emb.service_id.clone());
                // Accumulate embeddings for centroid calculation
                if cluster_data.average_embedding.len() == svc_emb.embedding.len() {
                    for (i, &val) in svc_emb.embedding.iter().enumerate() {
                        cluster_data.average_embedding[i] += val;
                    }
                } else if cluster_data.average_embedding.is_empty() && !svc_emb.embedding.is_empty() {
                    // This is the first non-empty embedding for this cluster
                    cluster_data.average_embedding = svc_emb.embedding.clone();
                } else if !svc_emb.embedding.is_empty() { // average_embedding is not empty but dimensions mismatch
                     warn!("Embedding dimension mismatch for service {} in cluster {}. Expected {}, got {}. Skipping accumulation for this service.",
                           svc_emb.service_id.0, svc_emb.cluster_id.0, cluster_data.average_embedding.len(), svc_emb.embedding.len());
                }
                // If svc_emb.embedding is empty, we do nothing for accumulation, but service_count is incremented.
            })
            .or_insert_with(|| {
                let mut service_ids_set = HashSet::new();
                service_ids_set.insert(svc_emb.service_id.clone());
                ClusterData {
                    cluster_id: svc_emb.cluster_id.clone(),
                    service_count: 1,
                    average_embedding: svc_emb.embedding.clone(), // Initialize with first service's embedding
                    service_ids: service_ids_set,
                }
            });
    }
    cluster_map
}

/// Calculate centroid embeddings for each cluster and filter out small/empty ones
fn calculate_and_filter_cluster_centroids(
    clusters_map: &mut HashMap<ServiceGroupClusterId, ClusterData>,
    min_cluster_size: usize,
) {
    clusters_map.retain(|_id, cluster_data| {
        if cluster_data.service_count == 0 || cluster_data.average_embedding.is_empty() {
            debug!("Removing cluster {} from centroid calculation due to zero services or empty average embedding.", cluster_data.cluster_id.0);
            return false; // Remove if no services or if the initial embedding was empty and never updated
        }
        if cluster_data.service_count < min_cluster_size {
             debug!("Cluster {} with {} services is smaller than min_cluster_size {}, removing from consolidation consideration.",
                   cluster_data.cluster_id.0, cluster_data.service_count, min_cluster_size);
            return false; // Remove if too small
        }

        // Average the accumulated embeddings for the centroid
        // This check is technically redundant due to the retain condition above, but good for clarity
        if cluster_data.service_count > 0 { 
            for val in cluster_data.average_embedding.iter_mut() {
                *val /= cluster_data.service_count as f32;
            }
        }
        debug!(
            "Cluster {} centroid calculated. Size: {}, Embedding dim: {}",
            cluster_data.cluster_id.0,
            cluster_data.service_count,
            cluster_data.average_embedding.len()
        );
        true // Keep this cluster
    });
}


/// Cache for storing computed cluster-pair similarities
struct ClusterSimilarityPairCache {
    cache: HashMap<(String, String), f64>,
    max_size: usize,
    hits: u64,
    misses: u64,
}

impl ClusterSimilarityPairCache {
    fn new(max_size: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_size.min(1024)), // Cap initial capacity
            max_size,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, id1: &str, id2: &str) -> Option<f64> {
        let key = if id1 < id2 { (id1.to_string(), id2.to_string()) } else { (id2.to_string(), id1.to_string()) };
        if let Some(similarity) = self.cache.get(&key).copied() {
            self.hits += 1;
            Some(similarity)
        } else {
            self.misses += 1;
            None
        }
    }

    fn insert(&mut self, id1: &str, id2: &str, similarity: f64) {
        if self.cache.len() >= self.max_size && self.max_size > 0 {
            // Simple eviction: remove a fraction of entries if full
            let num_to_remove = (self.max_size / 10).max(1);
            let keys_to_remove: Vec<_> = self.cache.keys().take(num_to_remove).cloned().collect();
            for key in keys_to_remove {
                self.cache.remove(&key);
            }
        }
        let key = if id1 < id2 { (id1.to_string(), id2.to_string()) } else { (id2.to_string(), id1.to_string()) };
        self.cache.insert(key, similarity);
    }
}


/// Find service clusters that should be merged based on similarity threshold
async fn find_merge_candidates_for_services(
    clusters: &[ClusterData],
    config: &ConsolidationConfig,
    similarity_cache: &mut ClusterSimilarityPairCache,
) -> Result<Vec<(ServiceGroupClusterId, ServiceGroupClusterId, f64)>> {
    let mut merge_candidates = Vec::new();
    let mut comparisons_made = 0;

    for i in 0..clusters.len() {
        for j in (i + 1)..clusters.len() {
            let cluster1 = &clusters[i];
            let cluster2 = &clusters[j];

            if cluster1.average_embedding.is_empty() || cluster2.average_embedding.is_empty() {
                warn!("Skipping similarity calculation between {} and {} due to empty centroid.", cluster1.cluster_id.0, cluster2.cluster_id.0);
                continue;
            }
            if cluster1.average_embedding.len() != cluster2.average_embedding.len() {
                warn!(
                    "Cluster {} (dim {}) and {} (dim {}) have different embedding dimensions, skipping comparison.",
                    cluster1.cluster_id.0, cluster1.average_embedding.len(),
                    cluster2.cluster_id.0, cluster2.average_embedding.len()
                );
                continue;
            }

            // Check cache first
            if let Some(cached_similarity) = similarity_cache.get(&cluster1.cluster_id.0, &cluster2.cluster_id.0) {
                if cached_similarity >= config.similarity_threshold {
                    merge_candidates.push((
                        cluster1.cluster_id.clone(),
                        cluster2.cluster_id.clone(),
                        cached_similarity,
                    ));
                }
                continue; // Move to next pair if cache hit
            }
            
            comparisons_made +=1;
            match cosine_similarity_candle(&cluster1.average_embedding, &cluster2.average_embedding) {
                Ok(similarity) => {
                    similarity_cache.insert(&cluster1.cluster_id.0, &cluster2.cluster_id.0, similarity);
                    if similarity >= config.similarity_threshold {
                        debug!(
                            "Found similar service clusters: {} and {} (similarity: {:.4})",
                            cluster1.cluster_id.0, cluster2.cluster_id.0, similarity
                        );
                        merge_candidates.push((
                            cluster1.cluster_id.clone(),
                            cluster2.cluster_id.clone(),
                            similarity,
                        ));
                    }
                }
                Err(e_candle) => {
                    // Fallback to manual calculation
                    warn!("Candle cosine similarity failed for service clusters {} and {}: {}. Trying manual calculation.",
                          cluster1.cluster_id.0, cluster2.cluster_id.0, e_candle);
                    if let Some(manual_similarity) = cosine_similarity_manual(&cluster1.average_embedding, &cluster2.average_embedding) {
                        similarity_cache.insert(&cluster1.cluster_id.0, &cluster2.cluster_id.0, manual_similarity);
                        if manual_similarity >= config.similarity_threshold {
                             merge_candidates.push((
                                cluster1.cluster_id.clone(),
                                cluster2.cluster_id.clone(),
                                manual_similarity,
                            ));
                        }
                    } else {
                        warn!("Manual cosine similarity also failed for service clusters {} and {}. Similarity set to 0.0",
                              cluster1.cluster_id.0, cluster2.cluster_id.0);
                        similarity_cache.insert(&cluster1.cluster_id.0, &cluster2.cluster_id.0, 0.0); // Cache failure as 0.0
                    }
                }
            }
        }
        if i > 0 && i % 100 == 0 { // Log progress
            debug!("Processed {} clusters for merge candidates. Comparisons: {}, Cache hits: {}, misses: {}",
                i, comparisons_made, similarity_cache.hits, similarity_cache.misses);
        }
    }

    // Sort by similarity score (highest first) to prioritize best matches
    merge_candidates.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
    Ok(merge_candidates)
}

/// Execute service cluster merges in the database
async fn execute_service_cluster_merges(
    transaction: &Transaction<'_>,
    merge_candidates: Vec<(ServiceGroupClusterId, ServiceGroupClusterId, f64)>,
    pipeline_run_id: &str,
    _pool: &PgPool, // _pool is not used in the current implementation of this function after corrections
) -> Result<usize> {
    let mut merged_count = 0;
    let mut already_merged_ids: HashSet<String> = HashSet::new(); // Tracks original IDs that have been merged away

    for (target_cluster_id_obj, source_cluster_id_obj, similarity) in merge_candidates {
        let target_cluster_id = target_cluster_id_obj.0; // Keep this one
        let source_cluster_id = source_cluster_id_obj.0; // Merge this one into target

        // Skip if either cluster was already involved in a merge (source became target, or target was merged away)
        if already_merged_ids.contains(&target_cluster_id) || already_merged_ids.contains(&source_cluster_id) {
            debug!("Skipping merge between {} and {}: one or both already processed.", source_cluster_id, target_cluster_id);
            continue;
        }

        // Perform the merge: update all service_group records from source_cluster_id to target_cluster_id
        let update_result = transaction
            .execute(
                "UPDATE public.service_group
                 SET group_cluster_id = $1,
                     updated_at = $3
                 WHERE group_cluster_id = $2",
                &[&target_cluster_id, &source_cluster_id, &Utc::now()],
            )
            .await
            .context(format!("Failed to update service_group cluster assignments from {} to {}", source_cluster_id, target_cluster_id))?;

        if update_result > 0 {
            // Update target_cluster_id's service_count and service_group_count
            let new_counts = {
                // Corrected query for counts:
                let count_row_corrected = transaction.query_one(
                    "WITH services_in_target_cluster AS (
                        SELECT service_id_1 AS service_id FROM public.service_group WHERE group_cluster_id = $1
                        UNION
                        SELECT service_id_2 AS service_id FROM public.service_group WHERE group_cluster_id = $1
                    )
                    SELECT
                        (SELECT COUNT(*) FROM services_in_target_cluster) AS unique_service_count,
                        (SELECT COUNT(*) FROM public.service_group WHERE group_cluster_id = $1) AS group_count",
                    &[&target_cluster_id]
                ).await.context(format!("Failed to query corrected new counts for target cluster {}", target_cluster_id))?;

                (count_row_corrected.get::<_, i64>("unique_service_count") as i32, count_row_corrected.get::<_, i64>("group_count") as i32)
            };


            transaction
                .execute(
                    "UPDATE public.service_group_cluster
                     SET service_count = $1,
                         service_group_count = $2,
                         updated_at = $3
                     WHERE id = $4",
                    &[&new_counts.0, &new_counts.1, &Utc::now(), &target_cluster_id],
                )
                .await
                .context(format!("Failed to update service_group_cluster metadata for {}", target_cluster_id))?;

            // Mark source_cluster_id as inactive or delete it
            transaction
                .execute(
                    "DELETE FROM public.service_group_cluster WHERE id = $1",
                    &[&source_cluster_id],
                )
                .await
                .context(format!("Failed to remove merged service_group_cluster {}", source_cluster_id))?;

            // Log the merge
            transaction
                .execute(
                    "INSERT INTO clustering_metadata.cluster_merge_log
                     (id, source_cluster_id, target_cluster_id, similarity_score, pipeline_run_id, created_at, item_type)
                     VALUES ($1, $2, $3, $4, $5, $6, 'SERVICE_CLUSTER')", // Added item_type
                    &[
                        &Uuid::new_v4().to_string(),
                        &source_cluster_id,
                        &target_cluster_id,
                        &similarity,
                        &pipeline_run_id,
                        &Utc::now(),
                    ],
                )
                .await
                .context("Failed to log service cluster merge")?;

            already_merged_ids.insert(source_cluster_id.clone()); // Add the merged-away ID
            merged_count += 1;

            info!(
                "Merged service cluster {} into {} (similarity: {:.4}, {} service_group records updated)",
                source_cluster_id, target_cluster_id, similarity, update_result
            );
        } else {
            // If no records were updated, it might mean source_cluster_id was already empty or processed.
            // Still, attempt to delete it if it exists, to clean up.
            debug!("No service_group records found for source_cluster_id {} during merge attempt. Attempting to delete cluster record if it exists.", source_cluster_id);
             let del_res = transaction
                .execute(
                    "DELETE FROM public.service_group_cluster WHERE id = $1",
                    &[&source_cluster_id],
                )
                .await
                .context(format!("Failed to remove (potentially empty) merged service_group_cluster {}", source_cluster_id))?;
            if del_res > 0 {
                info!("Cleaned up empty or already processed source_cluster_id: {}", source_cluster_id);
                 already_merged_ids.insert(source_cluster_id.clone()); // Also mark as processed if deleted
            }
        }
    }

    Ok(merged_count)
}


/// Helper function to ensure consolidation tables exist
pub async fn ensure_consolidation_tables_exist(pool: &PgPool) -> Result<()> {
    let client = pool.get().await.context("Failed to get DB connection")?;

    // Added item_type to distinguish between entity and service cluster merges if using the same log table.
    let table_sql = "
        CREATE TABLE IF NOT EXISTS clustering_metadata.cluster_merge_log (
            id TEXT PRIMARY KEY,
            source_cluster_id TEXT NOT NULL,
            target_cluster_id TEXT NOT NULL,
            similarity_score DOUBLE PRECISION NOT NULL,
            pipeline_run_id TEXT NOT NULL,
            item_type TEXT DEFAULT 'ENTITY_CLUSTER', -- Added item_type
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_cluster_merge_log_pipeline_run
        ON clustering_metadata.cluster_merge_log(pipeline_run_id);
        
        CREATE INDEX IF NOT EXISTS idx_cluster_merge_log_created_at
        ON clustering_metadata.cluster_merge_log(created_at);

        CREATE INDEX IF NOT EXISTS idx_cluster_merge_log_item_type -- Index for new column
        ON clustering_metadata.cluster_merge_log(item_type);
    ";

    client
        .batch_execute(table_sql) // batch_execute for multiple statements
        .await
        .context("Failed to create/update consolidation tables (cluster_merge_log)")?;
    info!("Ensured cluster_merge_log table exists and is up-to-date.");
    Ok(())
}
