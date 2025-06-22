// src/bin/rl_retrofit.rs
use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{info, warn, error, debug};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use dedupe_lib::models::stats_models::MatchMethodType;
use dedupe_lib::rl::orchestrator::RLOrchestrator;
use dedupe_lib::rl::feature_cache::{create_shared_cache, SharedFeatureCache};
use dedupe_lib::utils::db_connect::{connect, PgPool};
use dedupe_lib::utils::env::load_env;

// Configuration constants
const BATCH_SIZE: usize = 500;
const FEATURE_EXTRACTION_CONCURRENCY: usize = 10;
const MAX_RETRY_ATTEMPTS: usize = 3;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct RetrofitArgs {
    /// Resume from specific group ID
    #[arg(long)]
    resume_from: Option<String>,
    
    /// Batch size for processing
    #[arg(long, default_value_t = 500)]
    batch_size: usize,
    
    /// Only process specific clusters
    #[arg(long)]
    cluster_ids: Option<Vec<String>>,
    
    /// Dry run mode (don't update database)
    #[arg(long)]
    dry_run: bool,
    
    /// Skip edge weight recalculation
    #[arg(long)]
    skip_edge_weights: bool,
    
    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

#[derive(Debug, Clone)]
struct RetrofitContext {
    total_groups: usize,
    processed_groups: usize,
    updated_groups: usize,
    failed_extractions: usize,
    clusters_updated: usize,
    start_time: Instant,
}

#[derive(Debug, Clone)]
struct EntityGroupRetrofitRecord {
    id: String,
    entity_id_1: String,
    entity_id_2: String,
    method_type: String,
    pre_rl_confidence_score: f64,
    current_confidence_score: f64,
    cluster_id: Option<String>,
}

#[derive(Debug)]
enum GroupUpdateResult {
    Success { group_id: String, new_confidence: f64 },
    Cached { group_id: String, confidence: f64 },
    Failed { group_id: String, error: String },
}

impl GroupUpdateResult {
    fn success(group_id: String, new_confidence: f64) -> Self {
        Self::Success { group_id, new_confidence }
    }
    
    fn cached(group_id: String, confidence: f64) -> Self {
        Self::Cached { group_id, confidence }
    }
    
    fn failed(group_id: String, error: String) -> Self {
        Self::Failed { group_id, error }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RetrofitCheckpoint {
    run_id: String,
    last_processed_group_id: String,
    total_groups_processed: usize,
    groups_updated: usize,
    clusters_updated: usize,
    failed_extractions: Vec<String>,
    started_at: chrono::NaiveDateTime,
    last_checkpoint: chrono::NaiveDateTime,
}

impl RetrofitCheckpoint {
    async fn save(&self, pool: &PgPool) -> Result<()> {
        let conn = pool.get().await.context("Failed to get DB connection for checkpoint save")?;
        
        let query = "
            INSERT INTO clustering_metadata.pipeline_run (id, run_timestamp, description, total_entities, total_groups, total_clusters, total_service_matches, entity_processing_time, matching_time, clustering_time, service_matching_time, total_processing_time, total_visualization_edges, visualization_edge_calculation_time, context_feature_extraction_time)
            VALUES ($1, $2, $3, $4, $5, $6, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0, 0.0)
            ON CONFLICT (id) DO UPDATE SET
                description = EXCLUDED.description,
                total_groups = EXCLUDED.total_groups,
                total_clusters = EXCLUDED.total_clusters
        ";
        
        let description = format!(
            "RL Retrofit Checkpoint: {}/{} groups processed, {} updated, {} clusters updated",
            self.total_groups_processed, self.total_groups_processed, self.groups_updated, self.clusters_updated
        );
        
        conn.execute(query, &[
            &self.run_id,
            &self.last_checkpoint,
            &description,
            &(self.total_groups_processed as i64),
            &(self.groups_updated as i64),
            &(self.clusters_updated as i64),
        ]).await.context("Failed to save checkpoint")?;
        
        Ok(())
    }
    
    async fn load(pool: &PgPool, run_id: &str) -> Result<Option<Self>> {
        let conn = pool.get().await.context("Failed to get DB connection for checkpoint load")?;
        
        let row_opt = conn.query_opt(
            "SELECT description FROM clustering_metadata.pipeline_run WHERE id = $1",
            &[&run_id]
        ).await.context("Failed to query for checkpoint")?;
        
        // For simplicity, we'll return None here since implementing full checkpoint
        // persistence would require additional schema changes
        Ok(None)
    }
}

struct RLRetrofitEngine {
    orchestrator: Arc<Mutex<RLOrchestrator>>,
    feature_cache: SharedFeatureCache,
    confidence_cache: Arc<Mutex<HashMap<String, f64>>>,
}

impl RLRetrofitEngine {
    async fn new(pool: &PgPool) -> Result<Self> {
        let orchestrator_instance = RLOrchestrator::new(pool).await
            .context("Failed to initialize RLOrchestrator")?;
        let orchestrator = Arc::new(Mutex::new(orchestrator_instance));
        let feature_cache = create_shared_cache();
        let confidence_cache = Arc::new(Mutex::new(HashMap::new()));
        
        Ok(Self {
            orchestrator,
            feature_cache,
            confidence_cache,
        })
    }
    
    async fn process_group_batch(
        &self,
        pool: &PgPool,
        batch: Vec<EntityGroupRetrofitRecord>,
        progress_callback: Option<&ProgressBar>,
    ) -> Result<Vec<GroupUpdateResult>> {
        let mut results = Vec::new();
        
        // Process in smaller concurrent chunks
        for chunk in batch.chunks(FEATURE_EXTRACTION_CONCURRENCY) {
            let futures: Vec<_> = chunk.iter().map(|group| {
                self.process_single_group(pool, group.clone())
            }).collect();
            
            let chunk_results = futures::future::join_all(futures).await;
            results.extend(chunk_results);
            
            if let Some(pb) = progress_callback {
                pb.inc(chunk.len() as u64);
            }
        }
        
        Ok(results)
    }
    
    async fn process_single_group(
        &self,
        pool: &PgPool,
        group: EntityGroupRetrofitRecord
    ) -> GroupUpdateResult {
        // 1. Check confidence cache first
        let cache_key = format!("{}:{}:{}", 
            group.entity_id_1, group.entity_id_2, group.method_type);
        
        if let Some(cached_confidence) = self.confidence_cache.lock().await.get(&cache_key) {
            return GroupUpdateResult::cached(group.id, *cached_confidence);
        }
        
        // 2. Extract 31-element feature vector
        let features = match self.orchestrator.lock().await
            .get_pair_features(pool, &group.entity_id_1, &group.entity_id_2).await {
            Ok(f) => f,
            Err(e) => return GroupUpdateResult::failed(group.id, e.to_string())
        };
        
        // 3. Apply RL tuning
        let method_type = MatchMethodType::from_str(&group.method_type);
        
        let tuned_confidence = match self.orchestrator.lock().await
            .get_tuned_confidence(&method_type, group.pre_rl_confidence_score, &features) {
            Ok(conf) => conf,
            Err(e) => return GroupUpdateResult::failed(group.id, e.to_string())
        };
        
        // 4. Cache result
        self.confidence_cache.lock().await.insert(cache_key, tuned_confidence);
        
        GroupUpdateResult::success(group.id, tuned_confidence)
    }
}

struct DatabaseUpdateEngine {
    pool: PgPool,
}

impl DatabaseUpdateEngine {
    fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    
    async fn batch_update_group_confidences(
        &self,
        updates: Vec<(String, f64)>, // (group_id, new_confidence)
        dry_run: bool,
    ) -> Result<usize> {
        if dry_run {
            info!("DRY RUN: Would update {} group confidences", updates.len());
            return Ok(updates.len());
        }
        
        if updates.is_empty() {
            return Ok(0);
        }
        
        let mut client = self.pool.get().await?;
        let transaction = client.transaction().await?;
        
        // Prepare batch update query using array operations for better performance
        let group_ids: Vec<String> = updates.iter().map(|(id, _)| id.clone()).collect();
        let confidences: Vec<f64> = updates.iter().map(|(_, conf)| *conf).collect();
        
        let query = "
            UPDATE public.entity_group 
            SET confidence_score = data_table.new_confidence,
                updated_at = CURRENT_TIMESTAMP
            FROM (
                SELECT unnest($1::text[]) as group_id, 
                       unnest($2::double precision[]) as new_confidence
            ) as data_table
            WHERE entity_group.id = data_table.group_id
        ";
        
        let rows_updated = transaction.execute(query, &[&group_ids, &confidences]).await?;
        transaction.commit().await?;
        
        Ok(rows_updated as usize)
    }
    
    async fn recalculate_cluster_edge_weights(
        &self,
        cluster_ids: HashSet<String>,
        dry_run: bool,
    ) -> Result<usize> {
        if dry_run {
            info!("DRY RUN: Would recalculate edge weights for {} clusters", cluster_ids.len());
            return Ok(cluster_ids.len());
        }
        
        let mut total_updated = 0;
        
        for cluster_id in cluster_ids {
            // 1. Load all entity_group records for this cluster
            let groups = self.load_cluster_groups(&cluster_id).await?;
            
            // 2. Rebuild the edge weight map using updated confidences
            let edge_updates = self.calculate_new_edge_weights(groups).await?;
            
            // 3. Batch update entity_edge_visualization
            let updated = self.batch_update_edge_weights(&cluster_id, edge_updates).await?;
            total_updated += updated;
        }
        
        Ok(total_updated)
    }
    
    async fn load_cluster_groups(&self, cluster_id: &str) -> Result<Vec<EntityGroupRecord>> {
        let conn = self.pool.get().await?;
        
        let query = "
            SELECT entity_id_1, entity_id_2, method_type, confidence_score
            FROM public.entity_group 
            WHERE group_cluster_id = $1
        ";
        
        let rows = conn.query(query, &[&cluster_id]).await?;
        let mut groups = Vec::new();
        
        for row in rows {
            groups.push(EntityGroupRecord {
                entity_id_1: row.get("entity_id_1"),
                entity_id_2: row.get("entity_id_2"),
                method_type: row.get("method_type"),
                confidence_score: row.get("confidence_score"),
            });
        }
        
        Ok(groups)
    }
    
    async fn calculate_new_edge_weights(
        &self,
        groups: Vec<EntityGroupRecord>
    ) -> Result<HashMap<(String, String), f64>> {
        // Implement the same sophisticated edge weight calculation
        // from entity_clustering.rs but using updated confidence scores
        let mut edge_map: HashMap<(String, String), Vec<(String, f64)>> = HashMap::new();
        
        for group in groups {
            let edge_key = if group.entity_id_1 < group.entity_id_2 {
                (group.entity_id_1, group.entity_id_2)
            } else {
                (group.entity_id_2, group.entity_id_1)
            };
            
            edge_map.entry(edge_key).or_default()
                .push((group.method_type, group.confidence_score));
        }
        
        let mut result = HashMap::new();
        for (edge_key, methods) in edge_map {
            let weight = calculate_sophisticated_edge_weight(&methods);
            result.insert(edge_key, weight);
        }
        
        Ok(result)
    }
    
    async fn batch_update_edge_weights(
        &self,
        cluster_id: &str,
        edge_updates: HashMap<(String, String), f64>
    ) -> Result<usize> {
        if edge_updates.is_empty() {
            return Ok(0);
        }
        
        let conn = self.pool.get().await?;
        let mut total_updated = 0;
        
        for ((entity_id_1, entity_id_2), new_weight) in edge_updates {
            let query = "
                UPDATE public.entity_edge_visualization 
                SET edge_weight = $1, updated_at = CURRENT_TIMESTAMP
                WHERE cluster_id = $2 
                  AND ((entity_id_1 = $3 AND entity_id_2 = $4) 
                    OR (entity_id_1 = $4 AND entity_id_2 = $3))
            ";
            
            let rows = conn.execute(query, &[
                &new_weight,
                &cluster_id,
                &entity_id_1,
                &entity_id_2,
            ]).await?;
            
            total_updated += rows as usize;
        }
        
        Ok(total_updated)
    }
}

#[derive(Debug)]
struct EntityGroupRecord {
    entity_id_1: String,
    entity_id_2: String,
    method_type: String,
    confidence_score: f64,
}

async fn load_groups_for_retrofit(
    pool: &PgPool,
    resume_from_id: Option<String>,
    cluster_filter: Option<Vec<String>>,
) -> Result<Vec<EntityGroupRetrofitRecord>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;
    
    let mut query = "
        SELECT id, entity_id_1, entity_id_2, method_type, 
               pre_rl_confidence_score, confidence_score, group_cluster_id
        FROM public.entity_group 
        WHERE pre_rl_confidence_score IS NOT NULL
    ".to_string();
    
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    let mut param_index = 1;
    
    if let Some(id) = resume_from_id {
        query.push_str(&format!(" AND id > ${}", param_index));
        params.push(Box::new(id));
        param_index += 1;
    }
    
    if let Some(clusters) = cluster_filter {
        query.push_str(&format!(" AND group_cluster_id = ANY(${})", param_index));
        params.push(Box::new(clusters));
    }
    
    query.push_str(" ORDER BY id");
    
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
    params.iter().map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
    
    let rows = conn.query(&query, &param_refs).await
        .context("Failed to query entity groups for retrofit")?;
    
    let mut records = Vec::new();
    for row in rows {
        records.push(EntityGroupRetrofitRecord {
            id: row.get("id"),
            entity_id_1: row.get("entity_id_1"),
            entity_id_2: row.get("entity_id_2"),
            method_type: row.get("method_type"),
            pre_rl_confidence_score: row.get("pre_rl_confidence_score"),
            current_confidence_score: row.get::<_, Option<f64>>("confidence_score").unwrap_or(0.0),
            cluster_id: row.get("group_cluster_id"),
        });
    }
    
    Ok(records)
}

async fn identify_clusters_needing_update(
    updated_group_ids: &[String],
    all_groups: &[EntityGroupRetrofitRecord],
) -> HashSet<String> {
    let updated_set: HashSet<&String> = updated_group_ids.iter().collect();
    
    all_groups.iter()
        .filter(|group| updated_set.contains(&group.id))
        .filter_map(|group| group.cluster_id.clone())
        .collect()
}

fn create_retrofit_progress_tracker(
    total_groups: usize,
    multi_progress: Option<MultiProgress>
) -> Option<ProgressBar> {
    if let Some(mp) = multi_progress {
        let pb = mp.add(ProgressBar::new(total_groups as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("🔧 [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Starting RL retrofit...");
        Some(pb)
    } else {
        None
    }
}

// Import the sophisticated edge weight calculation from entity_clustering.rs
fn calculate_sophisticated_edge_weight(contributing_methods: &[(String, f64)]) -> f64 {
    if contributing_methods.is_empty() {
        return 0.0;
    }

    let mut product_of_disbeliefs = 1.0;
    const LOW_CONF_THRESHOLD: f64 = 0.5;
    const LOW_CONF_SCALING_FACTOR: f64 = 0.2;
    const HIGH_CONF_POWER: f64 = 1.5;
    const EPSILON: f64 = 1e-9;

    for &(_, confidence) in contributing_methods {
        let clamped_confidence = confidence.max(0.0).min(1.0);

        let transformed_c = if clamped_confidence < LOW_CONF_THRESHOLD {
            clamped_confidence * LOW_CONF_SCALING_FACTOR
        } else {
            clamped_confidence.powf(HIGH_CONF_POWER)
        };

        let disbelief = (1.0 - transformed_c).max(EPSILON);
        product_of_disbeliefs *= disbelief;
    }

    (1.0 - product_of_disbeliefs).max(0.0).min(1.0)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging and environment
    env_logger::init();
    load_env();
    
    let args = RetrofitArgs::parse();
    
    if args.verbose {
        log::set_max_level(log::LevelFilter::Debug);
    }
    
    let pool = connect().await?;
    let run_id = Uuid::new_v4().to_string();
    let start_time = Instant::now();
    
    // Initialize progress tracking
    let multi_progress = if !args.verbose {
        Some(MultiProgress::new())
    } else {
        None
    };
    
    // Check for existing checkpoint
    let checkpoint = RetrofitCheckpoint::load(&pool, &run_id).await?;
    let resume_from = checkpoint.as_ref().map(|c| c.last_processed_group_id.clone());
    
    info!("🚀 Starting RL retrofit{}", 
        if resume_from.is_some() { " (resuming from checkpoint)" } else { "" });
    
    if args.dry_run {
        warn!("🔍 DRY RUN MODE: No database changes will be made");
    }
    
    // Load groups to process
    let groups = load_groups_for_retrofit(&pool, resume_from, args.cluster_ids.clone()).await?;
    let total_groups = groups.len();
    
    info!("📊 Loaded {} entity groups for RL retrofit", total_groups);
    
    if total_groups == 0 {
        info!("✅ No groups to process. Exiting.");
        return Ok(());
    }
    
    // Initialize engines
    let rl_engine = RLRetrofitEngine::new(&pool).await?;
    let db_engine = DatabaseUpdateEngine::new(pool.clone());
    
    // Create progress tracker
    let progress_pb = create_retrofit_progress_tracker(total_groups, multi_progress.clone());
    
    // Process in batches
    let mut processed = 0;
    let mut updated_groups = Vec::new();
    let mut affected_clusters = HashSet::new();
    let batch_size = args.batch_size;
    
    info!("🔄 Processing {} groups in batches of {}", total_groups, batch_size);
    
    for batch in groups.chunks(batch_size) {
        info!("Processing batch of {} groups...", batch.len());
        
        // Process batch with RL tuning
        let results = rl_engine.process_group_batch(
            &pool, 
            batch.to_vec(),
            progress_pb.as_ref(),
        ).await?;
        
        // Collect successful updates
        let mut batch_updates = Vec::new();
        for result in results {
            match result {
                GroupUpdateResult::Success { group_id, new_confidence } |
                GroupUpdateResult::Cached { group_id, confidence: new_confidence } => {
                    batch_updates.push((group_id.clone(), new_confidence));
                    if let Some(group) = batch.iter().find(|g| g.id == group_id) {
                        if let Some(cluster_id) = &group.cluster_id {
                            affected_clusters.insert(cluster_id.clone());
                        }
                    }
                }
                GroupUpdateResult::Failed { group_id, error } => {
                    warn!("Failed to process group {}: {}", group_id, error);
                }
            }
        }
        
        // Batch update database
        if !batch_updates.is_empty() {
            let updated_count = db_engine.batch_update_group_confidences(batch_updates, args.dry_run).await?;
            updated_groups.extend(batch.iter().map(|g| g.id.clone()));
            
            info!("✅ Updated {} group confidences in batch", updated_count);
        }
        
        processed += batch.len();
        
        // Update progress
        if let Some(pb) = &progress_pb {
            pb.set_position(processed as u64);
            pb.set_message(format!(
                "Processed: {}/{}, Clusters affected: {}", 
                processed, total_groups, affected_clusters.len()
            ));
        }
        
        // Save checkpoint periodically
        if processed % (batch_size * 10) == 0 {
            let checkpoint = RetrofitCheckpoint {
                run_id: run_id.clone(),
                last_processed_group_id: batch.last().unwrap().id.clone(),
                total_groups_processed: processed,
                groups_updated: updated_groups.len(),
                clusters_updated: 0,
                failed_extractions: Vec::new(),
                started_at: Utc::now().naive_utc(),
                last_checkpoint: Utc::now().naive_utc(),
            };
            
            if let Err(e) = checkpoint.save(&pool).await {
                warn!("Failed to save checkpoint: {}", e);
            }
        }
    }
    
    // Phase 2: Recalculate cluster edge weights
    let clusters_updated = if !args.skip_edge_weights && !affected_clusters.is_empty() {
        info!("🔄 Recalculating edge weights for {} affected clusters", affected_clusters.len());
        db_engine.recalculate_cluster_edge_weights(affected_clusters, args.dry_run).await?
    } else {
        0
    };
    
    let total_duration = start_time.elapsed();
    
    // Final summary
    if let Some(pb) = &progress_pb {
        pb.finish_with_message(format!(
            "✅ RL retrofit complete: {} groups updated, {} clusters recalculated", 
            updated_groups.len(), clusters_updated
        ));
    }
    
    info!("🎉 RL retrofit completed successfully!");
    info!("📈 Groups updated: {}/{}", updated_groups.len(), total_groups);
    info!("🔗 Clusters recalculated: {}", clusters_updated);
    info!("⏱️  Total time: {:.2?}", total_duration);
    
    if args.dry_run {
        info!("🔍 DRY RUN completed - no actual changes were made");
    }
    
    Ok(())
}