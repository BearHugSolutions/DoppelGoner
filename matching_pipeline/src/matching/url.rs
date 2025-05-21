// src/matching/url.rs - Fully refactored for feature caching
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::future::{self, try_join_all};
use log::{debug, error, info, trace, warn};
use lru::LruCache;
use num_cpus;
use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZero;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::sleep;
use url::Url as StdUrl;
use uuid::Uuid;

use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType, EntityGroupId, EntityId, MatchMethodType, MatchValues, NewSuggestedAction,
    SuggestionStatus, UrlMatchValue,
};
use crate::reinforcement::entity::feature_cache_service::SharedFeatureCache;
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, MatchMethodStats, UrlMatchResult};
use serde_json;

// Confidence score tiers (unchanged)
const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;

// Batch parameters - adjusted for better performance
const LARGE_DOMAIN_BATCH_SIZE: usize = 50;
const MEDIUM_DOMAIN_BATCH_SIZE: usize = 100;
const SMALL_DOMAIN_BATCH_SIZE: usize = 200;
const MAX_PAIRS_TO_PROCESS: usize = 10000;

// Parallelization settings
const DEFAULT_CONCURRENT_DOMAINS: usize = 8; // Default to 8 concurrent domains, can be overridden
const FEATURE_CACHE_SIZE: usize = 10000; // LRU cache size for feature vectors

// Domain size categories
const LARGE_DOMAIN_THRESHOLD: usize = 100;
const MEDIUM_DOMAIN_THRESHOLD: usize = 20;

// Geospatial filter settings - Only consider entities within this distance
const MAX_LOCATION_DISTANCE_METERS: f64 = 2000.0; // 2km

// High-volume domains that require special handling (unchanged)
const HIGH_VOLUME_DOMAINS: [&str; 10] = [
    "kingcounty.gov",
    "seamar.org",
    "ccsww.org",
    "providence.org",
    "plannedparenthood.org",
    "seattle.gov",
    "vmfh.org",
    "uwmedicine.org",
    "extension.wsu.edu",
    "neighborcare.org",
];

// Domain types that should use location-aware matching (unchanged)
const LOCATION_SENSITIVE_DOMAINS: [&str; 5] = [".gov", ".org", ".edu", ".us", ".net"];

// SQL query for inserting into entity_group (unchanged)
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING";

// SQL for creating the domain processing status table
const CREATE_DOMAIN_STATUS_TABLE_SQL_TABLE: &str = "
CREATE TABLE IF NOT EXISTS clustering_metadata.domain_processing_status (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    domain TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    status TEXT NOT NULL, -- 'pending', 'in_progress', 'completed', 'error'
    total_entities INTEGER NOT NULL,
    processed_entity_pairs INTEGER NOT NULL DEFAULT 0,
    last_processed_entity_id TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    UNIQUE (domain, pipeline_run_id)
);";
const CREATE_DOMAIN_STATUS_TABLE_SQL_INDEX: &str = "
CREATE INDEX IF NOT EXISTS idx_domain_processing_pipeline_run
ON clustering_metadata.domain_processing_status(pipeline_run_id);
";

// Cached data for URL normalization (unchanged)
#[derive(Clone)]
struct NormalizedUrlData {
    domain: String,
    path_slugs: Vec<String>,
    original_url: String,
    domain_type: DomainType,
}

// Domain categorization for specialized handling (unchanged)
#[derive(Clone, PartialEq, Debug)]
enum DomainType {
    Government,
    Healthcare,
    Education,
    Commercial,
    Social,
    Other,
}

// Structure to hold entity information for processing (unchanged)
#[derive(Clone)]
struct EntityUrlInfo {
    entity_id: EntityId,
    original_url: String,
    normalized_data: NormalizedUrlData,
    entity_name: Option<String>,
    has_location_data: bool,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

// Structure for locationless URL matching (unchanged)
struct DomainEntityMap<'a> {
    domain: String,
    entity_urls: Vec<&'a EntityUrlInfo>,
}

// Structure for domain processing status
#[derive(Debug, Clone)]
struct DomainProcessingStatus {
    domain: String,
    status: String,
    total_entities: usize,
    processed_entity_pairs: usize,
    last_processed_entity_id: Option<String>,
}

// Helper structure for spatial binning
#[derive(Debug, Eq, PartialEq, Hash)]
struct SpatialBin {
    x: i32,
    y: i32,
}

// Task item for domain worker queue
struct DomainTask {
    domain: String,
    entities: Vec<EntityUrlInfo>,
    domain_size_category: String, // "large", "medium", "small"
    status_id: String,
}

impl DomainTask {
    fn get_batch_size(&self) -> usize {
        match self.domain_size_category.as_str() {
            "large" => LARGE_DOMAIN_BATCH_SIZE,
            "medium" => MEDIUM_DOMAIN_BATCH_SIZE,
            _ => SMALL_DOMAIN_BATCH_SIZE,
        }
    }
}

// Main entry point for URL matching (updated to use feature cache)
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Added feature_cache parameter
) -> Result<AnyMatchResult> {
    let start_time = Instant::now();
    info!(
        "Starting optimized URL matching (run ID: {}){}...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with ML guidance"
        } else {
            ""
        }
    );

    // Ensure domain processing status table exists
    ensure_domain_status_table_exists(pool).await?;

    // Step 1: Fetch existing pairs to avoid reprocessing
    let existing_processed_pairs = fetch_existing_pairs(pool).await?;
    info!(
        "URL: Found {} existing URL-matched pairs to skip.",
        existing_processed_pairs.len()
    );

    // Step 2: Fetch entity info with URLs and location data
    let entities_with_urls = fetch_entities_with_urls_and_locations(pool).await?;
    info!(
        "URL: Found {} entities with URLs for processing.",
        entities_with_urls.len()
    );

    // Create domain statistics for logging
    let mut domain_stats: HashMap<String, usize> = HashMap::new();
    for entity in &entities_with_urls {
        *domain_stats
            .entry(entity.normalized_data.domain.clone())
            .or_insert(0) += 1;
    }

    // Log top domains for monitoring
    let mut domain_count_vec: Vec<_> = domain_stats.iter().collect();
    domain_count_vec.sort_by(|a, b| b.1.cmp(a.1));
    info!("URL: Top 10 domains by entity count:");
    for (idx, (domain, count)) in domain_count_vec.iter().take(10).enumerate() {
        info!("URL: #{}: {} - {} entities", idx + 1, domain, count);
    }

    // Create domain tasks and update status
    let domain_entities_map = group_entities_by_domain(&entities_with_urls);

    // Initialize task queues for different domain sizes
    let large_domain_queue = Arc::new(Mutex::new(VecDeque::new()));
    let medium_domain_queue = Arc::new(Mutex::new(VecDeque::new()));
    let small_domain_queue = Arc::new(Mutex::new(VecDeque::new()));

    // Create a global state for tracking progress
    let completed_domains = Arc::new(AtomicUsize::new(0));
    let total_domains = domain_entities_map.len();
    let new_pairs = Arc::new(Mutex::new(Vec::new()));
    let entities_in_new_pairs = Arc::new(Mutex::new(HashSet::new()));
    let confidence_scores = Arc::new(Mutex::new(Vec::new()));

    // Check for domains that have already been processed in this run
    let processed_domains = fetch_processed_domains(pool, pipeline_run_id).await?;
    info!(
        "URL: Found {} domains already processed in this run",
        processed_domains.len()
    );

    // Create tasks for unprocessed domains
    for (domain, entities) in domain_entities_map {
        if processed_domains.contains(&domain) {
            info!("URL: Skipping already processed domain: {}", domain);
            continue;
        }

        if entities.len() < 2 {
            continue; // Skip domains with only one entity
        }

        // Insert domain status
        let status_id =
            insert_domain_status(pool, &domain, pipeline_run_id, entities.len()).await?;

        // Calculate domain size category before moving entities
        let entities_len = entities.len();
        let domain_size_category = if entities_len > LARGE_DOMAIN_THRESHOLD {
            "large".to_string()
        } else if entities_len > MEDIUM_DOMAIN_THRESHOLD {
            "medium".to_string()
        } else {
            "small".to_string()
        };

        // Now create task with the pre-calculated category
        let task = DomainTask {
            domain: domain.clone(),
            entities,
            domain_size_category,
            status_id,
        };

        // Add to appropriate queue
        if task.domain_size_category == "large" {
            large_domain_queue.lock().await.push_back(task);
        } else if task.domain_size_category == "medium" {
            medium_domain_queue.lock().await.push_back(task);
        } else {
            small_domain_queue.lock().await.push_back(task);
        }
    }

    // Determine optimal concurrency based on system resources
    let cpu_count = num_cpus::get();
    let concurrent_domains =
        std::cmp::min(DEFAULT_CONCURRENT_DOMAINS, std::cmp::max(1, cpu_count - 2));

    // Create semaphores to limit concurrent domain processing
    let large_domain_semaphore = Arc::new(Semaphore::new(1)); // Only process 1 large domain at a time
    let medium_domain_semaphore = Arc::new(Semaphore::new(2)); // Process 2 medium domains concurrently
    let small_domain_semaphore = Arc::new(Semaphore::new(concurrent_domains)); // Use remaining concurrency for small domains

    // Track domain workers
    let mut domain_workers = Vec::new();

    // Process large domains
    {
        let large_queue = large_domain_queue.clone();
        let semaphore = large_domain_semaphore.clone();
        let pool_clone = pool.clone();
        let rl_clone = reinforcement_orchestrator_option.clone();
        let run_id = pipeline_run_id.to_string();
        let completed = completed_domains.clone();
        let pairs = new_pairs.clone();
        let entities = entities_in_new_pairs.clone();
        let scores = confidence_scores.clone();
        let existing = existing_processed_pairs.clone();
        let feature_cache_clone = feature_cache.clone();

        let worker = tokio::spawn(async move {
            process_domain_queue(
                "large",
                large_queue,
                semaphore,
                pool_clone,
                rl_clone,
                run_id,
                completed,
                total_domains,
                pairs,
                entities,
                scores,
                existing,
                feature_cache_clone,
            )
            .await
        });

        domain_workers.push(worker);
    }

    // Process medium domains
    {
        let medium_queue = medium_domain_queue.clone();
        let semaphore = medium_domain_semaphore.clone();
        let pool_clone = pool.clone();
        let rl_clone = reinforcement_orchestrator_option.clone();
        let run_id = pipeline_run_id.to_string();
        let completed = completed_domains.clone();
        let pairs = new_pairs.clone();
        let entities = entities_in_new_pairs.clone();
        let scores = confidence_scores.clone();
        let existing = existing_processed_pairs.clone();
        let feature_cache_clone = feature_cache.clone();

        let worker = tokio::spawn(async move {
            process_domain_queue(
                "medium",
                medium_queue,
                semaphore,
                pool_clone,
                rl_clone,
                run_id,
                completed,
                total_domains,
                pairs,
                entities,
                scores,
                existing,
                feature_cache_clone,
            )
            .await
        });

        domain_workers.push(worker);
    }

    // Process small domains
    {
        let small_queue = small_domain_queue.clone();
        let semaphore = small_domain_semaphore.clone();
        let pool_clone = pool.clone();
        let rl_clone = reinforcement_orchestrator_option.clone();
        let run_id = pipeline_run_id.to_string();
        let completed = completed_domains.clone();
        let pairs = new_pairs.clone();
        let entities = entities_in_new_pairs.clone();
        let scores = confidence_scores.clone();
        let existing = existing_processed_pairs.clone();
        let feature_cache_clone = feature_cache.clone();

        let worker = tokio::spawn(async move {
            process_domain_queue(
                "small",
                small_queue,
                semaphore,
                pool_clone,
                rl_clone,
                run_id,
                completed,
                total_domains,
                pairs,
                entities,
                scores,
                existing,
                feature_cache_clone,
            )
            .await
        });

        domain_workers.push(worker);
    }

    // Wait for all domain processing to complete
    for worker in domain_workers {
        if let Err(e) = worker.await {
            warn!("URL: Domain worker failed: {}", e);
        }
    }

    // Calculate stats
    let new_pairs = new_pairs.lock().await;
    let entities_in_new_pairs = entities_in_new_pairs.lock().await;
    let confidence_scores = confidence_scores.lock().await;

    let total_new_pairs = new_pairs.len();
    let avg_confidence = if !confidence_scores.is_empty() {
        confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Url,
        groups_created: total_new_pairs,
        entities_matched: entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if total_new_pairs > 0 { 2.0 } else { 0.0 },
    };

    let elapsed = start_time.elapsed();
    info!(
        "URL matching complete in {:.2?}: created {} new pairs with avg confidence {:.4}",
        elapsed, total_new_pairs, avg_confidence
    );

    Ok(AnyMatchResult::Url(UrlMatchResult {
        groups_created: total_new_pairs,
        stats: method_stats,
    }))
}

// Main worker function for processing domain queues - Updated to use feature cache
async fn process_domain_queue(
    category: &str,
    queue: Arc<Mutex<VecDeque<DomainTask>>>,
    semaphore: Arc<Semaphore>,
    pool: PgPool,
    reinforcement_orchestrator: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: String,
    completed_domains: Arc<AtomicUsize>,
    total_domains: usize,
    new_pairs: Arc<Mutex<Vec<(EntityId, EntityId)>>>,
    entities_in_new_pairs: Arc<Mutex<HashSet<EntityId>>>,
    confidence_scores: Arc<Mutex<Vec<f64>>>,
    existing_processed_pairs: HashSet<(EntityId, EntityId)>,
    feature_cache: Option<SharedFeatureCache>, // Added feature_cache parameter
) -> Result<()> {
    loop {
        // Get next task
        let task_opt = {
            let mut queue_guard = queue.lock().await;
            queue_guard.pop_front()
        };

        let Some(task) = task_opt else {
            // No more tasks in queue
            break;
        };

        // Acquire semaphore
        let _permit = semaphore.acquire().await?;

        info!(
            "URL: Processing {} domain: {} with {} entities",
            category,
            task.domain,
            task.entities.len()
        );

        // Update domain status to in_progress
        update_domain_status(&pool, &task.status_id, "in_progress", None, None, None).await?;

        // Process domain
        let domain_start = Instant::now();
        let result = match process_domain_task(
            &task,
            &pool,
            reinforcement_orchestrator.as_ref(),
            &pipeline_run_id,
            &existing_processed_pairs,
            feature_cache.as_ref(), // Pass the feature cache
        )
        .await
        {
            Ok(pairs) => {
                // Update local state with results
                let mut pairs_guard = new_pairs.lock().await;
                let mut entities_guard = entities_in_new_pairs.lock().await;
                let mut scores_guard = confidence_scores.lock().await;

                let mut pair_count = 0;

                for (entity_id_1, entity_id_2, conf) in pairs {
                    pairs_guard.push((entity_id_1.clone(), entity_id_2.clone()));
                    entities_guard.insert(entity_id_1);
                    entities_guard.insert(entity_id_2);
                    scores_guard.push(conf);
                    pair_count += 1;
                }

                // Update domain status to completed
                update_domain_status(
                    &pool,
                    &task.status_id,
                    "completed",
                    Some(task.entities.len()),
                    Some(pair_count),
                    None,
                )
                .await?;

                Ok(pair_count)
            }
            Err(e) => {
                // Update domain status to error
                update_domain_status(
                    &pool,
                    &task.status_id,
                    "error",
                    None,
                    None,
                    Some(&e.to_string()),
                )
                .await?;

                Err(e)
            }
        };

        // Report progress
        let completed = completed_domains.fetch_add(1, Ordering::SeqCst) + 1;

        match result {
            Ok(pairs_created) => {
                info!(
                    "URL: Completed {} domain: {} in {:.2?}. Created {} pairs. Progress: [{}/{}]",
                    category,
                    task.domain,
                    domain_start.elapsed(),
                    pairs_created,
                    completed,
                    total_domains
                );
            }
            Err(e) => {
                warn!(
                    "URL: Failed to process {} domain: {}. Error: {}. Progress: [{}/{}]",
                    category, task.domain, e, completed, total_domains
                );
            }
        }
    }

    Ok(())
}

// Process a single domain task with optimized algorithm - Updated to use feature cache
async fn process_domain_task(
    task: &DomainTask,
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    existing_processed_pairs: &HashSet<(EntityId, EntityId)>,
    feature_cache: Option<&SharedFeatureCache>, // Added feature_cache parameter
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    let domain = task.domain.clone();
    let entities_owned = task.entities.clone();
    let batch_size = task.get_batch_size();

    // Convert for handling
    let entities: Vec<&EntityUrlInfo> = entities_owned.iter().collect();

    // Use appropriate processing strategy based on domain characteristics
    let is_high_volume = HIGH_VOLUME_DOMAINS.iter().any(|d| *d == domain);
    let is_location_sensitive = LOCATION_SENSITIVE_DOMAINS
        .iter()
        .any(|suffix| domain.ends_with(suffix));

    let mut new_pairs = Vec::new();

    if is_high_volume || entities.len() > LARGE_DOMAIN_THRESHOLD || is_location_sensitive {
        // Use spatial binning for large or location-sensitive domains
        process_domain_with_spatial_binning(
            &domain,
            &entities,
            existing_processed_pairs,
            pool,
            reinforcement_orchestrator,
            pipeline_run_id,
            feature_cache,
            &mut new_pairs,
        )
        .await?;
    } else {
        // Use batch processing for regular domains
        process_domain_in_batches(
            &domain,
            &entities,
            batch_size,
            existing_processed_pairs,
            pool,
            reinforcement_orchestrator,
            pipeline_run_id,
            feature_cache,
            &mut new_pairs,
        )
        .await?;
    }

    Ok(new_pairs)
}

// Process a domain with spatial binning for large/location-sensitive domains - Updated to use feature cache
async fn process_domain_with_spatial_binning(
    domain: &str,
    entities: &[&EntityUrlInfo],
    existing_processed_pairs: &HashSet<(EntityId, EntityId)>,
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>, // Added feature_cache parameter
    new_pairs: &mut Vec<(EntityId, EntityId, f64)>,
) -> Result<()> {
    // Separate entities with and without location data
    let mut entities_with_location: Vec<&EntityUrlInfo> = Vec::new();
    let mut entities_without_location: Vec<&EntityUrlInfo> = Vec::new();

    for entity in entities {
        if entity.has_location_data {
            entities_with_location.push(*entity);
        } else {
            entities_without_location.push(*entity);
        }
    }

    // Create spatial bins for entities with location data
    let mut spatial_bins: HashMap<SpatialBin, Vec<&EntityUrlInfo>> = HashMap::new();

    // Bin size in degrees (approximate conversion - 111km per degree)
    let bin_size_deg = MAX_LOCATION_DISTANCE_METERS / 111000.0;

    for entity in &entities_with_location {
        if !entity.has_location_data {
            continue;
        }

        let lat = entity.latitude.unwrap();
        let lon = entity.longitude.unwrap();

        let bin_x = (lon / bin_size_deg).floor() as i32;
        let bin_y = (lat / bin_size_deg).floor() as i32;

        let bin = SpatialBin { x: bin_x, y: bin_y };
        spatial_bins.entry(bin).or_default().push(*entity);
    }

    // Process each spatial bin
    for (bin, bin_entities) in &spatial_bins {
        // Also include entities from neighboring bins
        let mut combined_entities = bin_entities.clone();

        // Add neighboring bins
        for dx in -1..=1 {
            for dy in -1..=1 {
                if dx == 0 && dy == 0 {
                    continue; // Skip the current bin
                }

                let neighbor_bin = SpatialBin {
                    x: bin.x + dx,
                    y: bin.y + dy,
                };

                if let Some(neighbor_entities) = spatial_bins.get(&neighbor_bin) {
                    combined_entities.extend(neighbor_entities);
                }
            }
        }

        // Remove duplicates
        combined_entities.sort_by_key(|e| e.entity_id.0.clone());
        combined_entities.dedup_by_key(|e| e.entity_id.0.clone());

        // Process entities in this bin
        process_entity_batch(
            domain,
            &combined_entities,
            existing_processed_pairs,
            pool,
            reinforcement_orchestrator,
            pipeline_run_id,
            feature_cache,
            new_pairs,
        )
        .await?;
    }

    // Process entities without location data
    if !entities_without_location.is_empty() {
        // 1. Match against entities with location (if any)
        for entity1 in &entities_without_location {
            // Take samples from each bin to compare with
            let mut sample_entities = Vec::new();
            for (_, bin_entities) in &spatial_bins {
                // Take up to 10 samples from each bin
                for entity in bin_entities.iter().take(10) {
                    sample_entities.push(*entity);
                }
            }

            // Process this entity against the samples
            for entity2 in &sample_entities {
                // Skip if already processed
                let pair = if entity1.entity_id.0 < entity2.entity_id.0 {
                    (entity1.entity_id.clone(), entity2.entity_id.clone())
                } else {
                    (entity2.entity_id.clone(), entity1.entity_id.clone())
                };

                if existing_processed_pairs.contains(&pair) {
                    continue;
                }

                // Process potential match
                process_potential_match(
                    *entity1,
                    *entity2,
                    pool,
                    reinforcement_orchestrator,
                    pipeline_run_id,
                    feature_cache,
                    new_pairs,
                )
                .await?;
            }
        }

        // 2. Match entities without location against each other
        // Process in reasonable-sized batches
        let batch_size = 100;
        for i in 0..entities_without_location.len() {
            let end = std::cmp::min(i + batch_size, entities_without_location.len());
            let batch = &entities_without_location[i..end];

            process_entity_batch(
                domain,
                batch,
                existing_processed_pairs,
                pool,
                reinforcement_orchestrator,
                pipeline_run_id,
                feature_cache,
                new_pairs,
            )
            .await?;
        }
    }

    Ok(())
}

// Process a domain in batches for regular domains - Updated to use feature cache
async fn process_domain_in_batches(
    domain: &str,
    entities: &[&EntityUrlInfo],
    batch_size: usize,
    existing_processed_pairs: &HashSet<(EntityId, EntityId)>,
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>, // Added feature_cache parameter
    new_pairs: &mut Vec<(EntityId, EntityId, f64)>,
) -> Result<()> {
    // Process in batches
    for i in 0..entities.len() {
        let end = std::cmp::min(i + batch_size, entities.len());
        let batch = &entities[i..end];

        process_entity_batch(
            domain,
            batch,
            existing_processed_pairs,
            pool,
            reinforcement_orchestrator,
            pipeline_run_id,
            feature_cache,
            new_pairs,
        )
        .await?;
    }

    Ok(())
}

// Process a batch of entities for potential matches - Updated to use feature cache
async fn process_entity_batch(
    domain: &str,
    entities: &[&EntityUrlInfo],
    existing_processed_pairs: &HashSet<(EntityId, EntityId)>,
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>, // Added feature_cache parameter
    new_pairs: &mut Vec<(EntityId, EntityId, f64)>,
) -> Result<()> {
    // Collect all potential matches
    let mut batch_pairs = Vec::new();

    // Check all pairs in batch
    for (i, entity1) in entities.iter().enumerate() {
        for entity2 in entities.iter().skip(i + 1) {
            // Skip if same entity (same ID)
            if entity1.entity_id.0 == entity2.entity_id.0 {
                continue;
            }
            // Skip if already processed
            let pair = if entity1.entity_id.0 < entity2.entity_id.0 {
                (entity1.entity_id.clone(), entity2.entity_id.clone())
            } else {
                (entity2.entity_id.clone(), entity1.entity_id.clone())
            };

            if existing_processed_pairs.contains(&pair) {
                continue;
            }

            // Pre-check to see if worth comparing
            if !should_compare_entities(*entity1, *entity2, domain) {
                continue;
            }

            batch_pairs.push((*entity1, *entity2));
        }
    }

    // Process all collected pairs
    if batch_pairs.is_empty() {
        return Ok(());
    }

    // Create batches for database operations
    let mut entity_groups = Vec::new();

    // Process each potential match
    for (entity1, entity2) in batch_pairs {
        // Process potential match
        if let Some((entity_id_1, entity_id_2, confidence)) = process_potential_match(
            entity1,
            entity2,
            pool,
            reinforcement_orchestrator,
            pipeline_run_id,
            feature_cache,
            new_pairs,
        )
        .await?
        {
            // Add to results
            new_pairs.push((entity_id_1, entity_id_2, confidence));
        }
    }

    // Batch insert entity groups if any
    if !entity_groups.is_empty() {
        batch_insert_entity_groups(pool, &entity_groups).await?;
    }

    Ok(())
}

// Process a potential match between two entities - Updated to use feature cache
async fn process_potential_match(
    entity1: &EntityUrlInfo,
    entity2: &EntityUrlInfo,
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>, // Added feature_cache parameter
    new_pairs: &mut Vec<(EntityId, EntityId, f64)>,
) -> Result<Option<(EntityId, EntityId, f64)>> {
    // Skip if same entity
    if entity1.entity_id.0 == entity2.entity_id.0 {
        return Ok(None);
    }

    // Skip if different domains (shouldn't happen with our grouping)
    if entity1.normalized_data.domain != entity2.normalized_data.domain {
        return Ok(None);
    }

    // Calculate distance if both have location data
    let distance = if entity1.has_location_data && entity2.has_location_data {
        Some(calculate_distance(
            entity1.latitude.unwrap(),
            entity1.longitude.unwrap(),
            entity2.latitude.unwrap(),
            entity2.longitude.unwrap(),
        ))
    } else {
        None
    };

    // Skip if distance exceeds threshold, unless they have matching path segments
    if let Some(dist) = distance {
        let should_skip_by_distance = dist > MAX_LOCATION_DISTANCE_METERS;
        let matching_slugs = count_matching_slugs(
            &entity1.normalized_data.path_slugs,
            &entity2.normalized_data.path_slugs,
        );

        // Different locations but similar paths - might be related
        if should_skip_by_distance && matching_slugs < 2 {
            return Ok(None);
        }

        // Different paths but close locations - also might be related
        if matching_slugs == 0
            && domain_requires_path_matching(&entity1.normalized_data.domain)
            && dist > 500.0
        {
            return Ok(None);
        }
    }

    // Count matching slugs
    let matching_slugs = count_matching_slugs(
        &entity1.normalized_data.path_slugs,
        &entity2.normalized_data.path_slugs,
    );

    // For location-less entities with domain requiring path matching
    if distance.is_none()
        && domain_requires_path_matching(&entity1.normalized_data.domain)
        && matching_slugs < 1
    {
        return Ok(None);
    }

    // Calculate confidence
    let (confidence_score, match_values) =
        calculate_url_match_confidence(entity1, entity2, matching_slugs, distance);

    // Apply RL tuning if available, using cache if available
    let mut final_confidence_score = confidence_score;
    let mut features_for_snapshot: Option<Vec<f64>> = None;

    if let Some(orch_arc) = reinforcement_orchestrator {
        // Use the feature cache if available
        let features = match if let Some(cache) = feature_cache {
            // Use the cache through the orchestrator
            let orchestrator_guard = orch_arc.lock().await;
            orchestrator_guard
                .get_pair_features(pool, &entity1.entity_id, &entity2.entity_id)
                .await
        } else {
            // Fall back to direct extraction if no cache
            MatchingOrchestrator::extract_pair_context_features(
                pool,
                &entity1.entity_id,
                &entity2.entity_id,
            )
            .await
        } {
            Ok(f) => {
                features_for_snapshot = Some(f.clone());
                Some(f)
            }
            Err(e) => {
                warn!("URL: Failed to extract features: {}", e);
                None
            }
        };

        // Apply tuning if features were extracted
        if let Some(extracted_features) = features {
            if !extracted_features.is_empty() {
                let orchestrator_guard = orch_arc.lock().await;
                match orchestrator_guard.get_tuned_confidence(
                    &MatchMethodType::Url,
                    confidence_score,
                    &extracted_features,
                ) {
                    Ok(tuned_score) => final_confidence_score = tuned_score,
                    Err(e) => {
                        warn!("URL: Failed to get tuned confidence: {}", e);
                    }
                }
            }
        }
    }

    // Create entity group
    let entity_group_id = EntityGroupId(Uuid::new_v4().to_string());
    let match_values_json =
        serde_json::to_value(&match_values).context("Failed to serialize match values")?;

    // Ensure consistent order
    let (e1, e2) = if entity1.entity_id.0 < entity2.entity_id.0 {
        (&entity1.entity_id, &entity2.entity_id)
    } else {
        (&entity2.entity_id, &entity1.entity_id)
    };

    // Execute INSERT directly instead of calling another function
    // This reduces the number of database connections needed
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for entity group creation")?;

    let rows_affected = conn
        .execute(
            INSERT_ENTITY_GROUP_SQL,
            &[
                &entity_group_id.0,
                &e1.0,
                &e2.0,
                &MatchMethodType::Url.as_str(),
                &match_values_json,
                &final_confidence_score,
                &confidence_score,
            ],
        )
        .await?;

    // Return the match if created
    if rows_affected > 0 {
        // Create suggestion for low confidence if needed
        if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
            let priority = if final_confidence_score < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
                2
            } else {
                1
            };

            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Url.as_str(),
                "entity_group_id": entity_group_id.0,
                "rule_based_confidence": confidence_score,
                "final_confidence": final_confidence_score,
            });

            let suggestion = NewSuggestedAction {
                pipeline_run_id: Some(pipeline_run_id.to_string()),
                action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
                entity_id: None,
                group_id_1: Some(entity_group_id.0.clone()),
                group_id_2: None,
                cluster_id: None,
                triggering_confidence: Some(final_confidence_score),
                details: Some(details_json),
                reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                reason_message: Some(format!(
                    "URL match with low confidence: {:.4}",
                    final_confidence_score
                )),
                priority,
                status: SuggestionStatus::PendingReview.as_str().to_string(),
                reviewer_id: None,
                reviewed_at: None,
                review_notes: None,
            };

            // Using deref() to access the inner client
            if let Err(e) = db::insert_suggestion(conn.deref(), &suggestion).await {
                warn!("URL: Failed to insert suggestion: {}", e);
            }
        }

        // Log decision to orchestrator if available
        if let (Some(orch_arc), Some(features)) =
            (reinforcement_orchestrator, features_for_snapshot)
        {
            let orchestrator_guard = orch_arc.lock().await;
            if let Err(e) = orchestrator_guard
                .log_decision_snapshot(
                    pool,
                    &entity_group_id.0,
                    pipeline_run_id,
                    &features,
                    &MatchMethodType::Url,
                    confidence_score,
                    final_confidence_score,
                )
                .await
            {
                warn!("URL: Failed to log decision: {}", e);
            }
        }

        return Ok(Some((e1.clone(), e2.clone(), final_confidence_score)));
    }

    Ok(None)
}

// Pre-check if two entities should be compared
fn should_compare_entities(entity1: &EntityUrlInfo, entity2: &EntityUrlInfo, domain: &str) -> bool {
    // Skip extraction if domains don't match (shouldn't happen with our grouping)
    if entity1.normalized_data.domain != entity2.normalized_data.domain {
        return false;
    }

    // For location-aware domains, skip if too far apart
    if entity1.has_location_data && entity2.has_location_data {
        let distance = calculate_distance(
            entity1.latitude.unwrap(),
            entity1.longitude.unwrap(),
            entity2.latitude.unwrap(),
            entity2.longitude.unwrap(),
        );

        if distance > MAX_LOCATION_DISTANCE_METERS * 2.0 {
            return false;
        }
    }

    // For path-matching required domains, skip if no common path elements
    if domain_requires_path_matching(domain) {
        let matching_slugs = count_matching_slugs(
            &entity1.normalized_data.path_slugs,
            &entity2.normalized_data.path_slugs,
        );

        if matching_slugs == 0 {
            return false;
        }
    }

    true
}

struct QueryParam {
    value: Box<dyn tokio_postgres::types::ToSql + Sync + Send>,
}

async fn batch_insert_entity_groups(
    pool: &PgPool,
    entity_groups: &[(EntityGroupId, EntityId, EntityId, MatchValues, f64, f64)],
) -> Result<usize> {
    if entity_groups.is_empty() {
        return Ok(0);
    }

    // Build query for batch insert
    let mut query = String::from(
        "INSERT INTO public.entity_group
        (id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
        pre_rl_confidence_score) VALUES ",
    );

    // Create a vector of boxed values that own their data
    let mut param_values: Vec<QueryParam> = Vec::new();

    // Store method type string once
    let method_type_str = MatchMethodType::Url.as_str().to_string();

    for (i, (id, e1, e2, values, conf, pre_conf)) in entity_groups.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }

        // Convert values to JSON
        let match_values_json = serde_json::to_value(values)?;

        // Add parameters index placeholders to query
        query.push_str(&format!(
            "(${},${},${},${},${},${},${})",
            i * 7 + 1,
            i * 7 + 2,
            i * 7 + 3,
            i * 7 + 4,
            i * 7 + 5,
            i * 7 + 6,
            i * 7 + 7
        ));

        // Add owned values to parameter vec (using to_owned() to create owned copies)
        param_values.push(QueryParam {
            value: Box::new(id.0.clone()),
        });
        param_values.push(QueryParam {
            value: Box::new(e1.0.clone()),
        });
        param_values.push(QueryParam {
            value: Box::new(e2.0.clone()),
        });
        param_values.push(QueryParam {
            value: Box::new(method_type_str.clone()),
        });
        param_values.push(QueryParam {
            value: Box::new(match_values_json),
        });
        param_values.push(QueryParam {
            value: Box::new(*conf),
        });
        param_values.push(QueryParam {
            value: Box::new(*pre_conf),
        });
    }

    // Add ON CONFLICT clause
    query.push_str(" ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING");

    // Create a vector of references for the execute call
    let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = param_values
        .iter()
        .map(|p| p.value.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    // Execute the batch insert using slice syntax
    let conn = pool.get().await?;
    let result = conn
        .execute(&query, &param_refs[..])
        .await
        .context("Failed to update domain status")?;

    Ok(result as usize)
}

// Ensure domain status table exists
async fn ensure_domain_status_table_exists(pool: &PgPool) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for creating domain status table")?;

    // Execute CREATE TABLE statement
    conn.execute(CREATE_DOMAIN_STATUS_TABLE_SQL_TABLE, &[])
        .await
        .context("Failed to create domain processing status table (TABLE part)")?;

    // Execute CREATE INDEX statement
    conn.execute(CREATE_DOMAIN_STATUS_TABLE_SQL_INDEX, &[])
        .await
        .context("Failed to create domain processing status table (INDEX part)")?;

    Ok(())
}

// Insert domain status
async fn insert_domain_status(
    pool: &PgPool,
    domain: &str,
    pipeline_run_id: &str,
    total_entities: usize,
) -> Result<String> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for inserting domain status")?;

    let query = "
        INSERT INTO clustering_metadata.domain_processing_status
        (domain, pipeline_run_id, status, total_entities)
        VALUES ($1, $2, 'pending', $3)
        ON CONFLICT (domain, pipeline_run_id) DO UPDATE SET
            status = 'pending',
            total_entities = $3,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id
    ";

    let row = conn
        .query_one(
            query,
            &[&domain, &pipeline_run_id, &(total_entities as i32)],
        )
        .await
        .context("Failed to insert domain status")?;

    let id: String = row.get(0);

    Ok(id)
}

// Update domain status
async fn update_domain_status(
    pool: &PgPool,
    status_id: &str,
    status: &str,
    total_entities: Option<usize>,
    processed_entity_pairs: Option<usize>,
    error_message: Option<&str>,
) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for updating domain status")?;

    let mut query = "
        UPDATE clustering_metadata.domain_processing_status
        SET status = $1, updated_at = CURRENT_TIMESTAMP
    "
    .to_string();

    let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&status];

    let entities_int;
    let pairs_int;
    let now_timestamp;
    let error_str;

    if let Some(entities) = total_entities {
        entities_int = entities as i32; // Assign to pre-declared variable
        query.push_str(", total_entities = $2");
        params.push(&entities_int);
    }

    if let Some(pairs) = processed_entity_pairs {
        pairs_int = pairs as i32; // Assign to pre-declared variable
        let idx = params.len() + 1;
        query.push_str(&format!(", processed_entity_pairs = ${}", idx));
        params.push(&pairs_int);
    }

    if let Some(error) = error_message {
        error_str = error; // Assign to pre-declared variable
        let idx = params.len() + 1;
        query.push_str(&format!(", error_message = ${}", idx));
        params.push(&error_str);
    }

    if status == "completed" || status == "error" {
        now_timestamp = chrono::Utc::now().naive_utc(); // Assign to pre-declared variable
        let idx = params.len() + 1;
        query.push_str(&format!(", completed_at = ${}", idx));
        params.push(&now_timestamp);
    }

    query.push_str(" WHERE id = $");
    query.push_str(&(params.len() + 1).to_string());

    params.push(&status_id);

    conn.execute(&query, &params[..]) // Using slice syntax
        .await
        .context("Failed to update domain status")?;

    Ok(())
}

// Fetch processed domains
async fn fetch_processed_domains(pool: &PgPool, pipeline_run_id: &str) -> Result<HashSet<String>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for fetching processed domains")?;

    let query = "
        SELECT domain
        FROM clustering_metadata.domain_processing_status
        WHERE pipeline_run_id = $1
        AND status = 'completed'
    ";

    let rows = conn
        .query(query, &[&pipeline_run_id])
        .await
        .context("Failed to fetch processed domains")?;

    let mut domains = HashSet::with_capacity(rows.len());

    for row in rows {
        let domain: String = row.get(0);
        domains.insert(domain);
    }

    Ok(domains)
}

// Fetches entities with URLs and location data where available
async fn fetch_entities_with_urls_and_locations(pool: &PgPool) -> Result<Vec<EntityUrlInfo>> {
    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for fetching entities with URLs")?;

    let query = r#"
    WITH EntityURLs AS (
        -- URLs from organizations
        SELECT 
            e.id AS entity_id,
            o.url AS url,
            e.name AS entity_name
        FROM 
            public.entity e
        JOIN 
            public.organization o ON e.organization_id = o.id
        WHERE 
            o.url IS NOT NULL AND o.url != '' AND o.url !~ '^\s*$'
        UNION ALL
        -- URLs from services
        SELECT 
            e.id AS entity_id,
            s.url AS url,
            e.name AS entity_name
        FROM 
            public.entity e
        JOIN 
            public.entity_feature ef ON e.id = ef.entity_id
        JOIN 
            public.service s ON ef.table_id = s.id AND ef.table_name = 'service'
        WHERE 
            s.url IS NOT NULL AND s.url != '' AND s.url !~ '^\s*$'
    ),
    EntityLocations AS (
        -- Get entity locations where available
        SELECT 
            e.id AS entity_id,
            l.latitude,
            l.longitude
        FROM 
            public.entity e
        JOIN 
            public.entity_feature ef ON e.id = ef.entity_id
        JOIN 
            public.location l ON ef.table_id = l.id AND ef.table_name = 'location'
        WHERE 
            l.latitude IS NOT NULL AND l.longitude IS NOT NULL
        GROUP BY 
            e.id, l.latitude, l.longitude
    )
    -- Join entities with their URLs and locations (if available)
    SELECT 
        u.entity_id,
        u.url,
        u.entity_name,
        l.latitude,
        l.longitude
    FROM 
        EntityURLs u
    LEFT JOIN 
        EntityLocations l ON u.entity_id = l.entity_id
    "#;

    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to fetch entities with URLs and locations")?;

    let mut entities = Vec::with_capacity(rows.len());

    for row in rows {
        let entity_id: String = row.get("entity_id");
        let url: String = row.get("url");
        let entity_name: Option<String> = row.get("entity_name");
        let latitude: Option<f64> = row.try_get("latitude").ok();
        let longitude: Option<f64> = row.try_get("longitude").ok();

        if let Some(normalized_data) = normalize_url_with_slugs(&url) {
            entities.push(EntityUrlInfo {
                entity_id: EntityId(entity_id),
                original_url: url,
                normalized_data,
                entity_name,
                has_location_data: latitude.is_some() && longitude.is_some(),
                latitude,
                longitude,
            });
        }
    }

    // Filter out social media and ignored domains
    entities.retain(|e| !is_ignored_domain(&e.normalized_data.domain));

    Ok(entities)
}

// Fetch existing URL matched pairs
async fn fetch_existing_pairs(pool: &PgPool) -> Result<HashSet<(EntityId, EntityId)>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for fetching existing pairs")?;

    let query = "
        SELECT entity_id_1, entity_id_2
        FROM public.entity_group
        WHERE method_type = $1";

    let rows = conn
        .query(query, &[&MatchMethodType::Url.as_str()])
        .await
        .context("Failed to query existing URL-matched pairs")?;

    let mut existing_pairs = HashSet::with_capacity(rows.len());

    for row in rows {
        let id1: String = row.get("entity_id_1");
        let id2: String = row.get("entity_id_2");

        // Ensure consistent ordering
        if id1 < id2 {
            existing_pairs.insert((EntityId(id1), EntityId(id2)));
        } else {
            existing_pairs.insert((EntityId(id2), EntityId(id1)));
        }
    }

    Ok(existing_pairs)
}

// Group entities by domain for batch processing
fn group_entities_by_domain(entities: &[EntityUrlInfo]) -> HashMap<String, Vec<EntityUrlInfo>> {
    let mut domain_map: HashMap<String, Vec<EntityUrlInfo>> = HashMap::new();

    for entity in entities {
        domain_map
            .entry(entity.normalized_data.domain.clone())
            .or_default()
            .push(entity.clone());
    }

    domain_map
}

// Calculate the Haversine distance between two coordinates in meters
fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS: f64 = 6371000.0; // Earth radius in meters

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS * c
}

// Count matching path segments between two URLs
fn count_matching_slugs(slugs1: &[String], slugs2: &[String]) -> usize {
    let mut count = 0;
    let min_len = slugs1.len().min(slugs2.len());

    for i in 0..min_len {
        if slugs1[i] == slugs2[i] {
            count += 1;
        } else {
            break; // Stop at first non-matching segment
        }
    }

    count
}

// Calculate confidence score based on URL match quality
fn calculate_url_match_confidence(
    entity1: &EntityUrlInfo,
    entity2: &EntityUrlInfo,
    matching_slug_count: usize,
    distance_meters: Option<f64>,
) -> (f64, MatchValues) {
    let is_full_path_match = matching_slug_count == entity1.normalized_data.path_slugs.len()
        && matching_slug_count == entity2.normalized_data.path_slugs.len()
        && !entity1.normalized_data.path_slugs.is_empty();

    // Base confidence from path matching
    let mut confidence_score = if is_full_path_match {
        CONFIDENCE_DOMAIN_FULL_PATH_MATCH
    } else {
        match matching_slug_count {
            0 => CONFIDENCE_DOMAIN_ONLY,
            1 => CONFIDENCE_DOMAIN_PLUS_ONE_SLUG,
            2 => CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS,
            _ => CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS,
        }
    };

    // Adjust confidence based on distance if available
    if let Some(distance) = distance_meters {
        // Closer locations = higher confidence
        if distance < 100.0 && matching_slug_count > 0 {
            confidence_score = (confidence_score + 0.05).min(0.95);
        } else if distance > 5000.0 && matching_slug_count == 0 {
            confidence_score = (confidence_score - 0.05).max(0.65);
        }
    }

    // Create match values
    let match_values = MatchValues::Url(UrlMatchValue {
        original_url1: entity1.original_url.clone(),
        original_url2: entity2.original_url.clone(),
        normalized_shared_domain: entity1.normalized_data.domain.clone(),
        matching_slug_count,
    });

    (confidence_score, match_values)
}

// Normalize a URL, extract the domain and path slugs
fn normalize_url_with_slugs(url_str: &str) -> Option<NormalizedUrlData> {
    let trimmed_url = url_str.trim();
    if trimmed_url.is_empty()
        || trimmed_url.starts_with("mailto:")
        || trimmed_url.starts_with("tel:")
    {
        return None;
    }

    // Add scheme if missing
    let url_with_scheme = if !trimmed_url.contains("://") {
        format!("https://{}", trimmed_url)
    } else {
        trimmed_url.to_string()
    };

    match StdUrl::parse(&url_with_scheme) {
        Ok(parsed_url) => {
            // Extract domain
            let domain_opt = parsed_url.host_str().and_then(|host| {
                let host_lower = host.to_lowercase();
                let domain = host_lower.trim_start_matches("www.").to_string();
                if domain.is_empty() || !domain.contains('.') || is_ip_address(&domain) {
                    None
                } else {
                    Some(domain)
                }
            });

            domain_opt.map(|domain| {
                // Extract path segments
                let path_slugs: Vec<String> =
                    parsed_url
                        .path_segments()
                        .map_or_else(Vec::new, |segments| {
                            segments
                                .filter(|s| !s.is_empty())
                                .map(str::to_string)
                                .collect()
                        });

                // Determine domain type
                let domain_type = categorize_domain(&domain);

                NormalizedUrlData {
                    domain,
                    path_slugs,
                    original_url: url_str.to_string(),
                    domain_type,
                }
            })
        }
        Err(_) => {
            // Fallback for unparseable URLs
            let domain_part = trimmed_url
                .split('/')
                .next()
                .unwrap_or("")
                .split('?')
                .next()
                .unwrap_or("")
                .split(':')
                .next()
                .unwrap_or("");

            if domain_part.contains('.') && !is_ip_address(domain_part) {
                let normalized_domain = domain_part
                    .to_lowercase()
                    .trim_start_matches("www.")
                    .to_string();

                Some(NormalizedUrlData {
                    domain: normalized_domain.clone(),
                    path_slugs: Vec::new(),
                    original_url: url_str.to_string(),
                    domain_type: categorize_domain(&normalized_domain),
                })
            } else {
                None
            }
        }
    }
}

// Determine if a domain is on the ignore list
fn is_ignored_domain(domain: &str) -> bool {
    // List of social media and URL shortening domains to ignore
    static IGNORED_DOMAINS: [&str; 10] = [
        "facebook.com",
        "twitter.com",
        "instagram.com",
        "linkedin.com",
        "youtube.com",
        "tiktok.com",
        "bit.ly",
        "t.co",
        "goo.gl",
        "tinyurl.com",
    ];

    // Check for exact match or subdomain
    IGNORED_DOMAINS
        .iter()
        .any(|ignored| domain == *ignored || domain.ends_with(&format!(".{}", ignored)))
}

// Check if a domain is one that requires path matching
fn domain_requires_path_matching(domain: &str) -> bool {
    // Check if domain is in the high volume list
    HIGH_VOLUME_DOMAINS
        .iter()
        .any(|&high_vol| domain == high_vol)
}

// Categorize domain by type for specialized handling
fn categorize_domain(domain: &str) -> DomainType {
    if domain.ends_with(".gov") {
        DomainType::Government
    } else if domain.ends_with(".edu") || domain.contains("school") || domain.contains("college") {
        DomainType::Education
    } else if domain.ends_with(".org")
        || domain.contains("health")
        || domain.contains("clinic")
        || domain.contains("hospital")
    {
        DomainType::Healthcare
    } else if domain.ends_with(".com") || domain.ends_with(".net") || domain.ends_with(".io") {
        DomainType::Commercial
    } else if domain.contains("facebook") || domain.contains("twitter") || domain.contains("social")
    {
        DomainType::Social
    } else {
        DomainType::Other
    }
}

// Helper to check for IP addresses
fn is_ip_address(domain: &str) -> bool {
    // Simple IPv4 check
    let parts: Vec<&str> = domain.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return true;
    }

    // Simple IPv6 check
    domain.contains(':') && domain.split(':').count() > 1
}
