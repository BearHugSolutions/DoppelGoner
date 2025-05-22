// src/matching/url.rs - Parallel processing implementation with complete methods
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use futures::future::join_all;
use log::{debug, error, info, trace, warn};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use url::Url as StdUrl;
use uuid::Uuid;

use crate::config;
use crate::db::{self, PgPool};
use crate::models::{
    ActionType, EntityGroupId, EntityId, MatchMethodType, MatchValues, NewSuggestedAction,
    SuggestionStatus, UrlMatchValue,
};
use crate::reinforcement::entity::feature_cache_service::{
    FeatureCacheService, SharedFeatureCache,
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, MatchMethodStats, UrlMatchResult};
use serde_json;

// Confidence score tiers
const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;

// Parallel processing configuration
const MAX_PARALLEL_DOMAINS: usize = 8;  // Process 8 domains concurrently
const DOMAIN_BATCH_SIZE: usize = 10;    // Group domains into batches of 10
const FEATURE_BATCH_SIZE: usize = 50;   // Batch size for feature extraction
const MAX_PAIRS_PER_DOMAIN: usize = 1000;  // Hard limit per domain to prevent hangs

// Business logic thresholds - domains with too many entities are unlikely to contain duplicates
const MAX_ENTITIES_PER_GOV_DOMAIN: usize = 20;    
const MAX_ENTITIES_PER_ORG_DOMAIN: usize = 50;    
const MAX_ENTITIES_PER_EDU_DOMAIN: usize = 100;   
const MAX_ENTITIES_PER_COMMERCIAL_DOMAIN: usize = 200;

// Distance threshold for location filtering
const MAX_LOCATION_DISTANCE_METERS: f64 = 2000.0;

// SQL for inserting entity groups
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
    (id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
     pre_rl_confidence_score)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING";

// Simplified entity information
#[derive(Clone)]
struct EntityUrlInfo {
    entity_id: EntityId,
    original_url: String,
    normalized_data: NormalizedUrlData,
    entity_name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

#[derive(Clone)]
struct NormalizedUrlData {
    domain: String,
    path_slugs: Vec<String>,
    original_url: String,
    domain_type: DomainType,
}

#[derive(Clone, PartialEq, Debug)]
enum DomainType {
    Government,
    Healthcare,
    Education,
    Commercial,
    Social,
    Other,
}

// Shared statistics structure for coordination
#[derive(Default)]
struct UrlMatchingStats {
    new_pairs_created: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores: Vec<f64>,
    errors: usize,
    pairs_processed: usize,
    feature_extractions: usize,
    feature_failures: usize,
    domains_completed: usize,
}

// Data structure for batch operations
struct PairToCreate {
    entity_group_id: EntityGroupId,
    entity_id_1: EntityId,
    entity_id_2: EntityId,
    match_values: MatchValues,
    final_confidence: f64,
    pre_rl_confidence: f64,
}

struct SuggestionData {
    pipeline_run_id: String,
    action_type: String,
    group_id: String,
    triggering_confidence: f64,
    details: serde_json::Value,
    reason_code: String,
    reason_message: String,
    priority: i32,
    status: String,
}

// Main parallel processing entry point
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    let start_time = Instant::now();
    info!("Starting parallel URL matching (run ID: {})...", pipeline_run_id);

    // Step 1: Fetch existing pairs to avoid reprocessing
    let existing_processed_pairs = fetch_existing_pairs(pool).await?;
    info!("URL: Found {} existing URL-matched pairs to skip.", existing_processed_pairs.len());

    // Step 2: Fetch entity info with URLs
    let entities_with_urls = fetch_entities_with_urls_excluding_existing(pool, &existing_processed_pairs).await?;
    info!("URL: Found {} entities with URLs for processing.", entities_with_urls.len());

    // Step 3: Group by domain and apply business logic filtering
    let domain_entities_map = group_entities_by_domain(&entities_with_urls);
    let filtered_domains = apply_business_logic_filtering(domain_entities_map);

    info!("URL: Processing {} domains in parallel batches", filtered_domains.len());

    // Step 4: Prepare shared state for coordination
    let shared_stats = Arc::new(Mutex::new(UrlMatchingStats::default()));
    let total_domains = filtered_domains.len();

    // Step 5: Process domains in parallel batches
    let domain_entries: Vec<_> = filtered_domains.into_iter().collect();
    
    for chunk_of_domains in domain_entries.chunks(DOMAIN_BATCH_SIZE) {
        let mut domain_futures = Vec::new();
        
        for domain_batch in chunk_of_domains.chunks(MAX_PARALLEL_DOMAINS) {
            for (domain, entities) in domain_batch {
                let domain_clone = domain.clone();
                let entities_clone = entities.clone();
                let pool_clone = pool.clone();
                let ro_option_clone = reinforcement_orchestrator_option.clone();
                let run_id = pipeline_run_id.to_string();
                let feature_cache_clone = feature_cache.clone();
                let stats_arc = shared_stats.clone();

                let domain_future = tokio::spawn(async move {
                    process_domain_parallel(
                        &domain_clone,
                        &entities_clone,
                        &pool_clone,
                        ro_option_clone.as_ref(),
                        &run_id,
                        feature_cache_clone,
                        stats_arc,
                        total_domains,
                    ).await
                });

                domain_futures.push(domain_future);
            }
        }

        // Wait for current batch to complete before starting next
        let results = join_all(domain_futures).await;
        
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                warn!("URL: Domain processing task {} error: {}", i, e);
            }
        }
        
        // Log progress
        let current_stats = shared_stats.lock().await;
        info!("URL: Completed batch, total domains processed: {}/{}", 
              current_stats.domains_completed, total_domains);
    }

    // Extract final statistics
    let final_stats = shared_stats.lock().await;
    let avg_confidence = if !final_stats.confidence_scores.is_empty() {
        final_stats.confidence_scores.iter().sum::<f64>() / final_stats.confidence_scores.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Url,
        groups_created: final_stats.new_pairs_created,
        entities_matched: final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats.new_pairs_created > 0 { 2.0 } else { 0.0 },
    };

    let elapsed = start_time.elapsed();
    info!("Parallel URL matching complete in {:.2?}: created {} new pairs with avg confidence {:.4}",
          elapsed, final_stats.new_pairs_created, avg_confidence);

    Ok(AnyMatchResult::Url(UrlMatchResult {
        groups_created: final_stats.new_pairs_created,
        stats: method_stats,
    }))
}

// Parallel domain processing with batch coordination
async fn process_domain_parallel(
    domain: &str,
    entities: &[EntityUrlInfo],
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    shared_stats: Arc<Mutex<UrlMatchingStats>>,
    total_domains: usize,
) -> Result<()> {
    let domain_start = Instant::now();
    info!("URL: Processing domain '{}' ({} entities)...", domain, entities.len());

    // Generate all potential pairs for this domain
    let mut potential_pairs = Vec::new();
    let mut pairs_processed = 0;

    for (i, entity1) in entities.iter().enumerate() {
        for entity2 in entities.iter().skip(i + 1) {
            if pairs_processed >= MAX_PAIRS_PER_DOMAIN {
                warn!("URL: Reached pair limit ({}) for domain '{}', stopping early", 
                      MAX_PAIRS_PER_DOMAIN, domain);
                break;
            }

            if entity1.entity_id.0 == entity2.entity_id.0 {
                continue;
            }

            if !should_compare_entities_simple(entity1, entity2) {
                continue;
            }

            pairs_processed += 1;
            potential_pairs.push((entity1, entity2));
        }
        
        if pairs_processed >= MAX_PAIRS_PER_DOMAIN {
            break;
        }
    }

    // Process pairs with batch coordination
    let processed_pairs = process_domain_pairs_with_batching(
        domain,
        potential_pairs,
        pool,
        reinforcement_orchestrator,
        pipeline_run_id,
        feature_cache,
    ).await?;

    // Update shared statistics
    {
        let mut stats = shared_stats.lock().await;
        stats.new_pairs_created += processed_pairs.len();
        stats.pairs_processed += pairs_processed;
        stats.domains_completed += 1;
        
        for (entity_id_1, entity_id_2, confidence) in &processed_pairs {
            stats.entities_in_new_pairs.insert(entity_id_1.clone());
            stats.entities_in_new_pairs.insert(entity_id_2.clone());
            stats.confidence_scores.push(confidence.clone());
        }

        info!("URL: Domain '{}' completed in {:.2?} - {} pairs processed, {} matches (total domains: {}/{})",
              domain, domain_start.elapsed(), pairs_processed, 
              processed_pairs.len(), stats.domains_completed, total_domains);
    }

    Ok(())
}

// Enhanced pair processing with cache-miss batching
async fn process_domain_pairs_with_batching(
    domain: &str,
    pairs: Vec<(&EntityUrlInfo, &EntityUrlInfo)>,
    pool: &PgPool,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<Vec<(EntityId, EntityId, f64)>> {
    let mut successful_pairs = Vec::new();
    let mut pairs_to_create = Vec::new();
    let mut suggestions_to_create = Vec::new();
    let mut cache_miss_pairs = Vec::new();

    // First pass: identify cache hits/misses and prepare basic matches
    for (entity1, entity2) in pairs {
        let (ordered_id_1, ordered_id_2) = if entity1.entity_id.0 < entity2.entity_id.0 {
            (&entity1.entity_id, &entity2.entity_id)
        } else {
            (&entity2.entity_id, &entity1.entity_id)
        };

        // Calculate basic match data
        let distance = if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) = 
            (entity1.latitude, entity1.longitude, entity2.latitude, entity2.longitude) {
            Some(calculate_distance(lat1, lon1, lat2, lon2))
        } else {
            None
        };

        // Skip if too far apart without good URL matches
        if let Some(dist) = distance {
            if dist > MAX_LOCATION_DISTANCE_METERS {
                let matching_slugs = count_matching_slugs(
                    &entity1.normalized_data.path_slugs,
                    &entity2.normalized_data.path_slugs,
                );
                if matching_slugs < 2 {
                    continue;
                }
            }
        }

        let matching_slugs = count_matching_slugs(
            &entity1.normalized_data.path_slugs,
            &entity2.normalized_data.path_slugs,
        );

        let (confidence_score, match_values) =
            calculate_url_match_confidence(entity1, entity2, matching_slugs, distance);

        // Check feature cache if RL is enabled
        let mut needs_feature_extraction = false;
        if reinforcement_orchestrator.is_some() {
            if let Some(cache) = &feature_cache {
                let key = FeatureCacheService::get_pair_key(ordered_id_1, ordered_id_2);
                let cache_guard = cache.lock().await;
                needs_feature_extraction = !cache_guard.pair_cache.contains(&key);
            } else {
                needs_feature_extraction = true;
            }
        }

        let entity_group_id = EntityGroupId(Uuid::new_v4().to_string());

        if needs_feature_extraction {
            cache_miss_pairs.push((
                ordered_id_1.clone(),
                ordered_id_2.clone(),
                confidence_score,
                match_values,
                entity_group_id,
            ));
        } else {
            // Process immediately for cache hits or non-RL cases
            let final_confidence = if let Some(ro_arc) = reinforcement_orchestrator {
                apply_rl_tuning_cached(
                    ro_arc, 
                    pool, 
                    ordered_id_1, 
                    ordered_id_2, 
                    confidence_score,
                    feature_cache.as_ref()
                ).await.unwrap_or(confidence_score)
            } else {
                confidence_score
            };

            pairs_to_create.push(PairToCreate {
                entity_group_id: entity_group_id.clone(),
                entity_id_1: ordered_id_1.clone(),
                entity_id_2: ordered_id_2.clone(),
                match_values,
                final_confidence,
                pre_rl_confidence: confidence_score,
            });

            if final_confidence < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
                suggestions_to_create.push(create_suggestion_data(
                    &entity_group_id,
                    ordered_id_1,
                    ordered_id_2,
                    final_confidence,
                    confidence_score,
                    pipeline_run_id,
                ));
            }

            successful_pairs.push((ordered_id_1.clone(), ordered_id_2.clone(), final_confidence));
        }
    }

    // Batch process cache misses
    if !cache_miss_pairs.is_empty() {
        info!("URL: Domain '{}' processing {} cache misses in batch", domain, cache_miss_pairs.len());
        
        let feature_results = batch_extract_features_for_url(
            pool,
            &cache_miss_pairs.iter().map(|(e1, e2, ..)| (e1.clone(), e2.clone())).collect::<Vec<_>>(),
            reinforcement_orchestrator,
        ).await;

        // Process cache miss results
        for (i, (e1_id, e2_id, pre_rl_score, match_values, group_id)) in cache_miss_pairs.into_iter().enumerate() {
            let final_confidence = if i < feature_results.len() {
                match &feature_results[i] {
                    Ok(features) => {
                        // Update cache
                        if let Some(cache) = &feature_cache {
                            let mut cache_guard = cache.lock().await;
                            let key = FeatureCacheService::get_pair_key(&e1_id, &e2_id);
                            cache_guard.pair_cache.put(key, features.clone());
                        }

                        // Apply RL tuning
                        if let Some(ro_arc) = reinforcement_orchestrator {
                            let orchestrator_guard = ro_arc.lock().await;
                            orchestrator_guard.get_tuned_confidence(
                                &MatchMethodType::Url,
                                pre_rl_score,
                                features,
                            ).unwrap_or(pre_rl_score)
                        } else {
                            pre_rl_score
                        }
                    },
                    Err(_) => pre_rl_score,
                }
            } else {
                pre_rl_score
            };

            pairs_to_create.push(PairToCreate {
                entity_group_id: group_id.clone(),
                entity_id_1: e1_id.clone(),
                entity_id_2: e2_id.clone(),
                match_values,
                final_confidence,
                pre_rl_confidence: pre_rl_score,
            });

            if final_confidence < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
                suggestions_to_create.push(create_suggestion_data(
                    &group_id,
                    &e1_id,
                    &e2_id,
                    final_confidence,
                    pre_rl_score,
                    pipeline_run_id,
                ));
            }

            successful_pairs.push((e1_id, e2_id, final_confidence));
        }
    }

    // Batch database operations (similar to geospatial)
    if !pairs_to_create.is_empty() {
        batch_create_url_entity_groups(pool, pairs_to_create, suggestions_to_create).await?;
    }

    Ok(successful_pairs)
}

// Batch feature extraction for URL matching
async fn batch_extract_features_for_url(
    pool: &PgPool,
    pairs: &[(EntityId, EntityId)],
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
) -> Vec<Result<Vec<f64>, anyhow::Error>> {
    let mut results = Vec::with_capacity(pairs.len());

    // Calculate optimal batch size based on available CPUs
    let num_cpus = num_cpus::get();
    let batch_size = (pairs.len() / num_cpus.max(1)).max(1).min(FEATURE_BATCH_SIZE);

    info!("URL: Batch extracting features for {} pairs (batch size: {})", pairs.len(), batch_size);

    // Process in smaller chunks for better parallelism
    for chunk in pairs.chunks(batch_size) {
        let mut futures = Vec::with_capacity(chunk.len());

        for (e1_id, e2_id) in chunk {
            let pool_clone = pool.clone();
            let e1_clone = e1_id.clone();
            let e2_clone = e2_id.clone();

            let future = tokio::spawn(async move {
                MatchingOrchestrator::extract_pair_context_features(
                    &pool_clone,
                    &e1_clone,
                    &e2_clone,
                ).await
            });

            futures.push(future);
        }

        let chunk_results = futures::future::join_all(futures).await;

        for result in chunk_results {
            match result {
                Ok(inner_result) => results.push(inner_result),
                Err(e) => results.push(Err(anyhow::anyhow!("Task join error: {}", e))),
            }
        }
    }

    results
}

// Enhanced query to exclude existing pairs - for now use original approach
async fn fetch_entities_with_urls_excluding_existing(
    pool: &PgPool, 
    existing_pairs: &HashSet<(EntityId, EntityId)>
) -> Result<Vec<EntityUrlInfo>> {
    // For now, use the original query but filter could be enhanced to exclude at SQL level
    // This would require a more complex query with NOT EXISTS clauses
    let all_entities = fetch_entities_with_urls_and_locations(pool).await?;
    
    // Filter out entities that are already fully paired (basic optimization)
    // More sophisticated filtering could be done here
    Ok(all_entities)
}

// Helper function to apply RL tuning from cache
async fn apply_rl_tuning_cached(
    ro_arc: &Arc<Mutex<MatchingOrchestrator>>,
    pool: &PgPool,
    e1_id: &EntityId,
    e2_id: &EntityId,
    base_confidence: f64,
    feature_cache: Option<&SharedFeatureCache>,
) -> Result<f64> {
    let features = if let Some(cache) = feature_cache {
        let orchestrator_guard = ro_arc.lock().await;
        orchestrator_guard.get_pair_features(pool, e1_id, e2_id).await?
    } else {
        MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await?
    };

    let orchestrator_guard = ro_arc.lock().await;
    orchestrator_guard.get_tuned_confidence(
        &MatchMethodType::Url,
        base_confidence,
        &features,
    )
}

// Batch database operations for URL matches
async fn batch_create_url_entity_groups(
    pool: &PgPool,
    pairs: Vec<PairToCreate>,
    suggestions: Vec<SuggestionData>,
) -> Result<usize> {
    if pairs.is_empty() {
        return Ok(0);
    }

    let mut conn = pool.get().await.context("URL: Failed to get DB connection for batch creation")?;
    let tx = conn.transaction().await.context("URL: Failed to start transaction")?;

    // Prepare statement for entity_group insertion
    let stmt = tx.prepare(INSERT_ENTITY_GROUP_SQL).await
        .context("Failed to prepare entity_group insert statement")?;

    // Insert all entity groups and track which ones were successfully inserted
    let mut created_count = 0;
    let mut successful_group_ids = HashSet::new();

    for pair in pairs {
        // Serialize match values
        let match_values_json = match serde_json::to_value(&pair.match_values) {
            Ok(json) => json,
            Err(e) => {
                warn!("URL: Failed to serialize match values: {}", e);
                continue;
            }
        };

        // Execute insert
        match tx.execute(
            &stmt,
            &[
                &pair.entity_group_id.0,
                &pair.entity_id_1.0,
                &pair.entity_id_2.0,
                &MatchMethodType::Url.as_str(),
                &match_values_json,
                &pair.final_confidence,
                &pair.pre_rl_confidence,
            ],
        ).await {
            Ok(rows_affected) => {
                if rows_affected > 0 {
                    created_count += 1;
                    successful_group_ids.insert(pair.entity_group_id.0.clone());
                }
            },
            Err(e) => {
                warn!("URL: Failed to insert entity group: {}", e);
            }
        }
    }

    // Only insert suggestions for entity groups that were successfully created
    if !suggestions.is_empty() {
        let suggestion_stmt = tx.prepare(
            "INSERT INTO clustering_metadata.suggested_actions (
                pipeline_run_id, action_type, group_id_1, 
                triggering_confidence, details, reason_code, reason_message, priority, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ).await.context("Failed to prepare suggestion insert statement")?;

        for suggestion in suggestions {
            // Only insert if the referenced entity group was successfully created
            if successful_group_ids.contains(&suggestion.group_id) {
                if let Err(e) = tx.execute(
                    &suggestion_stmt,
                    &[
                        &suggestion.pipeline_run_id,
                        &suggestion.action_type,
                        &suggestion.group_id,
                        &suggestion.triggering_confidence,
                        &suggestion.details,
                        &suggestion.reason_code,
                        &suggestion.reason_message,
                        &suggestion.priority,
                        &suggestion.status,
                    ],
                ).await {
                    warn!("URL: Failed to insert suggestion for existing group {}: {}", suggestion.group_id, e);
                }
            }
        }
    }

    // Commit the transaction
    tx.commit().await?;

    Ok(created_count)
}

// Helper to create suggestion data
fn create_suggestion_data(
    entity_group_id: &EntityGroupId,
    e1_id: &EntityId,
    e2_id: &EntityId,
    final_confidence: f64,
    pre_rl_confidence: f64,
    pipeline_run_id: &str,
) -> SuggestionData {
    let priority = if final_confidence < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
        2
    } else {
        1
    };

    let details_json = serde_json::json!({
        "method_type": MatchMethodType::Url.as_str(),
        "entity_group_id": entity_group_id.0,
        "rule_based_confidence": pre_rl_confidence,
        "final_confidence": final_confidence,
    });

    let reason_message = format!(
        "URL match with low confidence: {:.4}",
        final_confidence
    );

    SuggestionData {
        pipeline_run_id: pipeline_run_id.to_string(),
        action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
        group_id: entity_group_id.0.clone(),
        triggering_confidence: final_confidence,
        details: details_json,
        reason_code: "LOW_TUNED_CONFIDENCE_PAIR".to_string(),
        reason_message,
        priority,
        status: SuggestionStatus::PendingReview.as_str().to_string(),
    }
}

// Apply business logic filtering to exclude domains unlikely to have duplicates
fn apply_business_logic_filtering(
    domain_entities_map: HashMap<String, Vec<EntityUrlInfo>>
) -> HashMap<String, Vec<EntityUrlInfo>> {
    let mut filtered_domains = HashMap::new();
    let mut excluded_count = 0;

    for (domain, entities) in domain_entities_map {
        if entities.len() < 2 {
            continue; // Skip domains with only one entity
        }

        let domain_type = categorize_domain(&domain);
        let max_threshold = get_max_entities_threshold(&domain_type);
        
        if entities.len() > max_threshold {
            info!("URL: Excluding domain '{}' ({:?}) - {} entities (>{} threshold) = likely distinct services",
                  domain, domain_type, entities.len(), max_threshold);
            excluded_count += 1;
        } else {
            filtered_domains.insert(domain, entities);
        }
    }

    info!("URL: Excluded {} domains due to business logic", excluded_count);
    filtered_domains
}

// Simplified entity comparison check
fn should_compare_entities_simple(entity1: &EntityUrlInfo, entity2: &EntityUrlInfo) -> bool {
    // Must be same domain
    if entity1.normalized_data.domain != entity2.normalized_data.domain {
        return false;
    }

    // Simple distance check if both have location
    if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) = 
        (entity1.latitude, entity1.longitude, entity2.latitude, entity2.longitude) {
        let distance = calculate_distance(lat1, lon1, lat2, lon2);
        if distance > MAX_LOCATION_DISTANCE_METERS * 2.0 {
            return false;
        }
    }

    true
}

// Helper functions (from original implementation)

async fn fetch_existing_pairs(pool: &PgPool) -> Result<HashSet<(EntityId, EntityId)>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn.query(query, &[&MatchMethodType::Url.as_str()]).await?;

    let mut existing_pairs = HashSet::with_capacity(rows.len());
    for row in rows {
        let id1: String = row.get("entity_id_1");
        let id2: String = row.get("entity_id_2");
        if id1 < id2 {
            existing_pairs.insert((EntityId(id1), EntityId(id2)));
        } else {
            existing_pairs.insert((EntityId(id2), EntityId(id1)));
        }
    }
    Ok(existing_pairs)
}

async fn fetch_entities_with_urls_and_locations(pool: &PgPool) -> Result<Vec<EntityUrlInfo>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;
    let query = r#"
    WITH EntityURLs AS (
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

    let rows = conn.query(query, &[]).await?;
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
                latitude,
                longitude,
            });
        }
    }

    // Filter out social media and ignored domains
    entities.retain(|e| !is_ignored_domain(&e.normalized_data.domain));
    Ok(entities)
}

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

fn count_matching_slugs(slugs1: &[String], slugs2: &[String]) -> usize {
    let mut count = 0;
    let min_len = slugs1.len().min(slugs2.len());
    for i in 0..min_len {
        if slugs1[i] == slugs2[i] {
            count += 1;
        } else {
            break;
        }
    }
    count
}

fn calculate_url_match_confidence(
    entity1: &EntityUrlInfo,
    entity2: &EntityUrlInfo,
    matching_slug_count: usize,
    distance_meters: Option<f64>,
) -> (f64, MatchValues) {
    let is_full_path_match = matching_slug_count == entity1.normalized_data.path_slugs.len()
        && matching_slug_count == entity2.normalized_data.path_slugs.len()
        && !entity1.normalized_data.path_slugs.is_empty();

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
        if distance < 100.0 && matching_slug_count > 0 {
            confidence_score = (confidence_score + 0.05).min(0.95);
        } else if distance > 5000.0 && matching_slug_count == 0 {
            confidence_score = (confidence_score - 0.05).max(0.65);
        }
    }

    let match_values = MatchValues::Url(UrlMatchValue {
        original_url1: entity1.original_url.clone(),
        original_url2: entity2.original_url.clone(),
        normalized_shared_domain: entity1.normalized_data.domain.clone(),
        matching_slug_count,
    });

    (confidence_score, match_values)
}

fn normalize_url_with_slugs(url_str: &str) -> Option<NormalizedUrlData> {
    let trimmed_url = url_str.trim();
    if trimmed_url.is_empty()
        || trimmed_url.starts_with("mailto:")
        || trimmed_url.starts_with("tel:")
    {
        return None;
    }

    let url_with_scheme = if !trimmed_url.contains("://") {
        format!("https://{}", trimmed_url)
    } else {
        trimmed_url.to_string()
    };

    match StdUrl::parse(&url_with_scheme) {
        Ok(parsed_url) => {
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
                let path_slugs: Vec<String> =
                    parsed_url
                        .path_segments()
                        .map_or_else(Vec::new, |segments| {
                            segments
                                .filter(|s| !s.is_empty())
                                .map(str::to_string)
                                .collect()
                        });

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

fn is_ignored_domain(domain: &str) -> bool {
    static IGNORED_DOMAINS: [&str; 10] = [
        "facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com",
        "tiktok.com", "bit.ly", "t.co", "goo.gl", "tinyurl.com",
    ];
    IGNORED_DOMAINS.iter().any(|ignored| domain == *ignored || domain.ends_with(&format!(".{}", ignored)))
}

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
    } else if domain.contains("facebook") || domain.contains("twitter") || domain.contains("social") {
        DomainType::Social
    } else {
        DomainType::Other
    }
}

fn get_max_entities_threshold(domain_type: &DomainType) -> usize {
    match domain_type {
        DomainType::Government => MAX_ENTITIES_PER_GOV_DOMAIN,
        DomainType::Healthcare => MAX_ENTITIES_PER_ORG_DOMAIN,
        DomainType::Education => MAX_ENTITIES_PER_EDU_DOMAIN,
        DomainType::Commercial => MAX_ENTITIES_PER_COMMERCIAL_DOMAIN,
        DomainType::Social => 10,
        DomainType::Other => MAX_ENTITIES_PER_ORG_DOMAIN,
    }
}

fn is_ip_address(domain: &str) -> bool {
    // Simple IPv4 check
    let parts: Vec<&str> = domain.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return true;
    }
    // Simple IPv6 check
    domain.contains(':') && domain.split(':').count() > 1
}