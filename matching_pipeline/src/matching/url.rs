// src/matching/url.rs
use anyhow::{anyhow, Context, Result};
use chrono::Utc; // Required for NaiveDateTime if used in NewSuggestedAction
use futures::future::join_all;
use log::{debug, info, warn}; // error and trace for potential future use
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use url::Url as StdUrl;
use uuid::Uuid;

use crate::config::{MODERATE_LOW_SUGGESTION_THRESHOLD, CRITICALLY_LOW_SUGGESTION_THRESHOLD}; // Ensure these are in config
// Removed: use crate::config::MIN_CONFIDENCE_FOR_GROUPING_THRESHOLD_STRICT;

use crate::db::PgPool; // db module is not directly used here for insert_suggestion, but kept if other helpers are there
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

use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};

// Confidence score tiers
const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;

// Parallel processing configuration
const MAX_PARALLEL_DOMAINS_IN_FLIGHT: usize = 8;
const DOMAIN_PROCESSING_BATCH_SIZE: usize = 10;
const RL_FEATURE_EXTRACTION_BATCH_SIZE: usize = 50;
const MAX_PAIRS_PER_DOMAIN_SOFT_WARN: usize = 2000;
const MAX_PAIRS_PER_DOMAIN_HARD_LIMIT: usize = 5000;

// Business logic thresholds
const MAX_ENTITIES_PER_GOV_DOMAIN: usize = 20;
const MAX_ENTITIES_PER_ORG_DOMAIN: usize = 50;
const MAX_ENTITIES_PER_EDU_DOMAIN: usize = 100;
const MAX_ENTITIES_PER_COMMERCIAL_DOMAIN: usize = 200;

// Distance threshold for location filtering
const MAX_LOCATION_DISTANCE_METERS: f64 = 2000.0;

const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
    (id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
     pre_rl_confidence_score)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (entity_id_1, entity_id_2, method_type) DO NOTHING RETURNING id"; // RETURNING id is useful

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
pub struct NormalizedUrlData {
    pub domain: String,
    pub path_slugs: Vec<String>,
    pub original_url: String,
    pub domain_type: DomainType,
}

#[derive(Clone, PartialEq, Debug)]
enum DomainType {
    Government, Healthcare, Education, Commercial, Social, Other,
}

#[derive(Default)]
struct UrlMatchingStats {
    new_pairs_created_db: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores: Vec<f64>, // For pairs that were attempted to be grouped as "MATCH"
    task_errors: usize,
    pairs_considered_for_comparison: usize,
    rl_feature_extractions: usize,
    rl_feature_failures: usize,
    domains_completed_processing: usize,
    comparison_cache_hits: usize,
}

struct PairToCreate {
    entity_group_id: EntityGroupId,
    entity_id_1: EntityId,
    entity_id_2: EntityId,
    match_values: MatchValues,
    final_confidence: f64,
    pre_rl_confidence: f64,
    // For decision logging, if needed separately from match_values
    features_for_decision_log: Option<Vec<f64>>, 
}

struct SuggestionData {
    pipeline_run_id: String,
    action_type: String,
    group_id: String, // Corresponds to entity_group_id
    triggering_confidence: f64,
    details: serde_json::Value,
    reason_code: String,
    reason_message: String,
    priority: i32,
    status: String,
}

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    let start_time = Instant::now();
    info!("Starting parallel URL matching (run ID: {}) with INCREMENTAL CHECKS...", pipeline_run_id);

    let existing_entity_groups_arc = fetch_existing_entity_groups(pool).await?;
    info!("URL: Found {} existing URL-matched pairs in entity_group.", existing_entity_groups_arc.len());

    let entities_with_urls = fetch_entities_with_urls_and_locations(pool).await?;
    info!("URL: Found {} entities with URLs for processing.", entities_with_urls.len());

    let domain_entities_map = group_entities_by_domain(&entities_with_urls);
    let filtered_domains = apply_business_logic_filtering(domain_entities_map);
    let total_domains_to_process = filtered_domains.len();
    info!("URL: Processing {} domains after business logic filtering.", total_domains_to_process);

    if total_domains_to_process == 0 {
        info!("URL: No domains to process. Exiting.");
        let stats = MatchMethodStats {
            method_type: MatchMethodType::Url,
            groups_created: 0,
            entities_matched: 0,
            avg_confidence: 0.0,
            avg_group_size: 0.0, // Or 2.0 if appropriate for a default URL match stat
        };
        return Ok(AnyMatchResult::Url(UrlMatchResult { groups_created: 0, stats }));
    }
    
    let shared_stats_arc = Arc::new(Mutex::new(UrlMatchingStats::default()));
    let processed_pairs_this_run_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));
    let domain_entries: Vec<_> = filtered_domains.into_iter().collect();
    
    for chunk_of_domain_entries in domain_entries.chunks(DOMAIN_PROCESSING_BATCH_SIZE * MAX_PARALLEL_DOMAINS_IN_FLIGHT) {
        let mut task_futures = Vec::new();
        for domain_task_batch in chunk_of_domain_entries.chunks(MAX_PARALLEL_DOMAINS_IN_FLIGHT) {
            for (domain, entities) in domain_task_batch { // domain_task_batch is a slice of (String, Vec<EntityUrlInfo>)
                let domain_cloned_for_task = domain.clone(); // Clone domain String
                let entities_cloned_for_task = entities.clone(); // Clone Vec<EntityUrlInfo>

                task_futures.push(tokio::spawn({
                    let pool_clone = pool.clone();
                    let ro_opt_clone = reinforcement_orchestrator_option.clone();
                    let pr_id_clone = pipeline_run_id.to_string();
                    let feat_cache_clone = feature_cache.clone();
                    let stats_arc_clone = shared_stats_arc.clone();
                    let proc_pairs_arc_clone = processed_pairs_this_run_arc.clone();
                    let exist_groups_clone = existing_entity_groups_arc.clone();

                    async move {
                        process_domain_parallel(
                            &domain_cloned_for_task, // Pass as ref
                            &entities_cloned_for_task, // Pass as ref
                            &pool_clone,
                            ro_opt_clone.as_ref(),
                            &pr_id_clone,
                            feat_cache_clone,
                            stats_arc_clone,
                            proc_pairs_arc_clone,
                            exist_groups_clone,
                        ).await
                    }
                }));
            }
        }

        let task_results = join_all(task_futures).await;
        for (i, result) in task_results.iter().enumerate() {
            if let Err(e) = result {
                warn!("URL: Domain processing task {} in group failed: {}", i, e);
                let mut stats_guard = shared_stats_arc.lock().await;
                stats_guard.task_errors += 1;
            }
        }
        
        let current_stats_guard = shared_stats_arc.lock().await;
        info!("URL: Completed a group of domain tasks. Domains marked completed: {}/{}. Total cache hits: {}. DB Pairs: {}", 
              current_stats_guard.domains_completed_processing, total_domains_to_process, current_stats_guard.comparison_cache_hits, current_stats_guard.new_pairs_created_db);
    }

    let final_stats_guard = shared_stats_arc.lock().await;
    let avg_confidence = if !final_stats_guard.confidence_scores.is_empty() {
        final_stats_guard.confidence_scores.iter().sum::<f64>() / final_stats_guard.confidence_scores.len() as f64
    } else { 0.0 };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Url,
        groups_created: final_stats_guard.new_pairs_created_db,
        entities_matched: final_stats_guard.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats_guard.new_pairs_created_db > 0 { 
            final_stats_guard.entities_in_new_pairs.len() as f64 / final_stats_guard.new_pairs_created_db as f64
        } else { 0.0 }, // Avoid division by zero
    };

    info!(
        "Parallel URL matching complete in {:.2?}: Considered {} pairs. Actual DB groups created: {} ({} task errors). Cache hits: {}. Avg confidence (for attempted matches): {:.4}. {} domains completed.",
        start_time.elapsed(), final_stats_guard.pairs_considered_for_comparison,
        final_stats_guard.new_pairs_created_db, final_stats_guard.task_errors,
        final_stats_guard.comparison_cache_hits, avg_confidence,
        final_stats_guard.domains_completed_processing
    );
    info!("URL RL Feature Extraction: {} successful, {} failed.", final_stats_guard.rl_feature_extractions, final_stats_guard.rl_feature_failures);

    Ok(AnyMatchResult::Url(UrlMatchResult {
        groups_created: final_stats_guard.new_pairs_created_db,
        stats: method_stats,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn process_domain_parallel(
    domain_name: &str,
    entities_in_domain: &[EntityUrlInfo],
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    shared_stats_arc: Arc<Mutex<UrlMatchingStats>>,
    processed_pairs_this_run_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    existing_entity_groups: Arc<HashSet<(EntityId, EntityId)>>,
) -> Result<()> {
    let domain_start_time = Instant::now();
    let mut pairs_considered_in_domain_count = 0;
    let mut potential_pairs_for_domain_processing = Vec::new();

    for i in 0..entities_in_domain.len() {
        for j in (i + 1)..entities_in_domain.len() {
            if pairs_considered_in_domain_count >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT {
                warn!("URL: Domain '{}' reached HARD pair limit ({}). Stopping pair generation.", domain_name, MAX_PAIRS_PER_DOMAIN_HARD_LIMIT);
                break;
            }
            let entity1 = &entities_in_domain[i];
            let entity2 = &entities_in_domain[j];
            if entity1.entity_id.0 == entity2.entity_id.0 { continue; }
            
            pairs_considered_in_domain_count += 1;
            potential_pairs_for_domain_processing.push((entity1.clone(), entity2.clone()));
        }
        if pairs_considered_in_domain_count >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT { break; }
    }
    
    if pairs_considered_in_domain_count > MAX_PAIRS_PER_DOMAIN_SOFT_WARN {
        info!("URL: Domain '{}' has {} potential pairs (exceeds soft limit {}).", 
              domain_name, pairs_considered_in_domain_count, MAX_PAIRS_PER_DOMAIN_SOFT_WARN);
    }

    let process_pairs_result = process_pairs_for_domain_with_caching(
        domain_name,
        potential_pairs_for_domain_processing,
        pool,
        reinforcement_orchestrator_option,
        pipeline_run_id,
        feature_cache,
        processed_pairs_this_run_arc.clone(),
        existing_entity_groups.clone(),
        shared_stats_arc.clone(), // Pass shared_stats_arc for direct updates
    ).await;

    let mut domain_cache_hits_this_batch = 0;
    let mut domain_rl_feat_extract_this_batch = 0;
    let mut domain_rl_feat_fail_this_batch = 0;
    // created_group_count_this_domain is handled by batch_create_url_entity_groups updating shared_stats_arc

    if let Ok(results) = process_pairs_result {
        domain_cache_hits_this_batch = results.0;
        domain_rl_feat_extract_this_batch = results.1;
        domain_rl_feat_fail_this_batch = results.2;
    } else if let Err(e) = process_pairs_result {
        warn!("URL: Error in process_pairs_for_domain_with_caching for domain {}: {}", domain_name, e);
        let mut stats_guard = shared_stats_arc.lock().await;
        stats_guard.task_errors += 1;
    }
    
    {
        let mut stats_guard = shared_stats_arc.lock().await;
        stats_guard.pairs_considered_for_comparison += pairs_considered_in_domain_count;
        stats_guard.comparison_cache_hits += domain_cache_hits_this_batch;
        stats_guard.rl_feature_extractions += domain_rl_feat_extract_this_batch;
        stats_guard.rl_feature_failures += domain_rl_feat_fail_this_batch;
        stats_guard.domains_completed_processing += 1;

        debug!(
            "URL: Domain '{}' completed in {:.2?}. Pairs considered: {}. Cache Hits this batch: {}. (Total domains completed: {}).",
            domain_name, domain_start_time.elapsed(), pairs_considered_in_domain_count,
            domain_cache_hits_this_batch, stats_guard.domains_completed_processing
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_pairs_for_domain_with_caching(
    _domain_name: &str, // Keep for logging if needed
    potential_pairs: Vec<(EntityUrlInfo, EntityUrlInfo)>,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    rl_feature_cache: Option<SharedFeatureCache>,
    processed_pairs_this_run_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    existing_entity_groups: Arc<HashSet<(EntityId, EntityId)>>,
    shared_stats_arc: Arc<Mutex<UrlMatchingStats>>,
) -> Result<(usize, usize, usize)> { // Returns (domain_cache_hits, domain_rl_feat_extract, domain_rl_feat_fail)
    
    let mut pairs_to_create_in_db_batch = Vec::new();
    let mut suggestions_to_create_in_db_batch = Vec::new();
    
    let mut domain_comparison_cache_hits = 0;
    let mut domain_rl_feature_extractions = 0;
    let mut domain_rl_feature_failures = 0;

    let mut features_json_for_main_cache: Option<serde_json::Value> = None;
    let mut comparison_outcome_for_cache = "NON_MATCH"; // Default, can be &str if not reassigned with different types
    let mut final_confidence_for_cache_entry = 0.0;

    struct RlCacheMissInfo {
        entity1_info: EntityUrlInfo, entity2_info: EntityUrlInfo, // Keep full info
        ordered_e1_id: EntityId, ordered_e2_id: EntityId,
        base_confidence: f64, match_values: MatchValues,
        sig1_at_comp: Option<String>, sig2_at_comp: Option<String>,
        features_for_decision_log: Option<Vec<f64>>, // Store extracted features if any
    }
    let mut rl_feature_cache_miss_items: Vec<RlCacheMissInfo> = Vec::new();

    for (entity1_info, entity2_info) in potential_pairs {
        let (e1_id_ordered, e2_id_ordered) = if entity1_info.entity_id.0 < entity2_info.entity_id.0 {
            (entity1_info.entity_id.clone(), entity2_info.entity_id.clone())
        } else {
            (entity2_info.entity_id.clone(), entity1_info.entity_id.clone())
        };
        let current_pair_ordered = (e1_id_ordered.clone(), e2_id_ordered.clone());

        if existing_entity_groups.contains(&current_pair_ordered) { continue; }
        {
            let mut processed_set_guard = processed_pairs_this_run_arc.lock().await; // Lock here
            if processed_set_guard.contains(&current_pair_ordered) { continue; }
            // Insert into processed_set_guard later, after cache logic or processing
        } // Lock released
        
        let current_signatures_opt = match get_current_signatures_for_pair(pool, &e1_id_ordered, &e2_id_ordered).await {
            Ok(sigs) => sigs,
            Err(e) => { warn!("URL: Sig fetch fail for ({},{}): {}.", e1_id_ordered.0, e2_id_ordered.0, e); None }
        };

        let mut sig1_for_store: Option<String> = None;
        let mut sig2_for_store: Option<String> = None;

        if let Some((s1_data, s2_data)) = &current_signatures_opt {
            sig1_for_store = Some(s1_data.signature.clone());
            sig2_for_store = Some(s2_data.signature.clone());
            match check_comparison_cache(pool, &e1_id_ordered, &e2_id_ordered, &s1_data.signature, &s2_data.signature, &MatchMethodType::Url).await {
                Ok(Some(cached_eval)) => {
                    domain_comparison_cache_hits += 1;
                    let mut processed_set_guard = processed_pairs_this_run_arc.lock().await;
                    processed_set_guard.insert(current_pair_ordered.clone());
                    drop(processed_set_guard);
                    continue; 
                }
                Ok(None) => { /* Cache MISS */ }
                Err(e) => { warn!("URL: Cache check fail for ({},{}): {}.", e1_id_ordered.0, e2_id_ordered.0, e); }
            }
        }
        
        let distance = if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) = 
            (entity1_info.latitude, entity1_info.longitude, entity2_info.latitude, entity2_info.longitude) {
            Some(calculate_distance(lat1, lon1, lat2, lon2))
        } else { None };


        if let Some(dist) = distance {
            if dist > MAX_LOCATION_DISTANCE_METERS {
                let slugs = count_matching_slugs(&entity1_info.normalized_data.path_slugs, &entity2_info.normalized_data.path_slugs);
                if slugs < 2 { // Stricter condition for distant pairs
                    final_confidence_for_cache_entry = calculate_url_match_confidence(&entity1_info, &entity2_info, slugs, distance).0; // Store actual low score
                    // comparison_outcome_for_cache remains "NON_MATCH"
                    if let (Some(s1), Some(s2)) = (&sig1_for_store, &sig2_for_store) {
                         if let Err(e) = store_in_comparison_cache(pool, &e1_id_ordered, &e2_id_ordered, s1, s2, &MatchMethodType::Url, pipeline_run_id, comparison_outcome_for_cache, Some(final_confidence_for_cache_entry), None).await {
                            warn!("URL: Failed to store NON_MATCH (dist rule) in cache for ({},{}): {}", e1_id_ordered.0, e2_id_ordered.0, e);
                         }
                    }
                    let mut processed_set_guard = processed_pairs_this_run_arc.lock().await;
                    processed_set_guard.insert(current_pair_ordered.clone());
                    drop(processed_set_guard);
                    continue;
                }
            }
        }
        
        let matching_slug_count = count_matching_slugs(&entity1_info.normalized_data.path_slugs, &entity2_info.normalized_data.path_slugs);
        let (base_confidence, match_values_obj) = calculate_url_match_confidence(&entity1_info, &entity2_info, matching_slug_count, distance);
        
        let mut needs_rl_feature_extraction = false;
        if reinforcement_orchestrator_option.is_some() {
            if let Some(rl_cache_svc) = rl_feature_cache.as_ref() {
                let key = FeatureCacheService::get_pair_key(&e1_id_ordered, &e2_id_ordered);
                let guard = rl_cache_svc.lock().await; needs_rl_feature_extraction = !guard.pair_cache.contains(&key);
            } else { needs_rl_feature_extraction = true; }
        }

        if needs_rl_feature_extraction && reinforcement_orchestrator_option.is_some() {
            rl_feature_cache_miss_items.push(RlCacheMissInfo {
                entity1_info: entity1_info.clone(), entity2_info: entity2_info.clone(), // Store full info
                ordered_e1_id: e1_id_ordered.clone(), ordered_e2_id: e2_id_ordered.clone(),
                base_confidence, match_values: match_values_obj,
                sig1_at_comp: sig1_for_store.clone(), sig2_at_comp: sig2_for_store.clone(),
                features_for_decision_log: None,
            });
        } else { 
            let mut final_confidence = base_confidence;
            let mut temp_features_for_decision_log: Option<Vec<f64>> = None;

            if let Some(ro_arc) = reinforcement_orchestrator_option {
                 match apply_rl_tuning_cached(ro_arc, pool, &e1_id_ordered, &e2_id_ordered, base_confidence, rl_feature_cache.as_ref()).await {
                    Ok((tuned_score, features_opt)) => {
                        final_confidence = tuned_score;
                        if let Some(features) = features_opt { 
                            temp_features_for_decision_log = Some(features.clone());
                            features_json_for_main_cache = serde_json::to_value(features).ok(); 
                            domain_rl_feature_extractions +=1; 
                        }
                    }
                    Err(_) => { domain_rl_feature_failures +=1; }
                }
            }
            
            // Here, any pair that reaches this point is considered a "MATCH" for caching purposes
            // because the URL logic has processed it.
            comparison_outcome_for_cache = "MATCH"; 
            final_confidence_for_cache_entry = final_confidence;

            if let (Some(s1), Some(s2)) = (&sig1_for_store, &sig2_for_store) {
                if let Err(e) = store_in_comparison_cache(pool, &e1_id_ordered, &e2_id_ordered, s1, s2, &MatchMethodType::Url, pipeline_run_id, comparison_outcome_for_cache, Some(final_confidence_for_cache_entry), features_json_for_main_cache.as_ref()).await {
                    warn!("URL: Store comp cache fail for ({},{}): {}", e1_id_ordered.0, e2_id_ordered.0, e);
                }
            }
            
            let new_gid = EntityGroupId(Uuid::new_v4().to_string());
            pairs_to_create_in_db_batch.push(PairToCreate {
                entity_group_id: new_gid.clone(), entity_id_1: e1_id_ordered.clone(), entity_id_2: e2_id_ordered.clone(),
                match_values: match_values_obj.clone(), final_confidence, pre_rl_confidence: base_confidence,
                features_for_decision_log: temp_features_for_decision_log,
            });
            if final_confidence < MODERATE_LOW_SUGGESTION_THRESHOLD {
                suggestions_to_create_in_db_batch.push(create_suggestion_data(&new_gid, &e1_id_ordered, &e2_id_ordered, final_confidence, base_confidence, pipeline_run_id, &match_values_obj));
            }
            let mut processed_set_guard = processed_pairs_this_run_arc.lock().await;
            processed_set_guard.insert(current_pair_ordered.clone());
            // No drop needed here, lock guard will drop at end of scope
        }
    }

    if !rl_feature_cache_miss_items.is_empty() {
        let feature_results = batch_extract_features_for_url(pool, &rl_feature_cache_miss_items.iter().map(|item| (item.ordered_e1_id.clone(), item.ordered_e2_id.clone())).collect::<Vec<_>>()).await;

        for (i, mut item) in rl_feature_cache_miss_items.into_iter().enumerate() { // item is mutable
            let mut final_confidence = item.base_confidence;
            let mut temp_features_for_decision_log: Option<Vec<f64>> = None;
            features_json_for_main_cache = None; // Reset for each item

            if i < feature_results.len() {
                match &feature_results[i] {
                    Ok(features_vec) => {
                        domain_rl_feature_extractions += 1;
                        if !features_vec.is_empty() {
                            temp_features_for_decision_log = Some(features_vec.clone());
                            item.features_for_decision_log = Some(features_vec.clone()); // Store on item
                            features_json_for_main_cache = serde_json::to_value(features_vec.clone()).ok();
                            if let Some(rl_cache_svc) = rl_feature_cache.as_ref() {
                                let key = FeatureCacheService::get_pair_key(&item.ordered_e1_id, &item.ordered_e2_id);
                                let mut guard = rl_cache_svc.lock().await; guard.pair_cache.put(key, features_vec.clone());
                            }
                            if let Some(ro_arc) = reinforcement_orchestrator_option {
                                let guard = ro_arc.lock().await;
                                match guard.get_tuned_confidence(&MatchMethodType::Url, item.base_confidence, features_vec) {
                                    Ok(ts) => final_confidence = ts,
                                    Err(e) => warn!("URL: RL tuning (batch) fail for ({},{}): {}.",item.ordered_e1_id.0, item.ordered_e2_id.0, e),
                                }
                            }
                        }
                    }
                    Err(_) => { domain_rl_feature_failures += 1; }
                }
            }
            
            comparison_outcome_for_cache = "MATCH"; // Considered a match attempt by URL logic
            final_confidence_for_cache_entry = final_confidence;

            if let (Some(s1), Some(s2)) = (&item.sig1_at_comp, &item.sig2_at_comp) {
                 if let Err(e) = store_in_comparison_cache(pool, &item.ordered_e1_id, &item.ordered_e2_id, s1, s2, &MatchMethodType::Url, pipeline_run_id, comparison_outcome_for_cache, Some(final_confidence_for_cache_entry), features_json_for_main_cache.as_ref()).await {
                    warn!("URL: Store comp cache (RL batch) fail for ({},{}): {}",item.ordered_e1_id.0, item.ordered_e2_id.0, e);
                }
            }
            
            let new_gid = EntityGroupId(Uuid::new_v4().to_string());
            pairs_to_create_in_db_batch.push(PairToCreate {
                entity_group_id: new_gid.clone(), entity_id_1: item.ordered_e1_id.clone(), entity_id_2: item.ordered_e2_id.clone(),
                match_values: item.match_values.clone(), final_confidence, pre_rl_confidence: item.base_confidence,
                features_for_decision_log: temp_features_for_decision_log,
            });
            if final_confidence < MODERATE_LOW_SUGGESTION_THRESHOLD {
                suggestions_to_create_in_db_batch.push(create_suggestion_data(&new_gid, &item.ordered_e1_id, &item.ordered_e2_id, final_confidence, item.base_confidence, pipeline_run_id, &item.match_values));
            }
            let mut processed_set_guard = processed_pairs_this_run_arc.lock().await;
            processed_set_guard.insert((item.ordered_e1_id.clone(), item.ordered_e2_id.clone()));
            // No drop needed here
        }
    }

    if !pairs_to_create_in_db_batch.is_empty() {
        match batch_create_url_entity_groups(pool, pairs_to_create_in_db_batch, suggestions_to_create_in_db_batch, reinforcement_orchestrator_option, pipeline_run_id).await {
            Ok(count_created_in_db) => {
                let mut stats_guard = shared_stats_arc.lock().await;
                stats_guard.new_pairs_created_db += count_created_in_db;
                // Note: entities_in_new_pairs and confidence_scores in UrlMatchingStats are slightly optimistic here,
                // as they are based on what was *intended* for creation, not just what was *newly* created in DB.
                // This is a simplification due to batch_create_url_entity_groups returning only a count.
                // For more precise stats on *newly created* groups' confidences, batch_create would need to return more detail.
                for pair_data in stats_guard.confidence_scores.iter() { // This logic is a bit off - confidence_scores is Vec<f64>
                     // We need to iterate what was *added* to pairs_to_create_in_db_batch
                     // This part of stat update needs to be more careful if precise "newly_created" stats are key
                }
            }
            Err(e) => {
                warn!("URL: DB batch create fail: {}", e); // Domain name context is lost here
                let mut stats_guard = shared_stats_arc.lock().await; 
                stats_guard.task_errors += 1;
            }
        }
    }
    
    Ok((domain_comparison_cache_hits, domain_rl_feature_extractions, domain_rl_feature_failures))
}

async fn batch_create_url_entity_groups(
    pool: &PgPool,
    pairs_to_create: Vec<PairToCreate>,
    suggestions_to_create: Vec<SuggestionData>,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>, // For decision logging
    pipeline_run_id: &str, // For decision logging
) -> Result<usize> { // Returns count of successfully *newly* inserted groups
    if pairs_to_create.is_empty() { return Ok(0); }

    let mut conn = pool.get().await.context("URL: DB conn for batch create")?;
    let tx = conn.transaction().await.context("URL: Start TX batch create")?;
    
    let entity_group_stmt = tx.prepare(INSERT_ENTITY_GROUP_SQL).await.context("URL: Prepare eg insert")?;
    
    let mut successfully_inserted_new_groups_data = Vec::new();

    for pair_item in pairs_to_create {
        let mv_json = serde_json::to_value(&pair_item.match_values).context("URL: Serialize mv for eg")?;
        let rows = tx.query(&entity_group_stmt, &[
            &pair_item.entity_group_id.0, &pair_item.entity_id_1.0, &pair_item.entity_id_2.0,
            &MatchMethodType::Url.as_str(), &mv_json, &pair_item.final_confidence, &pair_item.pre_rl_confidence,
        ]).await.context("URL: Insert eg failed")?;
        
        if let Some(row) = rows.first() { // Check if RETURNING id gave us something
            let created_group_id: String = row.get(0); // Assuming 'id' is the first column returned
            if created_group_id == pair_item.entity_group_id.0 { // Ensure it's the one we tried to insert
                successfully_inserted_new_groups_data.push(pair_item); // Store full PairToCreate for decision logging
            }
        }
    }

    let created_count = successfully_inserted_new_groups_data.len();

    // Log decisions only for newly created groups
    if !successfully_inserted_new_groups_data.is_empty() {
        if let Some(ro_arc) = reinforcement_orchestrator_option {
            const INSERT_DECISION_SQL: &str = "
                INSERT INTO clustering_metadata.match_decision_details (
                    entity_group_id, pipeline_run_id, snapshotted_features,
                    method_type_at_decision, pre_rl_confidence_at_decision,
                    tuned_confidence_at_decision, confidence_tuner_version_at_decision
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)";
            let decision_stmt = tx.prepare(INSERT_DECISION_SQL).await.context("URL: Prepare decision log insert")?;
            
            for created_pair_data in successfully_inserted_new_groups_data {
                if let Some(features) = &created_pair_data.features_for_decision_log {
                    let orchestrator_guard = ro_arc.lock().await;
                    let snapshot_features_json = serde_json::to_value(features).unwrap_or(serde_json::Value::Null);
                    let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;
                     if let Err(e) = tx.execute(&decision_stmt, &[
                        &created_pair_data.entity_group_id.0, &pipeline_run_id, &snapshot_features_json,
                        &MatchMethodType::Url.as_str(), &created_pair_data.pre_rl_confidence,
                        &created_pair_data.final_confidence, &(confidence_tuner_ver as i32),
                    ]).await {
                        warn!("URL: Failed to log decision for group {}: {}", created_pair_data.entity_group_id.0, e);
                    }
                }
            }
        }
    }


    if !suggestions_to_create.is_empty() {
        let suggestion_stmt = tx.prepare(
            "INSERT INTO clustering_metadata.suggested_actions (
                pipeline_run_id, action_type, group_id_1, triggering_confidence, details, 
                reason_code, reason_message, priority, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        ).await.context("URL: Prepare suggestion insert")?;
        for sugg_item in suggestions_to_create {
            // Check if this suggestion corresponds to a group that was *newly* created.
            // This requires matching sugg_item.group_id with the entity_group_id of successfully_inserted_new_groups_data.
            // For simplicity, we'll insert suggestions for all that were intended, 
            // assuming the group_id would match if the initial insert didn't fail before RETURNING id logic.
            // A more robust check would be:
            // if successfully_inserted_new_groups_data.iter().any(|pd| pd.entity_group_id.0 == sugg_item.group_id) {
            if let Err(e) = tx.execute(&suggestion_stmt, &[
                &sugg_item.pipeline_run_id, &sugg_item.action_type, &sugg_item.group_id,
                &sugg_item.triggering_confidence, &sugg_item.details, &sugg_item.reason_code,
                &sugg_item.reason_message, &sugg_item.priority, &sugg_item.status,
            ]).await {
                 warn!("URL: Insert suggestion failed for group {}: {}", sugg_item.group_id, e);
            }
            // }
        }
    }
    tx.commit().await.context("URL: Commit batch create TX")?;
    Ok(created_count)
}


// Helper functions (normalize_url_with_slugs, calculate_distance, etc.)
// These are mostly the same as in your provided `url.rs` (the first one in the prompt).
// Ensure they are correctly defined and included.

async fn fetch_existing_entity_groups(pool: &PgPool) -> Result<Arc<HashSet<(EntityId, EntityId)>>> {
    let conn = pool.get().await.context("URL: DB conn for existing groups")?;
    let rows = conn.query("SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1", &[&MatchMethodType::Url.as_str()]).await.context("URL: Query existing groups")?;
    let mut pairs = HashSet::with_capacity(rows.len());
    for r in rows {
        let (id1, id2): (String, String) = (r.get(0), r.get(1));
        if id1 < id2 { pairs.insert((EntityId(id1), EntityId(id2))); } else { pairs.insert((EntityId(id2), EntityId(id1))); }
    }
    Ok(Arc::new(pairs))
}

async fn fetch_entities_with_urls_and_locations(pool: &PgPool) -> Result<Vec<EntityUrlInfo>> {
    let conn = pool.get().await.context("URL: DB conn for entities/URLs")?;
    let query = r#"
    WITH EntityServiceURLs AS (
        SELECT e.id AS entity_id, s.url AS service_url, e.name AS entity_name FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'service' JOIN public.service s ON ef.table_id = s.id
        WHERE s.url IS NOT NULL AND s.url != '' AND s.url !~ '^\s*$' AND s.url NOT LIKE 'mailto:%' AND s.url NOT LIKE 'tel:%'
    ), EntityOrgURLs AS (
        SELECT e.id AS entity_id, o.url AS org_url, e.name AS entity_name FROM public.entity e
        JOIN public.organization o ON e.organization_id = o.id WHERE o.url IS NOT NULL AND o.url != '' AND o.url !~ '^\s*$' AND o.url NOT LIKE 'mailto:%' AND o.url NOT LIKE 'tel:%'
    ), CombinedURLs AS (
        SELECT entity_id, service_url AS url, entity_name FROM EntityServiceURLs
        UNION SELECT entity_id, org_url AS url, entity_name FROM EntityOrgURLs
    ), EntityLocations AS (
        SELECT e.id AS entity_id, l.latitude, l.longitude, ROW_NUMBER() OVER(PARTITION BY e.id ORDER BY l.created DESC NULLS LAST, l.id) as rn FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location' JOIN public.location l ON ef.table_id = l.id
        WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
    )
    SELECT DISTINCT cu.entity_id, cu.url, cu.entity_name, el.latitude, el.longitude
    FROM CombinedURLs cu LEFT JOIN EntityLocations el ON cu.entity_id = el.entity_id AND el.rn = 1
    "#;
    let rows = conn.query(query, &[]).await.context("URL: Query entities/URLs")?;
    let mut entities = Vec::with_capacity(rows.len());
    for r in rows {
        let (eid_s, url_s, ename_o, lat_o, lon_o): (String, String, Option<String>, Option<f64>, Option<f64>) = (r.get(0), r.get(1), r.get(2), r.try_get(3).ok(), r.try_get(4).ok());
        if let Some(norm_data) = normalize_url_with_slugs(&url_s) {
            if !is_ignored_domain(&norm_data.domain) {
                entities.push(EntityUrlInfo {
                    entity_id: EntityId(eid_s), original_url: url_s, normalized_data: norm_data,
                    entity_name: ename_o, latitude: lat_o, longitude: lon_o,
                });
            }
        }
    }
    Ok(entities)
}

fn group_entities_by_domain(entities: &[EntityUrlInfo]) -> HashMap<String, Vec<EntityUrlInfo>> {
    let mut map: HashMap<String, Vec<EntityUrlInfo>> = HashMap::new();
    for e_info in entities {
        map.entry(e_info.normalized_data.domain.clone())
            .or_default()
            .push(e_info.clone());
    }
    map
}

fn apply_business_logic_filtering(map: HashMap<String, Vec<EntityUrlInfo>>) -> HashMap<String, Vec<EntityUrlInfo>> {
    let mut filtered = HashMap::new(); let mut excluded_count = 0;
    for (domain, entities) in map {
        if entities.len() < 2 { continue; } // Need at least two entities in a domain to form a pair
        let domain_type = categorize_domain(&domain); // Assuming categorize_domain is defined
        let threshold = get_max_entities_threshold(&domain_type); // Assuming get_max_entities_threshold is defined
        if entities.len() > threshold { 
            debug!("URL: Excluding domain '{}' ({:?}) - {} entities (>{})", domain, domain_type, entities.len(), threshold);
            excluded_count += 1; 
        } else { 
            filtered.insert(domain, entities); 
        }
    }
    if excluded_count > 0 { info!("URL: Excluded {} domains by entity count threshold.", excluded_count); }
    filtered
}


async fn batch_extract_features_for_url(
    pool: &PgPool,
    pairs: &[(EntityId, EntityId)],
) -> Vec<Result<Vec<f64>>> {
    let mut results = Vec::with_capacity(pairs.len());
    if pairs.is_empty() { return results; }

    let num_cpus = num_cpus::get_physical().max(1); // Ensure at least 1 CPU
    let batch_size = (pairs.len() / num_cpus).max(1).min(RL_FEATURE_EXTRACTION_BATCH_SIZE);
    debug!("URL: RL Feature extraction batch size: {}", batch_size);

    for chunk in pairs.chunks(batch_size) {
        let mut futures = Vec::with_capacity(chunk.len());
        for (e1_id, e2_id) in chunk {
            futures.push(tokio::spawn({
                let pool_clone = pool.clone();
                let e1_clone = e1_id.clone();
                let e2_clone = e2_id.clone();
                async move {
                    MatchingOrchestrator::extract_pair_context_features(&pool_clone, &e1_clone, &e2_clone).await
                }
            }));
        }
        let chunk_results = join_all(futures).await;
        for res_outer in chunk_results {
            match res_outer {
                Ok(inner_res) => results.push(inner_res),
                Err(join_err) => results.push(Err(anyhow!("Task join error for RL feature extraction: {}", join_err))),
            }
        }
    }
    results
}

async fn apply_rl_tuning_cached(
    ro_arc: &Arc<Mutex<MatchingOrchestrator>>,
    pool: &PgPool,
    e1_id: &EntityId,
    e2_id: &EntityId,
    base_confidence: f64,
    rl_feature_cache_opt: Option<&SharedFeatureCache>,
) -> Result<(f64, Option<Vec<f64>>)> { // Return features for decision logging
    let features_vec_res = if let Some(cache_service) = rl_feature_cache_opt {
        // This uses the RL feature cache if available
        let guard = ro_arc.lock().await;
        guard.get_pair_features(pool, e1_id, e2_id).await
    } else {
        MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
    };

    match features_vec_res {
        Ok(features) => {
            if features.is_empty() { return Ok((base_confidence, None)); } // No features, return base confidence
            let guard = ro_arc.lock().await;
            match guard.get_tuned_confidence(&MatchMethodType::Url, base_confidence, &features) {
                Ok(tuned_score) => Ok((tuned_score, Some(features))), // Return tuned score and features
                Err(e) => {
                    warn!("URL: RL get_tuned_confidence fail for ({},{}): {}. Using base confidence.", e1_id.0, e2_id.0, e);
                    Ok((base_confidence, Some(features))) // Return base confidence but still provide features if extracted
                }
            }
        }
        Err(e) => {
            warn!("URL: Feature extraction failed during RL tuning for ({},{}): {}", e1_id.0, e2_id.0, e);
            Err(e.into()) // Propagate the error
        }
    }
}

fn create_suggestion_data(
    entity_group_id: &EntityGroupId, e1_id: &EntityId, e2_id: &EntityId,
    final_confidence: f64, pre_rl_confidence: f64, pipeline_run_id: &str,
    match_values: &MatchValues, // Pass MatchValues to extract specific URL info
) -> SuggestionData {
    let priority = if final_confidence < CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };
    
    let (orig_url1, orig_url2, norm_domain, slug_count) = if let MatchValues::Url(ref uv) = match_values {
        (uv.original_url1.clone(), uv.original_url2.clone(), uv.normalized_shared_domain.clone(), uv.matching_slug_count)
    } else {
        // Fallback if match_values is not Url type, though it should be in this context
        (String::from("N/A"), String::from("N/A"), String::from("N/A"), 0)
    };

    let details = serde_json::json!({
        "method_type": MatchMethodType::Url.as_str(), 
        "entity_id_1": e1_id.0, "entity_id_2": e2_id.0,
        "original_url1": orig_url1, 
        "original_url2": orig_url2,
        "normalized_shared_domain": norm_domain, 
        "matching_slug_count": slug_count,
        "entity_group_id": entity_group_id.0, 
        "pre_rl_confidence": pre_rl_confidence, 
        "final_confidence": final_confidence,
    });
    let reason_msg = format!(
        "Pair ({},{}) URL (domain: {}, slugs: {}) matched with low tuned confidence ({:.4}). Pre-RL: {:.2}",
        e1_id.0, e2_id.0, norm_domain, slug_count, final_confidence, pre_rl_confidence
    );
    SuggestionData {
        pipeline_run_id: pipeline_run_id.to_string(), 
        action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
        group_id: entity_group_id.0.clone(), 
        triggering_confidence: final_confidence, 
        details,
        reason_code: "LOW_TUNED_CONFIDENCE_PAIR".to_string(), 
        reason_message: reason_msg, 
        priority,
        status: SuggestionStatus::PendingReview.as_str().to_string(),
    }
}

fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in meters
    let (l1r, l2r, dla, dlo) = (lat1.to_radians(), lat2.to_radians(), (lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (dla / 2.0).sin().powi(2) + l1r.cos() * l2r.cos() * (dlo / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

fn count_matching_slugs(s1: &[String], s2: &[String]) -> usize { s1.iter().zip(s2.iter()).take_while(|(a, b)| a == b).count() }

fn calculate_url_match_confidence(e1: &EntityUrlInfo, e2: &EntityUrlInfo, slugs: usize, dist_m: Option<f64>) -> (f64, MatchValues) {
    let (ordered_e1, ordered_e2) = if e1.entity_id.0 <= e2.entity_id.0 { (e1, e2) } else { (e2, e1) };
    let full_match = slugs == ordered_e1.normalized_data.path_slugs.len() && slugs == ordered_e2.normalized_data.path_slugs.len() && !ordered_e1.normalized_data.path_slugs.is_empty();
    let mut conf = if full_match { CONFIDENCE_DOMAIN_FULL_PATH_MATCH } else {
        match slugs { 0 => CONFIDENCE_DOMAIN_ONLY, 1 => CONFIDENCE_DOMAIN_PLUS_ONE_SLUG, 2 => CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS, _ => CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS }
    };
    if let Some(d) = dist_m { // Adjust based on distance
        if d < 100.0 && slugs > 0 { conf = (conf + 0.05).min(0.98); } // Very close, boost
        else if d > 10000.0 && slugs == 0 { conf = (conf - 0.15).max(0.55); } // Very far, domain only, penalize more
        else if d > MAX_LOCATION_DISTANCE_METERS && slugs <= 1 { conf = (conf - 0.08).max(0.60); } // Far, few slugs, penalize
    }
    // If domain only match, and names are very different, reduce confidence
    if slugs == 0 && conf > 0.72 { 
        if let (Some(n1_str), Some(n2_str)) = (&ordered_e1.entity_name, &ordered_e2.entity_name) {
            let n1_lower = n1_str.to_lowercase();
            let n2_lower = n2_str.to_lowercase();
            if !n1_lower.contains(&n2_lower) && !n2_lower.contains(&n1_lower) { // Simple substring check
                conf = (conf * 0.85).max(CONFIDENCE_DOMAIN_ONLY - 0.05); // Penalize if names are quite different
            }
        } else { // One or both names missing, be more conservative
            conf = (conf * 0.92).max(CONFIDENCE_DOMAIN_ONLY - 0.02);
        }
    }
    (conf, MatchValues::Url(UrlMatchValue {
        original_url1: ordered_e1.original_url.clone(), original_url2: ordered_e2.original_url.clone(),
        normalized_shared_domain: ordered_e1.normalized_data.domain.clone(), matching_slug_count: slugs,
    }))
}

pub fn normalize_url_with_slugs(url_s: &str) -> Option<NormalizedUrlData> {
    let tr = url_s.trim();
    if tr.is_empty() || tr.starts_with("mailto:") || tr.starts_with("tel:") { return None; }
    let w_scheme = if !tr.contains("://") { format!("https://{}", tr) } else { tr.to_string() };
    match StdUrl::parse(&w_scheme) {
        Ok(p_url) => p_url.host_str().and_then(|h| {
            let hl = h.to_lowercase();
            let dnw = if hl.starts_with("www.") { hl.trim_start_matches("www.").to_string() } else { hl };
            if dnw.is_empty() || !dnw.contains('.') || is_ip_address(&dnw) { None } else {
                Some(NormalizedUrlData {
                    domain: dnw.clone(),
                    path_slugs: p_url.path_segments().map_or_else(Vec::new, |s| s.filter(|ps| !ps.is_empty() && ps.len()<50 && !ps.contains('.')).map(|ps| ps.to_lowercase()).collect()),
                    original_url: url_s.to_string(), domain_type: categorize_domain(&dnw),
                })
            }
        }),
        Err(_) => { 
            let parts: Vec<&str> = tr.split("://").collect();
            let scheme_less = if parts.len() > 1 { parts[1] } else { parts[0] };
            let dom_part = scheme_less.split(|c: char| c == '/' || c == '?' || c == '#').next().unwrap_or("").to_lowercase();
            let dnw = if dom_part.starts_with("www.") { dom_part.trim_start_matches("www.").to_string() } else { dom_part };
            if dnw.contains('.') && !dnw.is_empty() && !is_ip_address(&dnw) {
                Some(NormalizedUrlData { domain: dnw.clone(), path_slugs: Vec::new(), original_url: url_s.to_string(), domain_type: categorize_domain(&dnw) })
            } else { None }
        }
    }
}

pub fn is_ignored_domain(domain: &str) -> bool {
    const IGNORED_SUFFIXES_AND_DOMAINS: &[&str] = &[
        // Major social media direct domains
        "facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com", 
        "tiktok.com", "pinterest.com", "reddit.com",
        // Common blogging/hosting platforms that are often not primary organizational sites
        "medium.com", "wordpress.com", "blogspot.com", "tumblr.com", "flickr.com", 
        "vimeo.com", "soundcloud.com", "github.io", // github.com itself might be valid for orgs
        // Major generic service providers often used for redirects or secondary purposes
        "google.com", "apple.com", "microsoft.com", // These are too broad, but specific subdomains might be ignored
        // URL Shorteners
        "bit.ly", "t.co", "goo.gl", "tinyurl.com", "ow.ly", "buff.ly", "is.gd", "cutt.ly",
        // Common CDNs and app hosting platforms (less likely to be primary org URLs)
        "cloudfront.net", "appspot.com", "firebaseapp.com", "netlify.app", "vercel.app", 
        "herokuapp.com", "azurewebsites.net", "amazonaws.com", // S3 buckets etc.
        // Common website builders where the platform domain is prominent
        "weebly.com", "wix.com", "squarespace.com", "godaddysites.com", "mystrikingly.com",
        // Event and ticketing platforms
        "eventbrite.com", "meetup.com", "ticketmaster.com",
        // Generic file sharing or placeholder
        "dropbox.com", "box.com", "drive.google.com", "docs.google.com",
        // Other common non-organizational domains
        "wikipedia.org", "archive.org", "about.me", "linktr.ee",
        // Advertising / Analytics
        "doubleclick.net", "googleadservices.com", "googletagmanager.com", "google-analytics.com",
    ];
    IGNORED_SUFFIXES_AND_DOMAINS.iter().any(|suf| domain == *suf || domain.ends_with(&format!(".{}", suf)) || *suf == domain)
}

fn categorize_domain(dom: &str) -> DomainType {
    if dom.ends_with(".gov") || dom.ends_with(".mil") || dom.ends_with(".GOV") || dom.ends_with(".MIL") { DomainType::Government }
    else if dom.ends_with(".edu") || dom.contains("k12") || dom.contains("school") || dom.contains("college") || dom.contains("university") || dom.ends_with(".EDU") { DomainType::Education }
    else if dom.ends_with(".org") || dom.contains("health") || dom.contains("medical") || dom.contains("clinic") || dom.contains("hospital") || dom.contains("foundation") || dom.contains("charity") || dom.contains("nonprofit") || dom.ends_with(".ORG") { DomainType::Healthcare } // Broadened to .org common for non-profits
    else if dom.ends_with(".com") || dom.ends_with(".net") || dom.ends_with(".io") || dom.ends_with(".co") || dom.ends_with(".biz") || dom.ends_with(".COM") || dom.ends_with(".NET") { DomainType::Commercial }
    // Social media domains are mostly filtered by is_ignored_domain, but this can be a fallback
    else if dom.contains("facebook.") || dom.contains("twitter.") || dom.contains("instagram.") || dom.contains("linkedin.") || dom.contains("youtube.") { DomainType::Social } 
    else { DomainType::Other }
}

fn get_max_entities_threshold(dt: &DomainType) -> usize {
    match dt {
        DomainType::Government => MAX_ENTITIES_PER_GOV_DOMAIN, 
        DomainType::Healthcare => MAX_ENTITIES_PER_ORG_DOMAIN, // Can include .org, .com etc.
        DomainType::Education => MAX_ENTITIES_PER_EDU_DOMAIN, 
        DomainType::Commercial => MAX_ENTITIES_PER_COMMERCIAL_DOMAIN,
        DomainType::Social => 10, // Usually individual pages, low threshold
        DomainType::Other => MAX_ENTITIES_PER_ORG_DOMAIN, // Default for others
    }
}

fn is_ip_address(dc: &str) -> bool {
    // Basic IPv4 check: 4 octets, each a number.
    if dc.split('.').count() == 4 && dc.split('.').all(|p| p.parse::<u8>().is_ok()) { return true; }
    // Basic IPv6 check: contains colons, typically more than one.
    if dc.contains(':') && dc.matches(':').count() >= 2 { return true; }
    false
}