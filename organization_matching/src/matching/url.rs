// src/matching/url.rs - Progress Callback Integration
use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use url::Url as StdUrl;
use uuid::Uuid;

use crate::matching::db::{
    batch_insert_match_decision_details, batch_upsert_entity_groups, EntityGroupBatchData,
    MatchDecisionDetailBatchData,
};
use crate::models::matching::{AnyMatchResult, MatchResult, MatchValues, UrlMatchDetail, UrlMatchValues};
use crate::models::stats_models::{MatchMethodStats, MatchMethodType};
use crate::rl::orchestrator::RLOrchestrator;
use crate::rl::SharedFeatureCache;
use crate::utils::db_connect::PgPool;
use crate::utils::progress_bars::logging::MatchingLogger;
use crate::utils::pipeline_state::{
    batch_check_comparison_cache,
    batch_get_current_signatures_for_pairs,
    batch_store_in_comparison_cache,
    batch_check_entity_completion_status, // NEW
    batch_mark_entity_completion,         // NEW
    ComparisonCacheEntry,
    EntitySignature as SignatureData,
    EntityCompletionCheck,                // NEW
};
use crate::utils::constants::MAX_DISTANCE_FOR_SAME_CITY_METERS;
use tokio_postgres::types::ToSql; // Required for dynamic query parameters

// NEW IMPORTS: Progress callback functionality
use crate::utils::progress_bars::progress_callback::ProgressCallback;
use crate::{update_progress, update_detailed_progress};

const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;
const MAX_PARALLEL_DOMAINS_IN_FLIGHT: usize = 16;
const DOMAIN_PROCESSING_BATCH_SIZE: usize = 20;
const MAX_PAIRS_PER_DOMAIN_SOFT_WARN: usize = 2000;
const MAX_PAIRS_PER_DOMAIN_HARD_LIMIT: usize = 5000;
const MAX_ENTITIES_PER_GOV_DOMAIN: usize = 20;
const MAX_ENTITIES_PER_ORG_DOMAIN: usize = 50;
const MAX_ENTITIES_PER_EDU_DOMAIN: usize = 100;
const MAX_ENTITIES_PER_COMMERCIAL_DOMAIN: usize = 200;

#[derive(Clone)]
struct EntityUrlInfo {
    entity_id: String,
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
pub enum DomainType {
    Government,
    Healthcare,
    Education,
    Commercial,
    Social,
    Other,
}

#[derive(Default)]
struct UrlMatchingStats {
    new_pairs_created_db: usize,
    entities_in_new_pairs: HashSet<String>,
    confidence_scores: Vec<f64>,
    task_errors: usize,
    pairs_considered_for_comparison: usize,
    rl_feature_extractions: usize,
    rl_feature_failures: usize,
    domains_completed_processing: usize,
    comparison_cache_hits: usize,
    entities_skipped_complete: usize, // NEW: For completion tracking
    entities_total_potential: usize,  // NEW: For completion tracking
}

/// Holds data for a potential pair to be inserted.
struct PairToProcessUrl {
    entity_id_1: String, // Ordered
    entity_id_2: String, // Ordered
    match_values: MatchValues,
    final_confidence: f64,
    pre_rl_confidence: f64,
    features: Option<Vec<f64>>,
    original_signature_1: Option<SignatureData>,
    original_signature_2: Option<SignatureData>,
    comparison_cache_hit: bool,
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<RLOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<AnyMatchResult> {
    let logger = MatchingLogger::new(MatchMethodType::Url);
    logger.log_start(pipeline_run_id, reinforcement_orchestrator_option.is_some());

    // PROGRESS UPDATE: Starting phase
    update_progress!(progress_callback, "Starting", "Initializing URL matching");

    logger.log_phase("Loading existing groups", Some("querying entity_group table"));
    update_progress!(progress_callback, "Loading existing groups", "querying entity_group table");
    
    let existing_groups = fetch_existing_entity_groups(pool).await?;
    logger.log_existing_pairs(existing_groups.len());

    // PROGRESS UPDATE: Existing groups loaded
    update_progress!(progress_callback, "Loading existing groups", 
        format!("Found {} existing pairs", existing_groups.len()));

    // NEW: Check entity completion before loading URL data
    logger.log_phase("Checking entity completion status", Some("filtering completed entities"));
    update_progress!(progress_callback, "Loading URL data", "checking entity completion status");

    // First get all potential entity IDs
    let all_potential_entities = fetch_entities_with_urls_and_locations_filtered(pool, None).await?;
    let all_potential_entity_ids: Vec<String> = all_potential_entities
        .iter()
        .map(|e| e.entity_id.clone())
        .collect::<std::collections::HashSet<_>>() // Deduplicate
        .into_iter()
        .collect();

    // PROGRESS UPDATE: Potential entities found
    update_progress!(progress_callback, "Loading URL data", 
        format!("Found {} entities with URLs", all_potential_entity_ids.len()));

    // Check completion status
    let completion_status = batch_check_entity_completion_status(
        pool,
        &all_potential_entity_ids,
        &MatchMethodType::Url
    ).await?;

    // Filter out completed entities
    let incomplete_entity_ids: Vec<String> = completion_status
        .iter()
        .filter_map(|(entity_id, status)| {
            if !status.is_complete {
                Some(entity_id.clone())
            } else {
                None
            }
        })
        .collect();

    let completed_count = all_potential_entity_ids.len() - incomplete_entity_ids.len();
    logger.log_debug(&format!(
        "URL: {} total entities, {} already complete, {} to process",
        all_potential_entity_ids.len(),
        completed_count,
        incomplete_entity_ids.len()
    ));

    // PROGRESS UPDATE: Completion status results
    update_progress!(progress_callback, "Loading URL data", 
        format!("{} to process ({} already complete)", incomplete_entity_ids.len(), completed_count));

    // Initialize stats with total potential entities and skipped count
    let stats_arc = Arc::new(Mutex::new(UrlMatchingStats {
        entities_total_potential: all_potential_entity_ids.len(),
        entities_skipped_complete: completed_count,
        ..Default::default()
    }));

    if incomplete_entity_ids.is_empty() {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No incomplete entities to process");
        
        // All entities are already complete, return early
        logger.log_completion(0, 0, 0.0, 0); // Log with 0 new groups
        let final_stats = stats_arc.lock().await;
        return Ok(AnyMatchResult::Url(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Url,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: final_stats.entities_skipped_complete, // Pass skipped count
                entities_total: final_stats.entities_total_potential,           // Pass total potential
            },
        }));
    }

    logger.log_phase("Loading entities with URLs", Some("querying entities and related URL data (incomplete only)"));
    update_progress!(progress_callback, "Loading URL data", "querying URL records for incomplete entities");
    
    let entities_with_urls = fetch_entities_with_urls_and_locations_filtered(pool, Some(&incomplete_entity_ids)).await?;
    logger.log_data_loaded(entities_with_urls.len(), "URL");

    // PROGRESS UPDATE: Data loaded
    update_progress!(progress_callback, "Loading URL data", 
        format!("Loaded {} URL records", entities_with_urls.len()));

    logger.log_phase("Grouping by domain and applying business logic", None);
    update_progress!(progress_callback, "Processing domains", "grouping URLs by domain");
    
    let domain_map = group_entities_by_domain(&entities_with_urls);
    let filtered_domains = apply_business_logic_filtering(domain_map);

    let total_domains_to_process = filtered_domains.len();
    let total_entities_in_filtered_domains: usize = filtered_domains.values().map(|v| v.len()).sum();
    let avg_entities_per_domain = if total_domains_to_process > 0 {
        total_entities_in_filtered_domains as f64 / total_domains_to_process as f64
    } else {
        0.0
    };
    let largest_domain_size = filtered_domains.values().map(|v| v.len()).max().unwrap_or(0);

    logger.log_domain_stats(total_domains_to_process, avg_entities_per_domain, largest_domain_size);

    // PROGRESS UPDATE: Domain processing stats
    update_progress!(progress_callback, "Processing domains", 
        format!("{} domains to process after filtering", total_domains_to_process));

    info!(
        "URL: Processing {} domains after business logic filtering.",
        total_domains_to_process
    );

    if total_domains_to_process == 0 {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No domains to process");
        
        logger.log_completion(0, 0, 0.0, 0);
        let final_stats = stats_arc.lock().await;
        return Ok(AnyMatchResult::Url(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Url,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: final_stats.entities_skipped_complete, // Pass skipped count
                entities_total: final_stats.entities_total_potential,           // Pass total potential
            },
        }));
    }

    logger.log_phase("Processing domains in parallel", Some(&format!("max {} concurrent tasks", MAX_PARALLEL_DOMAINS_IN_FLIGHT)));
    update_progress!(progress_callback, "Processing domains", "starting parallel domain processing");
    
    // stats_arc is already initialized above
    let processed_pairs_cache_arc = Arc::new(Mutex::new(HashSet::<(String, String)>::new()));

    let domain_entries: Vec<_> = filtered_domains.into_iter().collect();

    // Create progress bar for domain processing
    let domain_pb = ProgressBar::new(domain_entries.len() as u64);
    domain_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  🌐 [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} {msg}",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    domain_pb.set_message("Processing domains...");

    let chunk_size_for_outer_loop = DOMAIN_PROCESSING_BATCH_SIZE * MAX_PARALLEL_DOMAINS_IN_FLIGHT;
    let mut domains_processed = 0;

    for domain_chunk in domain_entries.chunks(chunk_size_for_outer_loop) {
        let mut task_futures = Vec::new();
        for (domain_str, entities_in_domain) in domain_chunk {
            let unique_entities_for_domain = {
                let mut unique_list = Vec::new();
                let mut seen_entity_ids = HashSet::new();
                for entity_info in entities_in_domain {
                    if seen_entity_ids.insert(entity_info.entity_id.clone()) {
                        unique_list.push(entity_info.clone());
                    }
                }
                unique_list
            };

            if unique_entities_for_domain.len() < 2 {
                domain_pb.inc(1);
                domains_processed += 1;
                continue;
            }
            if unique_entities_for_domain.len() > MAX_PAIRS_PER_DOMAIN_HARD_LIMIT * 2 {
                logger.log_warning(&format!(
                    "Domain '{}' has {} unique entities, potentially too many pairs. Skipping.",
                    domain_str,
                    unique_entities_for_domain.len()
                ));
                domain_pb.inc(1);
                domains_processed += 1;
                continue;
            }

            task_futures.push(tokio::spawn({
                let domain_c = domain_str.clone();
                let entities_c = unique_entities_for_domain.clone();
                let pool_c = pool.clone();
                let ro_opt_c = reinforcement_orchestrator_option.clone();
                let run_id_c = pipeline_run_id.to_string();
                let fc_opt_c = feature_cache.clone();
                let stats_c = Arc::clone(&stats_arc);
                let processed_cache_c = Arc::clone(&processed_pairs_cache_arc);
                let existing_groups_c = Arc::clone(&existing_groups);
                let completion_status_c = completion_status.clone(); // Capture completion_status
                let progress_callback_c = progress_callback.clone(); // NEW: Pass progress callback

                async move {
                    process_domain_task(
                        &domain_c,
                        &entities_c,
                        &pool_c,
                        ro_opt_c.as_ref(),
                        &run_id_c,
                        fc_opt_c.as_ref(),
                        stats_c,
                        processed_cache_c,
                        existing_groups_c,
                        completion_status_c, // Pass completion_status
                        progress_callback_c, // NEW: Pass progress callback
                    )
                    .await
                }
            }));
        }

        let task_results = join_all(task_futures).await;
        let mut completed_domains_in_chunk = 0;
        for res in task_results {
            completed_domains_in_chunk += 1;
            domain_pb.inc(1);
            domains_processed += 1;

            if let Err(e) = res {
                logger.log_warning(&format!("A domain processing task panicked or failed: {:?}", e));
                let mut stats_guard = stats_arc.lock().await;
                stats_guard.task_errors += 1;
            }
        }

        // Log progress periodically
        if domains_processed % 50 == 0 || domains_processed == domain_entries.len() {
            let stats_guard = stats_arc.lock().await;
            logger.log_progress_update(
                domains_processed,
                domain_entries.len(),
                Some(&format!("{} groups created so far", stats_guard.new_pairs_created_db))
            );
            
            // PROGRESS UPDATE: Domain processing progress
            update_detailed_progress!(progress_callback, "Processing domains", 
                domains_processed, domain_entries.len(),
                format!("{} groups created so far", stats_guard.new_pairs_created_db));
        }

        domain_pb.set_message(format!(
            "🌐 Processed {} domains...",
            domains_processed
        ));
    }

    domain_pb.finish_with_message(format!(
        "🌐 URL matching complete: {} domains processed",
        domain_entries.len()
    ));

    logger.log_phase("Finalizing results", Some("aggregating statistics and preparing output"));
    update_progress!(progress_callback, "Finalizing results", "aggregating statistics");
    
    let final_stats = stats_arc.lock().await;
    let avg_confidence = if !final_stats.confidence_scores.is_empty() {
        final_stats.confidence_scores.iter().sum::<f64>()
            / final_stats.confidence_scores.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Url,
        groups_created: final_stats.new_pairs_created_db,
        entities_matched: final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats.new_pairs_created_db > 0 {
            2.0
        } else {
            0.0
        },
        entities_skipped_complete: final_stats.entities_skipped_complete, // Pass skipped count
        entities_total: final_stats.entities_total_potential,           // Pass total potential
    };

    logger.log_completion(
        final_stats.new_pairs_created_db,
        final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        final_stats.pairs_considered_for_comparison
    );

    logger.log_performance_summary(
        final_stats.comparison_cache_hits,
        final_stats.task_errors,
        Some((
            final_stats.rl_feature_extractions,
            final_stats.rl_feature_extractions - final_stats.rl_feature_failures,
            final_stats.rl_feature_failures
        ))
    );

    info!(
        "URL matching V2 (Direct DB) complete: {} domains processed, {} pairs considered, {} cache hits, {} new groups ({} task errors).",
        final_stats.domains_completed_processing,
        final_stats.pairs_considered_for_comparison,
        final_stats.comparison_cache_hits,
        final_stats.new_pairs_created_db,
        final_stats.task_errors
    );

    // FINAL PROGRESS UPDATE: Completion
    update_progress!(progress_callback, "Completed", 
        format!("{} URL groups created, {} entities matched", 
                final_stats.new_pairs_created_db, final_stats.entities_in_new_pairs.len()));

    Ok(AnyMatchResult::Url(MatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
#[allow(clippy::too_many_arguments)]
async fn process_domain_task(
    domain: &str,
    entities: &[EntityUrlInfo],
    pool: &PgPool,
    ro_opt: Option<&Arc<Mutex<RLOrchestrator>>>,
    run_id: &str,
    fc_opt: Option<&SharedFeatureCache>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
    processed_pairs_cache_arc: Arc<Mutex<HashSet<(String, String)>>>,
    existing_groups_arc: Arc<HashSet<(String, String)>>,
    completion_status: HashMap<String, EntityCompletionCheck>, // NEW: Added completion_status
    progress_callback: Option<ProgressCallback>, // NEW: Added progress_callback
) -> Result<()> {
    let mut pairs_for_domain_preparation = Vec::new();
    let mut pairs_considered_this_domain = 0;
    let mut geospatial_filtered = 0;

    // Track entities within this domain that were involved in comparisons
    let mut entity_comparison_counts: HashMap<String, i32> = HashMap::new();
    let mut entities_with_comparisons: HashSet<String> = HashSet::new();

    // PROGRESS UPDATE: Domain processing start
    update_progress!(progress_callback, "Processing domains", 
        format!("Processing domain '{}' with {} entities", domain, entities.len()));

    for i in 0..entities.len() {
        for j in (i + 1)..entities.len() {
            let e1_info = &entities[i];
            let e2_info = &entities[j];

            if e1_info.entity_id == e2_info.entity_id {
                continue;
            }

            if pairs_considered_this_domain >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT {
                debug!(
                    "URL: Domain '{}' has {} entities, potentially too many pairs. Skipping further pairs for this domain.",
                    domain, MAX_PAIRS_PER_DOMAIN_HARD_LIMIT
                );
                break;
            }

            // Geospatial pre-filtering using MAX_DISTANCE_FOR_SAME_CITY_METERS
            if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) = (
                e1_info.latitude,
                e1_info.longitude,
                e2_info.latitude,
                e2_info.longitude,
            ) {
                let distance = calculate_distance(lat1, lon1, lat2, lon2);
                if distance > MAX_DISTANCE_FOR_SAME_CITY_METERS {
                    geospatial_filtered += 1;
                    debug!(
                        "URL: Skipping pair ({}, {}) due to large geospatial distance: {:.2}m > {:.2}m",
                        e1_info.entity_id, e2_info.entity_id, distance, MAX_DISTANCE_FOR_SAME_CITY_METERS
                    );
                    continue;
                }
            }

            pairs_considered_this_domain += 1;

            let (e1_id_ordered, e2_id_ordered) = if e1_info.entity_id < e2_info.entity_id {
                (e1_info.entity_id.clone(), e2_info.entity_id.clone())
            } else {
                (e2_info.entity_id.clone(), e1_info.entity_id.clone())
            };
            let current_pair_key = (e1_id_ordered.clone(), e2_id_ordered.clone()); // Clone for local use

            if existing_groups_arc.contains(&current_pair_key) {
                continue;
            }
            {
                let processed_cache = processed_pairs_cache_arc.lock().await;
                if processed_cache.contains(&current_pair_key) {
                    continue;
                }
            }

            // Increment comparison counts for both entities in the pair
            *entity_comparison_counts.entry(e1_id_ordered.clone()).or_insert(0) += 1;
            *entity_comparison_counts.entry(e2_id_ordered.clone()).or_insert(0) += 1;
            entities_with_comparisons.insert(e1_id_ordered);
            entities_with_comparisons.insert(e2_id_ordered);

            pairs_for_domain_preparation.push((e1_info.clone(), e2_info.clone()));
        }
        if pairs_considered_this_domain >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT {
            break;
        }
    }

    if pairs_considered_this_domain > MAX_PAIRS_PER_DOMAIN_SOFT_WARN {
        debug!(
            "URL: Domain '{}' generated {} candidate pairs after geospatial filter, exceeding soft warning limit {}.",
            domain, pairs_considered_this_domain, MAX_PAIRS_PER_DOMAIN_SOFT_WARN
        );
    }

    {
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.pairs_considered_for_comparison += pairs_considered_this_domain;
    }

    if pairs_for_domain_preparation.is_empty() {
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.domains_completed_processing += 1;
        return Ok(());
    }

    // PROGRESS UPDATE: Pair preparation
    update_progress!(progress_callback, "Processing domains", 
        format!("Domain '{}': {} pairs to process", domain, pairs_for_domain_preparation.len()));

    let db_ready_pairs = prepare_pairs_for_db(
        pairs_for_domain_preparation,
        pool,
        ro_opt,
        run_id,
        fc_opt,
        processed_pairs_cache_arc,
        stats_arc.clone(),
        progress_callback.clone(), // NEW: Pass progress callback
    )
    .await?;

    if !db_ready_pairs.is_empty() {
        // PROGRESS UPDATE: Database operations
        update_progress!(progress_callback, "Processing domains", 
            format!("Domain '{}': inserting {} pairs", domain, db_ready_pairs.len()));
        
        if let Err(e) =
            run_inserts_for_domain_pairs(
                pool,
                db_ready_pairs,
                run_id,
                ro_opt,
                stats_arc.clone(),
                &completion_status, // Pass completion_status
                &entity_comparison_counts, // Pass entity_comparison_counts
                progress_callback.clone(), // NEW: Pass progress callback
            )
            .await
        {
            debug!(
                "URL: DB insert/update failed for some pairs in domain {}: {}",
                domain, e
            );
            return Err(e);
        }
    }

    {
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.domains_completed_processing += 1;
    }
    Ok(())
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
#[allow(clippy::too_many_arguments)]
async fn prepare_pairs_for_db(
    candidate_pairs: Vec<(EntityUrlInfo, EntityUrlInfo)>,
    pool: &PgPool,
    ro_opt: Option<&Arc<Mutex<RLOrchestrator>>>,
    run_id: &str,
    fc_opt: Option<&SharedFeatureCache>,
    processed_pairs_cache_arc: Arc<Mutex<HashSet<(String, String)>>>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
    progress_callback: Option<ProgressCallback>, // NEW: Added progress_callback
) -> Result<Vec<PairToProcessUrl>> {
    let mut db_ready_pairs = Vec::new();

    // PROGRESS UPDATE: Pair preparation start
    update_progress!(progress_callback, "Processing domains", 
        format!("Preparing {} candidate pairs for DB operations", candidate_pairs.len()));

    let mut pairs_for_signature_fetch: Vec<(String, String)> = Vec::new();
    for (e1_info, e2_info) in &candidate_pairs {
        let (ordered_e1_id, ordered_e2_id) = if e1_info.entity_id < e2_info.entity_id {
            (e1_info.entity_id.clone(), e2_info.entity_id.clone())
        } else {
            (e2_info.entity_id.clone(), e1_info.entity_id.clone())
        };
        pairs_for_signature_fetch.push((ordered_e1_id, ordered_e2_id));
    }

    let signatures_map =
        batch_get_current_signatures_for_pairs(pool, &pairs_for_signature_fetch).await?;

    let mut cache_check_inputs = Vec::new();
    for (e1_info, e2_info) in &candidate_pairs {
        let (ordered_e1_id, ordered_e2_id) = if e1_info.entity_id < e2_info.entity_id {
            (e1_info.entity_id.clone(), e2_info.entity_id.clone())
        } else {
            (e2_info.entity_id.clone(), e1_info.entity_id.clone())
        };
        let pair_key = (ordered_e1_id.clone(), ordered_e2_id.clone());
        if let Some((sig1_data, sig2_data)) = signatures_map.get(&pair_key) {
            cache_check_inputs.push((
                ordered_e1_id,
                ordered_e2_id,
                sig1_data.signature.clone(),
                sig2_data.signature.clone(),
                MatchMethodType::Url,
            ));
        }
    }

    let cache_results_map = batch_check_comparison_cache(pool, &cache_check_inputs).await?;

    for (pair_idx, (e1_info, e2_info)) in candidate_pairs.into_iter().enumerate() {
        // PROGRESS UPDATE: Detailed pair processing progress
        if pair_idx % 100 == 0 {
            update_progress!(progress_callback, "Processing domains", 
                format!("Processing pair {}/{}", pair_idx + 1, pairs_for_signature_fetch.len()));
        }
        
        let (ordered_e1_info, ordered_e2_info) = if e1_info.entity_id <= e2_info.entity_id {
            (e1_info, e2_info)
        } else {
            (e2_info, e1_info)
        };
        let current_pair_key = (
            ordered_e1_info.entity_id.clone(),
            ordered_e2_info.entity_id.clone(),
        );

        let sig1_opt = signatures_map
            .get(&current_pair_key)
            .map(|(s1, _)| s1.clone());
        let sig2_opt = signatures_map
            .get(&current_pair_key)
            .map(|(_, s2)| s2.clone());

        let mut current_pair_is_cache_hit = false;
        if let Some(_cached_eval) = cache_results_map.get(&current_pair_key) {
            current_pair_is_cache_hit = true;
            let mut stats_guard = stats_arc.lock().await;
            stats_guard.comparison_cache_hits += 1;
            drop(stats_guard);
            let mut processed_cache = processed_pairs_cache_arc.lock().await;
            processed_cache.insert(current_pair_key);
            continue;
        }

        let distance = if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) = (
            ordered_e1_info.latitude,
            ordered_e1_info.longitude,
            ordered_e2_info.latitude,
            ordered_e2_info.longitude,
        ) {
            Some(calculate_distance(lat1, lon1, lat2, lon2))
        } else {
            None
        };

        let matching_slugs = count_matching_slugs(
            &ordered_e1_info.normalized_data.path_slugs,
            &ordered_e2_info.normalized_data.path_slugs,
        );

        let (pre_rl_conf, match_values) = calculate_url_match_confidence(
            &ordered_e1_info,
            &ordered_e2_info,
            matching_slugs,
            distance,
        );
        let mut final_conf = pre_rl_conf;
        let mut features_vec: Option<Vec<f64>> = None;

        if let Some(ro_arc_ref) = ro_opt {
            let mut stats_guard = stats_arc.lock().await;
            stats_guard.rl_feature_extractions += 1;
            drop(stats_guard);

            match if let Some(fc_arc_ref) = fc_opt {
                let mut fc_guard = fc_arc_ref.lock().await;
                fc_guard
                    .get_pair_features(pool, &current_pair_key.0, &current_pair_key.1)
                    .await
            } else {
                RLOrchestrator::extract_pair_context_features(
                    pool,
                    &current_pair_key.0,
                    &current_pair_key.1,
                )
                .await
            } {
                Ok(features) => {
                    if !features.is_empty() {
                        features_vec = Some(features.clone());
                        match ro_arc_ref.lock().await.get_tuned_confidence(
                            &MatchMethodType::Url,
                            pre_rl_conf,
                            &features,
                        ) {
                            Ok(tuned) => final_conf = tuned,
                            Err(e) => debug!(
                                "URL: RL tuning failed for ({},{}): {}",
                                current_pair_key.0, current_pair_key.1, e
                            ),
                        }
                    }
                }
                Err(e) => {
                    let mut stats_guard = stats_arc.lock().await;
                    stats_guard.rl_feature_failures += 1;
                    drop(stats_guard);
                    debug!(
                        "URL: Feature extraction failed for ({},{}): {}",
                        current_pair_key.0, current_pair_key.1, e
                    );
                }
            }
        }

        db_ready_pairs.push(PairToProcessUrl {
            entity_id_1: current_pair_key.0.clone(),
            entity_id_2: current_pair_key.1.clone(),
            match_values,
            final_confidence: final_conf,
            pre_rl_confidence: pre_rl_conf,
            features: features_vec,
            original_signature_1: sig1_opt,
            original_signature_2: sig2_opt,
            comparison_cache_hit: current_pair_is_cache_hit,
        });
        let mut processed_cache = processed_pairs_cache_arc.lock().await;
        processed_cache.insert(current_pair_key);
    }
    Ok(db_ready_pairs)
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
async fn run_inserts_for_domain_pairs(
    pool: &PgPool,
    pairs_to_process: Vec<PairToProcessUrl>,
    pipeline_run_id: &str,
    ro_opt: Option<&Arc<Mutex<RLOrchestrator>>>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
    completion_status: &HashMap<String, EntityCompletionCheck>, // NEW PARAMETER
    entity_comparison_counts: &HashMap<String, i32>,          // NEW PARAMETER
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<()> {
    let mut batch_entity_group_data = Vec::new();
    let mut batch_decision_detail_data = Vec::new();
    let mut batch_cache_store_data = Vec::new();

    // PROGRESS UPDATE: Database operations start
    update_progress!(progress_callback, "Processing domains", 
        format!("Inserting {} pairs into database", pairs_to_process.len()));

    // Prepare data for batch upsert and cache store
    for pair_data in &pairs_to_process {
        let new_entity_group_id_str = Uuid::new_v4().to_string();
        batch_entity_group_data.push(EntityGroupBatchData {
            proposed_id: new_entity_group_id_str.clone(),
            entity_id_1: pair_data.entity_id_1.clone(),
            entity_id_2: pair_data.entity_id_2.clone(),
            confidence_score: pair_data.final_confidence,
            pre_rl_confidence_score: pair_data.pre_rl_confidence,
            method_type: MatchMethodType::Url,
            match_values: pair_data.match_values.clone(),
        });

        if let (Some(s1_data), Some(s2_data)) = (
            &pair_data.original_signature_1,
            &pair_data.original_signature_2,
        ) {
            batch_cache_store_data.push(ComparisonCacheEntry {
                entity_id_1: pair_data.entity_id_1.clone(),
                entity_id_2: pair_data.entity_id_2.clone(),
                signature_1: s1_data.signature.clone(),
                signature_2: s2_data.signature.clone(),
                method_type: MatchMethodType::Url,
                pipeline_run_id: pipeline_run_id.to_string(),
                comparison_result: "MATCH".to_string(),
                similarity_score: Some(pair_data.final_confidence),
                features_snapshot: pair_data
                    .features
                    .as_ref()
                    .map(|f| serde_json::to_value(f).unwrap_or_default()),
            });
        }
    }

    let upsert_results = batch_upsert_entity_groups(pool, batch_entity_group_data).await;
    match upsert_results {
        Ok(results_map) => {
            for pair_data in pairs_to_process.iter() {
                let pair_key = (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone());
                if let Some((group_id, was_newly_inserted)) = results_map.get(&pair_key) {
                    if *was_newly_inserted {
                        let mut stats_guard = stats_arc.lock().await;
                        stats_guard.new_pairs_created_db += 1;
                        stats_guard
                            .entities_in_new_pairs
                            .insert(pair_data.entity_id_1.clone());
                        stats_guard
                            .entities_in_new_pairs
                            .insert(pair_data.entity_id_2.clone());
                        stats_guard
                            .confidence_scores
                            .push(pair_data.final_confidence);
                        drop(stats_guard);

                        if let (Some(ro_arc_ref), Some(features_vec)) =
                            (ro_opt, &pair_data.features)
                        {
                            let version = ro_arc_ref.lock().await.confidence_tuner.version;
                            let features_json =
                                serde_json::to_value(features_vec).unwrap_or_default();
                            batch_decision_detail_data.push(MatchDecisionDetailBatchData {
                                entity_group_id: group_id.clone(),
                                pipeline_run_id: pipeline_run_id.to_string(),
                                snapshotted_features: features_json,
                                method_type_at_decision: MatchMethodType::Url.as_str().to_string(),
                                pre_rl_confidence_at_decision: pair_data.pre_rl_confidence,
                                tuned_confidence_at_decision: pair_data.final_confidence,
                                confidence_tuner_version_at_decision: Some(version as i32),
                            });
                        }
                    }
                }
            }
        }
        Err(e) => {
            debug!(
                "URL: Batch upsert failed in run_inserts_for_domain_pairs: {}",
                e
            );
            let mut stats_guard = stats_arc.lock().await;
            stats_guard.task_errors += pairs_to_process.len();
            drop(stats_guard);
            return Err(anyhow!(
                "Encountered errors during DB operations for domain batch: {}",
                e
            ));
        }
    }

    if !batch_decision_detail_data.is_empty() {
        if let Err(e) = batch_insert_match_decision_details(pool, batch_decision_detail_data).await
        {
            debug!(
                "URL: Batch insert decision details failed in run_inserts_for_domain_pairs: {}",
                e
            );
            let mut stats_guard = stats_arc.lock().await;
            stats_guard.task_errors += 1;
            drop(stats_guard);
        }
    }

    if !batch_cache_store_data.is_empty() {
        if let Err(e) = batch_store_in_comparison_cache(pool, batch_cache_store_data).await {
            debug!(
                "URL: Batch store comparison cache failed in run_inserts_for_domain_pairs: {}",
                e
            );
            let mut stats_guard = stats_arc.lock().await;
            stats_guard.task_errors += 1;
            drop(stats_guard);
        }
    }

    // NEW: Add completion tracking logic
    let mut completion_batch: Vec<(String, MatchMethodType, String, String, i32)> = Vec::new();
    for (entity_id, current_sig_check) in completion_status.iter() {
        if !current_sig_check.is_complete { // Only mark incomplete entities
            if let Some(current_sig) = &current_sig_check.current_signature {
                let comparison_count = entity_comparison_counts.get(entity_id).copied().unwrap_or(0);
                if comparison_count > 0 { // Only mark if comparisons were made in this run
                    completion_batch.push((
                        entity_id.clone(),
                        MatchMethodType::Url,
                        pipeline_run_id.to_string(),
                        current_sig.clone(),
                        comparison_count, // Use the count from this domain's processing
                    ));
                }
            }
        }
    }
    if !completion_batch.is_empty() {
        if let Err(e) = batch_mark_entity_completion(pool, &completion_batch).await {
            debug!("URL: Failed to batch mark {} entities as complete: {}", completion_batch.len(), e);
            // Don't propagate this error, but log it
        } else {
            debug!("URL: Marked {} entities as complete", completion_batch.len());
        }
    }

    Ok(())
}

async fn fetch_existing_entity_groups(pool: &PgPool) -> Result<Arc<HashSet<(String, String)>>> {
    let conn = pool
        .get()
        .await
        .context("URL: DB conn for existing groups")?;
    let rows = conn
        .query(
            "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1",
            &[&MatchMethodType::Url.as_str()],
        )
        .await
        .context("URL: Query existing groups")?;
    let pairs = rows
        .into_iter()
        .map(|r| {
            let (id1, id2): (String, String) = (r.get(0), r.get(1));
            if id1 < id2 {
                (id1, id2)
            } else {
                (id2, id1)
            }
        })
        .collect();
    Ok(Arc::new(pairs))
}

// Add new function signature and modify the main function
async fn fetch_entities_with_urls_and_locations_filtered(
    pool: &PgPool,
    entity_ids: Option<&[String]>,
) -> Result<Vec<EntityUrlInfo>> {
    let conn = pool.get().await.context("URL: DB conn for entities/URLs")?;

    let base_query = r#"
    WITH EntityServiceURLs AS (
        SELECT e.id AS entity_id, s.url AS service_url, e.name AS entity_name
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'service'
        JOIN public.service s ON ef.table_id = s.id
        WHERE s.url IS NOT NULL AND s.url != '' AND s.url !~ '^\s*$' AND s.url NOT LIKE 'mailto:%' AND s.url NOT LIKE 'tel:%'
    ), EntityOrgURLs AS (
        SELECT e.id AS entity_id, o.url AS org_url, e.name AS entity_name
        FROM public.entity e
        JOIN public.organization o ON e.organization_id = o.id
        WHERE o.url IS NOT NULL AND o.url != '' AND o.url !~ '^\s*$' AND o.url NOT LIKE 'mailto:%' AND o.url NOT LIKE 'tel:%'
    ), CombinedURLs AS (
        SELECT entity_id, service_url AS url, entity_name FROM EntityServiceURLs
        UNION ALL
        SELECT entity_id, org_url AS url, entity_name FROM EntityOrgURLs
    ), DeduplicatedURLs AS (
        SELECT DISTINCT entity_id, url, entity_name FROM CombinedURLs
    ), EntityLocations AS (
        SELECT e.id AS entity_id, l.latitude, l.longitude,
               ROW_NUMBER() OVER(PARTITION BY e.id ORDER BY l.created DESC NULLS LAST, l.id) as rn
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        JOIN public.location l ON ef.table_id = l.id
        WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
    )
    SELECT du.entity_id, du.url, du.entity_name, el.latitude, el.longitude
    FROM DeduplicatedURLs du
    LEFT JOIN EntityLocations el ON du.entity_id = el.entity_id AND el.rn = 1
    "#;

    let (query, params): (String, Vec<Box<dyn ToSql + Sync + Send>>) = if let Some(ids) = entity_ids {
        (format!("{} WHERE du.entity_id = ANY($1)", base_query),
         vec![Box::new(ids.to_vec())])
    } else {
        (base_query.to_string(), vec![])
    };

    // Use the modified query with parameters...
    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    let rows = conn
        .query(query.as_str(), params_slice.as_slice())
        .await
        .context("URL: Query entities/URLs (filtered)")?;

    let mut entities_info = Vec::new();
    let mut seen_entity_url_ids: HashSet<(String, String)> = HashSet::new();

    for r in rows {
        let entity_id_str: String = r.get(0);
        let url_str: String = r.get(1);
        let entity_name_opt: Option<String> = r.get(2);
        let latitude_opt: Option<f64> = r.try_get(3).ok();
        let longitude_opt: Option<f64> = r.try_get(4).ok();

        if let Some(normalized_data) = normalize_url_with_slugs(&url_str) {
            if !is_ignored_domain(&normalized_data.domain) {
                if seen_entity_url_ids.insert((entity_id_str.clone(), url_str.clone())) {
                    entities_info.push(EntityUrlInfo {
                        entity_id: entity_id_str,
                        original_url: url_str,
                        normalized_data,
                        entity_name: entity_name_opt,
                        latitude: latitude_opt,
                        longitude: longitude_opt,
                    });
                }
            }
        }
    }
    Ok(entities_info)
}

fn group_entities_by_domain(entities: &[EntityUrlInfo]) -> HashMap<String, Vec<EntityUrlInfo>> {
    let mut domain_map: HashMap<String, Vec<EntityUrlInfo>> = HashMap::new();
    for e_info in entities {
        domain_map.entry(e_info.normalized_data.domain.clone())
            .or_default()
            .push(e_info.clone());
    }
    domain_map
}

fn apply_business_logic_filtering(
    map: HashMap<String, Vec<EntityUrlInfo>>,
) -> HashMap<String, Vec<EntityUrlInfo>> {
    map.into_iter()
        .filter(|(domain, entities)| {
            if entities.len() < 2 {
                return false;
            }
            let domain_type = categorize_domain(domain);
            let threshold = get_max_entities_threshold(&domain_type);
            let keep = entities.len() <= threshold;
            if !keep {
                debug!(
                    "URL: Excluding domain '{}' ({:?}) due to entity count: {} (threshold: {})",
                    domain,
                    domain_type,
                    entities.len(),
                    threshold
                );
            }
            keep
        })
        .collect()
}

fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in meters
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let (delta_phi, delta_lambda) = ((lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (delta_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

fn count_matching_slugs(slugs1: &[String], slugs2: &[String]) -> usize {
    slugs1
        .iter()
        .zip(slugs2.iter())
        .take_while(|(s1, s2)| s1 == s2)
        .count()
}

fn calculate_url_match_confidence(
    e1_info: &EntityUrlInfo,
    e2_info: &EntityUrlInfo,
    matching_slugs_count: usize,
    distance_meters_opt: Option<f64>,
) -> (f64, MatchValues) {
    let (ordered_e1, ordered_e2) = if e1_info.entity_id <= e2_info.entity_id {
        (e1_info, e2_info)
    } else {
        (e2_info, e1_info)
    };

    let e1_slugs = &ordered_e1.normalized_data.path_slugs;
    let e2_slugs = &ordered_e2.normalized_data.path_slugs;

    let mut confidence = if matching_slugs_count == e1_slugs.len()
        && matching_slugs_count == e2_slugs.len()
        && !e1_slugs.is_empty()
    {
        CONFIDENCE_DOMAIN_FULL_PATH_MATCH
    } else {
        match matching_slugs_count {
            0 => CONFIDENCE_DOMAIN_ONLY,
            1 => CONFIDENCE_DOMAIN_PLUS_ONE_SLUG,
            2 => CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS,
            _ => CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS,
        }
    };

    if let Some(distance) = distance_meters_opt {
        if distance < 100.0 && matching_slugs_count > 0 {
            confidence = (confidence + 0.05).min(0.98);
        } else if distance > 10000.0 && matching_slugs_count == 0 {
            confidence = (confidence * 0.7).max(0.55);
        } else if distance > MAX_DISTANCE_FOR_SAME_CITY_METERS && matching_slugs_count <= 1 {
            confidence = (confidence * 0.85).max(0.60);
        }
    }
    confidence = confidence.max(0.0).min(1.0);

    let match_values = MatchValues::Url(UrlMatchDetail {
        values: UrlMatchValues {
            normalized_url1: ordered_e1.normalized_data.original_url.clone(),
            normalized_url2: ordered_e2.normalized_data.original_url.clone(),
            matched_domain: ordered_e1.normalized_data.domain.clone(),
            matching_slugs_count,
        },
        original1: ordered_e1.original_url.clone(),
        original2: ordered_e2.original_url.clone(),
        domain_shared: ordered_e1.normalized_data.domain.clone(),
        matching_slugs_count,
    });

    (confidence, match_values)
}

pub fn normalize_url_with_slugs(url_s: &str) -> Option<NormalizedUrlData> {
    let trimmed_url = url_s.trim();
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

    StdUrl::parse(&url_with_scheme).ok().and_then(|parsed_url| {
        parsed_url.host_str().and_then(|host| {
            let domain_lower = host.to_lowercase();
            let domain_no_www = domain_lower
                .strip_prefix("www.")
                .unwrap_or(&domain_lower)
                .to_string();

            if domain_no_www.is_empty()
                || !domain_no_www.contains('.')
                || is_ip_address(&domain_no_www)
            {
                None
            } else {
                let path_slugs: Vec<String> =
                    parsed_url
                        .path_segments()
                        .map_or_else(Vec::new, |segments| {
                            segments
                                .filter(|s| !s.is_empty() && s.len() < 50 && !s.contains('.'))
                                .map(|s| s.to_lowercase())
                                .collect()
                        });
                Some(NormalizedUrlData {
                    domain: domain_no_www.clone(),
                    path_slugs,
                    original_url: url_s.to_string(),
                    domain_type: categorize_domain(&domain_no_www),
                })
            }
        })
    })
}

pub fn is_ignored_domain(domain: &str) -> bool {
    const IGNORED_DOMAINS: &[&str] = &[
        "facebook.com",
        "twitter.com",
        "instagram.com",
        "linkedin.com",
        "youtube.com",
        "youtu.be",
        "google.com",
        "maps.google.com",
        "goo.gl",
        "bit.ly",
        "t.co",
        "ow.ly",
        "tiktok.com",
        "pinterest.com",
        "snapchat.com",
        "reddit.com",
        "wa.me",
        "t.me",
    ];
    const GENERIC_PLATFORMS: &[&str] = &[
        "eventbrite.com",
        "meetup.com",
        "wordpress.com",
        "blogspot.com",
        "wix.com",
        "squarespace.com",
        "godaddy.com",
        "sites.google.com",
        "forms.gle",
        "docs.google.com",
        "drive.google.com",
        "dropbox.com",
        "box.com",
        "onedrive.live.com",
        "zoom.us",
        "teams.microsoft.com",
        "github.io",
    ];

    IGNORED_DOMAINS
        .iter()
        .any(|d| *d == domain || domain.ends_with(&format!(".{}", d)))
        || GENERIC_PLATFORMS
            .iter()
            .any(|p| *p == domain || domain.ends_with(&format!(".{}", p)))
}

pub fn categorize_domain(domain_str: &str) -> DomainType {
    if domain_str.ends_with(".gov") || domain_str.ends_with(".mil") {
        DomainType::Government
    } else if domain_str.ends_with(".edu") || domain_str.contains("k12") {
        DomainType::Education
    } else if domain_str.ends_with(".org")
        || domain_str.contains("health")
        || domain_str.contains("hospital")
        || domain_str.contains("clinic")
    {
        DomainType::Healthcare
    } else if domain_str.ends_with(".com")
        || domain_str.ends_with(".net")
        || domain_str.ends_with(".biz")
        || domain_str.ends_with(".co")
    {
        DomainType::Commercial
    } else if domain_str.contains("facebook.")
        || domain_str.contains("twitter.")
        || domain_str.contains("linkedin.")
        || domain_str.contains("instagram.")
    {
        DomainType::Social
    } else {
        DomainType::Other
    }
}

pub fn get_max_entities_threshold(domain_type: &DomainType) -> usize {
    match domain_type {
        DomainType::Government => MAX_ENTITIES_PER_GOV_DOMAIN,
        DomainType::Healthcare => MAX_ENTITIES_PER_ORG_DOMAIN,
        DomainType::Education => MAX_ENTITIES_PER_EDU_DOMAIN,
        DomainType::Commercial => MAX_ENTITIES_PER_COMMERCIAL_DOMAIN,
        DomainType::Social => 10,
        DomainType::Other => MAX_ENTITIES_PER_ORG_DOMAIN,
    }
}

pub fn is_ip_address(domain_candidate: &str) -> bool {
    if domain_candidate.split('.').count() == 4
        && domain_candidate
            .split('.')
            .all(|part| part.parse::<u8>().is_ok())
    {
        return true;
    }
    if domain_candidate.contains(':') {
        return true;
    }
    false
}