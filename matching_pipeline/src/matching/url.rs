// src/matching/url.rs
use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use url::Url as StdUrl;
use uuid::Uuid;

use crate::config::{CRITICALLY_LOW_SUGGESTION_THRESHOLD, MODERATE_LOW_SUGGESTION_THRESHOLD};
use crate::db::{
    self, PgPool, // Using db::upsert_entity_group, db::insert_match_decision_detail_direct, db::insert_suggestion
};
use crate::models::{
    ActionType, EntityId, MatchMethodType, MatchValues, NewSuggestedAction, SuggestionStatus,
    UrlMatchValue,
};
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};
use crate::reinforcement::entity::feature_cache_service::{
    SharedFeatureCache, // FeatureCacheService is not directly used here, SharedFeatureCache is
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, MatchMethodStats, UrlMatchResult};
use serde_json;

const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;
const MAX_PARALLEL_DOMAINS_IN_FLIGHT: usize = 8;
const DOMAIN_PROCESSING_BATCH_SIZE: usize = 10; // How many domains to give to one process_domain_task
// const RL_FEATURE_EXTRACTION_BATCH_SIZE: usize = 50; // Not directly used here, feature extraction is per pair
const MAX_PAIRS_PER_DOMAIN_SOFT_WARN: usize = 2000;
const MAX_PAIRS_PER_DOMAIN_HARD_LIMIT: usize = 5000;
const MAX_LOCATION_DISTANCE_METERS: f64 = 2000.0; // Example: 2km
const MAX_ENTITIES_PER_GOV_DOMAIN: usize = 20;
const MAX_ENTITIES_PER_ORG_DOMAIN: usize = 50;
const MAX_ENTITIES_PER_EDU_DOMAIN: usize = 100;
const MAX_ENTITIES_PER_COMMERCIAL_DOMAIN: usize = 200; // Increased limit for commercial

#[derive(Clone)]
struct EntityUrlInfo {
    entity_id: EntityId,
    original_url: String,
    normalized_data: NormalizedUrlData,
    entity_name: Option<String>, // For context in suggestions
    latitude: Option<f64>,    // For distance calculations
    longitude: Option<f64>,
}

#[derive(Clone)]
pub struct NormalizedUrlData {
    pub domain: String,
    pub path_slugs: Vec<String>,
    pub original_url: String, // Keep original for reference
    pub domain_type: DomainType,
}

#[derive(Clone, PartialEq, Debug)]
enum DomainType {
    Government,
    Healthcare, // .org, or contains health/hospital
    Education,  // .edu, or k12
    Commercial, // .com, .net
    Social,     // facebook, twitter etc.
    Other,
}

#[derive(Default)]
struct UrlMatchingStats {
    new_pairs_created_db: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores: Vec<f64>,
    task_errors: usize, // Errors from spawned tasks or major processing steps
    pairs_considered_for_comparison: usize, // Pairs after domain grouping and initial filtering
    rl_feature_extractions: usize,
    rl_feature_failures: usize,
    domains_completed_processing: usize,
    comparison_cache_hits: usize,
}

/// Holds data for a potential pair to be inserted.
struct PairToProcessUrl {
    entity_id_1: EntityId, // Ordered
    entity_id_2: EntityId, // Ordered
    match_values: MatchValues,
    final_confidence: f64,
    pre_rl_confidence: f64,
    features: Option<Vec<f64>>,
    sig1: Option<String>, // Signature of entity_id_1
    sig2: Option<String>, // Signature of entity_id_2
}

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    let start_time = Instant::now();
    info!(
        "Starting V2 (Direct DB) parallel URL matching (run ID: {})",
        pipeline_run_id
    );

    let existing_groups = fetch_existing_entity_groups(pool).await?; // Arc<HashSet>
    let entities_with_urls = fetch_entities_with_urls_and_locations(pool).await?;
    let domain_map = group_entities_by_domain(&entities_with_urls);
    let filtered_domains = apply_business_logic_filtering(domain_map);

    let total_domains_to_process = filtered_domains.len();
    info!(
        "URL: Processing {} domains after business logic filtering.",
        total_domains_to_process
    );

    if total_domains_to_process == 0 {
        return Ok(AnyMatchResult::Url(UrlMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(MatchMethodType::Url),
        }));
    }

    let stats_arc = Arc::new(Mutex::new(UrlMatchingStats::default()));
    // Tracks (ordered_e1, ordered_e2) pairs that have been through prepare_pairs_for_db or cache hit
    let processed_pairs_cache_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));

    let domain_entries: Vec<_> = filtered_domains.into_iter().collect();

    // Process domains in chunks of parallel tasks
    for domain_chunk in
        domain_entries.chunks(DOMAIN_PROCESSING_BATCH_SIZE * MAX_PARALLEL_DOMAINS_IN_FLIGHT)
    {
        let mut task_futures = Vec::new();
        for (domain_str, entities_in_domain) in domain_chunk {
            if entities_in_domain.len() < 2 {
                continue;
            }
            if entities_in_domain.len() > MAX_PAIRS_PER_DOMAIN_HARD_LIMIT * 2 { // Heuristic
                 warn!("URL: Domain '{}' has {} entities, potentially too many pairs. Skipping.", domain_str, entities_in_domain.len());
                 continue;
            }


            task_futures.push(tokio::spawn({
                // Clone necessary Arcs and data for the spawned task
                let domain_c = domain_str.clone();
                let entities_c = entities_in_domain.clone();
                let pool_c = pool.clone();
                let ro_opt_c = reinforcement_orchestrator_option.clone();
                let run_id_c = pipeline_run_id.to_string();
                let fc_opt_c = feature_cache.clone();
                let stats_c = Arc::clone(&stats_arc);
                let processed_cache_c = Arc::clone(&processed_pairs_cache_arc);
                let existing_groups_c = Arc::clone(&existing_groups);

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
                    )
                    .await
                }
            }));
        }
        // Wait for the current chunk of domain processing tasks to complete
        let task_results = join_all(task_futures).await;
        for res in task_results {
            if let Err(e) = res {
                // Error from tokio::spawn (e.g. panic)
                warn!("URL: A domain processing task panicked or failed: {:?}", e);
                 let mut stats_guard = stats_arc.lock().await;
                 stats_guard.task_errors += 1;
            }
            // Inner Result<()> from process_domain_task handles its own errors by logging and updating stats.task_errors
        }
    }

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
            2.0 // Always 2 for pairwise
        } else {
            0.0
        },
    };

    info!(
        "URL matching V2 (Direct DB) complete in {:.2?}: {} domains processed, {} pairs considered, {} cache hits, {} new groups ({} task errors).",
        start_time.elapsed(),
        final_stats.domains_completed_processing,
        final_stats.pairs_considered_for_comparison,
        final_stats.comparison_cache_hits,
        final_stats.new_pairs_created_db,
        final_stats.task_errors
    );

    Ok(AnyMatchResult::Url(UrlMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn process_domain_task(
    domain: &str,
    entities: &[EntityUrlInfo],
    pool: &PgPool,
    ro_opt: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    run_id: &str,
    fc_opt: Option<&SharedFeatureCache>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
    processed_pairs_cache_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    existing_groups_arc: Arc<HashSet<(EntityId, EntityId)>>,
) -> Result<()> { // Result indicates if the task itself failed, not individual pair errors
    let mut pairs_for_domain_preparation = Vec::new();
    let mut pairs_considered_this_domain = 0;

    for i in 0..entities.len() {
        for j in (i + 1)..entities.len() {
            if pairs_considered_this_domain >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT {
                warn!("URL: Domain '{}' reached hard limit of {} pairs. Skipping further pairs for this domain.", domain, MAX_PAIRS_PER_DOMAIN_HARD_LIMIT);
                break;
            }
            pairs_considered_this_domain += 1;

            let e1_info = &entities[i];
            let e2_info = &entities[j];

            let (e1_id_ordered, e2_id_ordered) = if e1_info.entity_id.0 < e2_info.entity_id.0 {
                (e1_info.entity_id.clone(), e2_info.entity_id.clone())
            } else {
                (e2_info.entity_id.clone(), e1_info.entity_id.clone())
            };
            let current_pair_key = (e1_id_ordered, e2_id_ordered);

            if existing_groups_arc.contains(&current_pair_key) {
                continue;
            }
            { // Scope for processed_pairs_cache_arc lock
                let processed_cache = processed_pairs_cache_arc.lock().await;
                if processed_cache.contains(&current_pair_key) {
                    continue;
                }
            }
            // If not in existing groups or already processed this run, add for preparation
            pairs_for_domain_preparation.push((e1_info.clone(), e2_info.clone()));
        }
        if pairs_considered_this_domain >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT { break; }
    }
    if pairs_considered_this_domain > MAX_PAIRS_PER_DOMAIN_SOFT_WARN {
        warn!("URL: Domain '{}' generated {} candidate pairs, exceeding soft warning limit {}.", domain, pairs_considered_this_domain, MAX_PAIRS_PER_DOMAIN_SOFT_WARN);
    }


    { // Scope for stats_arc lock
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.pairs_considered_for_comparison += pairs_considered_this_domain;
    }

    if pairs_for_domain_preparation.is_empty() {
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.domains_completed_processing += 1;
        return Ok(());
    }

    // This function now handles cache checking internally for each pair
    let db_ready_pairs = prepare_pairs_for_db(
        pairs_for_domain_preparation, pool, ro_opt, run_id, fc_opt,
        processed_pairs_cache_arc, // Pass the Arc for cache updates
        stats_arc.clone(), // Pass Arc for stats updates
    ).await?;


    if !db_ready_pairs.is_empty() {
        // This function handles DB inserts and updates stats_arc for created groups
        if let Err(e) = run_inserts_for_domain_pairs(pool, db_ready_pairs, run_id, ro_opt, stats_arc.clone()).await {
            warn!("URL: DB insert/update failed for some pairs in domain {}: {}", domain, e);
            // Error is logged, and task_errors in stats might be incremented by run_inserts_for_domain_pairs
            // Propagate the error to indicate this task had issues.
            return Err(e);
        }
    }

    { // Scope for stats_arc lock
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.domains_completed_processing += 1;
    }
    Ok(())
}


#[allow(clippy::too_many_arguments)]
async fn prepare_pairs_for_db(
    candidate_pairs: Vec<(EntityUrlInfo, EntityUrlInfo)>,
    pool: &PgPool,
    ro_opt: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    run_id: &str,
    fc_opt: Option<&SharedFeatureCache>,
    processed_pairs_cache_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>, // Used to mark as processed
    stats_arc: Arc<Mutex<UrlMatchingStats>>, // For updating cache_hits
) -> Result<Vec<PairToProcessUrl>> {
    let mut db_ready_pairs = Vec::new();

    for (e1_info, e2_info) in candidate_pairs {
        // Ensure canonical order for cache key and DB storage
        let (ordered_e1_info, ordered_e2_info) = if e1_info.entity_id.0 < e2_info.entity_id.0 {
            (e1_info, e2_info)
        } else {
            (e2_info, e1_info) // Swap them
        };
        let current_pair_key = (ordered_e1_info.entity_id.clone(), ordered_e2_info.entity_id.clone());

        // Check cache first
        let mut sig1_opt: Option<String> = None;
        let mut sig2_opt: Option<String> = None;
        if let Ok(Some((s1_data, s2_data))) = get_current_signatures_for_pair(pool, &current_pair_key.0, &current_pair_key.1).await {
            sig1_opt = Some(s1_data.signature.clone());
            sig2_opt = Some(s2_data.signature.clone());
            if let Ok(Some(_)) = check_comparison_cache(pool, &current_pair_key.0, &current_pair_key.1, &s1_data.signature, &s2_data.signature, &MatchMethodType::Url).await {
                let mut stats_guard = stats_arc.lock().await;
                stats_guard.comparison_cache_hits += 1;
                drop(stats_guard); // Release lock
                let mut processed_cache = processed_pairs_cache_arc.lock().await;
                processed_cache.insert(current_pair_key); // Mark as processed due to cache hit
                continue;
            }
        }

        // Calculate confidence and features
        let distance = if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) =
            (ordered_e1_info.latitude, ordered_e1_info.longitude, ordered_e2_info.latitude, ordered_e2_info.longitude) {
            Some(calculate_distance(lat1, lon1, lat2, lon2))
        } else { None };

        let matching_slugs = count_matching_slugs(&ordered_e1_info.normalized_data.path_slugs, &ordered_e2_info.normalized_data.path_slugs);

        if let Some(dist) = distance { // Apply distance-based filtering early
            if dist > MAX_LOCATION_DISTANCE_METERS && matching_slugs < 2 {
                 if let (Some(s1), Some(s2)) = (&sig1_opt, &sig2_opt) {
                    store_in_comparison_cache(pool, &current_pair_key.0, &current_pair_key.1, s1, s2, &MatchMethodType::Url, run_id, "NON_MATCH", Some(0.0), None).await?;
                }
                let mut processed_cache = processed_pairs_cache_arc.lock().await;
                processed_cache.insert(current_pair_key);
                continue;
            }
        }

        let (pre_rl_conf, match_values) = calculate_url_match_confidence(&ordered_e1_info, &ordered_e2_info, matching_slugs, distance);
        let mut final_conf = pre_rl_conf;
        let mut features_vec: Option<Vec<f64>> = None;
        let mut features_json_for_cache: Option<serde_json::Value> = None;


        if let Some(ro_arc_ref) = ro_opt {
            let mut stats_guard = stats_arc.lock().await;
            stats_guard.rl_feature_extractions += 1;
            drop(stats_guard);

            match if let Some(fc_arc_ref) = fc_opt {
                let mut fc_guard = fc_arc_ref.lock().await;
                fc_guard.get_pair_features(pool, &current_pair_key.0, &current_pair_key.1).await
            } else {
                MatchingOrchestrator::extract_pair_context_features(pool, &current_pair_key.0, &current_pair_key.1).await
            } {
                Ok(features) => {
                    if !features.is_empty() {
                        features_vec = Some(features.clone());
                        features_json_for_cache = serde_json::to_value(features.clone()).ok();
                        match ro_arc_ref.lock().await.get_tuned_confidence(&MatchMethodType::Url, pre_rl_conf, &features) {
                            Ok(tuned) => final_conf = tuned,
                            Err(e) => warn!("URL: RL tuning failed for ({},{}): {}", current_pair_key.0.0, current_pair_key.1.0, e),
                        }
                    }
                }
                Err(e) => {
                    let mut stats_guard = stats_arc.lock().await;
                    stats_guard.rl_feature_failures += 1;
                    drop(stats_guard);
                    warn!("URL: Feature extraction failed for ({},{}): {}", current_pair_key.0.0, current_pair_key.1.0, e);
                }
            }
        }
        // Add to list for DB processing
        db_ready_pairs.push(PairToProcessUrl {
            entity_id_1: current_pair_key.0.clone(),
            entity_id_2: current_pair_key.1.clone(),
            match_values,
            final_confidence: final_conf,
            pre_rl_confidence: pre_rl_conf,
            features: features_vec,
            sig1: sig1_opt,
            sig2: sig2_opt,
        });
        // Mark as processed (cache miss path)
        let mut processed_cache = processed_pairs_cache_arc.lock().await;
        processed_cache.insert(current_pair_key);
    }
    Ok(db_ready_pairs)
}

async fn run_inserts_for_domain_pairs(
    pool: &PgPool,
    pairs_to_process: Vec<PairToProcessUrl>,
    pipeline_run_id: &str,
    ro_opt: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
) -> Result<()> { // Changed to Result<()> as new_groups_count is updated via stats_arc
    let mut local_task_errors = 0;

    for pair_data in pairs_to_process {
        let new_entity_group_id_str = Uuid::new_v4().to_string();
        match db::upsert_entity_group(
            pool,
            &new_entity_group_id_str,
            &pair_data.entity_id_1,
            &pair_data.entity_id_2,
            pair_data.final_confidence,
            pair_data.pre_rl_confidence,
            MatchMethodType::Url,
            pair_data.match_values.clone(),
        ).await {
            Ok((group_id, was_newly_inserted)) => {
                if was_newly_inserted {
                    let mut stats_guard = stats_arc.lock().await;
                    stats_guard.new_pairs_created_db += 1;
                    stats_guard.entities_in_new_pairs.insert(pair_data.entity_id_1.clone());
                    stats_guard.entities_in_new_pairs.insert(pair_data.entity_id_2.clone());
                    stats_guard.confidence_scores.push(pair_data.final_confidence);
                    drop(stats_guard);

                    if let (Some(ro_arc_ref), Some(features_vec)) = (ro_opt, &pair_data.features) {
                        let version = ro_arc_ref.lock().await.confidence_tuner.version;
                        let features_json = serde_json::to_value(features_vec).unwrap_or_default();
                        if let Err(e) = db::insert_match_decision_detail_direct(
                            pool, &group_id, pipeline_run_id, features_json,
                            MatchMethodType::Url.as_str(), pair_data.pre_rl_confidence,
                            pair_data.final_confidence, Some(version as i32)
                        ).await {
                            warn!("URL: Decision detail insert failed for group {}: {}. Group created, RL context lost.", group_id, e);
                        }
                    }

                    if pair_data.final_confidence < MODERATE_LOW_SUGGESTION_THRESHOLD {
                        let suggestion = create_suggestion_url(&group_id, &pair_data, pipeline_run_id);
                        if let Err(e) = db::insert_suggestion(pool, &suggestion).await {
                            warn!("URL: Suggestion insert failed for group {}: {}. Group and detail (if any) created.", group_id, e);
                        }
                    }
                }
                // Cache the result (MATCH, as we attempted to upsert)
                if let (Some(s1), Some(s2)) = (pair_data.sig1, pair_data.sig2) {
                    let _ = store_in_comparison_cache(
                        pool, &pair_data.entity_id_1, &pair_data.entity_id_2,
                        &s1, &s2, &MatchMethodType::Url, pipeline_run_id,
                        "MATCH", Some(pair_data.final_confidence),
                        pair_data.features.as_ref().map(|f| serde_json::to_value(f).unwrap_or_default()).as_ref()
                    ).await;
                }
            }
            Err(e) => {
                warn!("URL: upsert_entity_group failed for pair ({}, {}): {}",
                    pair_data.entity_id_1.0, pair_data.entity_id_2.0, e);
                local_task_errors += 1;
            }
        }
    }
    if local_task_errors > 0 {
        let mut stats_guard = stats_arc.lock().await;
        stats_guard.task_errors += local_task_errors;
         return Err(anyhow!("Encountered {} errors during DB operations for domain batch", local_task_errors));
    }
    Ok(())
}


fn create_suggestion_url(
    group_id: &str,
    pair: &PairToProcessUrl,
    pipeline_run_id: &str,
) -> NewSuggestedAction {
    let (orig_url1, orig_url2, norm_domain, slug_count) =
        if let MatchValues::Url(ref uv) = pair.match_values {
            (
                uv.original_url1.clone(),
                uv.original_url2.clone(),
                uv.normalized_shared_domain.clone(),
                uv.matching_slug_count,
            )
        } else {
            // Should not happen if match_values is correctly populated
            (String::from("N/A"), String::from("N/A"), String::from("N/A"), 0)
        };

    let details = serde_json::json!({
       "method_type": MatchMethodType::Url.as_str(),
       "original_url1": orig_url1,
       "original_url2": orig_url2,
       "normalized_shared_domain": norm_domain,
       "matching_slug_count": slug_count,
       "entity_group_id": group_id,
       "pre_rl_confidence": pair.pre_rl_confidence,
    });
    let reason_msg = format!(
        "Pair ({},{}) URL (domain: {}, slugs: {}) low confidence ({:.4}).",
        pair.entity_id_1.0, pair.entity_id_2.0, norm_domain, slug_count, pair.final_confidence
    );
    let priority = if pair.final_confidence < CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };

    NewSuggestedAction {
        pipeline_run_id: Some(pipeline_run_id.to_string()),
        action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
        entity_id: None,
        group_id_1: Some(group_id.to_string()),
        group_id_2: None,
        cluster_id: None,
        triggering_confidence: Some(pair.final_confidence),
        details: Some(details),
        reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
        reason_message: Some(reason_msg),
        priority,
        status: SuggestionStatus::PendingReview.as_str().to_string(),
        reviewer_id: None,
        reviewed_at: None,
        review_notes: None,
    }
}

async fn fetch_existing_entity_groups(pool: &PgPool) -> Result<Arc<HashSet<(EntityId, EntityId)>>> {
    let conn = pool.get().await.context("URL: DB conn for existing groups")?;
    let rows = conn.query(
        "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1",
        &[&MatchMethodType::Url.as_str()],
    ).await.context("URL: Query existing groups")?;
    let pairs = rows.into_iter().map(|r| {
        let (id1, id2): (String, String) = (r.get(0), r.get(1));
        if id1 < id2 { (EntityId(id1), EntityId(id2)) } else { (EntityId(id2), EntityId(id1)) }
    }).collect();
    Ok(Arc::new(pairs))
}

async fn fetch_entities_with_urls_and_locations(pool: &PgPool) -> Result<Vec<EntityUrlInfo>> {
    let conn = pool.get().await.context("URL: DB conn for entities/URLs")?;
    let query = r#"
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
        UNION -- Using UNION to get distinct entity_id, url combinations
        SELECT entity_id, org_url AS url, entity_name FROM EntityOrgURLs
    ), EntityLocations AS (
        SELECT e.id AS entity_id, l.latitude, l.longitude,
               ROW_NUMBER() OVER(PARTITION BY e.id ORDER BY l.created DESC NULLS LAST, l.id) as rn -- Get one location per entity
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        JOIN public.location l ON ef.table_id = l.id
        WHERE l.latitude IS NOT NULL AND l.longitude IS NOT NULL
    )
    SELECT DISTINCT cu.entity_id, cu.url, cu.entity_name, el.latitude, el.longitude
    FROM CombinedURLs cu
    LEFT JOIN EntityLocations el ON cu.entity_id = el.entity_id AND el.rn = 1
    "#;
    let rows = conn.query(query, &[]).await.context("URL: Query entities/URLs")?;
    let mut entities_info = Vec::new();
    for r in rows {
        let entity_id_str: String = r.get(0);
        let url_str: String = r.get(1);
        let entity_name_opt: Option<String> = r.get(2);
        let latitude_opt: Option<f64> = r.try_get(3).ok(); // Use try_get for Option
        let longitude_opt: Option<f64> = r.try_get(4).ok();

        if let Some(normalized_data) = normalize_url_with_slugs(&url_str) {
            if !is_ignored_domain(&normalized_data.domain) {
                entities_info.push(EntityUrlInfo {
                    entity_id: EntityId(entity_id_str),
                    original_url: url_str,
                    normalized_data,
                    entity_name: entity_name_opt,
                    latitude: latitude_opt,
                    longitude: longitude_opt,
                });
            }
        }
    }
    Ok(entities_info)
}

fn group_entities_by_domain(entities: &[EntityUrlInfo]) -> HashMap<String, Vec<EntityUrlInfo>> {
    entities.iter().fold(HashMap::new(), |mut map, e_info| {
        map.entry(e_info.normalized_data.domain.clone()).or_default().push(e_info.clone());
        map
    })
}

fn apply_business_logic_filtering(map: HashMap<String, Vec<EntityUrlInfo>>) -> HashMap<String, Vec<EntityUrlInfo>> {
    map.into_iter().filter(|(domain, entities)| {
        if entities.len() < 2 { return false; } // Need at least 2 entities to form a pair
        let domain_type = categorize_domain(domain);
        let threshold = get_max_entities_threshold(&domain_type);
        let keep = entities.len() <= threshold;
        if !keep {
            debug!("URL: Excluding domain '{}' ({:?}) due to entity count: {} (threshold: {})", domain, domain_type, entities.len(), threshold);
        }
        keep
    }).collect()
}

fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in meters
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let (delta_phi, delta_lambda) = ((lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (delta_phi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

fn count_matching_slugs(slugs1: &[String], slugs2: &[String]) -> usize {
    slugs1.iter().zip(slugs2.iter()).take_while(|(s1, s2)| s1 == s2).count()
}

fn calculate_url_match_confidence(
    e1_info: &EntityUrlInfo,
    e2_info: &EntityUrlInfo,
    matching_slugs_count: usize,
    distance_meters_opt: Option<f64>,
) -> (f64, MatchValues) {
    // Ensure ordered for MatchValues consistency, though confidence logic might not strictly need it if symmetrical
    let (ordered_e1, ordered_e2) = if e1_info.entity_id.0 <= e2_info.entity_id.0 { (e1_info, e2_info) } else { (e2_info, e1_info) };

    let e1_slugs = &ordered_e1.normalized_data.path_slugs;
    let e2_slugs = &ordered_e2.normalized_data.path_slugs;

    let mut confidence = if matching_slugs_count == e1_slugs.len() && matching_slugs_count == e2_slugs.len() && !e1_slugs.is_empty() {
        CONFIDENCE_DOMAIN_FULL_PATH_MATCH
    } else {
        match matching_slugs_count {
            0 => CONFIDENCE_DOMAIN_ONLY,
            1 => CONFIDENCE_DOMAIN_PLUS_ONE_SLUG,
            2 => CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS,
            _ => CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS,
        }
    };

    // Adjust confidence based on distance
    if let Some(distance) = distance_meters_opt {
        if distance < 100.0 && matching_slugs_count > 0 { // Very close and some path match
            confidence = (confidence + 0.05).min(0.98);
        } else if distance > 10000.0 && matching_slugs_count == 0 { // Very far and only domain match
            confidence = (confidence * 0.7).max(0.55); // More significant penalty
        } else if distance > MAX_LOCATION_DISTANCE_METERS && matching_slugs_count <=1 { // Far and weak path match
             confidence = (confidence * 0.85).max(0.60); // Moderate penalty
        }
    }
     // Ensure confidence is within [0,1]
    confidence = confidence.max(0.0).min(1.0);


    let match_values = MatchValues::Url(UrlMatchValue {
        original_url1: ordered_e1.original_url.clone(),
        original_url2: ordered_e2.original_url.clone(),
        normalized_shared_domain: ordered_e1.normalized_data.domain.clone(), // Domain is same for both
        matching_slug_count: matching_slugs_count,
    });

    (confidence, match_values)
}


pub fn normalize_url_with_slugs(url_s: &str) -> Option<NormalizedUrlData> {
    let trimmed_url = url_s.trim();
    if trimmed_url.is_empty() || trimmed_url.starts_with("mailto:") || trimmed_url.starts_with("tel:") {
        return None;
    }

    let url_with_scheme = if !trimmed_url.contains("://") {
        format!("https://{}", trimmed_url) // Default to https
    } else {
        trimmed_url.to_string()
    };

    StdUrl::parse(&url_with_scheme).ok().and_then(|parsed_url| {
        parsed_url.host_str().and_then(|host| {
            let domain_lower = host.to_lowercase();
            let domain_no_www = domain_lower.strip_prefix("www.").unwrap_or(&domain_lower).to_string();

            if domain_no_www.is_empty() || !domain_no_www.contains('.') || is_ip_address(&domain_no_www) {
                None // Invalid or IP address domain
            } else {
                let path_slugs: Vec<String> = parsed_url.path_segments().map_or_else(Vec::new, |segments| {
                    segments.filter(|s| !s.is_empty() && s.len() < 50 && !s.contains('.')) // Filter slugs
                            .map(|s| s.to_lowercase())
                            .collect()
                });
                Some(NormalizedUrlData {
                    domain: domain_no_www.clone(),
                    path_slugs,
                    original_url: url_s.to_string(), // Store original URL
                    domain_type: categorize_domain(&domain_no_www),
                })
            }
        })
    })
}

pub fn is_ignored_domain(domain: &str) -> bool {
    const IGNORED_DOMAINS: &[&str] = &[
        "facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com",
        "youtu.be", "google.com", "maps.google.com", "goo.gl", "bit.ly", "t.co", "ow.ly",
        "tiktok.com", "pinterest.com", "snapchat.com", "reddit.com", "wa.me", "t.me",
        // Add more as needed
    ];
    // Also ignore if it's a common file sharing or very generic platform not indicative of the org itself
    const GENERIC_PLATFORMS: &[&str] = &[
        "eventbrite.com", "meetup.com", "wordpress.com", "blogspot.com", "wix.com", "squarespace.com",
        "godaddy.com", "sites.google.com", "forms.gle", "docs.google.com", "drive.google.com",
        "dropbox.com", "box.com", "onedrive.live.com", "zoom.us", "teams.microsoft.com",
        "github.io", // Often for projects, not orgs
    ];

    IGNORED_DOMAINS.iter().any(|d| *d == domain || domain.ends_with(&format!(".{}", d))) ||
    GENERIC_PLATFORMS.iter().any(|p| *p == domain || domain.ends_with(&format!(".{}", p)))
}


fn categorize_domain(domain_str: &str) -> DomainType {
    if domain_str.ends_with(".gov") || domain_str.ends_with(".mil") { DomainType::Government }
    else if domain_str.ends_with(".edu") || domain_str.contains("k12") { DomainType::Education }
    else if domain_str.ends_with(".org") || domain_str.contains("health") || domain_str.contains("hospital") || domain_str.contains("clinic") { DomainType::Healthcare }
    else if domain_str.ends_with(".com") || domain_str.ends_with(".net") || domain_str.ends_with(".biz") || domain_str.ends_with(".co") { DomainType::Commercial }
    else if domain_str.contains("facebook.") || domain_str.contains("twitter.") || domain_str.contains("linkedin.") || domain_str.contains("instagram.") { DomainType::Social }
    else { DomainType::Other }
}

fn get_max_entities_threshold(domain_type: &DomainType) -> usize {
    match domain_type {
        DomainType::Government => MAX_ENTITIES_PER_GOV_DOMAIN,
        DomainType::Healthcare => MAX_ENTITIES_PER_ORG_DOMAIN, // .org can be broad
        DomainType::Education => MAX_ENTITIES_PER_EDU_DOMAIN,
        DomainType::Commercial => MAX_ENTITIES_PER_COMMERCIAL_DOMAIN,
        DomainType::Social => 10, // Social media domains are usually not primary org sites
        DomainType::Other => MAX_ENTITIES_PER_ORG_DOMAIN, // Default for other .org or unknown TLDs
    }
}

fn is_ip_address(domain_candidate: &str) -> bool {
    // Basic check for IPv4
    if domain_candidate.split('.').count() == 4 && domain_candidate.split('.').all(|part| part.parse::<u8>().is_ok()) {
        return true;
    }
    // Basic check for IPv6 (contains colons)
    if domain_candidate.contains(':') {
        return true; // This is a simplification; proper IPv6 validation is complex
    }
    false
}
