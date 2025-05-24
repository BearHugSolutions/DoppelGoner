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
    self, insert_entity_group_tx, insert_match_decision_detail_tx, insert_suggestion_tx, PgPool,
};
use crate::models::{
    ActionType, EntityGroupId, EntityId, MatchMethodType, MatchValues, NewSuggestedAction,
    SuggestionStatus, UrlMatchValue,
};
use crate::reinforcement::entity::feature_cache_service::{
    FeatureCacheService, SharedFeatureCache,
};
use crate::reinforcement::entity::orchestrator::MatchingOrchestrator;
use crate::results::{AnyMatchResult, MatchMethodStats, UrlMatchResult};
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};
use serde_json;

// Confidence score tiers & other constants remain the same
const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;
const MAX_PARALLEL_DOMAINS_IN_FLIGHT: usize = 8;
const DOMAIN_PROCESSING_BATCH_SIZE: usize = 10;
const RL_FEATURE_EXTRACTION_BATCH_SIZE: usize = 50;
const MAX_PAIRS_PER_DOMAIN_SOFT_WARN: usize = 2000;
const MAX_PAIRS_PER_DOMAIN_HARD_LIMIT: usize = 5000;
const MAX_LOCATION_DISTANCE_METERS: f64 = 2000.0;
const MAX_ENTITIES_PER_GOV_DOMAIN: usize = 20;
const MAX_ENTITIES_PER_ORG_DOMAIN: usize = 50;
const MAX_ENTITIES_PER_EDU_DOMAIN: usize = 100;
const MAX_ENTITIES_PER_COMMERCIAL_DOMAIN: usize = 200;


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
    confidence_scores: Vec<f64>,
    task_errors: usize,
    pairs_considered_for_comparison: usize,
    rl_feature_extractions: usize,
    rl_feature_failures: usize,
    domains_completed_processing: usize,
    comparison_cache_hits: usize,
}

/// Holds data for a potential pair to be inserted.
struct PairToProcessUrl {
    entity_id_1: EntityId,
    entity_id_2: EntityId,
    match_values: MatchValues,
    final_confidence: f64,
    pre_rl_confidence: f64,
    features: Option<Vec<f64>>,
    sig1: Option<String>,
    sig2: Option<String>,
}

/// Main entry point for URL matching.
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    let start_time = Instant::now();
    info!("Starting V2 (TX) parallel URL matching (run ID: {})", pipeline_run_id);

    let existing_groups = fetch_existing_entity_groups(pool).await?;
    let entities_with_urls = fetch_entities_with_urls_and_locations(pool).await?;
    let domain_map = group_entities_by_domain(&entities_with_urls);
    let filtered_domains = apply_business_logic_filtering(domain_map);
    let total_domains = filtered_domains.len();
    info!("URL: Processing {} domains after filtering.", total_domains);

    if total_domains == 0 {
        return Ok(AnyMatchResult::Url(UrlMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(MatchMethodType::Url),
        }));
    }

    let stats_arc = Arc::new(Mutex::new(UrlMatchingStats::default()));
    let processed_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));
    let domain_entries: Vec<_> = filtered_domains.into_iter().collect();

    for chunk in domain_entries.chunks(DOMAIN_PROCESSING_BATCH_SIZE * MAX_PARALLEL_DOMAINS_IN_FLIGHT) {
        let mut futures = Vec::new();
        for (domain, entities) in chunk {
             futures.push(tokio::spawn({
                let domain_clone = domain.clone();
                let entities_clone = entities.clone();
                let pool_clone = pool.clone();
                let ro_clone = reinforcement_orchestrator_option.clone();
                let run_id_clone = pipeline_run_id.to_string();
                let fc_clone = feature_cache.clone();
                let stats_clone = stats_arc.clone();
                let processed_clone = processed_arc.clone();
                let existing_clone = existing_groups.clone();
                async move {
                    process_domain_task(
                        &domain_clone, &entities_clone, &pool_clone, ro_clone,
                        &run_id_clone, fc_clone, stats_clone, processed_clone, existing_clone,
                    ).await
                }
            }));
        }
        join_all(futures).await; // Wait for chunk
    }

    let final_stats = stats_arc.lock().await;
    let avg_confidence = if !final_stats.confidence_scores.is_empty() {
        final_stats.confidence_scores.iter().sum::<f64>() / final_stats.confidence_scores.len() as f64
    } else { 0.0 };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Url,
        groups_created: final_stats.new_pairs_created_db,
        entities_matched: final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats.new_pairs_created_db > 0 {
            final_stats.entities_in_new_pairs.len() as f64 / final_stats.new_pairs_created_db as f64
        } else { 0.0 },
    };

     info!(
        "URL matching V2 complete in {:.2?}: {} pairs considered, {} hits, {} new groups ({} errors).",
        start_time.elapsed(), final_stats.pairs_considered_for_comparison,
        final_stats.comparison_cache_hits, final_stats.new_pairs_created_db, final_stats.task_errors
    );

    Ok(AnyMatchResult::Url(UrlMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Task to process all pairs within a single domain.
#[allow(clippy::too_many_arguments)]
async fn process_domain_task(
    domain: &str,
    entities: &[EntityUrlInfo],
    pool: &PgPool,
    ro_opt: Option<Arc<Mutex<MatchingOrchestrator>>>,
    run_id: &str,
    fc_opt: Option<SharedFeatureCache>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
    processed_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    existing_arc: Arc<HashSet<(EntityId, EntityId)>>,
) -> Result<()> {
    let mut pairs_to_process = Vec::new();
    let mut pairs_considered = 0;

    for i in 0..entities.len() {
        for j in (i + 1)..entities.len() {
            if pairs_considered >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT { break; }
            pairs_considered += 1;
            let e1 = &entities[i];
            let e2 = &entities[j];
            let (e1_id, e2_id) = if e1.entity_id.0 < e2.entity_id.0 {
                (e1.entity_id.clone(), e2.entity_id.clone())
            } else {
                (e2.entity_id.clone(), e1.entity_id.clone())
            };
            let current_pair = (e1_id.clone(), e2_id.clone());

            if existing_arc.contains(&current_pair) { continue; }
            { // Scoped lock
                let processed = processed_arc.lock().await;
                if processed.contains(&current_pair) { continue; }
            }

            pairs_to_process.push((e1.clone(), e2.clone()));
        }
         if pairs_considered >= MAX_PAIRS_PER_DOMAIN_HARD_LIMIT { break; }
    }

     { // Scoped lock
        let mut stats = stats_arc.lock().await;
        stats.pairs_considered_for_comparison += pairs_considered;
     }

    if pairs_to_process.is_empty() {
        let mut stats = stats_arc.lock().await;
        stats.domains_completed_processing += 1;
        return Ok(());
    }

    // Process these pairs, check cache, calculate confidence, prepare for DB.
    let prepared_pairs = prepare_pairs_for_db(
        pairs_to_process, pool, ro_opt.clone(), run_id, fc_opt, processed_arc.clone(), stats_arc.clone()
    ).await?;

    // Run the DB transaction for this domain's batch.
    if !prepared_pairs.is_empty() {
        match run_batch_db_transaction_url(pool, prepared_pairs, run_id, ro_opt, stats_arc.clone()).await {
            Ok(created_count) => {
                 let mut stats = stats_arc.lock().await;
                 stats.new_pairs_created_db += created_count;
            }
            Err(e) => {
                warn!("URL: DB TX failed for domain {}: {}", domain, e);
                let mut stats = stats_arc.lock().await;
                stats.task_errors += 1;
            }
        }
    }

     { // Scoped lock
        let mut stats = stats_arc.lock().await;
        stats.domains_completed_processing += 1;
     }

    Ok(())
}

/// Prepares pairs: checks cache, calculates scores, extracts features.
async fn prepare_pairs_for_db(
    pairs: Vec<(EntityUrlInfo, EntityUrlInfo)>,
    pool: &PgPool,
    ro_opt: Option<Arc<Mutex<MatchingOrchestrator>>>,
    run_id: &str,
    _fc_opt: Option<SharedFeatureCache>, // Feature cache is handled within orchestrator now
    processed_arc: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
) -> Result<Vec<PairToProcessUrl>> {
    let mut prepared = Vec::new();

    for (e1_info, e2_info) in pairs {
        let (e1_id, e2_id) = if e1_info.entity_id.0 < e2_info.entity_id.0 {
            (e1_info.entity_id.clone(), e2_info.entity_id.clone())
        } else {
            (e2_info.entity_id.clone(), e1_info.entity_id.clone())
        };
        let current_pair = (e1_id.clone(), e2_id.clone());

        let mut sig1_opt = None;
        let mut sig2_opt = None;

        if let Ok(Some((s1_data, s2_data))) = get_current_signatures_for_pair(pool, &e1_id, &e2_id).await {
            sig1_opt = Some(s1_data.signature.clone());
            sig2_opt = Some(s2_data.signature.clone());
            if let Ok(Some(_)) = check_comparison_cache(pool, &e1_id, &e2_id, &s1_data.signature, &s2_data.signature, &MatchMethodType::Url).await {
                let mut stats = stats_arc.lock().await;
                stats.comparison_cache_hits += 1;
                let mut processed = processed_arc.lock().await;
                processed.insert(current_pair);
                continue;
            }
        }

        let distance = if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) =
            (e1_info.latitude, e1_info.longitude, e2_info.latitude, e2_info.longitude)
        { Some(calculate_distance(lat1, lon1, lat2, lon2)) } else { None };

        let slugs = count_matching_slugs(&e1_info.normalized_data.path_slugs, &e2_info.normalized_data.path_slugs);

        if let Some(dist) = distance {
            if dist > MAX_LOCATION_DISTANCE_METERS && slugs < 2 {
                // If too far and not enough path match, consider NON_MATCH and cache it.
                 if let (Some(s1), Some(s2)) = (&sig1_opt, &sig2_opt) {
                    let _ = store_in_comparison_cache(pool, &e1_id, &e2_id, s1, s2, &MatchMethodType::Url, run_id, "NON_MATCH", Some(0.0), None).await;
                 }
                let mut processed = processed_arc.lock().await;
                processed.insert(current_pair);
                continue;
            }
        }

        let (pre_rl_conf, match_values) = calculate_url_match_confidence(&e1_info, &e2_info, slugs, distance);
        let mut final_conf = pre_rl_conf;
        let mut features: Option<Vec<f64>> = None;

        if let Some(ro) = &ro_opt {
            let mut stats = stats_arc.lock().await;
            match MatchingOrchestrator::extract_pair_context_features(pool, &e1_id, &e2_id).await {
                Ok(f) => {
                    stats.rl_feature_extractions += 1;
                    if !f.is_empty() {
                        features = Some(f.clone());
                         match ro.lock().await.get_tuned_confidence(&MatchMethodType::Url, pre_rl_conf, &f) {
                             Ok(tuned) => final_conf = tuned,
                             Err(e) => warn!("URL RL tuning failed ({},{}): {}", e1_id.0, e2_id.0, e),
                         }
                    }
                }
                Err(e) => {
                    stats.rl_feature_failures += 1;
                    warn!("URL Feature extraction failed ({},{}): {}", e1_id.0, e2_id.0, e);
                }
            }
        }

        prepared.push(PairToProcessUrl {
            entity_id_1: e1_id, entity_id_2: e2_id,
            match_values, final_confidence: final_conf, pre_rl_confidence: pre_rl_conf,
            features, sig1: sig1_opt, sig2: sig2_opt,
        });

        let mut processed = processed_arc.lock().await;
        processed.insert(current_pair);
    }
    Ok(prepared)
}

/// Runs the DB transaction for a batch of URL pairs.
async fn run_batch_db_transaction_url(
    pool: &PgPool,
    pairs_to_process: Vec<PairToProcessUrl>,
    pipeline_run_id: &str,
    ro_opt: Option<Arc<Mutex<MatchingOrchestrator>>>,
    stats_arc: Arc<Mutex<UrlMatchingStats>>,
) -> Result<usize> {
    let mut conn = pool.get().await.context("URL: DB conn for TX")?;
    let mut tx = conn.transaction().await.context("URL: Start TX")?;
    let mut new_groups_count = 0;

    for pair in pairs_to_process {
        let mut sp = tx.savepoint("url_pair").await?;
        let new_gid_str = Uuid::new_v4().to_string();

        match insert_entity_group_tx(
            &mut sp, &new_gid_str, &pair.entity_id_1, &pair.entity_id_2,
            pair.final_confidence, pair.pre_rl_confidence,
            MatchMethodType::Url, pair.match_values.clone(),
        ).await {
            Ok((group_id, was_new)) => {
                if was_new {
                    new_groups_count += 1;
                    { // Update stats within scope
                        let mut stats = stats_arc.lock().await;
                        stats.entities_in_new_pairs.insert(pair.entity_id_1.clone());
                        stats.entities_in_new_pairs.insert(pair.entity_id_2.clone());
                        stats.confidence_scores.push(pair.final_confidence);
                    }

                    let mut version = None;
                    if let (Some(ro), Some(feats)) = (&ro_opt, &pair.features) {
                        version = Some(ro.lock().await.confidence_tuner.version as i32);
                        let feats_json = serde_json::to_value(feats).unwrap_or_default();
                         if let Err(e) = insert_match_decision_detail_tx(
                            &mut sp, &group_id, pipeline_run_id, feats_json,
                            MatchMethodType::Url.as_str(), pair.pre_rl_confidence,
                            pair.final_confidence, version,
                        ).await {
                             warn!("URL: Decision detail insert failed ({}): {}", group_id, e);
                             sp.rollback().await?; continue;
                         }
                    }

                    if pair.final_confidence < MODERATE_LOW_SUGGESTION_THRESHOLD {
                        let sugg = create_suggestion_url(&group_id, &pair, pipeline_run_id);
                         if let Err(e) = insert_suggestion_tx(&mut sp, &sugg).await {
                            warn!("URL: Suggestion insert failed ({}): {}", group_id, e);
                            sp.rollback().await?; continue;
                         }
                    }
                }
                // Store in cache *after* successful processing attempt
                if let (Some(s1), Some(s2)) = (pair.sig1, pair.sig2) {
                     let _ = store_in_comparison_cache(
                        pool, &pair.entity_id_1, &pair.entity_id_2, &s1, &s2,
                        &MatchMethodType::Url, pipeline_run_id, "MATCH",
                        Some(pair.final_confidence), pair.features.as_ref().map(|f| serde_json::to_value(f).unwrap_or_default()).as_ref()
                    ).await;
                }
                sp.commit().await?; // Commit successful pair
            }
            Err(e) => {
                warn!("URL: Group insert failed ({}, {}): {}", pair.entity_id_1.0, pair.entity_id_2.0, e);
                sp.rollback().await?; // Rollback failed pair
            }
        }
    }

    tx.commit().await.context("URL: Commit main TX")?;
    Ok(new_groups_count)
}

/// Creates a suggestion object for URL matches.
fn create_suggestion_url(
    group_id: &str,
    pair: &PairToProcessUrl,
    pipeline_run_id: &str,
) -> NewSuggestedAction {
     let (orig_url1, orig_url2, norm_domain, slug_count) = if let MatchValues::Url(ref uv) = pair.match_values {
         (uv.original_url1.clone(), uv.original_url2.clone(), uv.normalized_shared_domain.clone(), uv.matching_slug_count)
     } else { (String::from("N/A"), String::from("N/A"), String::from("N/A"), 0) };

     let details = serde_json::json!({
        "method_type": MatchMethodType::Url.as_str(),
        "original_url1": orig_url1, "original_url2": orig_url2,
        "normalized_shared_domain": norm_domain, "matching_slug_count": slug_count,
        "entity_group_id": group_id, "pre_rl_confidence": pair.pre_rl_confidence,
     });
    let reason_msg = format!(
        "Pair ({},{}) URL (domain: {}, slugs: {}) low confidence ({:.4}).",
        pair.entity_id_1.0, pair.entity_id_2.0, norm_domain, slug_count, pair.final_confidence
    );
     let priority = if pair.final_confidence < CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };

    NewSuggestedAction {
        pipeline_run_id: Some(pipeline_run_id.to_string()),
        action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
        entity_id: None, group_id_1: Some(group_id.to_string()), group_id_2: None, cluster_id: None,
        triggering_confidence: Some(pair.final_confidence), details: Some(details),
        reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()), reason_message: Some(reason_msg),
        priority, status: SuggestionStatus::PendingReview.as_str().to_string(),
        reviewer_id: None, reviewed_at: None, review_notes: None,
    }
}


// --- Keep existing helper functions (fetch_*, group_*, normalize_*, etc.) ---
// These functions are assumed to be present and correct as per the provided code.
// Only functions directly involved in the transaction/batch processing are shown refactored.
// Add necessary helper functions here: fetch_existing_entity_groups,
// fetch_entities_with_urls_and_locations, group_entities_by_domain,
// apply_business_logic_filtering, calculate_distance, count_matching_slugs,
// calculate_url_match_confidence, normalize_url_with_slugs, is_ignored_domain,
// categorize_domain, get_max_entities_threshold, is_ip_address.
// (For brevity, these helpers are not repeated here, but they MUST be included
// in the actual file for it to compile and run).

async fn fetch_existing_entity_groups(pool: &PgPool) -> Result<Arc<HashSet<(EntityId, EntityId)>>> {
    let conn = pool.get().await.context("URL: DB conn for existing groups")?;
    let rows = conn.query("SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1", &[&MatchMethodType::Url.as_str()]).await.context("URL: Query existing groups")?;
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
    let mut entities = Vec::new();
    for r in rows {
        let (eid_s, url_s, ename_o, lat_o, lon_o): (String, String, Option<String>, Option<f64>, Option<f64>) =
            (r.get(0), r.get(1), r.get(2), r.try_get(3).ok(), r.try_get(4).ok());
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
    entities.iter().fold(HashMap::new(), |mut map, e_info| {
        map.entry(e_info.normalized_data.domain.clone()).or_default().push(e_info.clone());
        map
    })
}

fn apply_business_logic_filtering(map: HashMap<String, Vec<EntityUrlInfo>>) -> HashMap<String, Vec<EntityUrlInfo>> {
    map.into_iter().filter(|(domain, entities)| {
        if entities.len() < 2 { return false; }
        let domain_type = categorize_domain(domain);
        let threshold = get_max_entities_threshold(&domain_type);
        let keep = entities.len() <= threshold;
        if !keep { debug!("URL: Excluding domain '{}' ({:?}) - {} entities (>{})", domain, domain_type, entities.len(), threshold); }
        keep
    }).collect()
}

fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0;
    let (l1r, l2r, dla, dlo) = (lat1.to_radians(), lat2.to_radians(), (lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (dla / 2.0).sin().powi(2) + l1r.cos() * l2r.cos() * (dlo / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

fn count_matching_slugs(s1: &[String], s2: &[String]) -> usize {
    s1.iter().zip(s2.iter()).take_while(|(a, b)| a == b).count()
}

fn calculate_url_match_confidence(e1: &EntityUrlInfo, e2: &EntityUrlInfo, slugs: usize, dist_m: Option<f64>) -> (f64, MatchValues) {
    let (ordered_e1, ordered_e2) = if e1.entity_id.0 <= e2.entity_id.0 { (e1, e2) } else { (e2, e1) };
    let full_match = slugs == ordered_e1.normalized_data.path_slugs.len() && slugs == ordered_e2.normalized_data.path_slugs.len() && !ordered_e1.normalized_data.path_slugs.is_empty();
    let mut conf = if full_match { CONFIDENCE_DOMAIN_FULL_PATH_MATCH } else {
        match slugs { 0 => CONFIDENCE_DOMAIN_ONLY, 1 => CONFIDENCE_DOMAIN_PLUS_ONE_SLUG, 2 => CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS, _ => CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS }
    };
    if let Some(d) = dist_m {
        if d < 100.0 && slugs > 0 { conf = (conf + 0.05).min(0.98); }
        else if d > 10000.0 && slugs == 0 { conf = (conf - 0.15).max(0.55); }
        else if d > MAX_LOCATION_DISTANCE_METERS && slugs <= 1 { conf = (conf - 0.08).max(0.60); }
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
    StdUrl::parse(&w_scheme).ok().and_then(|p_url| p_url.host_str().and_then(|h| {
        let dnw = h.to_lowercase().trim_start_matches("www.").to_string();
        if dnw.is_empty() || !dnw.contains('.') || is_ip_address(&dnw) { None } else {
            Some(NormalizedUrlData {
                domain: dnw.clone(),
                path_slugs: p_url.path_segments().map_or_else(Vec::new, |s| s.filter(|ps| !ps.is_empty() && ps.len() < 50 && !ps.contains('.')).map(|ps| ps.to_lowercase()).collect()),
                original_url: url_s.to_string(), domain_type: categorize_domain(&dnw),
            })
        }
    }))
}

pub fn is_ignored_domain(domain: &str) -> bool {
     const IGNORED_DOMAINS: &[&str] = &[ "facebook.com", "twitter.com", "instagram.com", "linkedin.com", "youtube.com", "google.com", "bit.ly", "t.co", /* etc. */ ];
     IGNORED_DOMAINS.iter().any(|d| *d == domain || domain.ends_with(&format!(".{}", d)))
}

fn categorize_domain(dom: &str) -> DomainType {
    if dom.ends_with(".gov") || dom.ends_with(".mil") { DomainType::Government }
    else if dom.ends_with(".edu") || dom.contains("k12") { DomainType::Education }
    else if dom.ends_with(".org") || dom.contains("health") || dom.contains("hospital") { DomainType::Healthcare }
    else if dom.ends_with(".com") || dom.ends_with(".net") { DomainType::Commercial }
    else if dom.contains("facebook.") || dom.contains("twitter.") { DomainType::Social }
    else { DomainType::Other }
}

fn get_max_entities_threshold(dt: &DomainType) -> usize {
    match dt {
        DomainType::Government => MAX_ENTITIES_PER_GOV_DOMAIN,
        DomainType::Healthcare => MAX_ENTITIES_PER_ORG_DOMAIN,
        DomainType::Education => MAX_ENTITIES_PER_EDU_DOMAIN,
        DomainType::Commercial => MAX_ENTITIES_PER_COMMERCIAL_DOMAIN,
        DomainType::Social => 10,
        DomainType::Other => MAX_ENTITIES_PER_ORG_DOMAIN,
    }
}

fn is_ip_address(dc: &str) -> bool {
    (dc.split('.').count() == 4 && dc.split('.').all(|p| p.parse::<u8>().is_ok())) || dc.contains(':')
}