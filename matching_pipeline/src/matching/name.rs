// src/matching/name.rs - Refactored for memory efficiency and batching
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use log::{debug, info, trace, warn};
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use strsim::jaro_winkler;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{db, results::{AnyMatchResult, MatchMethodStats, NameMatchResult}};
use crate::utils::cosine_similarity_candle;
use crate::{
    config,
    db::PgPool, // Using db::upsert_entity_group, db::insert_match_decision_detail_direct, db::insert_suggestion
    reinforcement::entity::{
        feature_cache_service::SharedFeatureCache, orchestrator::MatchingOrchestrator,
    },
};
use crate::{
    config::{CRITICALLY_LOW_SUGGESTION_THRESHOLD, MODERATE_LOW_SUGGESTION_THRESHOLD},
    models::{
        ActionType, Entity, EntityId, MatchMethodType, MatchValues, NameMatchValue,
        NewSuggestedAction, OrganizationId, SuggestionStatus,
    },
};
use serde_json;

use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};

const MIN_FUZZY_SIMILARITY_THRESHOLD: f32 = 0.92;
const MIN_SEMANTIC_SIMILARITY_THRESHOLD: f32 = 0.94;
const COMBINED_SIMILARITY_THRESHOLD: f32 = 0.93; // Adjusted based on typical usage
const FUZZY_WEIGHT: f32 = 0.3;
const SEMANTIC_WEIGHT: f32 = 0.7;
const TOP_TOKENS_PER_ENTITY: usize = 10;
const MIN_TOKEN_OVERLAP: usize = 3;
pub const MIN_TOKEN_LENGTH: usize = 2;

const BATCH_SIZE: usize = 100;
const MAX_CONCURRENT_BATCHES: usize = 8;
const MAX_CANDIDATES_PER_ENTITY: usize = 50;
const MAX_TOTAL_CANDIDATE_PAIRS: usize = 75_000;

const INCOMPATIBLE_ORG_TYPES: [(&str, &str); 10] = [
    ("fire", "police"),
    ("fire", "school"),
    ("police", "school"),
    ("hospital", "school"),
    ("hospital", "church"),
    ("health", "education"),
    ("medical", "food"),
    ("college", "hospital"),
    ("prison", "school"),
    ("shelter", "college"),
];

const NOISE_PREFIXES: [&str; 2] = ["zzz - ", "re-add "];
const LOCATION_PREFIXES: [&str; 8] = [
    "north", "south", "east", "west", "sound", "valley", "pacific", "olympic",
];

pub const STOPWORDS: [&str; 130] = [
    "a", "an", "the", "and", "or", "but", "nor", "for", "yet", "so", "in", "on", "at", "by",
    "to", "with", "from", "of", "as", "into", "about", "before", "after", "during", "until",
    "since", "unless", "inc", "incorporated", "corp", "corporation", "llc", "ltd", "limited",
    "company", "co", "group", "holdings", "enterprises", "international", "global", "worldwide",
    "national", "american", "usa", "us", "service", "services", "solutions", "systems",
    "associates", "partners", "partnership", "organization", "organisation", "foundation",
    "institute", "association", "society", "council", "committee", "center", "centre",
    "department", "division", "unit", "office", "bureau", "agency", "authority", "board",
    "new", "old", "great", "greater", "best", "better", "first", "second", "third", "primary",
    "main", "central", "local", "regional", "official", "this", "that", "these", "those", "it",
    "they", "them", "their", "our", "your", "all", "any", "each", "every", "some", "such", "no",
    "not", "only", "very", "program", "community", "resource", "resources", "support", "help",
    "health", "care", "management", "professional", "public", "private", "general", "federal",
    "state", "county", "district", "area", "branch", "provider", "member",
    "directory", "guide", "network",
];

#[derive(Clone)]
struct EntityNameData {
    entity: Entity, // Keep the original Entity for ID and original name
    normalized_name: String,
    entity_type: Option<&'static str>, // Detected type
    tokens: HashSet<String>, // For quick overlap checks
    // weighted_tokens: Vec<(String, f32)>, // If TF-IDF or other weighting is used later
}

#[derive(Clone)]
struct SharedNameMatchingData {
    existing_entity_group_pairs: Arc<HashSet<(EntityId, EntityId)>>,
    org_embeddings_map: Arc<HashMap<EntityId, Option<Vec<f32>>>>,
    entity_data: Arc<Vec<EntityNameData>>, // Arc for shared access
}

struct NameMatchingStats {
    new_pairs_created_count: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores_for_stats: Vec<f64>,
    individual_operation_errors: usize,
    cache_hits_count: usize,
    feature_extraction_count: usize,
    feature_extraction_failures: usize,
    pairs_processed: usize, // Total pairs considered for detailed comparison
}

impl Default for NameMatchingStats {
    fn default() -> Self {
        Self {
            new_pairs_created_count: 0,
            entities_in_new_pairs: HashSet::new(),
            confidence_scores_for_stats: Vec::new(),
            individual_operation_errors: 0,
            cache_hits_count: 0,
            feature_extraction_count: 0,
            feature_extraction_failures: 0,
            pairs_processed: 0,
        }
    }
}

pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 memory-efficient name matching (run ID: {}) with INCREMENTAL CHECKS (direct DB calls)...",
        pipeline_run_id,
    );
    let start_time = Instant::now();
    let initial_memory = get_memory_usage().await;
    info!("Name: Initial memory usage: {} MB", initial_memory);

    let mut initial_conn = pool
        .get()
        .await
        .context("Name: Failed to get DB connection")?;

    let all_entities_with_names_vec = get_all_entities_with_names(&*initial_conn).await?;
    let total_entities = all_entities_with_names_vec.len();
    info!(
        "Name: Found {} entities with non-empty names.",
        total_entities
    );

    if total_entities < 2 {
        // Not enough entities to form pairs
        return Ok(AnyMatchResult::Name(NameMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(MatchMethodType::Name),
        }));
    }

    let existing_entity_group_pairs =
        Arc::new(fetch_existing_entity_group_pairs(&*initial_conn, MatchMethodType::Name).await?);
    info!(
        "Name: Found {} existing name-matched pairs.",
        existing_entity_group_pairs.len()
    );

    let org_embeddings_map =
        Arc::new(get_organization_embeddings(&*initial_conn, &all_entities_with_names_vec).await?);
    drop(initial_conn); // Release connection

    let stopwords: HashSet<String> = STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let (entity_data_vec, token_to_entities_idx_map, _) = // Removed token_stats as it's not directly used in pair generation logic here
        prepare_entity_data_and_index(&all_entities_with_names_vec, &stopwords).await;

    let entity_data_arc = Arc::new(entity_data_vec);

    let candidate_pairs_indices =
        generate_candidate_pairs_indices_limited(&entity_data_arc, &token_to_entities_idx_map)
            .await?;
    let total_candidates = candidate_pairs_indices.len();
    info!(
        "Name: Generated {} candidate pairs for comparison.",
        total_candidates
    );

    if total_candidates == 0 {
        return Ok(AnyMatchResult::Name(NameMatchResult {
            groups_created: 0,
            stats: MatchMethodStats::default_for(MatchMethodType::Name),
        }));
    }

    let shared_data = SharedNameMatchingData {
        existing_entity_group_pairs,
        org_embeddings_map,
        entity_data: entity_data_arc,
    };

    let stats = process_candidate_pairs_in_batches(
        candidate_pairs_indices,
        shared_data,
        pool,
        reinforcement_orchestrator_option,
        pipeline_run_id,
        feature_cache,
    )
    .await?;

    let final_memory = get_memory_usage().await;
    info!(
        "Name: Final memory usage: {} MB (delta: {} MB)",
        final_memory,
        final_memory.saturating_sub(initial_memory)
    );

    let avg_confidence: f64 = if !stats.confidence_scores_for_stats.is_empty() {
        stats.confidence_scores_for_stats.iter().sum::<f64>()
            / stats.confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Name,
        groups_created: stats.new_pairs_created_count,
        entities_matched: stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if stats.new_pairs_created_count > 0 {
            2.0
        } else {
            0.0
        },
    };

    info!(
        "Name matching complete in {:.2?}: {} pairs processed, {} cache hits, {} new pairs ({} errors), {} unique entities.",
        start_time.elapsed(),
        stats.pairs_processed,
        stats.cache_hits_count,
        method_stats.groups_created,
        stats.individual_operation_errors,
        method_stats.entities_matched
    );
    info!(
        "Name feature extraction: {} successful, {} failed",
        stats.feature_extraction_count, stats.feature_extraction_failures
    );

    Ok(AnyMatchResult::Name(NameMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

async fn process_candidate_pairs_in_batches(
    candidate_pairs_indices: Vec<(usize, usize)>,
    shared_data: SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<NameMatchingStats> {
    let total_pairs = candidate_pairs_indices.len();
    let total_batches = (total_pairs + BATCH_SIZE - 1) / BATCH_SIZE;
    info!(
        "Name: Processing {} pairs in {} batches of size {}",
        total_pairs, total_batches, BATCH_SIZE
    );

    let stats_mutex = Arc::new(Mutex::new(NameMatchingStats::default()));
    let processed_pairs_this_run = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));

    for (chunk_idx, chunk) in candidate_pairs_indices
        .chunks(BATCH_SIZE * MAX_CONCURRENT_BATCHES)
        .enumerate()
    {
        let mut batch_futures = Vec::new();
        for (batch_idx_in_chunk, batch_pair_indices) in chunk.chunks(BATCH_SIZE).enumerate() {
            let global_batch_num = chunk_idx * MAX_CONCURRENT_BATCHES + batch_idx_in_chunk + 1;
            let batch_data_for_task = batch_pair_indices.to_vec();

            let pool_clone = pool.clone();
            let shared_data_clone = shared_data.clone();
            let ro_option_clone = reinforcement_orchestrator_option.clone();
            let run_id_clone = pipeline_run_id.to_string();
            let feature_cache_clone = feature_cache.clone();
            let stats_arc_clone = Arc::clone(&stats_mutex);
            let processed_pairs_clone = Arc::clone(&processed_pairs_this_run);

            batch_futures.push(tokio::spawn(async move {
                process_name_batch(
                    global_batch_num,
                    total_batches,
                    batch_data_for_task,
                    shared_data_clone,
                    &pool_clone,
                    ro_option_clone.as_ref(),
                    &run_id_clone,
                    feature_cache_clone.as_ref(),
                    stats_arc_clone,
                    processed_pairs_clone,
                )
                .await
            }));
        }

        let results = futures::future::join_all(batch_futures).await;
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                warn!("Name: Batch processing task {} panicked or failed: {}", i, e);
                // Error already logged inside task if it's a processing error
            }
        }
        if chunk_idx % 5 == 0 {
            let current_memory = get_memory_usage().await;
            let stats_guard = stats_mutex.lock().await;
            info!(
                "Name: Chunk {}/{} completed. Memory: {} MB. Pairs processed so far: {}. New groups: {}",
                chunk_idx +1, (total_pairs + (BATCH_SIZE * MAX_CONCURRENT_BATCHES) -1) / (BATCH_SIZE*MAX_CONCURRENT_BATCHES),
                current_memory,
                stats_guard.pairs_processed,
                stats_guard.new_pairs_created_count
            );
        }
    }

    let final_stats_values = Arc::try_unwrap(stats_mutex)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap stats mutex"))?
        .into_inner();
    Ok(final_stats_values)
}

#[allow(clippy::too_many_arguments)]
async fn process_name_batch(
    batch_num_for_log: usize,
    total_batches_for_log: usize,
    batch_pair_indices: Vec<(usize, usize)>,
    shared_data: SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>,
    stats_mutex: Arc<Mutex<NameMatchingStats>>,
    processed_pairs_this_run: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
) -> Result<()> {
    debug!(
        "Name: Processing batch {}/{} ({} pairs)",
        batch_num_for_log,
        total_batches_for_log,
        batch_pair_indices.len()
    );
    let mut local_batch_errors = 0;

    for (idx1, idx2) in batch_pair_indices {
        let entity1_data = &shared_data.entity_data[idx1];
        let entity2_data = &shared_data.entity_data[idx2];

        let (e1_id, e2_id) = (&entity1_data.entity.id, &entity2_data.entity.id);
        let current_pair_ordered = if e1_id.0 < e2_id.0 {
            (e1_id.clone(), e2_id.clone())
        } else {
            (e2_id.clone(), e1_id.clone())
        };

        {
            let mut stats_guard = stats_mutex.lock().await;
            stats_guard.pairs_processed += 1;
        }
        {
            let processed_set = processed_pairs_this_run.lock().await;
            if processed_set.contains(&current_pair_ordered) {
                continue;
            }
        }

        match process_single_name_pair(
            entity1_data,
            entity2_data,
            &shared_data,
            pool,
            reinforcement_orchestrator_option,
            pipeline_run_id,
            feature_cache,
            &current_pair_ordered, // Pass the ordered pair
        )
        .await
        {
            Ok(pair_result_opt) => {
                let mut processed_set = processed_pairs_this_run.lock().await;
                processed_set.insert(current_pair_ordered); // Mark as processed
                drop(processed_set);

                if let Some(pair_result) = pair_result_opt {
                    let mut stats_guard = stats_mutex.lock().await;
                    if pair_result.was_group_created {
                        stats_guard.new_pairs_created_count += 1;
                        stats_guard.entities_in_new_pairs.insert(e1_id.clone());
                        stats_guard.entities_in_new_pairs.insert(e2_id.clone());
                        stats_guard.confidence_scores_for_stats.push(pair_result.final_confidence);
                    }
                    if pair_result.was_cache_hit {
                        stats_guard.cache_hits_count += 1;
                    }
                    if pair_result.feature_extraction_attempted {
                        if pair_result.feature_extraction_success {
                            stats_guard.feature_extraction_count += 1;
                        } else {
                            stats_guard.feature_extraction_failures += 1;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Name: Error processing pair ({}, {}): {}",
                    e1_id.0, e2_id.0, e
                );
                local_batch_errors += 1;
            }
        }
    }
    if local_batch_errors > 0 {
        let mut stats_guard = stats_mutex.lock().await;
        stats_guard.individual_operation_errors += local_batch_errors;
    }
    debug!("Name: Batch {}/{} completed.", batch_num_for_log, total_batches_for_log);
    Ok(())
}

struct SinglePairProcessingResult {
    final_confidence: f64,
    was_cache_hit: bool,
    feature_extraction_attempted: bool,
    feature_extraction_success: bool,
    was_group_created: bool,
}

#[allow(clippy::too_many_arguments)]
async fn process_single_name_pair(
    entity1_data_ref: &EntityNameData, // Renamed to avoid conflict
    entity2_data_ref: &EntityNameData, // Renamed to avoid conflict
    shared_data: &SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>, // Passed as Option<&SharedFeatureCache>
    current_pair_ordered: &(EntityId, EntityId), // Already ordered
) -> Result<Option<SinglePairProcessingResult>> { // Returns Option to indicate if a group was made or cache hit
    let (ordered_e1_id, ordered_e2_id) = current_pair_ordered;

    // Determine which entity_data corresponds to ordered_e1_id and ordered_e2_id
    let entity1_actual_data = if &entity1_data_ref.entity.id == ordered_e1_id {
        entity1_data_ref
    } else {
        entity2_data_ref
    };
    let entity2_actual_data = if &entity2_data_ref.entity.id == ordered_e2_id {
        entity2_data_ref
    } else {
        entity1_data_ref
    };

    let mut processing_result_state = SinglePairProcessingResult {
        final_confidence: 0.0, was_cache_hit: false, feature_extraction_attempted: false,
        feature_extraction_success: false, was_group_created: false,
    };

    let current_signatures_opt =
        match get_current_signatures_for_pair(pool, ordered_e1_id, ordered_e2_id).await {
            Ok(sigs) => sigs,
            Err(e) => {
                warn!("Name: Failed to get signatures for pair ({}, {}): {}. Proceeding without cache.", ordered_e1_id.0, ordered_e2_id.0, e);
                None
            }
        };

    if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
        match check_comparison_cache(
            pool, ordered_e1_id, ordered_e2_id, &sig1_data.signature, &sig2_data.signature, &MatchMethodType::Name,
        ).await {
            Ok(Some(_)) => {
                processing_result_state.was_cache_hit = true;
                return Ok(Some(processing_result_state));
            }
            Ok(None) => { /* Cache miss, continue */ }
            Err(e) => warn!("Name: Cache check error for ({}, {}): {}", ordered_e1_id.0, ordered_e2_id.0, e),
        }
    }

    let original_name1 = entity1_actual_data.entity.name.as_ref().cloned().unwrap_or_default();
    let original_name2 = entity2_actual_data.entity.name.as_ref().cloned().unwrap_or_default();
    let normalized_name1 = &entity1_actual_data.normalized_name;
    let normalized_name2 = &entity2_actual_data.normalized_name;

    if normalized_name1.is_empty() || normalized_name2.is_empty() { return Ok(None); }

    if let (Some(t1), Some(t2)) = (entity1_actual_data.entity_type, entity2_actual_data.entity_type) {
        if t1 != t2 && INCOMPATIBLE_ORG_TYPES.iter().any(|(it1, it2)| (t1 == *it1 && t2 == *it2) || (t1 == *it2 && t2 == *it1)) {
            return Ok(None);
        }
    }

    let fuzzy_score = jaro_winkler(normalized_name1, normalized_name2) as f32;
    if fuzzy_score < (MIN_FUZZY_SIMILARITY_THRESHOLD * 0.9) { // Early exit for very low fuzzy
        if let Some((s1, s2)) = &current_signatures_opt {
            store_in_comparison_cache(pool, ordered_e1_id, ordered_e2_id, &s1.signature, &s2.signature, &MatchMethodType::Name, pipeline_run_id, "NON_MATCH", Some(fuzzy_score as f64), None).await?;
        }
        return Ok(None);
    }

    let embedding1_opt = shared_data.org_embeddings_map.get(ordered_e1_id).and_then(|o| o.as_ref());
    let embedding2_opt = shared_data.org_embeddings_map.get(ordered_e2_id).and_then(|o| o.as_ref());
    let semantic_score = match (embedding1_opt, embedding2_opt) {
        (Some(e1), Some(e2)) => cosine_similarity_candle(e1, e2).unwrap_or(0.0) as f32,
        _ => 0.0,
    };

    let (pre_rl_score, pre_rl_match_type_str) = if semantic_score >= MIN_SEMANTIC_SIMILARITY_THRESHOLD {
        ((fuzzy_score * FUZZY_WEIGHT) + (semantic_score * SEMANTIC_WEIGHT), "combined")
    } else if fuzzy_score >= MIN_FUZZY_SIMILARITY_THRESHOLD {
        (fuzzy_score, "fuzzy")
    } else {
        if let Some((s1,s2)) = &current_signatures_opt {
             store_in_comparison_cache(pool, ordered_e1_id, ordered_e2_id, &s1.signature, &s2.signature, &MatchMethodType::Name, pipeline_run_id, "NON_MATCH", Some(fuzzy_score.max(semantic_score) as f64), None).await?;
        }
        return Ok(None);
    };

    let adjusted_pre_rl_score = apply_domain_rules(normalized_name1, normalized_name2, pre_rl_score, entity1_actual_data.entity_type, entity2_actual_data.entity_type);
    if adjusted_pre_rl_score < COMBINED_SIMILARITY_THRESHOLD {
        if let Some((s1,s2)) = &current_signatures_opt {
            store_in_comparison_cache(pool, ordered_e1_id, ordered_e2_id, &s1.signature, &s2.signature, &MatchMethodType::Name, pipeline_run_id, "NON_MATCH", Some(adjusted_pre_rl_score as f64), None).await?;
        }
        return Ok(None);
    }

    processing_result_state.final_confidence = adjusted_pre_rl_score as f64; // Start with pre-RL
    let mut features_json_for_cache: Option<serde_json::Value> = None;

    if let Some(ro_arc) = reinforcement_orchestrator_option {
        processing_result_state.feature_extraction_attempted = true;
        match if let Some(fc_arc) = feature_cache {
            let mut fc_guard = fc_arc.lock().await;
            fc_guard.get_pair_features(pool, ordered_e1_id, ordered_e2_id).await
        } else {
            MatchingOrchestrator::extract_pair_context_features(pool, ordered_e1_id, ordered_e2_id).await
        } {
            Ok(features) => {
                processing_result_state.feature_extraction_success = true;
                if !features.is_empty() {
                    features_json_for_cache = serde_json::to_value(features.clone()).ok();
                    match ro_arc.lock().await.get_tuned_confidence(&MatchMethodType::Name, adjusted_pre_rl_score as f64, &features) {
                        Ok(tuned_score) => processing_result_state.final_confidence = tuned_score,
                        Err(e) => warn!("Name: RL tuning failed for ({}, {}): {}", ordered_e1_id.0, ordered_e2_id.0, e),
                    }
                }
            }
            Err(e) => warn!("Name: Feature extraction failed for ({}, {}): {}", ordered_e1_id.0, ordered_e2_id.0, e),
        }
    }

    if let Some((s1,s2)) = &current_signatures_opt {
        store_in_comparison_cache(pool, ordered_e1_id, ordered_e2_id, &s1.signature, &s2.signature, &MatchMethodType::Name, pipeline_run_id, "MATCH", Some(processing_result_state.final_confidence), features_json_for_cache.as_ref()).await?;
    }

    let match_values = MatchValues::Name(NameMatchValue {
        original_name1, original_name2,
        normalized_name1: normalized_name1.clone(), normalized_name2: normalized_name2.clone(),
        pre_rl_match_type: Some(pre_rl_match_type_str.to_string()),
    });

    let new_entity_group_id_str = Uuid::new_v4().to_string();
    let (group_id, was_newly_inserted) = db::upsert_entity_group(
        pool, &new_entity_group_id_str, ordered_e1_id, ordered_e2_id,
        processing_result_state.final_confidence, adjusted_pre_rl_score as f64,
        MatchMethodType::Name, match_values.clone()
    ).await?;

    processing_result_state.was_group_created = was_newly_inserted;

    if was_newly_inserted {
        if let (Some(ro_arc), Some(features_val_json)) = (reinforcement_orchestrator_option, features_json_for_cache) { // Use features_json_for_cache
            let version = ro_arc.lock().await.confidence_tuner.version;
            db::insert_match_decision_detail_direct(
                pool, &group_id, pipeline_run_id, features_val_json, MatchMethodType::Name.as_str(),
                adjusted_pre_rl_score as f64, processing_result_state.final_confidence, Some(version as i32)
            ).await.map_err(|e| warn!("Name: Failed to insert decision detail for {}: {}", group_id, e)).ok();
        }

        if processing_result_state.final_confidence < MODERATE_LOW_SUGGESTION_THRESHOLD {
            let (orig_n1, orig_n2, norm_n1, norm_n2, prl_match_type) = if let MatchValues::Name(n) = match_values {
                (n.original_name1, n.original_name2, n.normalized_name1, n.normalized_name2, n.pre_rl_match_type.unwrap_or_default())
            } else { ("".into(),"".into(),"".into(),"".into(),"".into())};

            let details = serde_json::json!({
                "method_type": MatchMethodType::Name.as_str(), "original_name1": orig_n1, "original_name2": orig_n2,
                "normalized_name1": norm_n1, "normalized_name2": norm_n2,
                "pre_rl_score": adjusted_pre_rl_score, "pre_rl_match_type": prl_match_type,
                "entity_group_id": &group_id,
            });
            let reason = format!("Pair ({}, {}) Name low conf ({:.4}). Pre-RL: {:.2} ({}).", ordered_e1_id.0, ordered_e2_id.0, processing_result_state.final_confidence, adjusted_pre_rl_score, prl_match_type);
            let suggestion = NewSuggestedAction {
                pipeline_run_id: Some(pipeline_run_id.to_string()), action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
                entity_id: None, group_id_1: Some(group_id.clone()), group_id_2: None, cluster_id: None,
                triggering_confidence: Some(processing_result_state.final_confidence), details: Some(details),
                reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()), reason_message: Some(reason),
                priority: if processing_result_state.final_confidence < CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 },
                status: SuggestionStatus::PendingReview.as_str().to_string(), reviewer_id: None, reviewed_at: None, review_notes: None,
            };
            db::insert_suggestion(pool, &suggestion).await.map_err(|e| warn!("Name: Failed to insert suggestion for {}: {}", group_id, e)).ok();
        }
    }
    Ok(Some(processing_result_state))
}


// Helper functions (normalize_name, detect_entity_type, etc.) remain the same
// ... (All helper functions from the original file should be here) ...
async fn get_memory_usage() -> u64 {
    use sysinfo::System;
    let mut sys = System::new_all();
    sys.refresh_memory(); // Refresh memory information
    sys.used_memory() / (1024 * 1024) // Convert bytes to MB
}

pub fn normalize_name(name: &str) -> (String, Option<&'static str>) {
    let mut normalized = name.to_lowercase();
    for prefix in &NOISE_PREFIXES {
        if normalized.starts_with(prefix) {
            normalized = normalized[prefix.len()..].trim().to_string();
        }
    }
    let char_substitutions = [
        ("&", " and "),
        ("+", " plus "),
        ("/", " "),
        ("-", " "),
        (".", " "),
        ("'", ""),
        ("(", " "),
        (")", " "),
        (",", " "),
    ];
    for (pattern, replacement) in &char_substitutions {
        normalized = normalized.replace(pattern, replacement);
    }
    let entity_type = detect_entity_type(&normalized);
    if let Some(etype) = entity_type {
        match etype {
            "city" => {
                if let Some(stripped) = normalized.strip_prefix("city of ") {
                    let city_name = stripped.trim();
                    return (format!("city of {}", city_name), Some(etype));
                }
            }
            "police" => {
                normalized = normalized.replace("pd", "police department");
                normalized = normalized.replace("police dept", "police department");
            }
            "fire" => {
                normalized = normalized.replace("fd", "fire department");
                normalized = normalized.replace("fire dept", "fire department");
            }
            "hospital" => {
                normalized = normalized.replace("medical center", "hospital");
                normalized = normalized.replace("med ctr", "hospital");
                normalized = normalized.replace("med center", "hospital");
            }
            "education" => {
                normalized = normalized.replace("school dist", "school district");
                normalized = normalized.replace("sd", "school district");
            }
            _ => {}
        }
    }
    let prefixes = ["the ", "a ", "an "];
    for prefix in prefixes {
        if normalized.starts_with(prefix) {
            normalized = normalized[prefix.len()..].to_string();
        }
    }
    let suffixes = [
        " incorporated", " inc.", " inc", " corporation", " corp.", " corp",
        " limited liability company", " llc.", " llc", " limited", " ltd.", " ltd",
        " limited partnership", " lp.", " lp", " limited liability partnership", " llp.", " llp",
        " foundation", " trust", " charitable trust", " company", " co.", " co",
        " non-profit", " nonprofit", " nfp", " association", " assn.", " assn",
        " coop", " co-op", " cooperative", " npo", " organisation", " organization", " org.", " org",
        " coalition", " fund", " partnership", " academy", " consortium", " institute",
        " services", " group", " society", " network", " federation", " international", " global",
        " national", " alliance", " gmbh", " ag", " sarl", " bv", " spa", " pty", " plc", " p.c.", " pc",
    ];
    if entity_type != Some("city") { // Don't strip suffixes if it's a city
        for suffix in suffixes {
            if normalized.ends_with(suffix) {
                normalized = normalized[..normalized.len() - suffix.len()].trim_end().to_string();
            }
        }
    }

    // Handle parenthesized location suffixes like "(Tacoma)" or "(Seattle Office)"
    let paren_regex = Regex::new(r"\s*\((.*?)\)\s*$").unwrap(); // Pre-compile for efficiency if possible, or handle error
    if let Some(captures) = paren_regex.captures(&normalized) {
        if let Some(_location_match) = captures.get(1) { // We don't need to store it, just remove it
            normalized = paren_regex.replace(&normalized, "").trim().to_string();
        }
    }


    let replacements = [
        (r"\b(ctr|cntr|cent|cen)\b", "center"), (r"\b(assoc|assn)\b", "association"),
        (r"\b(dept|dpt)\b", "department"), (r"\b(intl|int'l)\b", "international"),
        (r"\b(nat'l|natl)\b", "national"), (r"\b(comm|cmty)\b", "community"),
        (r"\b(srv|svcs|serv|svc)\b", "service"), (r"\b(univ)\b", "university"),
        (r"\b(coll)\b", "college"), (r"\b(inst)\b", "institute"),
        (r"\b(mfg)\b", "manufacturing"), (r"\b(tech)\b", "technology"),
        (r"\b(st)\b", "saint"), // Avoid "st" -> "street" if it's "saint"
        (r"\bwa\b", "washington"),
        (r"\b(fd)\b", "fire department"), (r"\b(pd)\b", "police department"),
        (r"\binc\b", ""), (r"\bcorp\b", ""), (r"\bllc\b", ""), (r"\bltd\b", ""), // Remove common abbreviations if they survived suffix stripping
    ];

    for (pattern, replacement) in &replacements {
        match Regex::new(pattern) {
            Ok(re) => normalized = re.replace_all(&normalized, *replacement).into_owned(),
            Err(e) => warn!("Invalid regex pattern: '{}'. Error: {}", pattern, e),
        }
    }

    normalized = normalized.chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect();
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    (normalized.trim().to_string(), entity_type)
}

fn detect_entity_type(normalized_name: &str) -> Option<&'static str> {
    let type_indicators = [
        ("city of", "city"), ("county", "county"), ("police department", "police"),
        ("fire department", "fire"), ("hospital", "hospital"), ("medical center", "hospital"),
        ("health", "health"), ("school district", "education"), ("school", "education"),
        ("college", "education"), ("university", "education"), ("church", "religious"),
        ("food bank", "food"), ("library", "library"), ("senior center", "senior"),
        ("ymca", "recreation"), ("community center", "community"),
    ];
    for (indicator, entity_type) in &type_indicators {
        if normalized_name.contains(indicator) {
            return Some(entity_type);
        }
    }
    None
}

fn tokenize_name(
    normalized_name: &str,
    stopwords: &HashSet<String>,
    _entity_type: Option<&'static str>, // entity_type currently not used in token weighting here
) -> (HashSet<String>, Vec<(String, f32)>) {
    let mut tokens = HashSet::new();
    let mut weighted_tokens = Vec::new(); // Not currently used for tf-idf, but kept for structure

    let words: Vec<&str> = normalized_name.split_whitespace().collect();

    for word in &words {
        let token = word.to_lowercase();
        if !stopwords.contains(&token) && token.len() >= MIN_TOKEN_LENGTH {
            tokens.insert(token.clone());
            // Basic weighting (can be expanded)
            let weight = if LOCATION_PREFIXES.contains(&token.as_str()) { 0.8 } else { 1.0 };
            weighted_tokens.push((token, weight));
        }
    }
    // Add bigrams
    for i in 0..words.len().saturating_sub(1) {
        let bigram = format!("{} {}", words[i], words[i+1]).to_lowercase();
        if bigram.len() >= 2 * MIN_TOKEN_LENGTH { // Ensure bigram is reasonably long
            tokens.insert(bigram.clone());
            weighted_tokens.push((bigram, 1.5)); // Higher weight for bigrams
        }
    }
    (tokens, weighted_tokens)
}


fn apply_domain_rules(
    normalized_name1: &str,
    normalized_name2: &str,
    pre_rl_score: f32,
    entity_type1: Option<&'static str>,
    entity_type2: Option<&'static str>,
) -> f32 {
    // Rule 1: If both are "city of X" and X is different, penalize heavily.
    if normalized_name1.starts_with("city of ") && normalized_name2.starts_with("city of ") {
        let city1 = normalized_name1.strip_prefix("city of ").unwrap_or("").trim();
        let city2 = normalized_name2.strip_prefix("city of ").unwrap_or("").trim();
        if city1 != city2 {
            // If city names are substantially different (e.g., high Levenshtein distance)
            if levenshtein_distance(city1, city2) > city1.len().min(city2.len()) / 2 {
                return pre_rl_score * 0.6; // Heavy penalty
            }
            return pre_rl_score * 0.8; // Moderate penalty
        }
    }

    // Rule 2: If entity types are known and different, apply a penalty.
    if let (Some(t1), Some(t2)) = (entity_type1, entity_type2) {
        if t1 != t2 {
            // More nuanced penalty based on type incompatibility could be added here
            return pre_rl_score * 0.85;
        }
    }

    // Rule 3: Specific department types (police, fire) should generally not match if one is X dept and other is Y dept.
    if (normalized_name1.contains("department") || normalized_name2.contains("department")) &&
       (normalized_name1.contains("police") != normalized_name2.contains("police") ||
        normalized_name1.contains("fire") != normalized_name2.contains("fire")) {
        // This logic might be too simple; consider if "Police Dept" vs "Police Services" should match.
        // For now, if one has "police" and other doesn't (and both are depts), penalize.
        if (normalized_name1.contains("police") && !normalized_name2.contains("police")) ||
           (!normalized_name1.contains("police") && normalized_name2.contains("police")) ||
           (normalized_name1.contains("fire") && !normalized_name2.contains("fire")) ||
           (!normalized_name1.contains("fire") && normalized_name2.contains("fire")) {
            return pre_rl_score * 0.75;
        }
    }
    pre_rl_score // Return original score if no specific rule applied
}

fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let (len1, len2) = (s1_chars.len(), s2_chars.len());

    if len1 == 0 { return len2; }
    if len2 == 0 { return len1; }

    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

    for i in 0..=len1 { matrix[i][0] = i; }
    for j in 0..=len2 { matrix[0][j] = j; }

    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
            matrix[i][j] = (matrix[i - 1][j] + 1)
                .min(matrix[i][j - 1] + 1)
                .min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[len1][len2]
}

async fn generate_candidate_pairs_indices_limited(
    entity_data_arc: &std::sync::Arc<Vec<EntityNameData>>,
    token_to_entities_idx_map: &HashMap<String, Vec<usize>>,
) -> Result<Vec<(usize, usize)>> {
    let mut candidate_pairs_set: HashSet<(usize, usize)> = HashSet::new();
    let entity_data = entity_data_arc.as_ref();

    for (idx1, entity1) in entity_data.iter().enumerate() {
        let mut potential_matches: HashMap<usize, usize> = HashMap::new();

        // Find potential matches through token overlap
        for token in &entity1.tokens {
            if let Some(entity_indices) = token_to_entities_idx_map.get(token) {
                for &idx2 in entity_indices {
                    if idx1 != idx2 { // Don't match with self
                        *potential_matches.entry(idx2).or_insert(0) = 1;
                    }
                }
            }
        }

        // Filter by MIN_TOKEN_OVERLAP and sort by overlap count
        let mut filtered_matches: Vec<(usize, usize)> = potential_matches
            .into_iter()
            .filter(|&(_, overlap_count)| overlap_count >= MIN_TOKEN_OVERLAP)
            .collect();

        filtered_matches.sort_by(|a, b| b.1.cmp(&a.1)); // Sort descending by overlap count

        // Limit candidates per entity
        for (idx2, _) in filtered_matches.into_iter().take(MAX_CANDIDATES_PER_ENTITY) {
            let pair = if idx1 < idx2 { (idx1, idx2) } else { (idx2, idx1) };
            candidate_pairs_set.insert(pair);

            // Check if we reached the total limit
            if candidate_pairs_set.len() >= MAX_TOTAL_CANDIDATE_PAIRS {
                break;
            }
        }
        if candidate_pairs_set.len() >= MAX_TOTAL_CANDIDATE_PAIRS {
            info!("Reached MAX_TOTAL_CANDIDATE_PAIRS ({}) during generation. Truncating.", MAX_TOTAL_CANDIDATE_PAIRS);
            break;
        }
    }

    Ok(candidate_pairs_set.into_iter().collect())
}


#[derive(Debug, Clone)]
struct TokenStats {
    token: String,
    frequency: usize,
    idf: f32, // Inverse Document Frequency
}

async fn prepare_entity_data_and_index(
    all_entities: &[Entity],
    stopwords: &HashSet<String>,
) -> (Vec<EntityNameData>, HashMap<String, Vec<usize>>, HashMap<String, TokenStats>) {
    let mut entity_data_vec = Vec::with_capacity(all_entities.len());
    let mut token_frequency: HashMap<String, usize> = HashMap::new();

    for entity in all_entities {
        if let Some(name) = &entity.name {
            let (normalized_name, entity_type) = normalize_name(name);
            if !normalized_name.is_empty() {
                let (tokens, _weighted_tokens) = tokenize_name(&normalized_name, stopwords, entity_type);
                for token in &tokens {
                    *token_frequency.entry(token.clone()).or_insert(0) += 1;
                }
                entity_data_vec.push(EntityNameData {
                    entity: entity.clone(),
                    normalized_name,
                    entity_type,
                    tokens,
                    // weighted_tokens, // Not used in current candidate generation
                });
            }
        }
    }

    // Calculate IDF for tokens (optional, if needed for more advanced scoring)
    let total_docs = entity_data_vec.len() as f32;
    let mut token_stats_map = HashMap::new();
    for (token, freq) in token_frequency {
        let idf = (total_docs / (freq as f32 + 1.0)).ln_1p(); // ln(N / (df + 1)) + 1 (or similar variant)
        token_stats_map.insert(token.clone(), TokenStats { token, frequency: freq, idf });
    }

    // Create inverted index: token -> list of entity indices
    let mut token_to_entities_map: HashMap<String, Vec<usize>> = HashMap::new();
    let max_common_token_freq = (entity_data_vec.len() as f32 * 0.05).max(10.0) as usize; // Ignore very common tokens

    for (idx, data) in entity_data_vec.iter().enumerate() {
        for token in &data.tokens {
            if let Some(stats) = token_stats_map.get(token) {
                if stats.frequency <= max_common_token_freq && stats.frequency > 1 { // Only index somewhat rare tokens
                    token_to_entities_map.entry(token.clone()).or_default().push(idx);
                }
            }
        }
    }
    (entity_data_vec, token_to_entities_map, token_stats_map)
}


async fn get_all_entities_with_names(conn: &impl tokio_postgres::GenericClient) -> Result<Vec<Entity>> {
    let query = "SELECT id, organization_id, name, created_at, updated_at, source_system, source_id FROM public.entity WHERE name IS NOT NULL AND name != ''";
    let rows = conn.query(query, &[]).await.context("Failed to query entities with names")?;
    rows.iter().map(|row| Ok(Entity {
        id: EntityId(row.try_get("id")?),
        organization_id: OrganizationId(row.try_get("organization_id")?),
        name: row.try_get("name")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        source_system: row.try_get("source_system").ok(),
        source_id: row.try_get("source_id").ok(),
    })).collect()
}

async fn get_organization_embeddings(
    conn: &impl tokio_postgres::GenericClient,
    entities: &[Entity],
) -> Result<HashMap<EntityId, Option<Vec<f32>>>> {
    let mut embeddings_map = HashMap::new();
    let org_ids: Vec<String> = entities.iter().map(|e| e.organization_id.0.clone()).collect::<HashSet<_>>().into_iter().collect();

    if org_ids.is_empty() { return Ok(embeddings_map); }

    // Fetch in batches
    for org_ids_chunk in org_ids.chunks(100) { // Batch size of 100
        let query = "SELECT id, embedding FROM public.organization WHERE id = ANY($1::TEXT[]) AND embedding IS NOT NULL";
        let rows = conn.query(query, &[&org_ids_chunk]).await.context("Failed to query organization embeddings")?;
        for row in rows {
            let org_id_str: String = row.get("id");
            // Assuming pgvector::Vector can be converted to Vec<f32>
            let embedding_pgvector: Option<pgvector::Vector> = row.get("embedding");
            let embedding_vec_f32: Option<Vec<f32>> = embedding_pgvector.map(|v| v.to_vec());

            // Map this org_id's embedding to all entities associated with this org_id
            for entity in entities.iter().filter(|e| e.organization_id.0 == org_id_str) {
                embeddings_map.insert(entity.id.clone(), embedding_vec_f32.clone());
            }
        }
    }
     // Ensure all entities have an entry, even if it's None
    for entity in entities {
        embeddings_map.entry(entity.id.clone()).or_insert(None);
    }
    Ok(embeddings_map)
}


async fn fetch_existing_entity_group_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn.query(query, &[&method_type.as_str()]).await
        .with_context(|| format!("Failed to query existing {:?}-matched entity_group pairs", method_type))?;
    Ok(rows.into_iter().map(|row| {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        if id1_str < id2_str { (EntityId(id1_str), EntityId(id2_str)) } else { (EntityId(id2_str), EntityId(id1_str)) }
    }).collect())
}
