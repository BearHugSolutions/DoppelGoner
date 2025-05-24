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

use crate::{config::{CRITICALLY_LOW_SUGGESTION_THRESHOLD, MODERATE_LOW_SUGGESTION_THRESHOLD}, db::{insert_entity_group_tx, insert_match_decision_detail_tx, insert_suggestion_tx}, models::{
    ActionType, Entity, EntityGroupId, EntityId, MatchMethodType, MatchValues, NameMatchValue,
    NewSuggestedAction, OrganizationId, SuggestionStatus,
}};
use crate::results::{AnyMatchResult, MatchMethodStats, NameMatchResult};
use crate::utils::cosine_similarity_candle;
use crate::{
    config,
    db::PgPool,
    reinforcement::entity::{
        feature_cache_service::SharedFeatureCache, orchestrator::MatchingOrchestrator,
    },
};
use serde_json;

// Import pipeline_state_utils for caching and signature handling
use crate::pipeline_state_utils::{
    check_comparison_cache, get_current_signatures_for_pair, store_in_comparison_cache,
};

// Configuration constants
const MIN_FUZZY_SIMILARITY_THRESHOLD: f32 = 0.92;
const MIN_SEMANTIC_SIMILARITY_THRESHOLD: f32 = 0.94;
const COMBINED_SIMILARITY_THRESHOLD: f32 = 0.93;
const FUZZY_WEIGHT: f32 = 0.3;
const SEMANTIC_WEIGHT: f32 = 0.7;
const TOP_TOKENS_PER_ENTITY: usize = 10;
const MIN_TOKEN_OVERLAP: usize = 3; // Increased from 2 to reduce pairs
pub const MIN_TOKEN_LENGTH: usize = 2;

// Memory management constants
const BATCH_SIZE: usize = 100; // Process pairs in batches
const MAX_CONCURRENT_BATCHES: usize = 8; // Limit concurrent batch tasks
const MAX_CANDIDATES_PER_ENTITY: usize = 50; // Limit candidates per entity
const MAX_TOTAL_CANDIDATE_PAIRS: usize = 75_000; // Hard limit on total pairs

// Organizational types that should never match
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

pub const STOPWORDS: [&str; 131] = [
    // Common English articles, conjunctions, and prepositions
    "a",
    "an",
    "the",
    "and",
    "or",
    "but",
    "nor",
    "for",
    "yet",
    "so",
    "in",
    "on",
    "at",
    "by",
    "to",
    "with",
    "from",
    "of",
    "as",
    "into",
    "about",
    "before",
    "after",
    "during",
    "until",
    "since",
    "unless",
    // Common business terms
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "llc",
    "ltd",
    "limited",
    "company",
    "co",
    "group",
    "holdings",
    "enterprises",
    "international",
    "global",
    "worldwide",
    "national",
    "american",
    "usa",
    "us",
    "service",
    "services",
    "solutions",
    "systems",
    "associates",
    "partners",
    "partnership",
    // Generic organizational terms
    "organization",
    "organisation",
    "foundation",
    "institute",
    "association",
    "society",
    "council",
    "committee",
    "center",
    "centre",
    "department",
    "division",
    "unit",
    "office",
    "bureau",
    "agency",
    "authority",
    "board",
    // Common descriptive terms
    "new",
    "old",
    "great",
    "greater",
    "best",
    "better",
    "first",
    "second",
    "third",
    "primary",
    "main",
    "central",
    "local",
    "regional",
    "official",
    // Additional stopwords
    "this",
    "that",
    "these",
    "those",
    "it",
    "they",
    "them",
    "their",
    "our",
    "your",
    "all",
    "any",
    "each",
    "every",
    "some",
    "such",
    "no",
    "not",
    "only",
    "very",
    // Specific to your dataset
    "program",
    "community",
    "resource",
    "resources",
    "support",
    "help",
    "health",
    "care",
    "management",
    "professional",
    "public",
    "private",
    "general",
    "federal",
    "state",
    "county",
    "regional",
    "district",
    "area",
    "branch",
    "provider",
    "member",
    "directory",
    "guide",
    "network",
];

const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// Enhanced data structure to hold entity information with organization type detection
#[derive(Clone)]
struct EntityNameData {
    entity: Entity,
    normalized_name: String,
    entity_type: Option<&'static str>,
    tokens: HashSet<String>,
    weighted_tokens: Vec<(String, f32)>,
}

/// Shared data that doesn't need to be cloned for each batch
#[derive(Clone)]
struct SharedNameMatchingData {
    existing_entity_group_pairs: Arc<HashSet<(EntityId, EntityId)>>,
    org_embeddings_map: Arc<HashMap<EntityId, Option<Vec<f32>>>>,
    entity_data: Arc<Vec<EntityNameData>>,
}

/// Statistics for tracking processing
struct NameMatchingStats {
    new_pairs_created_count: usize,
    entities_in_new_pairs: HashSet<EntityId>,
    confidence_scores_for_stats: Vec<f64>,
    individual_operation_errors: usize,
    cache_hits_count: usize,
    feature_extraction_count: usize,
    feature_extraction_failures: usize,
    pairs_processed: usize,
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

/// Main function to find name-based matches
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 memory-efficient name matching (run ID: {}) with INCREMENTAL CHECKS...",
        pipeline_run_id,
    );
    let start_time = Instant::now();

    // Memory monitoring
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
        return Ok(AnyMatchResult::Name(NameMatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Name,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
            },
        }));
    }

    // Fetch existing pairs and embeddings
    let existing_entity_group_pairs =
        Arc::new(fetch_existing_entity_group_pairs(&*initial_conn, MatchMethodType::Name).await?);
    info!(
        "Name: Found {} existing name-matched pairs.",
        existing_entity_group_pairs.len()
    );

    let org_embeddings_map =
        Arc::new(get_organization_embeddings(&*initial_conn, &all_entities_with_names_vec).await?);
    drop(initial_conn);

    // Prepare entity data
    let stopwords: HashSet<String> = STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let (entity_data, token_to_entities, _token_stats) =
        prepare_entity_data_and_index(&all_entities_with_names_vec, &stopwords).await;

    let entity_data = Arc::new(entity_data);

    // Generate candidate pairs with limits
    let candidate_pairs_indices =
        generate_candidate_pairs_indices_limited(&entity_data, &token_to_entities).await?;
    let total_candidates = candidate_pairs_indices.len();
    info!(
        "Name: Generated {} candidate pairs for comparison.",
        total_candidates
    );

    if total_candidates == 0 {
        return Ok(AnyMatchResult::Name(NameMatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Name,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
            },
        }));
    }

    // Create shared data
    let shared_data = SharedNameMatchingData {
        existing_entity_group_pairs,
        org_embeddings_map,
        entity_data,
    };

    // Process pairs in batches
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
        "Name: Final memory usage: {} MB (delta: +{} MB)",
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

/// Process candidate pairs in memory-efficient batches
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

    // Process pairs in chunks to limit concurrent tasks
    for (chunk_idx, chunk) in candidate_pairs_indices
        .chunks(BATCH_SIZE * MAX_CONCURRENT_BATCHES)
        .enumerate()
    {
        let mut batch_futures = Vec::new();

        for (batch_idx, batch_pairs) in chunk.chunks(BATCH_SIZE).enumerate() {
            let global_batch_num = chunk_idx * MAX_CONCURRENT_BATCHES + batch_idx + 1;
            let batch_data = batch_pairs.to_vec();

            // Clone only what's needed for this batch
            let pool_clone = pool.clone();
            let shared_data_clone = shared_data.clone();
            let ro_option_clone = reinforcement_orchestrator_option.clone();
            let run_id_clone = pipeline_run_id.to_string();
            let feature_cache_clone = feature_cache.clone();
            let stats_arc_clone = stats_mutex.clone();
            let processed_pairs_clone = processed_pairs_this_run.clone();

            batch_futures.push(tokio::spawn(async move {
                process_name_batch(
                    global_batch_num,
                    total_batches,
                    batch_data,
                    shared_data_clone,
                    &pool_clone,
                    ro_option_clone.as_ref(),
                    &run_id_clone,
                    feature_cache_clone,
                    stats_arc_clone,
                    processed_pairs_clone,
                )
                .await
            }));
        }

        // Wait for this chunk of batches to complete
        let results = futures::future::join_all(batch_futures).await;
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                warn!("Name: Batch {} error: {}", i + 1, e);
                let mut stats_guard = stats_mutex.lock().await;
                stats_guard.individual_operation_errors += 1;
            }
        }

        // Memory monitoring between chunks
        if chunk_idx % 5 == 0 {
            let current_memory = get_memory_usage().await;
            let stats_guard = stats_mutex.lock().await;
            info!(
                "Name: Chunk {} completed. Memory: {} MB, Pairs processed: {}, New groups: {}",
                chunk_idx + 1,
                current_memory,
                stats_guard.pairs_processed,
                stats_guard.new_pairs_created_count
            );
        }
    }

    // Extract final stats
    let final_stats = {
        let stats_guard = stats_mutex.lock().await;
        NameMatchingStats {
            new_pairs_created_count: stats_guard.new_pairs_created_count,
            entities_in_new_pairs: stats_guard.entities_in_new_pairs.clone(),
            confidence_scores_for_stats: stats_guard.confidence_scores_for_stats.clone(),
            individual_operation_errors: stats_guard.individual_operation_errors,
            cache_hits_count: stats_guard.cache_hits_count,
            feature_extraction_count: stats_guard.feature_extraction_count,
            feature_extraction_failures: stats_guard.feature_extraction_failures,
            pairs_processed: stats_guard.pairs_processed,
        }
    };

    Ok(final_stats)
}

/// Process a single batch of name pairs
#[allow(clippy::too_many_arguments)]
async fn process_name_batch(
    batch_num: usize,
    total_batches: usize,
    batch_pairs: Vec<(usize, usize)>,
    shared_data: SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    stats_mutex: Arc<Mutex<NameMatchingStats>>,
    processed_pairs_this_run: Arc<Mutex<HashSet<(EntityId, EntityId)>>>,
) -> Result<()> {
    debug!(
        "Name: Processing batch {}/{} ({} pairs)",
        batch_num,
        total_batches,
        batch_pairs.len()
    );

    for (i, j) in batch_pairs {
        // Get entity data by index
        let entity1_data = &shared_data.entity_data[i];
        let entity2_data = &shared_data.entity_data[j];

        let (e1_id, e2_id) = (&entity1_data.entity.id, &entity2_data.entity.id);
        let (ordered_e1_id, ordered_e2_id) = if e1_id.0 < e2_id.0 {
            (e1_id.clone(), e2_id.clone())
        } else {
            (e2_id.clone(), e1_id.clone())
        };
        let current_pair_ordered = (ordered_e1_id.clone(), ordered_e2_id.clone());

        // Update pairs processed count
        {
            let mut stats_guard = stats_mutex.lock().await;
            stats_guard.pairs_processed += 1;
        }

        // Check if already exists or processed
        if shared_data
            .existing_entity_group_pairs
            .contains(&current_pair_ordered)
        {
            continue;
        }

        {
            let processed_set = processed_pairs_this_run.lock().await;
            if processed_set.contains(&current_pair_ordered) {
                continue;
            }
        }

        // Process the pair
        match process_single_name_pair(
            entity1_data,
            entity2_data,
            &shared_data,
            pool,
            reinforcement_orchestrator_option,
            pipeline_run_id,
            feature_cache.as_ref(),
            &current_pair_ordered,
        )
        .await
        {
            Ok(pair_result) => {
                // Mark as processed
                {
                    let mut processed_set = processed_pairs_this_run.lock().await;
                    processed_set.insert(current_pair_ordered);
                }

                // Update stats
                let mut stats_guard = stats_mutex.lock().await;
                if let Some(result) = pair_result {
                    stats_guard.new_pairs_created_count += 1;
                    stats_guard.entities_in_new_pairs.insert(e1_id.clone());
                    stats_guard.entities_in_new_pairs.insert(e2_id.clone());
                    stats_guard
                        .confidence_scores_for_stats
                        .push(result.final_confidence);
                    if result.was_cache_hit {
                        stats_guard.cache_hits_count += 1;
                    }
                    if result.feature_extraction_success {
                        stats_guard.feature_extraction_count += 1;
                    } else if result.feature_extraction_attempted {
                        stats_guard.feature_extraction_failures += 1;
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Name: Failed to process pair ({}, {}): {}",
                    e1_id.0, e2_id.0, e
                );
                let mut stats_guard = stats_mutex.lock().await;
                stats_guard.individual_operation_errors += 1;
            }
        }
    }

    debug!("Name: Batch {}/{} completed", batch_num, total_batches);
    Ok(())
}

/// Result of processing a single pair
struct PairProcessingResult {
    final_confidence: f64,
    was_cache_hit: bool,
    feature_extraction_attempted: bool,
    feature_extraction_success: bool,
}

/// Process a single name pair with all the matching logic
#[allow(clippy::too_many_arguments)]
async fn process_single_name_pair(
    entity1_data: &EntityNameData,
    entity2_data: &EntityNameData,
    shared_data: &SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>,
    current_pair_ordered: &(EntityId, EntityId),
) -> Result<Option<PairProcessingResult>> {
    let (ordered_e1_id, ordered_e2_id) = current_pair_ordered;
    let (e1_id, e2_id) = (&entity1_data.entity.id, &entity2_data.entity.id);

    // Get the correct entity data based on ordering
    let (entity1_actual, entity2_actual) = if e1_id == ordered_e1_id {
        (entity1_data, entity2_data)
    } else {
        (entity2_data, entity1_data)
    };

    // Check comparison cache first
    let current_signatures_opt =
        match get_current_signatures_for_pair(pool, ordered_e1_id, ordered_e2_id).await {
            Ok(sigs) => sigs,
            Err(e) => {
                warn!(
                    "Name: Failed to get signatures for pair ({}, {}): {}",
                    ordered_e1_id.0, ordered_e2_id.0, e
                );
                None
            }
        };

    if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
        match check_comparison_cache(
            pool,
            ordered_e1_id,
            ordered_e2_id,
            &sig1_data.signature,
            &sig2_data.signature,
            &MatchMethodType::Name,
        )
        .await
        {
            Ok(Some(_cached_eval)) => {
                debug!(
                    "Name: Cache HIT for pair ({}, {})",
                    ordered_e1_id.0, ordered_e2_id.0
                );
                return Ok(Some(PairProcessingResult {
                    final_confidence: 0.0, // Not used for cache hits
                    was_cache_hit: true,
                    feature_extraction_attempted: false,
                    feature_extraction_success: false,
                }));
            }
            Ok(None) => {
                debug!(
                    "Name: Cache MISS for pair ({}, {})",
                    ordered_e1_id.0, ordered_e2_id.0
                );
            }
            Err(e) => {
                warn!(
                    "Name: Error checking comparison cache for pair ({}, {}): {}",
                    ordered_e1_id.0, ordered_e2_id.0, e
                );
            }
        }
    }

    // Perform name matching logic
    let original_name1 = entity1_actual
        .entity
        .name
        .as_ref()
        .cloned()
        .unwrap_or_default();
    let original_name2 = entity2_actual
        .entity
        .name
        .as_ref()
        .cloned()
        .unwrap_or_default();
    let normalized_name1 = &entity1_actual.normalized_name;
    let normalized_name2 = &entity2_actual.normalized_name;

    if normalized_name1.is_empty() || normalized_name2.is_empty() {
        return Ok(None);
    }

    // Check entity type compatibility
    if let (Some(t1), Some(t2)) = (entity1_actual.entity_type, entity2_actual.entity_type) {
        if t1 != t2
            && INCOMPATIBLE_ORG_TYPES
                .iter()
                .any(|(it1, it2)| (t1 == *it1 && t2 == *it2) || (t1 == *it2 && t2 == *it1))
        {
            return Ok(None);
        }
    }

    // Calculate similarity scores
    let fuzzy_score = jaro_winkler(normalized_name1, normalized_name2) as f32;
    if fuzzy_score < (MIN_FUZZY_SIMILARITY_THRESHOLD * 0.9) {
        // Store NON_MATCH in cache and return
        if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
            let _ = store_in_comparison_cache(
                pool,
                ordered_e1_id,
                ordered_e2_id,
                &sig1_data.signature,
                &sig2_data.signature,
                &MatchMethodType::Name,
                pipeline_run_id,
                "NON_MATCH",
                Some(fuzzy_score as f64),
                None,
            )
            .await;
        }
        return Ok(None);
    }

    let embedding1_opt = shared_data
        .org_embeddings_map
        .get(e1_id)
        .and_then(|opt_emb| opt_emb.as_ref());
    let embedding2_opt = shared_data
        .org_embeddings_map
        .get(e2_id)
        .and_then(|opt_emb| opt_emb.as_ref());
    let semantic_score = match (embedding1_opt, embedding2_opt) {
        (Some(emb1), Some(emb2)) => match cosine_similarity_candle(emb1, emb2) {
            Ok(sim) => sim as f32,
            Err(_) => 0.0,
        },
        _ => 0.0,
    };

    let (pre_rl_score, pre_rl_match_type) = if semantic_score >= MIN_SEMANTIC_SIMILARITY_THRESHOLD {
        (
            (fuzzy_score * FUZZY_WEIGHT) + (semantic_score * SEMANTIC_WEIGHT),
            "combined".to_string(),
        )
    } else if fuzzy_score >= MIN_FUZZY_SIMILARITY_THRESHOLD {
        (fuzzy_score, "fuzzy".to_string())
    } else {
        // Store NON_MATCH in cache
        if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
            let _ = store_in_comparison_cache(
                pool,
                ordered_e1_id,
                ordered_e2_id,
                &sig1_data.signature,
                &sig2_data.signature,
                &MatchMethodType::Name,
                pipeline_run_id,
                "NON_MATCH",
                Some(fuzzy_score.max(semantic_score) as f64),
                None,
            )
            .await;
        }
        return Ok(None);
    };

    let adjusted_pre_rl_score = apply_domain_rules(
        normalized_name1,
        normalized_name2,
        pre_rl_score,
        entity1_actual.entity_type,
        entity2_actual.entity_type,
    );

    if adjusted_pre_rl_score < COMBINED_SIMILARITY_THRESHOLD {
        // Store NON_MATCH in cache
        if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
            let _ = store_in_comparison_cache(
                pool,
                ordered_e1_id,
                ordered_e2_id,
                &sig1_data.signature,
                &sig2_data.signature,
                &MatchMethodType::Name,
                pipeline_run_id,
                "NON_MATCH",
                Some(adjusted_pre_rl_score as f64),
                None,
            )
            .await;
        }
        return Ok(None);
    }

    // RL tuning
    let mut final_confidence = adjusted_pre_rl_score as f64;
    let mut features_for_snapshot_vec: Option<Vec<f64>> = None;
    let mut features_json_for_cache: Option<serde_json::Value> = None;
    let mut feature_extraction_attempted = false;
    let mut feature_extraction_success = false;

    if let Some(orchestrator_arc) = reinforcement_orchestrator_option {
        feature_extraction_attempted = true;
        match if let Some(cache) = feature_cache {
            let orchestrator_guard = orchestrator_arc.lock().await;
            orchestrator_guard
                .get_pair_features(pool, e1_id, e2_id)
                .await
        } else {
            MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
        } {
            Ok(features) => {
                feature_extraction_success = true;
                if !features.is_empty() {
                    features_for_snapshot_vec = Some(features.clone());
                    features_json_for_cache = serde_json::to_value(features.clone()).ok();
                    let orchestrator_guard = orchestrator_arc.lock().await;
                    match orchestrator_guard.get_tuned_confidence(
                        &MatchMethodType::Name,
                        adjusted_pre_rl_score as f64,
                        &features,
                    ) {
                        Ok(tuned_score) => final_confidence = tuned_score,
                        Err(e) => warn!(
                            "Name: RL tuning failed for ({}, {}): {}",
                            e1_id.0, e2_id.0, e
                        ),
                    }
                }
            }
            Err(e) => {
                warn!(
                    "Name: Feature extraction failed for ({}, {}): {}",
                    e1_id.0, e2_id.0, e
                );
            }
        }
    }

    // Store MATCH result in cache
    if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
        let _ = store_in_comparison_cache(
            pool,
            ordered_e1_id,
            ordered_e2_id,
            &sig1_data.signature,
            &sig2_data.signature,
            &MatchMethodType::Name,
            pipeline_run_id,
            "MATCH",
            Some(final_confidence),
            features_json_for_cache.as_ref(),
        )
        .await;
    }

    // Create entity group
    let match_values = MatchValues::Name(NameMatchValue {
        original_name1: original_name1.clone(),
        original_name2: original_name2.clone(),
        normalized_name1: normalized_name1.clone(),
        normalized_name2: normalized_name2.clone(),
        pre_rl_match_type: Some(pre_rl_match_type.clone()),
    });

    let created = create_entity_group(
        pool,
        e1_id,
        e2_id,
        &match_values,
        final_confidence,
        adjusted_pre_rl_score as f64,
        reinforcement_orchestrator_option,
        pipeline_run_id,
        features_for_snapshot_vec,
        feature_cache.cloned(),
    )
    .await?;

    if created {
        Ok(Some(PairProcessingResult {
            final_confidence,
            was_cache_hit: false,
            feature_extraction_attempted,
            feature_extraction_success,
        }))
    } else {
        Ok(None)
    }
}

/// Generate candidate pairs with memory limits
async fn generate_candidate_pairs_indices_limited(
    entity_data: &[EntityNameData],
    token_to_entities: &HashMap<String, Vec<usize>>,
) -> Result<Vec<(usize, usize)>> {
    let mut candidate_pairs_set = HashSet::new();
    let mut entity_candidate_counts: HashMap<usize, usize> = HashMap::new();

    for (i, data_i) in entity_data.iter().enumerate() {
        let mut potential_matches_for_i: HashMap<usize, usize> = HashMap::new();

        for token in &data_i.tokens {
            if let Some(posting_list) = token_to_entities.get(token) {
                for &j_idx in posting_list {
                    if i < j_idx {
                        // Check candidate limits
                        let i_count = entity_candidate_counts.get(&i).unwrap_or(&0);
                        let j_count = entity_candidate_counts.get(&j_idx).unwrap_or(&0);

                        if *i_count >= MAX_CANDIDATES_PER_ENTITY
                            || *j_count >= MAX_CANDIDATES_PER_ENTITY
                        {
                            continue;
                        }

                        *potential_matches_for_i.entry(j_idx).or_insert(0) += 1;
                    }
                }
            }
        }

        // Sort by overlap and take top candidates
        let mut sorted_matches: Vec<_> = potential_matches_for_i.into_iter().collect();
        sorted_matches.sort_by(|a, b| b.1.cmp(&a.1));

        for (j_idx, overlap) in sorted_matches.into_iter().take(MAX_CANDIDATES_PER_ENTITY) {
            if overlap >= MIN_TOKEN_OVERLAP {
                // Check global limit
                if candidate_pairs_set.len() >= MAX_TOTAL_CANDIDATE_PAIRS {
                    warn!("Name: Reached maximum candidate pairs limit ({}). Stopping pair generation.", MAX_TOTAL_CANDIDATE_PAIRS);
                    return Ok(candidate_pairs_set.into_iter().collect());
                }

                candidate_pairs_set.insert((i, j_idx));
                *entity_candidate_counts.entry(i).or_insert(0) += 1;
                *entity_candidate_counts.entry(j_idx).or_insert(0) += 1;
            }
        }
    }

    info!(
        "Name: Generated {} candidate pairs (limit: {})",
        candidate_pairs_set.len(),
        MAX_TOTAL_CANDIDATE_PAIRS
    );
    Ok(candidate_pairs_set.into_iter().collect())
}

/// Get current memory usage in MB
async fn get_memory_usage() -> u64 {
    use sysinfo::System;
    let mut sys = System::new_all();
    sys.refresh_all();
    sys.used_memory() / 1024 / 1024 // Convert to MB
}

// Include all the helper functions from the original file with minimal changes:
// - normalize_name
// - detect_entity_type
// - tokenize_name
// - get_token_weight
// - apply_domain_rules
// - levenshtein_distance
// - prepare_entity_data_and_index
// - get_all_entities_with_names
// - get_organization_embeddings
// - fetch_existing_entity_group_pairs
// - create_entity_group

// [Rest of the helper functions remain the same as in original name.rs]
// I'll include the key ones here for completeness:

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
        " incorporated",
        " inc.",
        " inc",
        " corporation",
        " corp.",
        " corp",
        " limited liability company",
        " llc.",
        " llc",
        " limited",
        " ltd.",
        " ltd",
        " limited partnership",
        " lp.",
        " lp",
        " limited liability partnership",
        " llp.",
        " llp",
        " foundation",
        " trust",
        " charitable trust",
        " company",
        " co.",
        " co",
        " non-profit",
        " nonprofit",
        " nfp",
        " association",
        " assn.",
        " assn",
        " coop",
        " co-op",
        " cooperative",
        " npo",
        " organisation",
        " organization",
        " org.",
        " org",
        " coalition",
        " fund",
        " partnership",
        " academy",
        " consortium",
        " institute",
        " services",
        " group",
        " society",
        " network",
        " federation",
        " international",
        " global",
        " national",
        " alliance",
        " gmbh",
        " ag",
        " sarl",
        " bv",
        " spa",
        " pty",
        " plc",
        " p.c.",
        " pc",
    ];
    if entity_type != Some("city") {
        for suffix in suffixes {
            if normalized.ends_with(suffix) {
                normalized = normalized[..normalized.len() - suffix.len()]
                    .trim_end()
                    .to_string();
            }
        }
    }
    let mut location_suffix = None;
    let paren_regex = Regex::new(r"\s*\((.*?)\)\s*$").unwrap_or_else(|_| Regex::new(r"").unwrap());
    if let Some(captures) = paren_regex.captures(&normalized) {
        if let Some(location_match) = captures.get(1) {
            location_suffix = Some(location_match.as_str().to_string());
            normalized = paren_regex.replace(&normalized, "").to_string();
        }
    }
    let replacements = [
        (r"\b(ctr|cntr|cent|cen)\b", "center"),
        (r"\b(assoc|assn)\b", "association"),
        (r"\b(dept|dpt)\b", "department"),
        (r"\b(intl|int'l)\b", "international"),
        (r"\b(nat'l|natl)\b", "national"),
        (r"\b(comm|cmty)\b", "community"),
        (r"\b(srv|svcs|serv|svc)\b", "service"),
        (r"\b(univ)\b", "university"),
        (r"\b(coll)\b", "college"),
        (r"\b(inst)\b", "institute"),
        (r"\b(mfg)\b", "manufacturing"),
        (r"\b(tech)\b", "technology"),
        (r"\b(st)\b", "saint"),
        (r"\bwa\b", "washington"),
        (r"\b(fd)\b", "fire department"),
        (r"\b(pd)\b", "police department"),
        (r"\binc\b", ""),
        (r"\bcorp\b", ""),
        (r"\bllc\b", ""),
        (r"\bltd\b", ""),
    ];
    for (pattern, replacement) in &replacements {
        match Regex::new(pattern) {
            Ok(re) => normalized = re.replace_all(&normalized, *replacement).into_owned(),
            Err(e) => warn!("Invalid regex pattern: '{}'. Error: {}", pattern, e),
        }
    }
    normalized = normalized
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect();
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if let Some(location) = location_suffix {
        if !location.is_empty() {
            normalized = format!("{} {}", normalized.trim(), location.trim());
        }
    }
    (normalized.trim().to_string(), entity_type)
}

fn detect_entity_type(normalized_name: &str) -> Option<&'static str> {
    let type_indicators = [
        ("city of", "city"),
        ("county", "county"),
        ("police department", "police"),
        ("fire department", "fire"),
        ("hospital", "hospital"),
        ("medical center", "hospital"),
        ("health", "health"),
        ("school district", "education"),
        ("school", "education"),
        ("college", "education"),
        ("university", "education"),
        ("church", "religious"),
        ("food bank", "food"),
        ("library", "library"),
        ("senior center", "senior"),
        ("ymca", "recreation"),
        ("community center", "community"),
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
    entity_type: Option<&'static str>,
) -> (HashSet<String>, Vec<(String, f32)>) {
    let mut tokens = HashSet::new();
    let mut weighted_tokens = Vec::new();
    let words: Vec<&str> = normalized_name.split_whitespace().collect();
    for word in &words {
        let token = word.to_lowercase();
        if !stopwords.contains(&token) && token.len() >= MIN_TOKEN_LENGTH {
            tokens.insert(token.clone());
            let weight = get_token_weight(&token, entity_type);
            weighted_tokens.push((token, weight));
        }
    }
    for i in 0..words.len().saturating_sub(1) {
        let bigram = format!("{} {}", words[i], words[i + 1]).to_lowercase();
        if bigram.len() >= 2 * MIN_TOKEN_LENGTH {
            tokens.insert(bigram.clone());
            weighted_tokens.push((bigram, 1.5));
        }
    }
    if entity_type == Some("city") && normalized_name.starts_with("city of ") {
        if let Some(city_name) = normalized_name.strip_prefix("city of ") {
            let city = city_name.trim().to_string();
            if !city.is_empty() {
                tokens.insert(city.clone());
                weighted_tokens.push((city, 3.0));
            }
        }
    }
    (tokens, weighted_tokens)
}

fn get_token_weight(token: &str, entity_type: Option<&str>) -> f32 {
    let common_words = [
        "the",
        "and",
        "of",
        "in",
        "for",
        "at",
        "with",
        "by",
        "on",
        "to",
        "service",
        "services",
        "center",
        "association",
        "organization",
    ];
    if common_words.contains(&token) {
        return 0.5;
    }
    if LOCATION_PREFIXES.contains(&token) {
        return 0.8;
    }
    let type_indicators = [
        "police", "fire", "hospital", "school", "college", "church", "bank", "library",
    ];
    if type_indicators.contains(&token) {
        return 2.0;
    }
    if let Some(etype) = entity_type {
        match etype {
            "city" => {
                if token == "city" {
                    0.5
                } else {
                    1.5
                }
            }
            "education" => {
                if token == "school" || token == "college" {
                    1.5
                } else {
                    1.2
                }
            }
            _ => 1.0,
        }
    } else {
        1.0
    }
}

fn apply_domain_rules(
    normalized_name1: &str,
    normalized_name2: &str,
    pre_rl_score: f32,
    entity_type1: Option<&'static str>,
    entity_type2: Option<&'static str>,
) -> f32 {
    let is_city1 = normalized_name1.starts_with("city of ");
    let is_city2 = normalized_name2.starts_with("city of ");
    if is_city1 && is_city2 {
        let city_name1 = normalized_name1
            .strip_prefix("city of ")
            .unwrap_or(normalized_name1)
            .trim();
        let city_name2 = normalized_name2
            .strip_prefix("city of ")
            .unwrap_or(normalized_name2)
            .trim();
        if city_name1 != city_name2 {
            let edit_distance = levenshtein_distance(city_name1, city_name2);
            let max_length = std::cmp::max(city_name1.len(), city_name2.len());
            if edit_distance > 3
                || (max_length > 0 && edit_distance as f32 / max_length as f32 > 0.3)
            {
                return pre_rl_score * 0.6;
            } else {
                return pre_rl_score * 0.8;
            }
        }
    }
    if let (Some(t1), Some(t2)) = (entity_type1, entity_type2) {
        if t1 != t2 {
            return pre_rl_score * 0.85;
        }
    }
    if normalized_name1.contains("department") || normalized_name2.contains("department") {
        let dept_types = [
            "police",
            "fire",
            "health",
            "public",
            "revenue",
            "transportation",
        ];
        for dept_type in &dept_types {
            if normalized_name1.contains(dept_type) != normalized_name2.contains(dept_type) {
                return pre_rl_score * 0.75;
            }
        }
    }
    let words1: Vec<&str> = normalized_name1.split_whitespace().collect();
    let words2: Vec<&str> = normalized_name2.split_whitespace().collect();
    let mut common_prefix_len = 0;
    for i in 0..std::cmp::min(words1.len(), words2.len()) {
        if words1[i] == words2[i] {
            common_prefix_len += 1;
        } else {
            break;
        }
    }
    if common_prefix_len > 0 && common_prefix_len < words1.len() && common_prefix_len < words2.len()
    {
        let total_words = words1.len() + words2.len();
        let common_prefix_percentage = (2 * common_prefix_len) as f32 / total_words as f32;
        if common_prefix_percentage > 0.3 {
            let remaining_similarity = jaro_winkler(
                &words1[common_prefix_len..].join(" "),
                &words2[common_prefix_len..].join(" "),
            );
            if remaining_similarity < 0.7 {
                return pre_rl_score * (0.9 - (common_prefix_percentage * 0.2));
            }
        }
    }
    for prefix in &LOCATION_PREFIXES {
        if normalized_name1.starts_with(prefix) && normalized_name2.starts_with(prefix) {
            let suffix1 = normalized_name1[prefix.len()..].trim();
            let suffix2 = normalized_name2[prefix.len()..].trim();
            if !suffix1.is_empty() && !suffix2.is_empty() && jaro_winkler(suffix1, suffix2) < 0.8 {
                return pre_rl_score * 0.8;
            }
        }
    }
    let location_regex = Regex::new(r"\b([a-z]+)$").unwrap_or_else(|_| Regex::new(r"").unwrap());
    if let (Some(loc1_match), Some(loc2_match)) = (
        location_regex.find(normalized_name1),
        location_regex.find(normalized_name2),
    ) {
        let loc1 = loc1_match.as_str();
        let loc2 = loc2_match.as_str();
        let name1_without_loc = &normalized_name1[0..loc1_match.start()].trim();
        let name2_without_loc = &normalized_name2[0..loc2_match.start()].trim();
        if name1_without_loc == name2_without_loc && loc1 != loc2 {
            return pre_rl_score * 0.7;
        }
    }
    pre_rl_score
}

fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let (len1, len2) = (s1_chars.len(), s2_chars.len());
    if len1 == 0 {
        return len2;
    }
    if len2 == 0 {
        return len1;
    }
    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];
    for i in 0..=len1 {
        matrix[i][0] = i;
    }
    for j in 0..=len2 {
        matrix[0][j] = j;
    }
    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] {
                0
            } else {
                1
            };
            matrix[i][j] = std::cmp::min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1)
                .min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[len1][len2]
}

async fn prepare_entity_data_and_index(
    all_entities: &[Entity],
    stopwords: &HashSet<String>,
) -> (
    Vec<EntityNameData>,
    HashMap<String, Vec<usize>>,
    HashMap<String, TokenStats>,
) {
    let mut entity_data_vec = Vec::with_capacity(all_entities.len());
    let mut token_frequency: HashMap<String, usize> = HashMap::new();

    for entity in all_entities {
        if let Some(name) = &entity.name {
            let (normalized_name, entity_type) = normalize_name(name);
            if !normalized_name.is_empty() {
                let (tokens, weighted_tokens) =
                    tokenize_name(&normalized_name, stopwords, entity_type);
                for token in &tokens {
                    *token_frequency.entry(token.clone()).or_insert(0) += 1;
                }
                entity_data_vec.push(EntityNameData {
                    entity: entity.clone(),
                    normalized_name,
                    entity_type,
                    tokens,
                    weighted_tokens,
                });
            }
        }
    }

    let total_docs = entity_data_vec.len() as f32;
    let mut token_stats_map = HashMap::new();
    for (token, freq) in token_frequency {
        let idf = (total_docs / (freq as f32 + 1.0)).ln_1p().max(0.0);
        token_stats_map.insert(
            token.clone(),
            TokenStats {
                token,
                frequency: freq,
                idf,
            },
        );
    }

    let max_common_token_freq = (entity_data_vec.len() as f32 * 0.05).max(10.0) as usize;
    let mut token_to_entities_map: HashMap<String, Vec<usize>> = HashMap::new();

    for (idx, data) in entity_data_vec.iter().enumerate() {
        let mut scored_tokens: Vec<(String, f32)> = data
            .tokens
            .iter()
            .filter_map(|token| {
                token_stats_map.get(token).and_then(|stats| {
                    if stats.frequency <= max_common_token_freq
                        && stats.frequency > 1
                        && token.len() >= MIN_TOKEN_LENGTH
                    {
                        Some((token.clone(), stats.idf))
                    } else {
                        None
                    }
                })
            })
            .collect();
        scored_tokens.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        for (token, _) in scored_tokens.into_iter().take(TOP_TOKENS_PER_ENTITY) {
            token_to_entities_map.entry(token).or_default().push(idx);
        }
    }
    (entity_data_vec, token_to_entities_map, token_stats_map)
}

async fn get_all_entities_with_names(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<Vec<Entity>> {
    let query = "
        SELECT e.id, e.organization_id, e.name, e.created_at, e.updated_at, e.source_system, e.source_id
        FROM public.entity e
        WHERE e.name IS NOT NULL AND e.name != ''
    ";
    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to query all entities with names")?;

    rows.iter()
        .map(|row| {
            Ok(Entity {
                id: EntityId(row.try_get("id").context("Failed to get 'id' for entity")?),
                organization_id: OrganizationId(
                    row.try_get("organization_id")
                        .context("Failed to get 'organization_id' for entity")?,
                ),
                name: row
                    .try_get("name")
                    .context("Failed to get 'name' for entity")?,
                created_at: row
                    .try_get("created_at")
                    .context("Failed to get 'created_at' for entity")?,
                updated_at: row
                    .try_get("updated_at")
                    .context("Failed to get 'updated_at' for entity")?,
                source_system: row.try_get("source_system").ok(),
                source_id: row.try_get("source_id").ok(),
            })
        })
        .collect::<Result<Vec<Entity>>>()
}

async fn get_organization_embeddings(
    conn: &impl tokio_postgres::GenericClient,
    entities: &[Entity],
) -> Result<HashMap<EntityId, Option<Vec<f32>>>> {
    let mut embeddings = HashMap::new();
    let org_ids: HashSet<String> = entities
        .iter()
        .map(|e| e.organization_id.0.clone())
        .collect();

    if org_ids.is_empty() {
        return Ok(embeddings);
    }
    let org_ids_vec: Vec<String> = org_ids.into_iter().collect();
    let batch_size = 100;
    debug!(
        "Fetching embeddings for {} unique organizations in batches of {}...",
        org_ids_vec.len(),
        batch_size
    );

    for batch_org_ids_str_slice in org_ids_vec.chunks(batch_size) {
        let batch_org_ids_refs: Vec<&str> =
            batch_org_ids_str_slice.iter().map(AsRef::as_ref).collect();
        let query = "SELECT id, embedding FROM public.organization WHERE id = ANY($1::TEXT[]) AND embedding IS NOT NULL";
        let rows = conn
            .query(query, &[&batch_org_ids_refs])
            .await
            .with_context(|| {
                format!(
                    "Failed to query org embeddings for batch: {:?}",
                    batch_org_ids_refs
                )
            })?;
        for row in rows {
            let org_id_str: String = row
                .try_get("id")
                .context("Failed to get 'id' for organization embedding row")?;
            let embedding_vec_opt: Option<Vec<f32>> = row.try_get("embedding").ok();
            if let Some(embedding_vec) = embedding_vec_opt {
                for entity in entities
                    .iter()
                    .filter(|e| e.organization_id.0 == org_id_str)
                {
                    embeddings.insert(entity.id.clone(), Some(embedding_vec.clone()));
                }
            }
        }
    }
    for entity in entities {
        embeddings.entry(entity.id.clone()).or_insert(None);
    }
    debug!(
        "Finished fetching embeddings. Found embeddings for {} entity IDs (out of {} total entities).",
        embeddings.values().filter(|v| v.is_some()).count(),
        entities.len()
    );
    Ok(embeddings)
}

async fn fetch_existing_entity_group_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&method_type.as_str()])
        .await
        .with_context(|| {
            format!(
                "Failed to query existing {:?}-matched entity_group pairs",
                method_type
            )
        })?;

    let mut existing_pairs = HashSet::new();
    for row in rows {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        if id1_str < id2_str {
            existing_pairs.insert((EntityId(id1_str), EntityId(id2_str)));
        } else {
            existing_pairs.insert((EntityId(id2_str), EntityId(id1_str)));
        }
    }
    Ok(existing_pairs)
}

async fn create_entity_group(
    pool: &PgPool,
    original_entity_id_1: &EntityId,
    original_entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    features_for_snapshot: Option<Vec<f64>>,
    _feature_cache: Option<SharedFeatureCache>,
) -> Result<bool> {

    let new_entity_group_id_str = Uuid::new_v4().to_string();

    let mut conn = pool.get().await.context("Name: Failed to get DB connection")?;
    let mut tx = conn.transaction().await.context("Name: Failed to start transaction")?;

    let (group_id, was_newly_inserted) = match insert_entity_group_tx(
        &mut tx,
        &new_entity_group_id_str,
        original_entity_id_1,
        original_entity_id_2,
        final_confidence_score,
        pre_rl_confidence_score,
        MatchMethodType::Name,
        match_values.clone(),
    ).await {
        Ok(result) => result,
        Err(e) => {
            tx.rollback().await.context("Name: Failed to rollback after insert_entity_group_tx error")?;
            return Err(e.context("Name: insert_entity_group_tx failed"));
        }
    };

    if was_newly_inserted {
        let mut version: Option<i32> = None;

        if let (Some(ro_arc), Some(features_vec)) = (reinforcement_orchestrator, features_for_snapshot.as_ref()) {
            let orchestrator_guard = ro_arc.lock().await;
            version = Some(orchestrator_guard.confidence_tuner.version as i32);
            let snapshot_features_json = serde_json::to_value(features_vec).unwrap_or(serde_json::Value::Null);

            if let Err(e) = insert_match_decision_detail_tx(
                &mut tx,
                &group_id,
                pipeline_run_id,
                snapshot_features_json,
                MatchMethodType::Name.as_str(),
                pre_rl_confidence_score,
                final_confidence_score,
                version,
            ).await {
                warn!("Name: Failed to log decision snapshot for new group {}: {}. Rolling back.", group_id, e);
                tx.rollback().await.context("Name: Failed to rollback after decision detail error")?;
                return Err(e.context("Name: insert_match_decision_detail_tx failed"));
            }
        }

        if final_confidence_score < MODERATE_LOW_SUGGESTION_THRESHOLD {
            let priority = if final_confidence_score < CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };
            let (n1, n2, norm1, norm2, m_type) = if let MatchValues::Name(n) = match_values {
                (n.original_name1.clone(), n.original_name2.clone(), n.normalized_name1.clone(), n.normalized_name2.clone(), n.pre_rl_match_type.clone().unwrap_or_default())
            } else { ("".into(), "".into(), "".into(), "".into(), "".into()) };

            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Name.as_str(),
                "original_name1": n1, "original_name2": n2,
                "normalized_name1": norm1, "normalized_name2": norm2,
                "pre_rl_score": pre_rl_confidence_score, "pre_rl_match_type": m_type,
                "entity_group_id": &group_id,
            });
            let reason_message = format!(
                "Pair ({}, {}) matched by Name with low tuned confidence ({:.4}). Pre-RL: {:.2} ({}).",
                original_entity_id_1.0, original_entity_id_2.0, final_confidence_score, pre_rl_confidence_score, m_type
            );

            let suggestion = NewSuggestedAction {
                pipeline_run_id: Some(pipeline_run_id.to_string()),
                action_type: ActionType::ReviewEntityInGroup.as_str().to_string(),
                entity_id: None, group_id_1: Some(group_id.clone()), group_id_2: None, cluster_id: None,
                triggering_confidence: Some(final_confidence_score),
                details: Some(details_json), reason_code: Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                reason_message: Some(reason_message), priority,
                status: SuggestionStatus::PendingReview.as_str().to_string(),
                reviewer_id: None, reviewed_at: None, review_notes: None,
            };

            if let Err(e) = insert_suggestion_tx(&mut tx, &suggestion).await {
                 warn!("Name: Failed to insert suggestion for new group {}: {}. Rolling back.", group_id, e);
                 tx.rollback().await.context("Name: Failed to rollback after suggestion error")?;
                 return Err(e.context("Name: insert_suggestion_tx failed"));
            }
        }

        tx.commit().await.context("Name: Failed to commit transaction (new group)")?;
        Ok(true)
    } else {
        debug!("Name: Group for pair ({}, {}) already exists or updated (ID: {}). Skipping.", original_entity_id_1.0, original_entity_id_2.0, group_id);
        tx.commit().await.context("Name: Failed to commit transaction (existing group)")?;
        Ok(false)
    }
}
#[derive(Debug, Clone)]
struct TokenStats {
    token: String,
    frequency: usize,
    idf: f32,
}
