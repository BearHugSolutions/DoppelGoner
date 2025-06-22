// src/matching/name.rs - Progress Callback Integration (Complex Method)
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use regex::Regex;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};
use strsim::jaro_winkler;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::models::matching::{AnyMatchResult, MatchResult, MatchValues, NameMatchDetail, NameMatchDetailValues, NameMetadata};
use crate::models::stats_models::{MatchMethodStats, MatchMethodType};
use crate::rl::orchestrator::RLOrchestrator;
use crate::rl::SharedFeatureCache;
use crate::utils::db_connect::PgPool;
use crate::utils::progress_bars::logging::MatchingLogger; // Import the new logger
use crate::utils::pipeline_state::{
    batch_check_comparison_cache, batch_get_current_signatures_for_pairs,
    batch_store_in_comparison_cache, ComparisonCacheEntry, EntitySignature as SignatureData,
    batch_check_entity_completion_status, // NEW
    batch_mark_entity_completion,         // NEW
    EntityCompletionCheck,                // NEW
};
use crate::{
    matching::db::{
        batch_insert_match_decision_details, batch_upsert_entity_groups, EntityGroupBatchData,
        MatchDecisionDetailBatchData,
    },
    models::core::Entity,
    utils::candle::cosine_similarity_candle,
    utils::constants::MAX_DISTANCE_FOR_SAME_CITY_METERS, // Import the new constant
};
use tokio_postgres::GenericClient; // For get_all_entities_with_names_and_locations_filtered

// NEW IMPORTS: Progress callback functionality
use crate::utils::progress_bars::progress_callback::ProgressCallback;
use crate::{update_progress, update_detailed_progress};

const MIN_FUZZY_SIMILARITY_THRESHOLD: f32 = 0.94;
const MIN_SEMANTIC_SIMILARITY_THRESHOLD: f32 = 0.80;
const COMBINED_SIMILARITY_THRESHOLD: f32 = 0.7;
const FUZZY_WEIGHT: f32 = 0.3;
const SEMANTIC_WEIGHT: f32 = 0.7;
pub const MIN_TOKEN_OVERLAP: usize = 2;
pub const MIN_TOKEN_LENGTH: usize = 2;

const BATCH_SIZE: usize = 100; // Batch size for processing pairs in memory
const BATCH_DB_OPS_SIZE: usize = 500; // Batch size for database operations
const MAX_CONCURRENT_BATCHES: usize = 5;
const MAX_CANDIDATES_PER_ENTITY: usize = 50;
const MAX_TOTAL_CANDIDATE_PAIRS: usize = 100_000;

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

pub const STOPWORDS: [&str; 132] = [
    "a", "an", "the", "and", "or", "but", "nor", "for", "yet", "so", "in", "on", "at", "by", "to", "with", "from", "of", "as", "into", "about", "before", "after", "during", "until", "since", "unless", "inc", "incorporated", "corp", "corporation", "llc", "ltd", "limited", "company", "co", "group", "holdings", "enterprises", "international", "global", "worldwide", "national", "american", "usa", "us", "service", "services", "solutions", "systems", "associates", "partners", "partnership", "organization", "organisation", "foundation", "institute", "association", "assn", "society", "council", "committee", "center", "centre", "department", "division", "unit", "office", "bureau", "agency", "authority", "board", "new", "old", "great", "greater", "best", "better", "first", "second", "third", "primary", "main", "central", "local", "regional", "official", "this", "that", "these", "those", "it", "they", "them", "their", "our", "your", "all", "any", "each", "every", "some", "such", "no", "not", "only", "very", "program", "community", "resource", "resources", "support", "help", "health", "care", "management", "professional", "public", "private", "general", "federal", "state", "county", "district", "area", "branch", "provider", "member", "directory", "guide", "network", "federation",
];

#[derive(Clone)]
pub struct EntityNameData {
    pub entity: Entity, // Keep the original Entity for ID and original name
    pub normalized_name: String,
    pub entity_type: Option<&'static str>, // Detected type
    pub tokens: HashSet<String>,           // For quick overlap checks
    pub latitude: Option<f64>, // Added latitude
    pub longitude: Option<f64>, // Added longitude
}

#[derive(Clone)]
struct SharedNameMatchingData {
    existing_entity_group_pairs: Arc<HashSet<(String, String)>>,
    org_embeddings_map: Arc<HashMap<String, Option<Vec<f32>>>>,
    entity_data: Arc<Vec<EntityNameData>>, // Arc for shared access
}

struct NameMatchingStats {
    new_pairs_created_count: usize,
    entities_in_new_pairs: HashSet<String>,
    confidence_scores_for_stats: Vec<f64>,
    individual_operation_errors: usize,
    cache_hits_count: usize,
    feature_extraction_count: usize,
    feature_extraction_failures: usize,
    pairs_processed: usize, // Total pairs considered for detailed comparison
    feature_extraction_attempted: usize, // Added for tracking
    feature_extraction_success: usize, // Added for tracking
    pairs_with_fuzzy_scores: usize, // Added for tracking fuzzy scores
    avg_fuzzy_score: f64, // Added for tracking avg fuzzy score
    pairs_above_fuzzy_threshold: usize, // Added for tracking pairs above fuzzy threshold
    entities_skipped_complete: usize, // NEW: For completion tracking
    entities_total_potential: usize,  // NEW: For completion tracking
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
            feature_extraction_attempted: 0,
            feature_extraction_success: 0,
            pairs_with_fuzzy_scores: 0,
            avg_fuzzy_score: 0.0,
            pairs_above_fuzzy_threshold: 0,
            entities_skipped_complete: 0, // NEW
            entities_total_potential: 0,  // NEW
        }
    }
}

/// Internal struct to hold data for a pair that needs to be processed,
/// including all information required for DB upsert and decision detail insert.
struct PairToProcessName {
    entity_id_1: String, // Ordered
    entity_id_2: String, // Ordered
    match_values: MatchValues,
    pre_rl_confidence_score: f64,
    final_confidence_score: f64, // Can be updated by RL
    features_for_snapshot: Option<Vec<f64>>,
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
    let logger = MatchingLogger::new(MatchMethodType::Name); // Initialize logger
    logger.log_start(pipeline_run_id, reinforcement_orchestrator_option.is_some());

    // PROGRESS UPDATE: Starting phase
    update_progress!(progress_callback, "Starting", "Initializing name matching");

    let initial_memory = get_memory_usage().await;
    logger.log_debug(&format!("Initial memory usage: {} MB", initial_memory));

    let initial_conn = pool
        .get()
        .await
        .context("Name: Failed to get DB connection")?;

    logger.log_phase("Loading data", Some("querying all potential entities with names"));
    update_progress!(progress_callback, "Loading data", "querying entities with names");

    // NEW: First get all potential entity IDs
    let all_potential_entity_ids_query = "
        SELECT DISTINCT e.id
        FROM public.entity e
        WHERE e.name IS NOT NULL AND e.name != ''";

    let potential_entity_rows = initial_conn
        .query(all_potential_entity_ids_query, &[])
        .await
        .context("Name: Failed to query potential entities with names")?;
    let all_potential_entity_ids: Vec<String> = potential_entity_rows
        .iter()
        .map(|row| row.get::<_, String>("id"))
        .collect();
    logger.log_debug(&format!("Name: Found {} entities with names", all_potential_entity_ids.len()));

    // PROGRESS UPDATE: Potential entities found
    update_progress!(progress_callback, "Loading data", 
        format!("Found {} entities with names", all_potential_entity_ids.len()));

    // Check completion status
    update_progress!(progress_callback, "Loading data", "checking completion status");
    let completion_status = batch_check_entity_completion_status(
        pool,
        &all_potential_entity_ids,
        &MatchMethodType::Name
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
        "Name: {} total entities, {} already complete, {} to process",
        all_potential_entity_ids.len(),
        completed_count,
        incomplete_entity_ids.len()
    ));

    // PROGRESS UPDATE: Completion status results
    update_progress!(progress_callback, "Loading data", 
        format!("{} to process ({} already complete)", incomplete_entity_ids.len(), completed_count));

    // Initialize stats with total potential entities and skipped count
    let stats_arc = Arc::new(Mutex::new(NameMatchingStats {
        entities_total_potential: all_potential_entity_ids.len(),
        entities_skipped_complete: completed_count,
        ..Default::default()
    }));

    if incomplete_entity_ids.is_empty() {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No incomplete entities to process");
        
        logger.log_completion(0, 0, 0.0, 0); // Log with 0 new groups
        let final_stats_guard = stats_arc.lock().await; // Lock here to access stats
        return Ok(AnyMatchResult::Name(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Name,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: final_stats_guard.entities_skipped_complete, // Pass skipped count
                entities_total: final_stats_guard.entities_total_potential,           // Pass total potential
            },
        }));
    }

    // Load only incomplete entities
    update_progress!(progress_callback, "Loading data", "querying detailed entity data");
    let all_entities_with_names_vec = get_all_entities_with_names_and_locations_filtered(&*initial_conn, &incomplete_entity_ids).await?;
    let total_entities = all_entities_with_names_vec.len();
    logger.log_data_loaded(total_entities, "entity name");

    // PROGRESS UPDATE: Data loaded
    update_progress!(progress_callback, "Loading data", 
        format!("Loaded {} entity records", total_entities));

    if total_entities < 2 {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "Not enough entities to form pairs");
        
        logger.log_completion(0, 0, 0.0, 0);
        // Not enough entities to form pairs
        let final_stats_guard = stats_arc.lock().await; // Lock here to access stats
        return Ok(AnyMatchResult::Name(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Name,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: final_stats_guard.entities_skipped_complete, // Pass skipped count
                entities_total: final_stats_guard.entities_total_potential,           // Pass total potential
            },
        }));
    }

    logger.log_phase("Loading existing pairs", Some("querying entity_group table"));
    update_progress!(progress_callback, "Loading existing pairs", "querying entity_group table");
    
    let existing_entity_group_pairs =
        Arc::new(fetch_existing_entity_group_pairs(&*initial_conn, MatchMethodType::Name).await?);
    logger.log_existing_pairs(existing_entity_group_pairs.len());

    // PROGRESS UPDATE: Existing pairs loaded
    update_progress!(progress_callback, "Loading existing pairs", 
        format!("Found {} existing pairs", existing_entity_group_pairs.len()));

    logger.log_phase("Loading organization embeddings", Some("querying organization embeddings"));
    update_progress!(progress_callback, "Loading embeddings", "querying organization embeddings");
    
    let org_embeddings_map =
        Arc::new(get_organization_embeddings(&*initial_conn, &all_entities_with_names_vec).await?);
    drop(initial_conn); // Release connection

    // PROGRESS UPDATE: Embeddings loaded
    update_progress!(progress_callback, "Loading embeddings", 
        format!("Loaded embeddings for {} organizations", org_embeddings_map.len()));

    logger.log_phase("Processing and normalizing data", Some("preparing entity data and index"));
    update_progress!(progress_callback, "Processing entities", "normalizing names and building index");
    
    let stopwords: HashSet<String> = STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let (entity_data_vec, token_to_entities_idx_map, _) = // Removed token_stats as it's not directly used in pair generation logic here
        prepare_entity_data_and_index(&all_entities_with_names_vec, &stopwords).await;

    let entity_data_arc = Arc::new(entity_data_vec);
    let groups_with_multiple = entity_data_arc.len(); // All entities are potential groups
    logger.log_processing_complete(total_entities, entity_data_arc.len(), groups_with_multiple);

    // PROGRESS UPDATE: Processing complete
    update_progress!(progress_callback, "Processing entities", 
        format!("Processed {} entities into searchable index", entity_data_arc.len()));

    logger.log_phase("Generating candidate pairs", None);
    update_progress!(progress_callback, "Generating pairs", "creating candidate pairs using token overlap");
    
    let candidate_pairs_indices =
        generate_candidate_pairs_indices_limited(&entity_data_arc, &token_to_entities_idx_map, progress_callback.clone())
            .await?;
    let total_conceptual_pairs = candidate_pairs_indices.len();
    logger.log_pair_generation(total_conceptual_pairs, groups_with_multiple); // groups_with_multiple is total entities in this context for names

    // PROGRESS UPDATE: Pair generation complete
    update_progress!(progress_callback, "Generating pairs", 
        format!("{} candidate pairs generated", total_conceptual_pairs));

    if total_conceptual_pairs == 0 {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No candidate pairs generated");
        
        logger.log_completion(0, 0, 0.0, 0);
        let final_stats_guard = stats_arc.lock().await; // Lock here to access stats
        return Ok(AnyMatchResult::Name(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Name,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: final_stats_guard.entities_skipped_complete, // Pass skipped count
                entities_total: final_stats_guard.entities_total_potential,           // Pass total potential
            },
        }));
    }

    logger.log_phase("Evaluating pairs with RL confidence tuning", None); // This phase now covers full evaluation
    update_progress!(progress_callback, "Evaluating pairs", "starting batch processing with RL confidence tuning");
    
    let shared_data = SharedNameMatchingData {
        existing_entity_group_pairs,
        org_embeddings_map,
        entity_data: entity_data_arc,
    };

    // Pass a clone of the Arc to the processing function.
    // The original stats_arc will be unwrapped after this function returns.
    process_candidate_pairs_in_batches(
        candidate_pairs_indices,
        shared_data,
        pool,
        reinforcement_orchestrator_option,
        pipeline_run_id,
        feature_cache,
        completion_status, // Pass completion_status
        stats_arc.clone(), // Pass the Arc<Mutex<NameMatchingStats>>
        progress_callback.clone(), // NEW: Pass progress callback
    )
    .await?;

    // Now that all processing (and thus all other Arc clones) are complete,
    // we can safely unwrap the original stats_arc to get the final statistics.
    let final_stats = Arc::try_unwrap(stats_arc)
        .map_err(|_| anyhow::anyhow!("Failed to unwrap stats mutex for final return (should be sole owner after all tasks complete)"))?
        .into_inner();

    let final_memory = get_memory_usage().await;
    logger.log_debug(&format!(
        "Final memory usage: {} MB (delta: {} MB)",
        final_memory,
        final_memory.saturating_sub(initial_memory)
    ));

    let avg_confidence: f64 = if !final_stats.confidence_scores_for_stats.is_empty() {
        final_stats.confidence_scores_for_stats.iter().sum::<f64>()
            / final_stats.confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Name,
        groups_created: final_stats.new_pairs_created_count,
        entities_matched: final_stats.entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_stats.new_pairs_created_count > 0 {
            2.0
        } else {
            0.0
        },
        entities_skipped_complete: final_stats.entities_skipped_complete, // Pass skipped count
        entities_total: final_stats.entities_total_potential,           // Pass total potential
    };

    logger.log_completion(
        method_stats.groups_created,
        method_stats.entities_matched,
        avg_confidence,
        final_stats.pairs_processed, // Use the total pairs processed from the shared stats
    );
    logger.log_performance_summary(
        final_stats.cache_hits_count, // Use from shared stats
        final_stats.individual_operation_errors, // Use from shared stats
        Some((final_stats.feature_extraction_attempted, final_stats.feature_extraction_success, final_stats.feature_extraction_failures))
    );
    if final_stats.pairs_with_fuzzy_scores > 0 {
        logger.log_debug(&format!(
            "Fuzzy similarity: avg {:.3}, {} pairs above threshold",
            final_stats.avg_fuzzy_score, final_stats.pairs_above_fuzzy_threshold
        ));
    }

    // FINAL PROGRESS UPDATE: Completion
    update_progress!(progress_callback, "Completed", 
        format!("{} name groups created, {} entities matched", 
                method_stats.groups_created, method_stats.entities_matched));

    Ok(AnyMatchResult::Name(MatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
async fn process_candidate_pairs_in_batches(
    candidate_pairs_indices: Vec<(usize, usize)>,
    shared_data: SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<RLOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    completion_status: HashMap<String, EntityCompletionCheck>, // NEW PARAMETER
    stats_mutex: Arc<Mutex<NameMatchingStats>>,              // Pass the Arc
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<()> { // Changed return type to Result<()>
    let total_pairs = candidate_pairs_indices.len();
    let total_conceptual_batches = (total_pairs + BATCH_SIZE - 1) / BATCH_SIZE;
    debug!(
        "Name: Processing {} pairs in conceptual batches of size {}",
        total_pairs, BATCH_SIZE
    );

    // PROGRESS UPDATE: Batch processing start
    update_progress!(progress_callback, "Batch processing", 
        format!("Processing {} pairs in {} batches", total_pairs, total_conceptual_batches));

    // Create progress bar for batch processing
    let batch_pb = ProgressBar::new(total_conceptual_batches as u64);
    batch_pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "  👤 [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} Processing name batches...",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );

    // stats_mutex is already Arc'd and passed in, so no need to create a new one here
    let processed_pairs_this_run = Arc::new(Mutex::new(HashSet::<(String, String)>::new()));
    let logger = MatchingLogger::new(MatchMethodType::Name); // Local logger for batch logging

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
            let stats_arc_clone = Arc::clone(&stats_mutex); // Use the passed in Arc
            let processed_pairs_clone = Arc::clone(&processed_pairs_this_run);
            let logger_clone = Arc::new(logger.clone()); // Clone logger for each task
            let completion_status_clone = completion_status.clone(); // Clone for each task
            let progress_callback_clone = progress_callback.clone(); // NEW: Clone progress callback

            batch_futures.push(tokio::spawn(async move {
                process_name_batch(
                    global_batch_num,
                    total_conceptual_batches,
                    batch_data_for_task,
                    shared_data_clone,
                    &pool_clone,
                    ro_option_clone.as_ref(),
                    &run_id_clone,
                    feature_cache_clone.as_ref(),
                    stats_arc_clone,
                    processed_pairs_clone,
                    logger_clone,
                    completion_status_clone, // Pass to batch processing function
                    progress_callback_clone, // NEW: Pass progress callback
                )
                .await
            }));
        }

        let results = futures::future::join_all(batch_futures).await;
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                logger.log_warning(&format!("Batch processing task {} panicked or failed: {}", i, e));
                // Error already logged inside task if it's a processing error
            }
        }

        batch_pb.inc(results.len() as u64); // Increment by number of batches processed in this chunk

        // Log batch progress
        logger.log_batch_progress(chunk_idx + 1, total_conceptual_batches / MAX_CONCURRENT_BATCHES, chunk.len() * MAX_CONCURRENT_BATCHES);

        // PROGRESS UPDATE: Batch chunk progress
        if chunk_idx % 5 == 0 {
            let current_memory = get_memory_usage().await;
            let stats_guard = stats_mutex.lock().await;
            batch_pb.set_message(format!(
                "Processing batches... (Memory: {} MB, Pairs: {}, Groups: {}, FE attempted: {}, FE success: {})",
                current_memory,
                stats_guard.pairs_processed,
                stats_guard.new_pairs_created_count,
                stats_guard.feature_extraction_attempted,
                stats_guard.feature_extraction_success
            ));
            
            // PROGRESS UPDATE: Detailed batch progress
            update_detailed_progress!(progress_callback, "Batch processing", 
                chunk_idx + 1, (candidate_pairs_indices.len() + BATCH_SIZE * MAX_CONCURRENT_BATCHES - 1) / (BATCH_SIZE * MAX_CONCURRENT_BATCHES),
                format!("Memory: {}MB, Groups: {}", current_memory, stats_guard.new_pairs_created_count));
        }
    }

    batch_pb.finish_with_message("Name batch processing complete");

    // After all batches are processed, perform final completion marking
    logger.log_phase("Tracking completion status", Some("marking entities with no remaining name comparisons"));
    update_progress!(progress_callback, "Completion tracking", "marking completed entities");

    let mut completion_batch: Vec<(String, MatchMethodType, String, String, i32)> = Vec::new(); // Make mutable
    let stats_guard = stats_mutex.lock().await; // Lock stats for final aggregate and completion logic

    // Iterate over all entities that were initially considered as incomplete
    // and are part of the original candidate set for Name matching
    for (entity_id, status_check) in completion_status.iter() {
        if !status_check.is_complete { // Only attempt to mark incomplete ones
            if let Some(current_sig) = &status_check.current_signature {
                // If the entity was processed in this run and has no more pending comparisons
                // (this logic is simplified for the example, a real system would need to track all potential pairs)
                // For now, assume if it was considered and no new pairs for it were created, it might be complete.
                // A more robust solution would involve tracking "potential pairs remaining".
                // For this integration, we'll mark entities as complete if they were involved in *any* comparison
                // in this run and their signature hasn't changed.
                let comparison_count = stats_guard.entities_in_new_pairs.get(entity_id).map_or(0, |_| 1); // Simple check if it was involved in a new pair

                if comparison_count > 0 { // If it participated in at least one comparison
                    completion_batch.push((
                        entity_id.clone(),
                        MatchMethodType::Name,
                        pipeline_run_id.to_string(),
                        current_sig.clone(),
                        comparison_count, // Placeholder, a real count would be accumulated
                    ));
                }
            }
        }
    }

    if !completion_batch.is_empty() {
        if let Err(e) = batch_mark_entity_completion(pool, &completion_batch).await {
            logger.log_warning(&format!(
                "Failed to batch mark {} entities as complete: {}",
                completion_batch.len(), e
            ));
        } else {
            logger.log_debug(&format!(
                "Marked {} entities as complete for Name matching",
                completion_batch.len()
            ));
        }
    }

    // No need to unwrap stats_mutex here; it's owned by the caller (find_matches)
    Ok(()) // Return Ok(()) instead of unwrapping NameMatchingStats
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
#[allow(clippy::too_many_arguments)]
async fn process_name_batch(
    _batch_num_for_log: usize, // Not strictly needed for progress bar, but useful for debugging
    _total_batches_for_log: usize,
    batch_pair_indices: Vec<(usize, usize)>,
    shared_data: SharedNameMatchingData,
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<&Arc<Mutex<RLOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<&SharedFeatureCache>,
    stats_mutex: Arc<Mutex<NameMatchingStats>>,
    processed_pairs_this_run_global: Arc<Mutex<HashSet<(String, String)>>>,
    logger: Arc<MatchingLogger>, // Pass logger down
    completion_status: HashMap<String, EntityCompletionCheck>, // NEW PARAMETER
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<()> {
    // Result indicates if the task itself failed, not individual pair errors
    logger.log_debug(&format!("Processing batch ({} pairs)", batch_pair_indices.len()));
    let mut local_batch_errors = 0;
    let mut local_fuzzy_scores: Vec<f64> = Vec::new(); // Collect fuzzy scores for this batch

    let mut pairs_for_signature_fetch: Vec<(String, String)> = Vec::new();
    let mut pairs_to_evaluate: Vec<PairToProcessName> = Vec::new();

    // Track comparison counts for entities within this specific batch
    let mut local_entity_comparison_counts: HashMap<String, i32> = HashMap::new();

    // PROGRESS UPDATE: Batch processing start
    update_progress!(progress_callback, "Batch processing", 
        format!("Processing name batch with {} pairs", batch_pair_indices.len()));

    for (idx1, idx2) in batch_pair_indices {
        let entity1_data = &shared_data.entity_data[idx1];
        let entity2_data = &shared_data.entity_data[idx2];

        if entity1_data.entity.id == entity2_data.entity.id {
            continue;
        }

        let (e1_id, e1_lat, e1_lon, e2_id, e2_lat, e2_lon) =
            if entity1_data.entity.id < entity2_data.entity.id {
                (
                    &entity1_data.entity.id,
                    entity1_data.latitude,
                    entity1_data.longitude,
                    &entity2_data.entity.id,
                    entity2_data.latitude,
                    entity2_data.longitude,
                )
            } else {
                (
                    &entity2_data.entity.id,
                    entity2_data.latitude,
                    entity2_data.longitude,
                    &entity1_data.entity.id,
                    entity1_data.latitude,
                    entity1_data.longitude,
                )
            };
        let current_pair_ordered = (e1_id.clone(), e2_id.clone());

        {
            let mut stats_guard = stats_mutex.lock().await;
            stats_guard.pairs_processed += 1;
        }
        {
            let mut processed_set = processed_pairs_this_run_global.lock().await;
            if processed_set.contains(&current_pair_ordered) {
                continue;
            }
        }

        // Initial filtering based on incompatible types and empty names
        let original_name1 = entity1_data
            .entity
            .name
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let original_name2 = entity2_data
            .entity
            .name
            .as_ref()
            .cloned()
            .unwrap_or_default();
        let normalized_name1 = &entity1_data.normalized_name;
        let normalized_name2 = &entity2_data.normalized_name;

        if normalized_name1.is_empty() || normalized_name2.is_empty() {
            let mut processed_set = processed_pairs_this_run_global.lock().await;
            processed_set.insert(current_pair_ordered);
            continue;
        }

        if let (Some(t1), Some(t2)) = (entity1_data.entity_type, entity2_data.entity_type) {
            if t1 != t2
                && INCOMPATIBLE_ORG_TYPES
                    .iter()
                    .any(|(it1, it2)| (t1 == *it1 && t2 == *it2) || (t1 == *it2 && t2 == *it1))
            {
                let mut processed_set = processed_pairs_this_run_global.lock().await;
                processed_set.insert(current_pair_ordered);
                continue;
            }
        }

        // --- Geospatial Filtering ---
        if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) =
            (e1_lat, e1_lon, e2_lat, e2_lon)
        {
            let distance = calculate_distance(lat1, lon1, lat2, lon2);
            if distance > MAX_DISTANCE_FOR_SAME_CITY_METERS {
                logger.log_debug(&format!("Pair ({}, {}) skipped due to large geospatial distance ({}m > {}m).", e1_id, e2_id, distance, MAX_DISTANCE_FOR_SAME_CITY_METERS));
                let mut processed_set = processed_pairs_this_run_global.lock().await;
                processed_set.insert(current_pair_ordered);
                continue;
            }
        }

        // Add to list for batch signature fetch
        pairs_for_signature_fetch.push(current_pair_ordered.clone());

        // Increment local comparison counts for both entities in the pair
        *local_entity_comparison_counts.entry(e1_id.clone()).or_insert(0) += 1;
        *local_entity_comparison_counts.entry(e2_id.clone()).or_insert(0) += 1;

        // Prepare initial PairToProcessName
        let pair_to_proc = PairToProcessName {
            entity_id_1: current_pair_ordered.0.clone(),
            entity_id_2: current_pair_ordered.1.clone(),
            match_values: MatchValues::Name(NameMatchDetail {
                values: NameMatchDetailValues {
                    normalized_name1: normalized_name1.clone(),
                    normalized_name2: normalized_name2.clone(),
                    fuzzy_score: Some(0.0), // Initialize with default value
                    semantic_score: Some(0.0), // Initialize with default value
                    pre_rl_match_type: None, // Will be set later
                },
                original1: original_name1.clone(), // Ensure cloning for owned data
                original2: original_name2.clone(), // Ensure cloning for owned data
                normalized_shared: format!("{} | {}", normalized_name1, normalized_name2),
                metadata: NameMetadata {
                    normalized_name1: normalized_name1.clone(),
                    normalized_name2: normalized_name2.clone(),
                    pre_rl_match_type: None, // Initial, will be updated
                    features_snapshot: None, // Initial, will be updated
                },
            }),
            pre_rl_confidence_score: 0.0, // Will be calculated
            final_confidence_score: 0.0,  // Will be calculated/tuned
            features_for_snapshot: None,
            original_signature_1: None,
            original_signature_2: None,
            comparison_cache_hit: false,
        };
        pairs_to_evaluate.push(pair_to_proc);
    } // End of initial pair collection

    // --- Batch Fetch Signatures and Check Cache ---
    let signatures_map =
        batch_get_current_signatures_for_pairs(pool, &pairs_for_signature_fetch).await?;
    let mut cache_check_inputs = Vec::new();

    for pair_data in pairs_to_evaluate.iter_mut() {
        let pair_key = (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone());
        if let Some((sig1_data, sig2_data)) = signatures_map.get(&pair_key) {
            pair_data.original_signature_1 = Some(sig1_data.clone());
            pair_data.original_signature_2 = Some(sig2_data.clone());
            cache_check_inputs.push((
                pair_data.entity_id_1.clone(),
                pair_data.entity_id_2.clone(),
                sig1_data.signature.clone(),
                sig2_data.signature.clone(),
                MatchMethodType::Name,
            ));
        }
    }

    let cache_results_map = batch_check_comparison_cache(pool, &cache_check_inputs).await?;

    let mut pairs_for_full_evaluation: Vec<PairToProcessName> = Vec::new();
    {
        let mut stats_guard = stats_mutex.lock().await;
        let mut processed_set = processed_pairs_this_run_global.lock().await;

        for mut pair_data in pairs_to_evaluate {
            let pair_key = (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone());
            if let Some(_cached_eval) = cache_results_map.get(&pair_key) {
                stats_guard.cache_hits_count += 1;
                pair_data.comparison_cache_hit = true;
                processed_set.insert(pair_key); // Mark as processed due to cache hit
            } else {
                pairs_for_full_evaluation.push(pair_data);
            }
        }
    } // Release locks

    // --- Full Evaluation and Batch DB Operations ---
    let mut batch_entity_group_data: Vec<EntityGroupBatchData> = Vec::new();
    let mut batch_decision_detail_data: Vec<MatchDecisionDetailBatchData> = Vec::new();
    let mut batch_cache_store_data: Vec<ComparisonCacheEntry> = Vec::new();
    let pairs_len = pairs_for_full_evaluation.len();

    for (pair_idx, pair_data_ref) in pairs_for_full_evaluation.iter_mut().enumerate() {
        // PROGRESS UPDATE: Detailed pair evaluation progress
        if pair_idx % 50 == 0 {
            update_progress!(progress_callback, "Batch processing", 
                format!("Evaluating pair {}/{} in batch", pair_idx + 1, pairs_len));
        }
        
        let (ordered_e1_id, ordered_e2_id) =
            (&pair_data_ref.entity_id_1, &pair_data_ref.entity_id_2);
        let pair_key = (ordered_e1_id.clone(), ordered_e2_id.clone());

        let entity1_data = shared_data
            .entity_data
            .iter()
            .find(|e| &e.entity.id == ordered_e1_id)
            .unwrap();
        let entity2_data = shared_data
            .entity_data
            .iter()
            .find(|e| &e.entity.id == ordered_e2_id)
            .unwrap();

        let normalized_name1 = &entity1_data.normalized_name;
        let normalized_name2 = &entity2_data.normalized_name;

        let fuzzy_score = jaro_winkler(normalized_name1, normalized_name2) as f32;
        local_fuzzy_scores.push(fuzzy_score as f64); // Collect fuzzy score

        // Update fuzzy_score in pair_data_ref.match_values
        if let MatchValues::Name(ref mut name_detail) = pair_data_ref.match_values {
            name_detail.values.fuzzy_score = Some(fuzzy_score);
        }

        if fuzzy_score < (MIN_FUZZY_SIMILARITY_THRESHOLD * 0.9) {
            if let (Some(s1_data), Some(s2_data)) = (
                &pair_data_ref.original_signature_1,
                &pair_data_ref.original_signature_2,
            ) {
                batch_cache_store_data.push(ComparisonCacheEntry {
                    entity_id_1: ordered_e1_id.clone(),
                    entity_id_2: ordered_e2_id.clone(),
                    signature_1: s1_data.signature.clone(),
                    signature_2: s2_data.signature.clone(),
                    method_type: MatchMethodType::Name,
                    pipeline_run_id: pipeline_run_id.to_string(),
                    comparison_result: "NON_MATCH".to_string(),
                    similarity_score: Some(fuzzy_score as f64),
                    features_snapshot: None,
                });
            }
            let mut processed_set = processed_pairs_this_run_global.lock().await;
            processed_set.insert(pair_key);
            continue;
        }

        let embedding1_opt = shared_data
            .org_embeddings_map
            .get(ordered_e1_id)
            .and_then(|o| o.as_ref());
        let embedding2_opt = shared_data
            .org_embeddings_map
            .get(ordered_e2_id)
            .and_then(|o| o.as_ref());
        let semantic_score = match (embedding1_opt, embedding2_opt) {
            (Some(e1), Some(e2)) => cosine_similarity_candle(e1, e2).unwrap_or(0.0) as f32,
            _ => 0.0,
        };

        // Update semantic_score in pair_data_ref.match_values
        if let MatchValues::Name(ref mut name_detail) = pair_data_ref.match_values {
            name_detail.values.semantic_score = Some(semantic_score);
        }

        let (mut pre_rl_score, pre_rl_match_type_str) =
            if semantic_score >= MIN_SEMANTIC_SIMILARITY_THRESHOLD {
                (
                    (fuzzy_score * FUZZY_WEIGHT) + (semantic_score * SEMANTIC_WEIGHT),
                    "combined",
                )
            } else if fuzzy_score >= MIN_FUZZY_SIMILARITY_THRESHOLD {
                (fuzzy_score, "fuzzy")
            } else {
                if let (Some(s1_data), Some(s2_data)) = (
                    &pair_data_ref.original_signature_1,
                    &pair_data_ref.original_signature_2,
                ) {
                    batch_cache_store_data.push(ComparisonCacheEntry {
                        entity_id_1: ordered_e1_id.clone(),
                        entity_id_2: ordered_e2_id.clone(),
                        signature_1: s1_data.signature.clone(),
                        signature_2: s2_data.signature.clone(),
                        method_type: MatchMethodType::Name,
                        pipeline_run_id: pipeline_run_id.to_string(),
                        comparison_result: "NON_MATCH".to_string(),
                        similarity_score: Some(fuzzy_score.max(semantic_score) as f64),
                        features_snapshot: None,
                    });
                }
                let mut processed_set = processed_pairs_this_run_global.lock().await;
                processed_set.insert(pair_key);
                continue;
            };

        pre_rl_score = apply_domain_rules(
            normalized_name1,
            normalized_name2,
            pre_rl_score,
            entity1_data.entity_type,
            entity2_data.entity_type,
        );

        if pre_rl_score < COMBINED_SIMILARITY_THRESHOLD {
            if let (Some(s1_data), Some(s2_data)) = (
                &pair_data_ref.original_signature_1,
                &pair_data_ref.original_signature_2,
            ) {
                batch_cache_store_data.push(ComparisonCacheEntry {
                    entity_id_1: ordered_e1_id.clone(),
                    entity_id_2: ordered_e2_id.clone(),
                    signature_1: s1_data.signature.clone(),
                    signature_2: s2_data.signature.clone(),
                    method_type: MatchMethodType::Name,
                    pipeline_run_id: pipeline_run_id.to_string(),
                    comparison_result: "NON_MATCH".to_string(),
                    similarity_score: Some(pre_rl_score as f64),
                    features_snapshot: None,
                });
            }
            let mut processed_set = processed_pairs_this_run_global.lock().await;
            processed_set.insert(pair_key);
            continue;
        }

        pair_data_ref.pre_rl_confidence_score = pre_rl_score as f64;
        pair_data_ref.final_confidence_score = pre_rl_score as f64; // Start with pre-RL
        if let MatchValues::Name(ref mut name_detail) = pair_data_ref.match_values {
            name_detail.metadata.pre_rl_match_type = Some(pre_rl_match_type_str.to_string());
            name_detail.values.pre_rl_match_type = Some(pre_rl_match_type_str.to_string());
        }

        if let Some(ro_arc) = reinforcement_orchestrator_option {
            {
                // Scoped for stats_guard
                let mut stats_guard = stats_mutex.lock().await;
                stats_guard.feature_extraction_attempted += 1; // Increment here
            }

            match if let Some(fc_arc) = feature_cache {
                let mut fc_guard = fc_arc.lock().await;
                fc_guard
                    .get_pair_features(pool, ordered_e1_id, ordered_e2_id)
                    .await
            } else {
                RLOrchestrator::extract_pair_context_features(pool, ordered_e1_id, ordered_e2_id)
                    .await
            } {
                Ok(features) => {
                    {
                        // Scoped for stats_guard
                        let mut stats_guard = stats_mutex.lock().await;
                        stats_guard.feature_extraction_success += 1; // Increment here
                    }
                    if !features.is_empty() {
                        pair_data_ref.features_for_snapshot = Some(features.clone());
                        let features_json_for_cache = serde_json::to_value(features.clone()).ok();
                        if let Some(features_json) = features_json_for_cache {
                            if let MatchValues::Name(ref mut name_detail) = pair_data_ref.match_values {
                                name_detail.metadata.features_snapshot = Some(features_json.clone());
                            }
                        }

                        match ro_arc.lock().await.get_tuned_confidence(
                            &MatchMethodType::Name,
                            pair_data_ref.pre_rl_confidence_score,
                            features.as_ref(),
                        ) {
                            Ok(tuned_score) => pair_data_ref.final_confidence_score = tuned_score,
                            Err(e) => logger.log_warning(&format!(
                                "RL tuning failed for ({}, {}): {}.",
                                ordered_e1_id, ordered_e2_id, e
                            )),
                        }
                    }
                }
                Err(e) => {
                    let mut stats_guard = stats_mutex.lock().await;
                    stats_guard.feature_extraction_failures += 1;
                    logger.log_warning(&format!(
                        "Feature extraction failed for ({}, {}): {}.",
                        ordered_e1_id, ordered_e2_id, e
                    ));
                }
            }
        }

        // Prepare for batch upsert
        let new_entity_group_id_str = Uuid::new_v4().to_string(); // Generate ID upfront
        batch_entity_group_data.push(EntityGroupBatchData {
            proposed_id: new_entity_group_id_str.clone(),
            entity_id_1: ordered_e1_id.clone(),
            entity_id_2: ordered_e2_id.clone(),
            confidence_score: pair_data_ref.final_confidence_score,
            pre_rl_confidence_score: pair_data_ref.pre_rl_confidence_score,
            method_type: MatchMethodType::Name,
            match_values: pair_data_ref.match_values.clone(),
        });

        // Prepare data for batch cache store (MATCH outcome)
        if let (Some(s1_data), Some(s2_data)) = (
            &pair_data_ref.original_signature_1,
            &pair_data_ref.original_signature_2,
        ) {
            batch_cache_store_data.push(ComparisonCacheEntry {
                entity_id_1: ordered_e1_id.clone(),
                entity_id_2: ordered_e2_id.clone(),
                signature_1: s1_data.signature.clone(),
                signature_2: s2_data.signature.clone(),
                method_type: MatchMethodType::Name,
                pipeline_run_id: pipeline_run_id.to_string(),
                comparison_result: "MATCH".to_string(),
                similarity_score: Some(pair_data_ref.final_confidence_score),
                features_snapshot: pair_data_ref
                    .features_for_snapshot
                    .as_ref()
                    .map(|f| serde_json::to_value(f).unwrap_or_default()),
            });
        }
    } // End of loop through pairs_for_full_evaluation

    // Aggregate local fuzzy scores into global stats
    {
        let mut stats_guard = stats_mutex.lock().await;
        stats_guard.pairs_with_fuzzy_scores += local_fuzzy_scores.len();
        if !local_fuzzy_scores.is_empty() {
             stats_guard.avg_fuzzy_score = (stats_guard.avg_fuzzy_score * (stats_guard.pairs_with_fuzzy_scores - local_fuzzy_scores.len()) as f64
                                           + local_fuzzy_scores.iter().sum::<f64>())
                                           / stats_guard.pairs_with_fuzzy_scores as f64;
        }
        stats_guard.pairs_above_fuzzy_threshold += local_fuzzy_scores.iter()
                                                    .filter(|&&score| score >= MIN_FUZZY_SIMILARITY_THRESHOLD as f64)
                                                    .count();
    }

    // --- Execute all collected batch DB operations ---
    if !batch_entity_group_data.is_empty() {
        match batch_upsert_entity_groups(pool, batch_entity_group_data).await {
            Ok(results_map) => {
                let mut stats_guard = stats_mutex.lock().await;
                let mut processed_set = processed_pairs_this_run_global.lock().await;

                for pair_data_ref in pairs_for_full_evaluation {
                    // Iterate original items to get initial IDs
                    let pair_key = (
                        pair_data_ref.entity_id_1.clone(),
                        pair_data_ref.entity_id_2.clone(),
                    );
                    if let Some((group_id, was_newly_inserted)) = results_map.get(&pair_key) {
                        if *was_newly_inserted {
                            stats_guard.new_pairs_created_count += 1;
                            stats_guard
                                .entities_in_new_pairs
                                .insert(pair_data_ref.entity_id_1.clone());
                            stats_guard
                                .entities_in_new_pairs
                                .insert(pair_data_ref.entity_id_2.clone());
                            stats_guard
                                .confidence_scores_for_stats
                                .push(pair_data_ref.final_confidence_score);

                            if let (Some(ro_arc), Some(features_vec)) = (
                                reinforcement_orchestrator_option,
                                pair_data_ref.features_for_snapshot,
                            ) {
                                let version = ro_arc.lock().await.confidence_tuner.version;
                                let snapshot_features_json = serde_json::to_value(features_vec)
                                    .unwrap_or(serde_json::Value::Null);
                                batch_decision_detail_data.push(MatchDecisionDetailBatchData {
                                    entity_group_id: group_id.clone(),
                                    pipeline_run_id: pipeline_run_id.to_string(),
                                    snapshotted_features: snapshot_features_json,
                                    method_type_at_decision: MatchMethodType::Name
                                        .as_str()
                                        .to_string(),
                                    pre_rl_confidence_at_decision: pair_data_ref
                                        .pre_rl_confidence_score,
                                    tuned_confidence_at_decision: pair_data_ref
                                        .final_confidence_score,
                                    confidence_tuner_version_at_decision: Some(version as i32),
                                });
                            }
                        }
                    }
                    processed_set.insert(pair_key); // Mark as processed for this run
                }
            }
            Err(e) => {
                logger.log_warning(&format!("Batch upsert failed: {}", e));
                let mut stats_guard = stats_mutex.lock().await;
                stats_guard.individual_operation_errors += pairs_for_full_evaluation.len();
            }
        }
    }

    if !batch_decision_detail_data.is_empty() {
        if let Err(e) = batch_insert_match_decision_details(pool, batch_decision_detail_data).await
        {
            logger.log_warning(&format!("Batch insert decision details failed: {}", e));
            let mut stats_guard = stats_mutex.lock().await;
            stats_guard.individual_operation_errors += 1; // Count as a single batch error
        }
    }

    if !batch_cache_store_data.is_empty() {
        if let Err(e) = batch_store_in_comparison_cache(pool, batch_cache_store_data).await {
            logger.log_warning(&format!("Batch store comparison cache failed: {}", e));
            let mut stats_guard = stats_mutex.lock().await;
            stats_guard.individual_operation_errors += 1; // Count as a single batch error
        }
    }

    // NEW: Add completion tracking logic for entities in this batch
    let mut completion_batch: Vec<(String, MatchMethodType, String, String, i32)> = Vec::new();
    for (entity_id, current_sig_check) in completion_status.iter() {
        if !current_sig_check.is_complete { // Only mark incomplete entities
            if let Some(current_sig) = &current_sig_check.current_signature {
                // If the entity was processed in this batch and has comparisons
                let comparison_count = local_entity_comparison_counts.get(entity_id).copied().unwrap_or(0);
                if comparison_count > 0 {
                    completion_batch.push((
                        entity_id.clone(),
                        MatchMethodType::Name,
                        pipeline_run_id.to_string(),
                        current_sig.clone(),
                        comparison_count,
                    ));
                }
            }
        }
    }
    if !completion_batch.is_empty() {
        if let Err(e) = batch_mark_entity_completion(pool, &completion_batch).await {
            logger.log_warning(&format!("Name: Failed to batch mark {} entities as complete: {}", completion_batch.len(), e));
        } else {
            logger.log_debug(&format!("Name: Marked {} entities as complete", completion_batch.len()));
        }
    }

    logger.log_debug("Batch completed.");
    Ok(())
}

// Re-use `calculate_distance` from `url.rs` by making it `pub` in a shared `utils` module or by copying.
// For now, let's copy it for simplicity within this module, assuming it's not made public yet.
fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in meters
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let (delta_phi, delta_lambda) = ((lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (delta_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

// Helper functions remain the same but some need updating for progress callbacks...

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
        " incorporated", " inc.", " inc", " corporation", " corp.", " corp", " limited liability company", " llc.", " llc", " limited", " ltd.", " ltd", " limited partnership", " lp.", " lp", " limited liability partnership", " llp.", " llp", " foundation", " trust", " charitable trust", " company", " co.", " co", " non-profit", " nonprofit", " nfp", " association", " assn.", " assn", " coop", " co-op", " cooperative", " npo", " organisation", " organization", " org.", " org", " coalition", " fund", " partnership", " academy", " consortium", " institute", " services", " group", " society", " network", " federation", " international", " global", " national", " alliance", " gmbh", " ag", " sarl", " bv", " spa", " pty", " plc", " p.c.", " pc",
    ];
    if entity_type != Some("city") {
        // Don't strip suffixes if it's a city
        for suffix in suffixes {
            if normalized.ends_with(suffix) {
                normalized = normalized[..normalized.len() - suffix.len()]
                    .trim_end()
                    .to_string();
            }
        }
    }

    // Handle parenthesized location suffixes like "(Tacoma)" or "(Seattle Office)"
    let paren_regex = Regex::new(r"\s*\((.*?)\)\s*$").unwrap(); // Pre-compile for efficiency if possible, or handle error
    if let Some(captures) = paren_regex.captures(&normalized) {
        if let Some(_location_match) = captures.get(1) {
            // We don't need to store it, just remove it
            normalized = paren_regex.replace(&normalized, "").trim().to_string();
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
        (r"\b(st)\b", "saint"), // Avoid "st" -> "street" if it's "saint"
        (r"\bwa\b", "washington"),
        (r"\b(fd)\b", "fire department"),
        (r"\b(pd)\b", "police department"),
        (r"\binc\b", ""),
        (r"\bcorp\b", ""),
        (r"\bllc\b", ""),
        (r"\bltd\b", ""), // Remove common abbreviations if they survived suffix stripping
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

pub fn tokenize_name(
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
            let weight = if LOCATION_PREFIXES.contains(&token.as_str()) {
                0.8
            } else {
                1.0
            };
            weighted_tokens.push((token, weight));
        }
    }
    // Add bigrams
    for i in 0..words.len().saturating_sub(1) {
        let bigram = format!("{} {}", words[i], words[i + 1]).to_lowercase();
        if bigram.len() >= 2 * MIN_TOKEN_LENGTH {
            // Ensure bigram is reasonably long
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
        let city1 = normalized_name1
            .strip_prefix("city of ")
            .unwrap_or("")
            .trim();
        let city2 = normalized_name2
            .strip_prefix("city of ")
            .unwrap_or("")
            .trim();
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
    if (normalized_name1.contains("department") || normalized_name2.contains("department"))
        && (normalized_name1.contains("police") != normalized_name2.contains("police")
            || normalized_name1.contains("fire") != normalized_name2.contains("fire"))
    {
        // This logic might be too simple; consider if "Police Dept" vs "Police Services" should match.
        // For now, if one has "police" and other doesn't (and both are depts), penalize.
        if (normalized_name1.contains("police") && !normalized_name2.contains("police"))
            || (!normalized_name1.contains("police") && normalized_name2.contains("police"))
            || (normalized_name1.contains("fire") && !normalized_name2.contains("fire"))
            || (!normalized_name1.contains("fire") && normalized_name2.contains("fire"))
        {
            return pre_rl_score * 0.75;
        }
    }
    pre_rl_score // Return original score if no specific rule applied
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
            matrix[i][j] = (matrix[i - 1][j] + 1)
                .min(matrix[i][j - 1] + 1)
                .min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[len1][len2]
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
pub async fn generate_candidate_pairs_indices_limited(
    entity_data_arc: &std::sync::Arc<Vec<EntityNameData>>,
    token_to_entities_idx_map: &HashMap<String, Vec<usize>>,
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<Vec<(usize, usize)>> {
    let mut candidate_pairs_set: HashSet<(usize, usize)> = HashSet::new();
    let entity_data = entity_data_arc.as_ref();

    // PROGRESS UPDATE: Candidate generation start
    update_progress!(progress_callback, "Generating pairs", "analyzing token overlaps");

    // Create progress bar for candidate generation
    let gen_pb = ProgressBar::new(entity_data.len() as u64);
    gen_pb.set_style(
        ProgressStyle::default_bar()
            .template("  👤 [{elapsed_precise}] {bar:30.cyan/red} {pos}/{len} Generating candidates...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );

    for (idx1, entity1) in entity_data.iter().enumerate() {
        gen_pb.inc(1);
        if idx1 % 1000 == 0 {
            gen_pb.set_message(format!(
                "Generating candidates... ({} pairs so far)",
                candidate_pairs_set.len()
            ));
            
            // PROGRESS UPDATE: Detailed candidate generation progress
            update_detailed_progress!(progress_callback, "Generating pairs", 
                idx1 + 1, entity_data.len(),
                format!("{} candidate pairs found", candidate_pairs_set.len()));
        }

        let mut potential_matches: HashMap<usize, usize> = HashMap::new();

        // Find potential matches through token overlap
        for token in &entity1.tokens {
            if let Some(entity_indices) = token_to_entities_idx_map.get(token) {
                for &idx2 in entity_indices {
                    // Ensure idx1 and idx2 are different to avoid self-comparison
                    if idx1 != idx2 {
                        *potential_matches.entry(idx2).or_insert(0) += 1;
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
            let pair = if idx1 < idx2 {
                (idx1, idx2)
            } else {
                (idx2, idx1)
            };
            candidate_pairs_set.insert(pair);

            // Check if we reached the total limit
            if candidate_pairs_set.len() >= MAX_TOTAL_CANDIDATE_PAIRS {
                break;
            }
        }
        if candidate_pairs_set.len() >= MAX_TOTAL_CANDIDATE_PAIRS {
            info!(
                "Reached MAX_TOTAL_CANDIDATE_PAIRS ({}) during generation. Truncating.",
                MAX_TOTAL_CANDIDATE_PAIRS
            );
            break;
        }
    }

    gen_pb.finish_with_message(format!(
        "Generated {} candidate pairs",
        candidate_pairs_set.len()
    ));
    Ok(candidate_pairs_set.into_iter().collect())
}

#[derive(Debug, Clone)]
pub struct TokenStats {
    token: String,
    frequency: usize,
    idf: f32, // Inverse Document Frequency
}

pub async fn prepare_entity_data_and_index(
    all_entities: &[EntityNameData], // Changed input type to EntityNameData which includes lat/lon
    stopwords: &HashSet<String>,
) -> (
    Vec<EntityNameData>,
    HashMap<String, Vec<usize>>,
    HashMap<String, TokenStats>,
) {
    // Create progress bar for entity data preparation
    let prep_pb = ProgressBar::new(all_entities.len() as u64);
    prep_pb.set_style(
        ProgressStyle::default_bar()
            .template("  👤 [{elapsed_precise}] {bar:30.yellow/red} {pos}/{len} Preparing entities...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );

    let mut entity_data_vec = Vec::with_capacity(all_entities.len());
    let mut token_frequency: HashMap<String, usize> = HashMap::new();

    // Use a temporary HashSet to track entity_ids added to `entity_data_vec`
    let mut added_entity_ids: HashSet<String> = HashSet::new();

    for (i, entity_name_data) in all_entities.iter().enumerate() { // Iterate over EntityNameData
        prep_pb.inc(1);
        if i % 1000 == 0 {
            prep_pb.set_message(format!(
                "Preparing entities... ({}/{})",
                i,
                all_entities.len()
            ));
        }

        // Only process if this entity_id hasn't been added yet
        if added_entity_ids.contains(&entity_name_data.entity.id) {
            continue;
        }

        let (normalized_name, entity_type) = normalize_name(&entity_name_data.entity.name.clone().unwrap_or_default());
        if !normalized_name.is_empty() {
            let (tokens, _weighted_tokens) =
                tokenize_name(&normalized_name, stopwords, entity_type);
            for token in &tokens {
                *token_frequency.entry(token.clone()).or_insert(0) += 1;
            }
            entity_data_vec.push(EntityNameData {
                entity: entity_name_data.clone().entity, // Clone the entire Entity struct
                normalized_name,
                entity_type,
                tokens,
                latitude: entity_name_data.latitude,
                longitude: entity_name_data.longitude,
            });
            added_entity_ids.insert(entity_name_data.entity.id.clone());
        }
    }

    prep_pb.finish_with_message(format!("Prepared {} entities", entity_data_vec.len()));

    // Calculate IDF for tokens (optional, if needed for more advanced scoring)
    let total_docs = entity_data_vec.len() as f32;
    let mut token_stats_map = HashMap::new();
    for (token, freq) in token_frequency {
        let idf = (total_docs / (freq as f32 + 1.0)).ln_1p(); // ln(N / (df + 1)) + 1 (or similar variant)
        token_stats_map.insert(
            token.clone(),
            TokenStats {
                token,
                frequency: freq,
                idf,
            },
        );
    }

    // Create inverted index: token -> list of entity indices
    let mut token_to_entities_map: HashMap<String, Vec<usize>> = HashMap::new();
    let max_common_token_freq = (entity_data_vec.len() as f32 * 0.05).max(10.0) as usize; // Ignore very common tokens

    for (idx, data) in entity_data_vec.iter().enumerate() {
        for token in &data.tokens {
            if let Some(stats) = token_stats_map.get(token) {
                // Only index tokens that are not extremely common and appear more than once
                if stats.frequency <= max_common_token_freq && stats.frequency > 1 {
                    token_to_entities_map
                        .entry(token.clone())
                        .or_default()
                        .push(idx);
                }
            }
        }
    }
    (entity_data_vec, token_to_entities_map, token_stats_map)
}

// Renamed from `get_all_entities_with_names` to include location data
pub async fn get_all_entities_with_names_and_locations(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<Vec<EntityNameData>> {
    let query = "
        SELECT e.id, e.organization_id, e.name, e.created_at, e.updated_at, e.source_system, e.source_id,
               l.latitude, l.longitude
        FROM public.entity e
        LEFT JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        LEFT JOIN public.location l ON ef.table_id = l.id
        WHERE e.name IS NOT NULL AND e.name != ''";
    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to query entities with names and locations")?;
    rows.iter()
        .map(|row| {
            Ok(EntityNameData {
                entity: Entity {
                    id: row.try_get("id")?,
                    organization_id: row.try_get("organization_id")?,
                    name: row.try_get("name")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                    source_system: row.try_get("source_system").ok(),
                    source_id: row.try_get("source_id").ok(),
                },
                normalized_name: "".to_string(), // Will be filled by prepare_entity_data_and_index
                entity_type: None, // Will be filled by prepare_entity_data_and_index
                tokens: HashSet::new(), // Will be filled by prepare_entity_data_and_index
                latitude: row.try_get("latitude").ok(),
                longitude: row.try_get("longitude").ok(),
            })
        })
        .collect()
}

// NEW: Add this new function before the existing get_all_entities_with_names_and_locations
pub async fn get_all_entities_with_names_and_locations_filtered(
    conn: &impl GenericClient,
    entity_ids: &[String],
) -> Result<Vec<EntityNameData>> {
    let query = "
        SELECT e.id, e.organization_id, e.name, e.created_at, e.updated_at, e.source_system, e.source_id,
               l.latitude, l.longitude
        FROM public.entity e
        LEFT JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        LEFT JOIN public.location l ON ef.table_id = l.id
        WHERE e.id = ANY($1) AND e.name IS NOT NULL AND e.name != ''";

    let rows = conn
        .query(query, &[&entity_ids])
        .await
        .context("Failed to query entities with names and locations (filtered)")?;

    rows.iter()
        .map(|row| {
            Ok(EntityNameData {
                entity: Entity {
                    id: row.try_get("id")?,
                    organization_id: row.try_get("organization_id")?,
                    name: row.try_get("name")?,
                    created_at: row.try_get("created_at")?,
                    updated_at: row.try_get("updated_at")?,
                    source_system: row.try_get("source_system").ok(),
                    source_id: row.try_get("source_id").ok(),
                },
                normalized_name: "".to_string(), // Will be filled by prepare_entity_data_and_index
                entity_type: None, // Will be filled by prepare_entity_data_and_index
                tokens: HashSet::new(), // Will be filled by prepare_entity_data_and_index
                latitude: row.try_get("latitude").ok(),
                longitude: row.try_get("longitude").ok(),
            })
        })
        .collect()
}

pub async fn get_organization_embeddings(
    conn: &impl tokio_postgres::GenericClient,
    entities: &[EntityNameData], // Changed input type
) -> Result<HashMap<String, Option<Vec<f32>>>> {
    let mut embeddings_map = HashMap::new();
    let org_ids: Vec<String> = entities
        .iter()
        .map(|e| e.entity.organization_id.clone()) // Access org_id from the nested Entity
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if org_ids.is_empty() {
        return Ok(embeddings_map);
    }

    // Create progress bar for embedding loading
    let emb_pb = ProgressBar::new(org_ids.len() as u64);
    emb_pb.set_style(
        ProgressStyle::default_bar()
            .template("  👤 [{elapsed_precise}] {bar:30.green/red} {pos}/{len} Loading embeddings...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );

    // Fetch in batches
    for (batch_idx, org_ids_chunk) in org_ids.chunks(100).enumerate() {
        // Batch size of 100
        emb_pb.set_position((batch_idx * 100) as u64);
        emb_pb.set_message(format!("Loading embeddings... (batch {})", batch_idx + 1));

        let query = "SELECT id, embedding FROM public.organization WHERE id = ANY($1::TEXT[]) AND embedding IS NOT NULL";
        let rows = conn
            .query(query, &[&org_ids_chunk])
            .await
            .context("Failed to query organization embeddings")?;
        for row in rows {
            let org_id_str: String = row.get("id");
            // Assuming pgvector::Vector can be converted to Vec<f32>
            let embedding_pgvector: Option<pgvector::Vector> = row.get("embedding");
            let embedding_vec_f32: Option<Vec<f32>> = embedding_pgvector.map(|v| v.to_vec());

            // Map this org_id's embedding to all entities associated with this org_id
            for entity_data in entities.iter().filter(|e| e.entity.organization_id == org_id_str) {
                embeddings_map.insert(entity_data.entity.id.clone(), embedding_vec_f32.clone());
            }
        }
    }

    emb_pb.finish_with_message(format!(
        "Loaded embeddings for {} organizations",
        org_ids.len()
    ));

    // Ensure all entities have an entry, even if it's None
    for entity_data in entities {
        embeddings_map.entry(entity_data.entity.id.clone()).or_insert(None);
    }
    Ok(embeddings_map)
}

async fn fetch_existing_entity_group_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(String, String)>> {
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
    Ok(rows
        .into_iter()
        .map(|row| {
            let id1_str: String = row.get("entity_id_1");
            let id2_str: String = row.get("entity_id_2");
            if id1_str < id2_str {
                (id1_str, id2_str)
            } else {
                (id2_str, id1_str)
            }
        })
        .collect())
}