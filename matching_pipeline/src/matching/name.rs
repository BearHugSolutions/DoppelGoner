// src/matching/name.rs - Refactored for hashing, caching, and single-transaction pattern
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

use crate::models::{
    ActionType, Entity, EntityGroupId, EntityId, MatchMethodType, MatchValues, NameMatchValue,
    NewSuggestedAction, OrganizationId, SuggestionStatus,
};
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
    // EntitySignatureData and CachedComparisonResult are implicitly used via the utils
};

// Updated configuration for stricter name matching
const MIN_FUZZY_SIMILARITY_THRESHOLD: f32 = 0.92;
const MIN_SEMANTIC_SIMILARITY_THRESHOLD: f32 = 0.94;
const COMBINED_SIMILARITY_THRESHOLD: f32 = 0.93;
const FUZZY_WEIGHT: f32 = 0.3;
const SEMANTIC_WEIGHT: f32 = 0.7;
// const INTERNAL_WORKERS_NAME_STRATEGY: usize = 2; // Not directly used with current semaphore approach

/// Number of discriminative tokens to use per entity
const TOP_TOKENS_PER_ENTITY: usize = 10;
/// Minimum token overlap required for candidate pair consideration
const MIN_TOKEN_OVERLAP: usize = 2;
// const MAX_CANDIDATES_PER_ENTITY: usize = 500; // Not directly used in current candidate generation
pub const MIN_TOKEN_LENGTH: usize = 2; // Ignore tokens shorter than this length

// Organizational types that should never match even with high similarity
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

// Known prefix noise patterns to clean
const NOISE_PREFIXES: [&str; 2] = ["zzz - ", "re-add "];

// Common prefixes that need special handling
const LOCATION_PREFIXES: [&str; 8] = [
    "north", "south", "east", "west", "sound", "valley", "pacific", "olympic",
];

// Expanded stopwords with more domain-specific irrelevant words
pub const STOPWORDS: [&str; 131] = [
    // Common English articles, conjunctions, and prepositions
    "a", "an", "the", "and", "or", "but", "nor", "for", "yet", "so", "in", "on", "at", "by",
    "to", "with", "from", "of", "as", "into", "about", "before", "after", "during", "until",
    "since", "unless",
    // Common business terms with little discriminative value
    "inc", "incorporated", "corp", "corporation", "llc", "ltd", "limited", "company", "co",
    "group", "holdings", "enterprises", "international", "global", "worldwide", "national",
    "american", "usa", "us", "service", "services", "solutions", "systems", "associates",
    "partners", "partnership",
    // Generic organizational terms
    "organization", "organisation", "foundation", "institute", "association", "society",
    "council", "committee", "center", "centre", "department", "division", "unit", "office",
    "bureau", "agency", "authority", "board",
    // Common descriptive terms
    "new", "old", "great", "greater", "best", "better", "first", "second", "third", "primary",
    "main", "central", "local", "regional", "official",
    // Additional stopwords
    "this", "that", "these", "those", "it", "they", "them", "their", "our", "your", "all",
    "any", "each", "every", "some", "such", "no", "not", "only", "very",
    // Specific to your dataset
    "program", "community", "resource", "resources", "support", "help", "health", "care",
    "management", "professional", "public", "private", "general", "federal", "state", "county",
    "regional", "district", "area", "branch", // "program" is repeated, kept one
    "provider", "member", "directory", "guide", "network",
];

// SQL query for inserting into entity_group
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// Enhanced data structure to hold entity information with organization type detection
#[derive(Clone)] // Added Clone derive
struct EntityNameData {
    entity: Entity,
    normalized_name: String,
    entity_type: Option<&'static str>,
    tokens: HashSet<String>,             // Tokens for this entity's name
    weighted_tokens: Vec<(String, f32)>, // Tokens with their context-based weights
}

/// Main function to find name-based matches
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 high-performance name matching (run ID: {}){} with INCREMENTAL CHECKS...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    let mut initial_conn = pool
        .get()
        .await
        .context("Name: Failed to get DB connection for initial reads")?;

    let all_entities_with_names_vec = get_all_entities_with_names(&*initial_conn).await?;
    let total_entities = all_entities_with_names_vec.len();
    info!(
        "Name: Found {} entities with non-empty names.",
        total_entities
    );

    if total_entities < 2 {
        let name_result = NameMatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Name,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
            },
        };
        return Ok(AnyMatchResult::Name(name_result));
    }

    // Fetch existing entity_group pairs to avoid re-creating them
    // This is a broader check than the comparison cache.
    let existing_entity_group_pairs_set =
        fetch_existing_entity_group_pairs(&*initial_conn, MatchMethodType::Name).await?;
    info!(
        "Name: Found {} existing name-matched entity_group pairs to skip direct re-creation.",
        existing_entity_group_pairs_set.len()
    );

    let org_embeddings_map =
        get_organization_embeddings(&*initial_conn, &all_entities_with_names_vec).await?;
    drop(initial_conn);

    let stopwords: HashSet<String> = STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let (entity_data, token_to_entities, _token_stats) =
        prepare_entity_data_and_index(&all_entities_with_names_vec, &stopwords).await;

    let candidate_pairs_indices = generate_candidate_pairs_indices(&entity_data, &token_to_entities).await;
    info!(
        "Name: Generated {} candidate pairs (by index) for comparison.",
        candidate_pairs_indices.len()
    );

    // Shared set to track pairs processed in this run to avoid redundant work after a cache miss.
    let processed_pairs_this_run_arc = Arc::new(Mutex::new(HashSet::<(EntityId, EntityId)>::new()));

    // Stats tracking: new_pairs, entities_set, conf_scores, errors, cache_hits
    let stats_mutex = Arc::new(Mutex::new((
        0,              // new_pairs_created_count
        HashSet::new(), // entities_in_new_pairs
        Vec::new(),     // confidence_scores_for_stats
        0,              // individual_operation_errors
        0,              // cache_hits_count
        0,              // feature_extraction_count
        0,              // feature_extraction_failures
    )));


    let semaphore = Arc::new(tokio::sync::Semaphore::new(15)); // Limit concurrent DB operations
    let candidate_pairs_len = candidate_pairs_indices.len();
    let mut tasks = Vec::new();

    for (i, j) in candidate_pairs_indices {
        let entity1_data = entity_data[i].clone(); // Clone for async task
        let entity2_data = entity_data[j].clone(); // Clone for async task
        
        let pool_clone = pool.clone();
        let ro_option_clone = reinforcement_orchestrator_option.clone();
        let run_id_clone = pipeline_run_id.to_string();
        let feature_cache_clone = feature_cache.clone();
        let stats_arc_clone = stats_mutex.clone();
        let processed_pairs_arc_clone = processed_pairs_this_run_arc.clone();
        let existing_entity_group_pairs_clone = existing_entity_group_pairs_set.clone();
        let org_embeddings_map_clone = org_embeddings_map.clone();
        let semaphore_clone = semaphore.clone();

        tasks.push(tokio::spawn(async move {
            let permit = match semaphore_clone.acquire().await {
                Ok(permit) => permit,
                Err(e) => {
                    warn!("Name: Failed to acquire semaphore permit: {}", e);
                    return; // Skip this pair if semaphore fails
                }
            };

            let e1_id = &entity1_data.entity.id;
            let e2_id = &entity2_data.entity.id;

            let (ordered_e1_id, ordered_e2_id) = if e1_id.0 < e2_id.0 {
                (e1_id.clone(), e2_id.clone())
            } else {
                (e2_id.clone(), e1_id.clone())
            };
            let current_pair_ordered = (ordered_e1_id.clone(), ordered_e2_id.clone());

            // 1. Check if entity_group already exists for this pair and method
            if existing_entity_group_pairs_clone.contains(&current_pair_ordered) {
                trace!("Name: Pair ({}, {}) already in entity_group. Skipping.", ordered_e1_id.0, ordered_e2_id.0);
                drop(permit);
                return;
            }
            
            // 2. Check if already processed in this run (e.g. after a cache miss)
            {
                let processed_set = processed_pairs_arc_clone.lock().await;
                if processed_set.contains(&current_pair_ordered) {
                    trace!("Name: Pair ({}, {}) already processed in this run. Skipping.", ordered_e1_id.0, ordered_e2_id.0);
                    drop(permit); // Release permit before returning
                    return;
                }
            } // Mutex guard dropped here


            // 3. --- INCREMENTAL PROCESSING LOGIC (Signatures & Cache) ---
            let current_signatures_opt = match get_current_signatures_for_pair(&pool_clone, &ordered_e1_id, &ordered_e2_id).await {
                Ok(sigs) => sigs,
                Err(e) => {
                    warn!("Name: Failed to get signatures for pair ({}, {}): {}. Proceeding without cache.", ordered_e1_id.0, ordered_e2_id.0, e);
                    None
                }
            };

            if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                match check_comparison_cache(&pool_clone, &ordered_e1_id, &ordered_e2_id, &sig1_data.signature, &sig2_data.signature, &MatchMethodType::Name).await {
                    Ok(Some(cached_eval)) => {
                        let mut stats_guard = stats_arc_clone.lock().await;
                        stats_guard.4 += 1; // cache_hits_count
                        drop(stats_guard);

                        debug!("Name: Cache HIT for pair ({}, {}). Result: {}, Score: {:?}", ordered_e1_id.0, ordered_e2_id.0, cached_eval.comparison_result, cached_eval.similarity_score);
                        
                        // Mark as processed to avoid re-processing
                        let mut processed_set = processed_pairs_arc_clone.lock().await;
                        processed_set.insert(current_pair_ordered.clone());
                        drop(processed_set);
                        drop(permit);
                        return; 
                    }
                    Ok(None) => {
                        debug!("Name: Cache MISS for pair ({}, {}). Signatures: ({}..., {}...). Proceeding with comparison.", ordered_e1_id.0, ordered_e2_id.0, &sig1_data.signature[..std::cmp::min(8, sig1_data.signature.len())], &sig2_data.signature[..std::cmp::min(8, sig2_data.signature.len())]);
                    }
                    Err(e) => {
                        warn!("Name: Error checking comparison cache for pair ({}, {}): {}. Proceeding with comparison.", ordered_e1_id.0, ordered_e2_id.0, e);
                    }
                }
            }
            // --- END INCREMENTAL PROCESSING LOGIC ---

            // If we reach here, it's a cache miss or signatures weren't available. Proceed with comparison.
            let original_name1 = entity1_data.entity.name.as_ref().cloned().unwrap_or_default();
            let original_name2 = entity2_data.entity.name.as_ref().cloned().unwrap_or_default();
            let normalized_name1 = entity1_data.normalized_name.clone();
            let normalized_name2 = entity2_data.normalized_name.clone();

            if normalized_name1.is_empty() || normalized_name2.is_empty() {
                drop(permit);
                return;
            }

            if let (Some(t1), Some(t2)) = (entity1_data.entity_type, entity2_data.entity_type) {
                if t1 != t2 && INCOMPATIBLE_ORG_TYPES.iter().any(|(it1, it2)| (t1 == *it1 && t2 == *it2) || (t1 == *it2 && t2 == *it1)) {
                    drop(permit);
                    return;
                }
            }

            let fuzzy_score = jaro_winkler(&normalized_name1, &normalized_name2) as f32;
            if fuzzy_score < (MIN_FUZZY_SIMILARITY_THRESHOLD * 0.9) { // Early exit if very low fuzzy
                // Store non-match in cache if signatures were available
                if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                    if let Err(e) = store_in_comparison_cache(
                        &pool_clone, &ordered_e1_id, &ordered_e2_id,
                        &sig1_data.signature, &sig2_data.signature,
                        &MatchMethodType::Name, &run_id_clone,
                        "NON_MATCH", Some(fuzzy_score as f64), // Store fuzzy score for context
                        None, // No features for early exit
                    ).await {
                        warn!("Name: Failed to store NON_MATCH (low fuzzy) in comparison_cache for ({}, {}): {}", ordered_e1_id.0, ordered_e2_id.0, e);
                    }
                }
                let mut processed_set = processed_pairs_arc_clone.lock().await;
                processed_set.insert(current_pair_ordered);
                drop(processed_set);
                drop(permit);
                return;
            }

            let embedding1_opt = org_embeddings_map_clone.get(e1_id).and_then(|opt_emb| opt_emb.as_ref());
            let embedding2_opt = org_embeddings_map_clone.get(e2_id).and_then(|opt_emb| opt_emb.as_ref());
            let semantic_score = match (embedding1_opt, embedding2_opt) {
                (Some(emb1), Some(emb2)) => match cosine_similarity_candle(emb1, emb2) {
                    Ok(sim) => sim as f32,
                    Err(e) => { warn!("Name: Cosine similarity failed for ({}, {}): {}. Defaulting to 0.0.", e1_id.0, e2_id.0, e); 0.0 }
                },
                _ => 0.0,
            };

            let (pre_rl_score, pre_rl_match_type) = if semantic_score >= MIN_SEMANTIC_SIMILARITY_THRESHOLD {
                ((fuzzy_score * FUZZY_WEIGHT) + (semantic_score * SEMANTIC_WEIGHT), "combined".to_string())
            } else if fuzzy_score >= MIN_FUZZY_SIMILARITY_THRESHOLD {
                (fuzzy_score, "fuzzy".to_string())
            } else {
                // Store non-match in cache if signatures were available
                if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                     if let Err(e) = store_in_comparison_cache(
                        &pool_clone, &ordered_e1_id, &ordered_e2_id,
                        &sig1_data.signature, &sig2_data.signature,
                        &MatchMethodType::Name, &run_id_clone,
                        "NON_MATCH", Some(fuzzy_score.max(semantic_score) as f64), // Store max of scores
                        None, 
                    ).await {
                        warn!("Name: Failed to store NON_MATCH (low combined) in comparison_cache for ({}, {}): {}", ordered_e1_id.0, ordered_e2_id.0, e);
                    }
                }
                let mut processed_set = processed_pairs_arc_clone.lock().await;
                processed_set.insert(current_pair_ordered);
                drop(processed_set);
                drop(permit);
                return;
            };

            let adjusted_pre_rl_score = apply_domain_rules(&normalized_name1, &normalized_name2, pre_rl_score, entity1_data.entity_type, entity2_data.entity_type);
            let comparison_outcome_for_cache = if adjusted_pre_rl_score >= COMBINED_SIMILARITY_THRESHOLD { "MATCH" } else { "NON_MATCH" };
            
            let mut final_confidence = adjusted_pre_rl_score as f64;
            let mut features_for_snapshot_vec: Option<Vec<f64>> = None;
            let mut features_json_for_cache: Option<serde_json::Value> = None;

            if let Some(orchestrator_arc) = ro_option_clone.as_ref() {
                let mut stats_g = stats_arc_clone.lock().await;
                match if let Some(cache) = feature_cache_clone.as_ref() {
                    let orchestrator_guard = orchestrator_arc.lock().await;
                    orchestrator_guard.get_pair_features(&pool_clone, e1_id, e2_id).await // Use original e1_id, e2_id for feature extraction if order matters there
                } else {
                    MatchingOrchestrator::extract_pair_context_features(&pool_clone, e1_id, e2_id).await
                } {
                    Ok(features) => {
                        stats_g.5 += 1; // feature_extraction_count
                        if !features.is_empty() {
                            features_for_snapshot_vec = Some(features.clone());
                            features_json_for_cache = serde_json::to_value(features.clone()).ok();
                            let orchestrator_guard = orchestrator_arc.lock().await;
                            match orchestrator_guard.get_tuned_confidence(&MatchMethodType::Name, adjusted_pre_rl_score as f64, &features) {
                                Ok(tuned_score) => final_confidence = tuned_score,
                                Err(e) => warn!("Name: Failed to get tuned confidence for pair ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e),
                            }
                        } else {
                            warn!("Name: Extracted features vector is empty for pair ({}, {}). Using pre-RL score.", e1_id.0, e2_id.0);
                        }
                    }
                    Err(e) => {
                        stats_g.6 += 1; // feature_extraction_failures
                        warn!("Name: Failed to extract features for pair ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e);
                    }
                }
            } // RL feature extraction and tuning done

            // Store result in comparison_cache if signatures were available
            if let Some((sig1_data, sig2_data)) = &current_signatures_opt {
                if let Err(e) = store_in_comparison_cache(
                    &pool_clone, &ordered_e1_id, &ordered_e2_id,
                    &sig1_data.signature, &sig2_data.signature,
                    &MatchMethodType::Name, &run_id_clone,
                    comparison_outcome_for_cache, Some(final_confidence),
                    features_json_for_cache.as_ref(),
                ).await {
                    warn!("Name: Failed to store {} in comparison_cache for ({}, {}): {}", comparison_outcome_for_cache, ordered_e1_id.0, ordered_e2_id.0, e);
                }
            }
            
            // Mark as processed in this run
            {
                let mut processed_set = processed_pairs_arc_clone.lock().await;
                processed_set.insert(current_pair_ordered.clone());
            }


            if comparison_outcome_for_cache == "MATCH" {
                 let match_values = MatchValues::Name(NameMatchValue {
                    original_name1: original_name1.clone(),
                    original_name2: original_name2.clone(),
                    normalized_name1: normalized_name1.clone(),
                    normalized_name2: normalized_name2.clone(),
                    pre_rl_match_type: Some(pre_rl_match_type.clone()),
                });

                match create_entity_group( // This function uses ordered_e1_id, ordered_e2_id internally for the DB insert
                    &pool_clone,
                    e1_id, // Pass original IDs, create_entity_group will order them
                    e2_id,
                    &match_values,
                    final_confidence,
                    adjusted_pre_rl_score as f64,
                    ro_option_clone.as_ref(),
                    &run_id_clone,
                    features_for_snapshot_vec, // Pass the Vec<f64> directly
                    feature_cache_clone, // Pass the feature cache for RL within create_entity_group if needed (though features already extracted)
                ).await {
                    Ok(created) => {
                        if created {
                            let mut stats_guard = stats_arc_clone.lock().await;
                            stats_guard.0 += 1; // new_pairs_created_count
                            stats_guard.1.insert(e1_id.clone());
                            stats_guard.1.insert(e2_id.clone());
                            stats_guard.2.push(final_confidence);
                        }
                    }
                    Err(e) => {
                        let mut stats_guard = stats_arc_clone.lock().await;
                        stats_guard.3 += 1; // individual_operation_errors
                        warn!("Name: Failed to process pair ({}, {}): {}", e1_id.0, e2_id.0, e);
                    }
                }
            }
            drop(permit); // Release semaphore permit
        }));
    }

    for task in tasks {
        if let Err(e) = task.await {
            warn!("Name: Task join error: {}", e);
             let mut stats_guard = stats_mutex.lock().await;
             stats_guard.3 += 1; // individual_operation_errors for task failure
        }
    }
    
    let (
        new_pairs_created_count,
        entities_in_new_pairs,
        confidence_scores_for_stats,
        individual_operation_errors,
        cache_hits_count,
        feature_extraction_count,
        feature_extraction_failures,
    ) = {
        let stats_guard = stats_mutex.lock().await;
        (
            stats_guard.0,
            stats_guard.1.clone(),
            stats_guard.2.clone(),
            stats_guard.3,
            stats_guard.4,
            stats_guard.5,
            stats_guard.6,
        )
    };


    let avg_confidence = if !confidence_scores_for_stats.is_empty() {
        confidence_scores_for_stats.iter().sum::<f64>() / confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Name,
        groups_created: new_pairs_created_count,
        entities_matched: entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if new_pairs_created_count > 0 { 2.0 } else { 0.0 },
    };

    info!(
        "Name matching complete in {:.2?}: {} candidate pairs processed. Cache hits: {}. Created {} new pairs ({} errors), involving {} unique entities.",
        start_time.elapsed(),
        candidate_pairs_len, // Total candidates considered
        cache_hits_count,
        method_stats.groups_created,
        individual_operation_errors,
        method_stats.entities_matched
    );
    info!(
        "Name feature extraction stats: {} successful, {} failed.",
        feature_extraction_count, feature_extraction_failures
    );


    Ok(AnyMatchResult::Name(NameMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Create entity group record, decision logging, and suggestions.
/// Takes original entity_id_1, entity_id_2 and orders them internally.
/// `features_for_snapshot` is now passed directly as Option<Vec<f64>>.
async fn create_entity_group(
    pool: &PgPool,
    original_entity_id_1: &EntityId, // Renamed for clarity
    original_entity_id_2: &EntityId, // Renamed for clarity
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    features_for_snapshot: Option<Vec<f64>>, // Changed parameter type
    _feature_cache: Option<SharedFeatureCache>, // Keep for signature consistency, though features might be pre-extracted
) -> Result<bool> {
    let mut conn = pool
        .get()
        .await
        .context("Name: Failed to get DB connection for entity group creation")?;
    let tx = conn
        .transaction()
        .await
        .context("Name: Failed to start transaction for entity group creation")?;

    let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());

    // Ensure entity_id_1 < entity_id_2 for the DB insert
    let (ordered_id_1, ordered_id_2, ordered_match_values) = 
        if original_entity_id_1.0 <= original_entity_id_2.0 {
            (original_entity_id_1, original_entity_id_2, match_values.clone())
        } else {
            // Swap IDs and adjust MatchValues if it's NameMatchValue
            let new_mv = if let MatchValues::Name(name_match) = match_values {
                MatchValues::Name(NameMatchValue {
                    original_name1: name_match.original_name2.clone(),
                    original_name2: name_match.original_name1.clone(),
                    normalized_name1: name_match.normalized_name2.clone(),
                    normalized_name2: name_match.normalized_name1.clone(),
                    pre_rl_match_type: name_match.pre_rl_match_type.clone(),
                })
            } else {
                match_values.clone()
            };
            (original_entity_id_2, original_entity_id_1, new_mv)
        };

    let match_values_json = serde_json::to_value(&ordered_match_values)
        .context("Name: Failed to serialize match values")?;

    // Extract name-specific data from ordered_match_values for suggestion details
    let (sugg_orig_name1, sugg_orig_name2, sugg_norm_name1, sugg_norm_name2, sugg_pre_rl_match_type) =
        if let MatchValues::Name(n) = &ordered_match_values {
            (
                n.original_name1.clone(),
                n.original_name2.clone(),
                n.normalized_name1.clone(),
                n.normalized_name2.clone(),
                n.pre_rl_match_type.clone().unwrap_or_default(),
            )
        } else {
            (String::new(), String::new(), String::new(), String::new(), String::new())
        };
    
    // Note: RL tuning and feature extraction for `final_confidence_score` and `features_for_snapshot`
    // are now expected to happen *before* calling `create_entity_group`.
    // `final_confidence_score` is passed in directly.

    let rows_affected = tx
        .execute(
            INSERT_ENTITY_GROUP_SQL,
            &[
                &new_entity_group_id.0,
                &ordered_id_1.0,
                &ordered_id_2.0,
                &MatchMethodType::Name.as_str(),
                &match_values_json,
                &final_confidence_score, // Use the passed-in final_confidence_score
                &pre_rl_confidence_score,
            ],
        )
        .await?;

    if rows_affected > 0 {
        if final_confidence_score < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
            let priority = if final_confidence_score < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD { 2 } else { 1 };
            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Name.as_str(),
                "original_name1": sugg_orig_name1, // Use extracted suggestion names
                "original_name2": sugg_orig_name2,
                "normalized_name1": sugg_norm_name1,
                "normalized_name2": sugg_norm_name2,
                "pre_rl_score": pre_rl_confidence_score,
                "pre_rl_match_type": sugg_pre_rl_match_type,
                "entity_group_id": &new_entity_group_id.0,
            });
            let reason_message = format!(
                "Pair ({}, {}) matched by Name with low tuned confidence ({:.4}). Pre-RL: {:.2} ({}).",
                ordered_id_1.0, ordered_id_2.0, final_confidence_score, pre_rl_confidence_score, sugg_pre_rl_match_type
            );
            const INSERT_SUGGESTION_SQL: &str = "
                INSERT INTO clustering_metadata.suggested_actions (
                    pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
                    triggering_confidence, details, reason_code, reason_message, priority, status,
                    reviewer_id, reviewed_at, review_notes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                RETURNING id";
            if let Err(e) = tx.query_one(INSERT_SUGGESTION_SQL, &[
                &Some(pipeline_run_id.to_string()), &ActionType::ReviewEntityInGroup.as_str(),
                &None::<String>, &Some(new_entity_group_id.0.clone()), &None::<String>, &None::<String>,
                &Some(final_confidence_score), &Some(details_json),
                &Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()), &Some(reason_message),
                &(priority as i32), &SuggestionStatus::PendingReview.as_str(),
                &None::<String>, &None::<NaiveDateTime>, &None::<String>,
            ]).await { warn!("Name: Failed to create suggestion: {}", e); }
        }

        if let (Some(ro_arc), Some(features_vec)) = (reinforcement_orchestrator, features_for_snapshot.as_ref()) {
            let orchestrator_guard = ro_arc.lock().await;
            let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;
            let snapshot_features_json = serde_json::to_value(features_vec).unwrap_or(serde_json::Value::Null);
            const INSERT_DECISION_SQL: &str = "
                INSERT INTO clustering_metadata.match_decision_details (
                    entity_group_id, pipeline_run_id, snapshotted_features,
                    method_type_at_decision, pre_rl_confidence_at_decision,
                    tuned_confidence_at_decision, confidence_tuner_version_at_decision
                ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id";
            if let Err(e) = tx.query_one(INSERT_DECISION_SQL, &[
                &new_entity_group_id.0, &pipeline_run_id, &snapshot_features_json,
                &MatchMethodType::Name.as_str(), &pre_rl_confidence_score,
                &final_confidence_score, &(confidence_tuner_ver as i32),
            ]).await { warn!("Name: Failed to log decision snapshot: {}", e); }
        }
        tx.commit().await?;
        Ok(true)
    } else {
        tx.commit().await?; // Commit even if no rows affected to release connection
        debug!("Name: No rows affected when inserting entity group for ({}, {}), pair may already exist or conflict occurred.", ordered_id_1.0, ordered_id_2.0);
        Ok(false)
    }
}

/// Fetches all entities that have a non-null and non-empty name.
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

/// Get organization embeddings for a set of entities.
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
        let rows = conn.query(query, &[&batch_org_ids_refs]).await
            .with_context(|| format!("Failed to query org embeddings for batch: {:?}", batch_org_ids_refs))?;
        for row in rows {
            let org_id_str: String = row.try_get("id").context("Failed to get 'id' for organization embedding row")?;
            let embedding_vec_opt: Option<Vec<f32>> = row.try_get("embedding").ok();
            if let Some(embedding_vec) = embedding_vec_opt {
                for entity in entities.iter().filter(|e| e.organization_id.0 == org_id_str) {
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

/// Helper to fetch existing entity_group pairs to avoid re-creating them.
/// This is distinct from the comparison_cache.
async fn fetch_existing_entity_group_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&method_type.as_str()])
        .await
        .with_context(|| format!("Failed to query existing {:?}-matched entity_group pairs", method_type))?;

    let mut existing_pairs = HashSet::new();
    for row in rows {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        // Ensure consistent order for the set
        if id1_str < id2_str {
            existing_pairs.insert((EntityId(id1_str), EntityId(id2_str)));
        } else {
            existing_pairs.insert((EntityId(id2_str), EntityId(id1_str)));
        }
    }
    Ok(existing_pairs)
}


/// Enhanced normalize_name function with entity type detection
pub fn normalize_name(name: &str) -> (String, Option<&'static str>) {
    let mut normalized = name.to_lowercase();
    for prefix in &NOISE_PREFIXES {
        if normalized.starts_with(prefix) {
            normalized = normalized[prefix.len()..].trim().to_string();
        }
    }
    let char_substitutions = [
        ("&", " and "), ("+", " plus "), ("/", " "), ("-", " "),
        (".", " "), ("'", ""), ("(", " "), (")", " "), (",", " "),
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
        " coop", " co-op", " cooperative", " npo", " organisation", " organization",
        " org.", " org", " coalition", " fund", " partnership", " academy", " consortium",
        " institute", " services", " group", " society", " network", " federation",
        " international", " global", " national", " alliance", " gmbh", " ag", " sarl",
        " bv", " spa", " pty", " plc", " p.c.", " pc",
    ];
    if entity_type != Some("city") {
        for suffix in suffixes {
            if normalized.ends_with(suffix) {
                normalized = normalized[..normalized.len() - suffix.len()].trim_end().to_string();
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
        (r"\b(ctr|cntr|cent|cen)\b", "center"), (r"\b(assoc|assn)\b", "association"),
        (r"\b(dept|dpt)\b", "department"), (r"\b(intl|int'l)\b", "international"),
        (r"\b(nat'l|natl)\b", "national"), (r"\b(comm|cmty)\b", "community"),
        (r"\b(srv|svcs|serv|svc)\b", "service"), (r"\b(univ)\b", "university"),
        (r"\b(coll)\b", "college"), (r"\b(inst)\b", "institute"),
        (r"\b(mfg)\b", "manufacturing"), (r"\b(tech)\b", "technology"),
        (r"\b(st)\b", "saint"), (r"\bwa\b", "washington"),
        (r"\b(fd)\b", "fire department"), (r"\b(pd)\b", "police department"),
        (r"\binc\b", ""), (r"\bcorp\b", ""), (r"\bllc\b", ""), (r"\bltd\b", ""),
    ];
    for (pattern, replacement) in &replacements {
        match Regex::new(pattern) {
            Ok(re) => normalized = re.replace_all(&normalized, *replacement).into_owned(),
            Err(e) => warn!("Invalid regex pattern: '{}'. Error: {}", pattern, e),
        }
    }
    normalized = normalized.chars().filter(|c| c.is_alphanumeric() || c.is_whitespace()).collect();
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if let Some(location) = location_suffix {
        if !location.is_empty() {
            normalized = format!("{} {}", normalized.trim(), location.trim());
        }
    }
    (normalized.trim().to_string(), entity_type)
}

/// Detect organization type based on name patterns
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
        if normalized_name.contains(indicator) { return Some(entity_type); }
    }
    None
}

/// Enhanced tokenization with context-aware weighting
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
        if bigram.len() >= 2 * MIN_TOKEN_LENGTH { // Ensure bigram is reasonably long
            tokens.insert(bigram.clone());
            weighted_tokens.push((bigram, 1.5)); // Bigrams get higher weight
        }
    }
    if entity_type == Some("city") && normalized_name.starts_with("city of ") {
        if let Some(city_name) = normalized_name.strip_prefix("city of ") {
            let city = city_name.trim().to_string();
            if !city.is_empty() {
                tokens.insert(city.clone());
                weighted_tokens.push((city, 3.0)); // Very high weight for city name
            }
        }
    }
    (tokens, weighted_tokens)
}

/// Calculate context-aware token weight
fn get_token_weight(token: &str, entity_type: Option<&str>) -> f32 {
    let common_words = [
        "the", "and", "of", "in", "for", "at", "with", "by", "on", "to",
        "service", "services", "center", "association", "organization",
    ];
    if common_words.contains(&token) { return 0.5; }
    if LOCATION_PREFIXES.contains(&token) { return 0.8; }
    let type_indicators = [
        "police", "fire", "hospital", "school", "college", "church", "bank", "library",
    ];
    if type_indicators.contains(&token) { return 2.0; }
    if let Some(etype) = entity_type {
        match etype {
            "city" => if token == "city" { 0.5 } else { 1.5 },
            "education" => if token == "school" || token == "college" { 1.5 } else { 1.2 },
            _ => 1.0,
        }
    } else { 1.0 }
}

/// Domain-specific rule application
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
        let city_name1 = normalized_name1.strip_prefix("city of ").unwrap_or(normalized_name1).trim();
        let city_name2 = normalized_name2.strip_prefix("city of ").unwrap_or(normalized_name2).trim();
        if city_name1 != city_name2 {
            let edit_distance = levenshtein_distance(city_name1, city_name2);
            let max_length = std::cmp::max(city_name1.len(), city_name2.len());
            if edit_distance > 3 || (max_length > 0 && edit_distance as f32 / max_length as f32 > 0.3) {
                return pre_rl_score * 0.6;
            } else { return pre_rl_score * 0.8; }
        }
    }
    if let (Some(t1), Some(t2)) = (entity_type1, entity_type2) {
        if t1 != t2 { return pre_rl_score * 0.85; }
    }
    if normalized_name1.contains("department") || normalized_name2.contains("department") {
        let dept_types = ["police", "fire", "health", "public", "revenue", "transportation"];
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
        if words1[i] == words2[i] { common_prefix_len += 1; } else { break; }
    }
    if common_prefix_len > 0 && common_prefix_len < words1.len() && common_prefix_len < words2.len() {
        let total_words = words1.len() + words2.len();
        let common_prefix_percentage = (2 * common_prefix_len) as f32 / total_words as f32;
        if common_prefix_percentage > 0.3 {
            let remaining_similarity = jaro_winkler(&words1[common_prefix_len..].join(" "), &words2[common_prefix_len..].join(" "));
            if remaining_similarity < 0.7 { return pre_rl_score * (0.9 - (common_prefix_percentage * 0.2)); }
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
     if let (Some(loc1_match), Some(loc2_match)) = (location_regex.find(normalized_name1), location_regex.find(normalized_name2)) {
        let loc1 = loc1_match.as_str();
        let loc2 = loc2_match.as_str();
        let name1_without_loc = &normalized_name1[0..loc1_match.start()].trim();
        let name2_without_loc = &normalized_name2[0..loc2_match.start()].trim();
        if name1_without_loc == name2_without_loc && loc1 != loc2 { return pre_rl_score * 0.7; }
    }
    pre_rl_score
}

/// Calculate Levenshtein edit distance
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let (len1, len2) = (s1_chars.len(), s2_chars.len());
    if len1 == 0 { return len2; } if len2 == 0 { return len1; }
    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];
    for i in 0..=len1 { matrix[i][0] = i; }
    for j in 0..=len2 { matrix[0][j] = j; }
    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
            matrix[i][j] = std::cmp::min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1).min(matrix[i - 1][j - 1] + cost);
        }
    }
    matrix[len1][len2]
}

/// Prepare entity data and create token inverted index.
async fn prepare_entity_data_and_index(
    all_entities: &[Entity],
    stopwords: &HashSet<String>,
) -> (
    Vec<EntityNameData>, // Stores processed entity data
    HashMap<String, Vec<usize>>, // Token -> List of entity indices
    HashMap<String, TokenStats>, // Token -> TokenStats (freq, idf)
) {
    let mut entity_data_vec = Vec::with_capacity(all_entities.len());
    let mut token_frequency: HashMap<String, usize> = HashMap::new();

    for entity in all_entities {
        if let Some(name) = &entity.name {
            let (normalized_name, entity_type) = normalize_name(name);
            if !normalized_name.is_empty() {
                let (tokens, weighted_tokens) = tokenize_name(&normalized_name, stopwords, entity_type);
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
        let idf = (total_docs / (freq as f32 + 1.0)).ln_1p().max(0.0); // ln_1p for stability, ensure non-negative
        token_stats_map.insert(token.clone(), TokenStats { token, frequency: freq, idf });
    }

    // Heuristic: ignore tokens present in more than 5% of entities or fewer than 2 entities, or very common tokens.
    let max_common_token_freq = (entity_data_vec.len() as f32 * 0.05).max(10.0) as usize;
    let mut token_to_entities_map: HashMap<String, Vec<usize>> = HashMap::new();

    for (idx, data) in entity_data_vec.iter().enumerate() {
        let mut scored_tokens: Vec<(String, f32)> = data.tokens.iter()
            .filter_map(|token| {
                token_stats_map.get(token).and_then(|stats| {
                    // Filter out very common tokens or tokens not meeting min length
                    if stats.frequency <= max_common_token_freq && stats.frequency > 1 && token.len() >= MIN_TOKEN_LENGTH {
                        Some((token.clone(), stats.idf)) // Use IDF as score for selecting top tokens
                    } else { None }
                })
            })
            .collect();
        // Sort by IDF (higher IDF means more discriminative)
        scored_tokens.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        for (token, _) in scored_tokens.into_iter().take(TOP_TOKENS_PER_ENTITY) {
            token_to_entities_map.entry(token).or_default().push(idx);
        }
    }
    (entity_data_vec, token_to_entities_map, token_stats_map)
}

/// Generate candidate pairs based on token overlap from the inverted index.
/// Returns pairs of indices into the `entity_data` vector.
async fn generate_candidate_pairs_indices(
    entity_data: &[EntityNameData],
    token_to_entities: &HashMap<String, Vec<usize>>,
) -> Vec<(usize, usize)> {
    let mut candidate_pairs_set = HashSet::new(); // Stores (idx1, idx2) where idx1 < idx2

    // Iterate through each entity's selected tokens to find potential matches
    for (i, data_i) in entity_data.iter().enumerate() {
        let mut potential_matches_for_i: HashMap<usize, usize> = HashMap::new(); // entity_idx -> overlap_count
        
        // Use only the TOP_TOKENS_PER_ENTITY for candidate generation for entity_i
        // This requires re-calculating or storing the top tokens per entity if not already done in prepare_entity_data_and_index
        // For simplicity here, we'll iterate through all tokens of data_i, assuming they are already somewhat filtered.
        // A more optimized approach would use the pre-selected TOP_TOKENS_PER_ENTITY for data_i.
        for token in &data_i.tokens { // Ideally, this would be the top N tokens for data_i
            if let Some(posting_list) = token_to_entities.get(token) {
                for &j_idx in posting_list {
                    if i < j_idx { // Ensure i < j_idx to avoid duplicate pairs (i,j) vs (j,i) and self-matches
                        *potential_matches_for_i.entry(j_idx).or_insert(0) += 1;
                    }
                }
            }
        }

        // Add pairs to set if they meet the minimum token overlap
        for (j_idx, overlap) in potential_matches_for_i {
            if overlap >= MIN_TOKEN_OVERLAP {
                candidate_pairs_set.insert((i, j_idx));
            }
        }
    }
    candidate_pairs_set.into_iter().collect()
}

/// Token statistics for efficient filtering and weighting.
#[derive(Debug, Clone)] // Added Clone
struct TokenStats {
    token: String,
    frequency: usize, // Number of entities containing this token
    idf: f32,         // Inverse Document Frequency
}
