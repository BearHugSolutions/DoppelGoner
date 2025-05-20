// src/matching/name.rs - Refactored for single-transaction pattern
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

use crate::{config, reinforcement::SharedFeatureCache};
use crate::db::PgPool;
use crate::models::{
    ActionType, Entity, EntityGroupId, EntityId, MatchMethodType, MatchValues, NameMatchValue,
    NewSuggestedAction, OrganizationId, SuggestionStatus,
};
use crate::reinforcement::MatchingOrchestrator;
use crate::results::{AnyMatchResult, MatchMethodStats, NameMatchResult};
use crate::utils::cosine_similarity_candle;
use serde_json;

// Updated configuration for stricter name matching
const MIN_FUZZY_SIMILARITY_THRESHOLD: f32 = 0.92; // Increased from 0.85
const MIN_SEMANTIC_SIMILARITY_THRESHOLD: f32 = 0.94; // Increased from 0.88
const COMBINED_SIMILARITY_THRESHOLD: f32 = 0.93; // Increased from 0.86
const FUZZY_WEIGHT: f32 = 0.3; // Decreased from 0.4
const SEMANTIC_WEIGHT: f32 = 0.7; // Increased from 0.6
const INTERNAL_WORKERS_NAME_STRATEGY: usize = 2; // Number of concurrent tasks for pair processing

/// Number of discriminative tokens to use per entity
const TOP_TOKENS_PER_ENTITY: usize = 10;
/// Minimum token overlap required for candidate pair consideration
const MIN_TOKEN_OVERLAP: usize = 2;
const MAX_CANDIDATES_PER_ENTITY: usize = 500; // Safety limit to prevent excessive comparisons
const MIN_TOKEN_LENGTH: usize = 2; // Ignore tokens shorter than this length

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
const STOPWORDS: [&str; 132] = [
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
    // Common business terms with little discriminative value
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
    "program",
    "provider",
    "member",
    "directory",
    "guide",
    "network",
];

// SQL query for inserting into entity_group
const INSERT_ENTITY_GROUP_SQL: &str = "
    INSERT INTO public.entity_group
(id, entity_id_1, entity_id_2, method_type, match_values, confidence_score, 
 pre_rl_confidence_score)
VALUES ($1, $2, $3, $4, $5, $6, $7)";

/// Enhanced data structure to hold entity information with organization type detection
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
    feature_cache: Option<SharedFeatureCache>, // Added feature_cache parameter
) -> Result<AnyMatchResult> {
    info!(
        "Starting V1 high-performance name matching (run ID: {}){}...",
        pipeline_run_id,
        if reinforcement_orchestrator_option.is_some() {
            " with RL confidence tuning"
        } else {
            " (RL tuner not provided)"
        }
    );
    let start_time = Instant::now();

    // Initial data loading
    let mut initial_conn = pool
        .get()
        .await
        .context("Name: Failed to get DB connection for initial reads")?;

    // 1. Fetch entities with names
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

    // 2. Fetch existing pairs to avoid duplicates
    let existing_processed_pairs_set = fetch_existing_pairs(&*initial_conn, MatchMethodType::Name).await?;
    info!(
        "Name: Found {} existing name-matched pairs to skip.",
        existing_processed_pairs_set.len()
    );

    // 3. Get organization embeddings for semantic comparison
    let org_embeddings_map = get_organization_embeddings(&*initial_conn, &all_entities_with_names_vec).await?;
    
    // Release initial connection
    drop(initial_conn);

    // 4. Prepare entity data and generate candidate pairs
    let stopwords: HashSet<String> = STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let (entity_data, token_to_entities, _token_stats) =
        prepare_entity_data_and_index(&all_entities_with_names_vec, &stopwords).await;

    let candidate_pairs = generate_candidate_pairs(&entity_data, &token_to_entities).await;
    info!(
        "Name: Generated {} candidate pairs for comparison.",
        candidate_pairs.len()
    );

    // 5. Stats tracking
    let mut new_pairs_created_count = 0;
    let mut entities_in_new_pairs: HashSet<EntityId> = HashSet::new();
    let mut confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;

    // 6. Process candidate pairs using a connection pool with limited concurrency
    let semaphore = Arc::new(tokio::sync::Semaphore::new(15)); // Limit concurrent DB operations
    
    let candidate_pairs_len = candidate_pairs.len();
    for (i, j) in candidate_pairs {
        // Extract entity data
        let entity1_data = &entity_data[i];
        let entity2_data = &entity_data[j];
        let e1_id = &entity1_data.entity.id;
        let e2_id = &entity2_data.entity.id;
        
        // Skip if already processed
        if existing_processed_pairs_set.contains(&(e1_id.clone(), e2_id.clone())) {
            trace!("Name: Pair ({}, {}) already processed. Skipping.", e1_id.0, e2_id.0);
            continue;
        }
        
        // Skip pairs with empty names
        let original_name1 = entity1_data.entity.name.as_ref().cloned().unwrap_or_default();
        let original_name2 = entity2_data.entity.name.as_ref().cloned().unwrap_or_default();
        let normalized_name1 = entity1_data.normalized_name.clone();
        let normalized_name2 = entity2_data.normalized_name.clone();
        
        if normalized_name1.is_empty() || normalized_name2.is_empty() {
            continue;
        }
        
        // Skip pairs with incompatible entity types
        if let (Some(t1), Some(t2)) = (entity1_data.entity_type, entity2_data.entity_type) {
            if t1 != t2 && INCOMPATIBLE_ORG_TYPES.iter().any(|(it1, it2)|
                (t1 == *it1 && t2 == *it2) || (t1 == *it2 && t2 == *it1)) {
                continue;
            }
        }
        
        // Calculate fuzzy score using Jaro-Winkler similarity
        let fuzzy_score = jaro_winkler(&normalized_name1, &normalized_name2) as f32;
        if fuzzy_score < (MIN_FUZZY_SIMILARITY_THRESHOLD * 0.9) {
            continue;
        }
        
        // Calculate semantic score using embeddings
        let embedding1_opt = org_embeddings_map.get(e1_id).and_then(|opt_emb| opt_emb.as_ref());
        let embedding2_opt = org_embeddings_map.get(e2_id).and_then(|opt_emb| opt_emb.as_ref());
        let semantic_score = match (embedding1_opt, embedding2_opt) {
            (Some(emb1), Some(emb2)) => {
                match cosine_similarity_candle(emb1, emb2) {
                    Ok(sim) => sim as f32,
                    Err(e) => {
                        warn!("Name: Cosine similarity calculation failed for pair ({}, {}): {}. Defaulting to 0.0.", e1_id.0, e2_id.0, e);
                        0.0
                    }
                }
            },
            _ => 0.0,
        };
        
        // Calculate combined score and determine match type
        let (pre_rl_score, pre_rl_match_type) =
            if semantic_score >= MIN_SEMANTIC_SIMILARITY_THRESHOLD {
                ((fuzzy_score * FUZZY_WEIGHT) + (semantic_score * SEMANTIC_WEIGHT), "combined".to_string())
            } else if fuzzy_score >= MIN_FUZZY_SIMILARITY_THRESHOLD {
                (fuzzy_score, "fuzzy".to_string())
            } else {
                continue;
            };
        
        // Apply domain rules
        let adjusted_pre_rl_score = apply_domain_rules(
            &normalized_name1, 
            &normalized_name2, 
            pre_rl_score, 
            entity1_data.entity_type, 
            entity2_data.entity_type
        );
        
        if adjusted_pre_rl_score < COMBINED_SIMILARITY_THRESHOLD {
            continue;
        }
        
        // Acquire a permit to limit concurrent DB operations
        let permit = match semaphore.acquire().await {
            Ok(permit) => permit,
            Err(e) => {
                warn!("Name: Failed to acquire semaphore permit: {}", e);
                continue;
            }
        };
        
        // Create match values
        let match_values = MatchValues::Name(NameMatchValue {
            original_name1: original_name1.clone(),
            original_name2: original_name2.clone(),
            normalized_name1: normalized_name1.clone(),
            normalized_name2: normalized_name2.clone(),
            pre_rl_match_type: Some(pre_rl_match_type.clone()),
        });
        
        // Extract features and apply RL tuning if available
        let mut final_confidence = adjusted_pre_rl_score as f64;
        let mut features_for_snapshot: Option<Vec<f64>> = None;
        
        if let Some(orchestrator) = reinforcement_orchestrator_option.as_ref() {
            // Use the feature cache if available
            match if let Some(cache) = feature_cache.as_ref() {
                // Use the cache through the orchestrator
                let orchestrator_guard = orchestrator.lock().await;
                orchestrator_guard.get_pair_features(pool, e1_id, e2_id).await
            } else {
                // Fall back to direct extraction if no cache
                MatchingOrchestrator::extract_pair_context_features(pool, e1_id, e2_id).await
            } {
                Ok(features) => {
                    if !features.is_empty() {
                        features_for_snapshot = Some(features.clone());
                        let orchestrator_guard = orchestrator.lock().await;
                        match orchestrator_guard.get_tuned_confidence(
                            &MatchMethodType::Name,
                            adjusted_pre_rl_score as f64,
                            &features,
                        ) {
                            Ok(tuned_score) => final_confidence = tuned_score,
                            Err(e) => {
                                warn!("Name: Failed to get tuned confidence for pair ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e);
                            }
                        }
                    } else {
                        warn!("Name: Extracted features vector is empty for pair ({}, {}). Using pre-RL score.", e1_id.0, e2_id.0);
                    }
                },
                Err(e) => {
                    warn!("Name: Failed to extract features for pair ({}, {}): {}. Using pre-RL score.", e1_id.0, e2_id.0, e);
                }
            }
        }
        
        // Create entity group with unified approach
        match create_entity_group(
            pool,
            e1_id,
            e2_id,
            &match_values,
            final_confidence,
            adjusted_pre_rl_score as f64, // Pre-RL score
            reinforcement_orchestrator_option.as_ref(),
            pipeline_run_id,
            feature_cache.clone(), // Pass the feature cache
        ).await {
            Ok(created) => {
                if created {
                    new_pairs_created_count += 1;
                    entities_in_new_pairs.insert(e1_id.clone());
                    entities_in_new_pairs.insert(e2_id.clone());
                    confidence_scores_for_stats.push(final_confidence);
                    
                    if new_pairs_created_count % 100 == 0 {
                        info!(
                            "Name: Created {} pairs so far ({:.2}% complete)", 
                            new_pairs_created_count,
                            (new_pairs_created_count as f32 / candidate_pairs_len as f32) * 100.0
                        );
                    }
                }
            },
            Err(e) => {
                individual_operation_errors += 1;
                warn!("Name: Failed to process pair ({}, {}): {}", e1_id.0, e2_id.0, e);
            }
        }
        
        // Release the permit
        drop(permit);
    }

    // Report errors if any
    if individual_operation_errors > 0 {
        warn!(
            "Name: Encountered {} errors during pair processing.",
            individual_operation_errors
        );
    }

    // Calculate stats
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
        "Name matching complete in {:.2?}: {} new pairs, {} unique entities.",
        start_time.elapsed(),
        method_stats.groups_created,
        method_stats.entities_matched
    );

    Ok(AnyMatchResult::Name(NameMatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

/// Create entity group record in a single transaction, along with decision logging and suggestions
async fn create_entity_group(
    pool: &PgPool,
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    match_values: &MatchValues,
    final_confidence_score: f64,
    pre_rl_confidence_score: f64,
    reinforcement_orchestrator: Option<&Arc<Mutex<MatchingOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>, // Added feature_cache parameter
) -> Result<bool> {
    // Get a single connection for all operations
    let mut conn = pool.get().await
        .context("Name: Failed to get DB connection for entity group creation")?;
    
    // Start a transaction that will include all operations
    let tx = conn.transaction().await
        .context("Name: Failed to start transaction for entity group creation")?;
    
    // Generate entity group ID
    let new_entity_group_id = EntityGroupId(Uuid::new_v4().to_string());
    
    // Ensure entity_id_1 < entity_id_2 to satisfy the check_entity_order constraint
    let (ordered_id_1, ordered_id_2);
    let ordered_match_values;
    
    if entity_id_1.0 <= entity_id_2.0 {
        ordered_id_1 = entity_id_1;
        ordered_id_2 = entity_id_2;
        ordered_match_values = match_values.clone(); // Clone the original match values
    } else {
        // Need to swap the entity IDs to satisfy the constraint
        ordered_id_1 = entity_id_2;
        ordered_id_2 = entity_id_1;
        
        // Create new match values with swapped fields for NameMatchValue
        if let MatchValues::Name(name_match) = match_values {
            ordered_match_values = MatchValues::Name(NameMatchValue {
                original_name1: name_match.original_name2.clone(),
                original_name2: name_match.original_name1.clone(),
                normalized_name1: name_match.normalized_name2.clone(),
                normalized_name2: name_match.normalized_name1.clone(),
                pre_rl_match_type: name_match.pre_rl_match_type.clone(),
            });
        } else {
            // This shouldn't happen for name matching
            ordered_match_values = match_values.clone();
        }
    }
    
    // Serialize match values
    let match_values_json = serde_json::to_value(&ordered_match_values)
        .context("Name: Failed to serialize match values")?;
    
    let empty_string = "".to_string();
    // Extract name-specific data (using ordered match values)
    let (original_name1, original_name2, normalized_name1, normalized_name2, pre_rl_match_type) = match &ordered_match_values {
        MatchValues::Name(n) => (
            &n.original_name1, 
            &n.original_name2, 
            &n.normalized_name1, 
            &n.normalized_name2, 
            n.pre_rl_match_type.as_ref().unwrap_or(&empty_string)
        ),
        _ => (&String::new(), &String::new(), &String::new(), &String::new(), &String::new()),
    };
    
    // Try to get tuned confidence score (RL-based) before inserting
    // IMPORTANT: Use ordered_id_1 and ordered_id_2 here to maintain consistency
    let mut tuned_confidence = final_confidence_score;
    let mut features_for_snapshot: Option<Vec<f64>> = None;
    
    if let Some(ro_arc) = reinforcement_orchestrator {
        // Use the feature cache if available
        match if let Some(cache) = feature_cache.as_ref() {
            // Use the cache through the orchestrator
            let orchestrator_guard = ro_arc.lock().await;
            orchestrator_guard.get_pair_features(pool, ordered_id_1, ordered_id_2).await
        } else {
            // Fall back to direct extraction if no cache
            MatchingOrchestrator::extract_pair_context_features(pool, ordered_id_1, ordered_id_2).await
        } {
            Ok(features_vec) => {
                if !features_vec.is_empty() {
                    features_for_snapshot = Some(features_vec.clone());
                    let orchestrator_guard = ro_arc.lock().await;
                    match orchestrator_guard.get_tuned_confidence(
                        &MatchMethodType::Name,
                        pre_rl_confidence_score,
                        &features_vec,
                    ) {
                        Ok(tuned_score) => tuned_confidence = tuned_score,
                        Err(e) => warn!("Name: Failed to get tuned confidence for ({}, {}): {}. Using pre-RL score.", 
                                       ordered_id_1.0, ordered_id_2.0, e),
                    }
                }
            },
            Err(e) => warn!("Name: Failed to extract features for ({}, {}): {}. Using pre-RL score.", 
                           ordered_id_1.0, ordered_id_2.0, e),
        }
    }
    
    // Insert entity group using ordered IDs
    let rows_affected = tx.execute(
        INSERT_ENTITY_GROUP_SQL,
        &[
            &new_entity_group_id.0,
            &ordered_id_1.0,  // Using the ordered entity ID
            &ordered_id_2.0,  // Using the ordered entity ID
            &MatchMethodType::Name.as_str(),
            &match_values_json,
            &tuned_confidence, // Now using tuned value
            &pre_rl_confidence_score,
        ],
    ).await?;
    
    if rows_affected > 0 {
        // Create suggestion for low confidence matches if needed
        if tuned_confidence < config::MODERATE_LOW_SUGGESTION_THRESHOLD {
            let priority = if tuned_confidence < config::CRITICALLY_LOW_SUGGESTION_THRESHOLD {
                2
            } else {
                1
            };
            
            let details_json = serde_json::json!({
                "method_type": MatchMethodType::Name.as_str(),
                "original_name1": original_name1,
                "original_name2": original_name2,
                "normalized_name1": normalized_name1,
                "normalized_name2": normalized_name2,
                "pre_rl_score": pre_rl_confidence_score,
                "pre_rl_match_type": pre_rl_match_type,
                "entity_group_id": &new_entity_group_id.0,
            });
            
            let reason_message = format!(
                "Pair ({}, {}) matched by Name with low tuned confidence ({:.4}). Pre-RL: {:.2} ({}).",
                ordered_id_1.0, ordered_id_2.0, tuned_confidence, pre_rl_confidence_score, pre_rl_match_type
            );
            
            // Insert suggestion directly in this transaction
            const INSERT_SUGGESTION_SQL: &str = "
                INSERT INTO clustering_metadata.suggested_actions (
                    pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
                    triggering_confidence, details, reason_code, reason_message, priority, status,
                    reviewer_id, reviewed_at, review_notes
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                RETURNING id";
                
            if let Err(e) = tx.query_one(
                INSERT_SUGGESTION_SQL,
                &[
                    &Some(pipeline_run_id.to_string()),
                    &ActionType::ReviewEntityInGroup.as_str(),
                    &None::<String>,
                    &Some(new_entity_group_id.0.clone()),
                    &None::<String>,
                    &None::<String>,
                    &Some(tuned_confidence),
                    &Some(details_json),
                    &Some("LOW_TUNED_CONFIDENCE_PAIR".to_string()),
                    &Some(reason_message),
                    &(priority as i32),
                    &SuggestionStatus::PendingReview.as_str(),
                    &None::<String>,
                    &None::<NaiveDateTime>,
                    &None::<String>,
                ],
            ).await {
                warn!("Name: Failed to create suggestion: {}", e);
            }
        }
        
        // Log decision snapshot if features were extracted
        if let (Some(ro_arc), Some(features_vec)) = (reinforcement_orchestrator, features_for_snapshot.as_ref()) {
            let orchestrator_guard = ro_arc.lock().await;
            let confidence_tuner_ver = orchestrator_guard.confidence_tuner.version;
            
            let snapshot_features_json = serde_json::to_value(features_vec)
                .unwrap_or(serde_json::Value::Null);
                
            // Insert decision directly with SQL to avoid verification
            const INSERT_DECISION_SQL: &str = "
                INSERT INTO clustering_metadata.match_decision_details (
                    entity_group_id, pipeline_run_id, snapshotted_features,
                    method_type_at_decision, pre_rl_confidence_at_decision,
                    tuned_confidence_at_decision, confidence_tuner_version_at_decision
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id";
                
            if let Err(e) = tx.query_one(
                INSERT_DECISION_SQL,
                &[
                    &new_entity_group_id.0,
                    &pipeline_run_id,
                    &snapshot_features_json,
                    &MatchMethodType::Name.as_str(),
                    &pre_rl_confidence_score,
                    &tuned_confidence,
                    &(confidence_tuner_ver as i32),
                ],
            ).await {
                warn!("Name: Failed to log decision snapshot: {}", e);
            }
        }
        
        // Commit the transaction with all operations
        tx.commit().await?;
        Ok(true)
    } else {
        // No rows affected, possibly already exists due to concurrent operations
        tx.commit().await?;
        debug!("Name: No rows affected when inserting entity group, pair may already exist");
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

    let batch_size = 100; // Or a configurable value
    debug!(
        "Fetching embeddings for {} unique organizations in batches of {}...",
        org_ids_vec.len(),
        batch_size
    );

    // Chunk processing for fetching embeddings
    for batch_org_ids_str_slice in org_ids_vec.chunks(batch_size) {
        // Convert slice of String to slice of &str for the query parameter
        let batch_org_ids_refs: Vec<&str> =
            batch_org_ids_str_slice.iter().map(AsRef::as_ref).collect();

        let query = "SELECT id, embedding FROM public.organization WHERE id = ANY($1::TEXT[]) AND embedding IS NOT NULL";

        let rows = conn
            .query(query, &[&batch_org_ids_refs]) // Pass as slice of &str
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
            let embedding_vec_opt: Option<Vec<f32>> = row.try_get("embedding").ok(); // .ok() converts Result to Option

            if let Some(embedding_vec) = embedding_vec_opt {
                // Find all entities associated with this org_id and store the embedding
                for entity in entities
                    .iter()
                    .filter(|e| e.organization_id.0 == org_id_str)
                {
                    embeddings.insert(entity.id.clone(), Some(embedding_vec.clone()));
                }
            }
        }
    }
    // Ensure all entities from the input list have an entry in the map, even if None
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

/// Helper to fetch existing pairs
async fn fetch_existing_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(EntityId, EntityId)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&method_type.as_str()])
        .await
        .with_context(|| format!("Failed to query existing {:?}-matched pairs", method_type))?;

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

/// Enhanced normalize_name function with entity type detection
fn normalize_name(name: &str) -> (String, Option<&'static str>) {
    let mut normalized = name.to_lowercase();

    // Clean noise prefixes
    for prefix in &NOISE_PREFIXES {
        if normalized.starts_with(prefix) {
            normalized = normalized[prefix.len()..].trim().to_string();
        }
    }

    // Handle special character substitutions
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

    // Detect organization type early for specialized handling
    let entity_type = detect_entity_type(&normalized);

    // Apply specialized normalization based on entity type
    if let Some(etype) = entity_type {
        match etype {
            "city" => {
                // For cities, preserve the city name part carefully
                if let Some(stripped) = normalized.strip_prefix("city of ") {
                    let city_name = stripped.trim();
                    // Don't remove suffixes from city names
                    return (format!("city of {}", city_name), Some(etype));
                }
            }
            "police" => {
                // Standardize police department names
                normalized = normalized.replace("pd", "police department");
                normalized = normalized.replace("police dept", "police department");
            }
            "fire" => {
                // Standardize fire department names
                normalized = normalized.replace("fd", "fire department");
                normalized = normalized.replace("fire dept", "fire department");
            }
            "hospital" => {
                // Standardize medical facility terms
                normalized = normalized.replace("medical center", "hospital");
                normalized = normalized.replace("med ctr", "hospital");
                normalized = normalized.replace("med center", "hospital");
            }
            "education" => {
                // Standardize educational institution terms
                normalized = normalized.replace("school dist", "school district");
                normalized = normalized.replace("sd", "school district");
            }
            _ => {}
        }
    }

    // Common prefixes to handle specially
    let prefixes = ["the ", "a ", "an "];

    for prefix in prefixes {
        if normalized.starts_with(prefix) {
            normalized = normalized[prefix.len()..].to_string();
        }
    }

    // Common suffixes to remove (ensure spaces for whole word, or handle carefully)
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

    // Skip suffix removal for city names
    if entity_type != Some("city") {
        for suffix in suffixes {
            if normalized.ends_with(suffix) {
                normalized = normalized[..normalized.len() - suffix.len()]
                    .trim_end()
                    .to_string();
            }
        }
    }

    // Extract location in parentheses for special handling (e.g., "YWCA (Seattle)")
    let mut location_suffix = None;
    let paren_regex = Regex::new(r"\s*\((.*?)\)\s*$").unwrap_or_else(|_| Regex::new(r"").unwrap());
    if let Some(captures) = paren_regex.captures(&normalized) {
        if let Some(location_match) = captures.get(1) {
            location_suffix = Some(location_match.as_str().to_string());
            normalized = paren_regex.replace(&normalized, "").to_string();
        }
    }

    // Regex-based replacements for common abbreviations within the name
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
        // Specific organization types for better matching
        (r"\b(fd)\b", "fire department"),
        (r"\b(pd)\b", "police department"),
        // Remove standalone legal terms
        (r"\binc\b", ""),
        (r"\bcorp\b", ""),
        (r"\bllc\b", ""),
        (r"\bltd\b", ""),
    ];

    for (pattern, replacement) in &replacements {
        match Regex::new(pattern) {
            Ok(re) => {
                normalized = re.replace_all(&normalized, *replacement).into_owned();
            }
            Err(e) => {
                warn!("Invalid regex pattern: '{}'. Error: {}", pattern, e);
            }
        }
    }

    // Remove all non-alphanumeric characters except spaces
    normalized = normalized
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect();

    // Normalize whitespace
    normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");

    // Reattach location suffix for branch/location awareness
    if let Some(location) = location_suffix {
        if !location.is_empty() {
            // Only reattach if not empty after normalization
            normalized = format!("{} {}", normalized.trim(), location.trim());
        }
    }

    (normalized.trim().to_string(), entity_type)
}

/// Detect organization type based on name patterns
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

/// Enhanced tokenization with context-aware weighting
fn tokenize_name(
    normalized_name: &str,
    stopwords: &HashSet<String>,
    entity_type: Option<&'static str>,
) -> (HashSet<String>, Vec<(String, f32)>) {
    let mut tokens = HashSet::new();
    let mut weighted_tokens = Vec::new();

    // Process words
    let words: Vec<&str> = normalized_name.split_whitespace().collect();

    // First pass: process individual words
    for word in &words {
        let token = word.to_lowercase();

        // Skip stopwords and very short tokens
        if !stopwords.contains(&token) && token.len() >= MIN_TOKEN_LENGTH {
            tokens.insert(token.clone());

            // Get context-aware weight for this token
            let weight = get_token_weight(&token, entity_type);
            weighted_tokens.push((token, weight));
        }
    }

    // Second pass: add bigrams for better phrase matching
    for i in 0..words.len().saturating_sub(1) {
        let bigram = format!("{} {}", words[i], words[i + 1]).to_lowercase();
        if bigram.len() >= 2 * MIN_TOKEN_LENGTH {
            tokens.insert(bigram.clone());

            // Bigrams get higher weight because they're more specific
            let weight = 1.5;
            weighted_tokens.push((bigram, weight));
        }
    }

    // Special case for cities - make the city name extremely important
    if entity_type == Some("city") && normalized_name.starts_with("city of ") {
        if let Some(city_name) = normalized_name.strip_prefix("city of ") {
            let city = city_name.trim().to_string();
            // Add the full city name as a special token with very high weight
            if !city.is_empty() {
                tokens.insert(city.clone());
                weighted_tokens.push((city, 3.0)); // Very high weight for city name
            }
        }
    }

    (tokens, weighted_tokens)
}

/// Calculate context-aware token weight based on token importance
fn get_token_weight(token: &str, entity_type: Option<&str>) -> f32 {
    // Common words get lower weight
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
        return 0.5; // Lower weight for common words
    }

    // Location names get medium weight
    if LOCATION_PREFIXES.contains(&token) {
        return 0.8; // Medium weight for location prefixes
    }

    // Entity type indicators get high weight
    let type_indicators = [
        "police", "fire", "hospital", "school", "college", "church", "bank", "library",
    ];

    if type_indicators.contains(&token) {
        return 2.0; // Higher weight for entity type indicators
    }

    // Adjust weight based on entity type if available
    if let Some(etype) = entity_type {
        match etype {
            "city" => {
                if token == "city" {
                    0.5
                } else {
                    1.5
                }
            } // City name more important than "city"
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
        1.0 // Default weight
    }
}

/// Domain-specific rule application for name matching
fn apply_domain_rules(
    normalized_name1: &str,
    normalized_name2: &str,
    pre_rl_score: f32,
    entity_type1: Option<&'static str>,
    entity_type2: Option<&'static str>,
) -> f32 {
    // Rule 1: Special handling for city matching
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

        // If city names differ at all, apply severe penalty based on how different they are
        if city_name1 != city_name2 {
            // Calculate edit distance between city names
            let edit_distance = levenshtein_distance(city_name1, city_name2);

            // If edit distance is more than 3 characters or >30% of length, severely penalize
            let max_length = std::cmp::max(city_name1.len(), city_name2.len());
            if edit_distance > 3
                || (max_length > 0 && edit_distance as f32 / max_length as f32 > 0.3)
            {
                return pre_rl_score * 0.6; // 40% penalty
            } else {
                // Less penalty for very similar city names
                return pre_rl_score * 0.8; // 20% penalty
            }
        }
    }

    // Rule 2: Organization type compatibility
    if let (Some(t1), Some(t2)) = (entity_type1, entity_type2) {
        if t1 != t2 {
            // Already checked for incompatible types in the main function
            // Here we just apply a penalty for different but not explicitly incompatible types
            return pre_rl_score * 0.85; // 15% penalty
        }
    }

    // Rule 3: Special handling for department names
    if normalized_name1.contains("department") || normalized_name2.contains("department") {
        // If only one contains "department" or they contain different department types
        let dept_types = [
            "police",
            "fire",
            "health",
            "public",
            "revenue",
            "transportation",
        ];

        for dept_type in &dept_types {
            let type1_present = normalized_name1.contains(dept_type);
            let type2_present = normalized_name2.contains(dept_type);

            if type1_present != type2_present {
                // Different department types
                return pre_rl_score * 0.75; // 25% penalty
            }
        }
    }

    // Rule 4: Names that share only common words/location prefixes
    let words1: Vec<&str> = normalized_name1.split_whitespace().collect();
    let words2: Vec<&str> = normalized_name2.split_whitespace().collect();

    // Find common prefix words
    let mut common_prefix_len = 0;
    for i in 0..std::cmp::min(words1.len(), words2.len()) {
        if words1[i] == words2[i] {
            common_prefix_len += 1;
        } else {
            break;
        }
    }

    // If names only share common prefix but are otherwise different
    if common_prefix_len > 0 && common_prefix_len < words1.len() && common_prefix_len < words2.len()
    {
        // Calculate what percentage of words are in the common prefix
        let total_words = words1.len() + words2.len();
        let common_prefix_percentage = (2 * common_prefix_len) as f32 / total_words as f32;

        // If common prefix is a significant portion of the names
        if common_prefix_percentage > 0.3 {
            // Check if the remaining words are distinctly different
            let remaining_similarity = jaro_winkler(
                &words1[common_prefix_len..].join(" "),
                &words2[common_prefix_len..].join(" "),
            );

            // If the remaining parts are significantly different
            if remaining_similarity < 0.7 {
                return pre_rl_score * (0.9 - (common_prefix_percentage * 0.2)); // Penalty based on prefix ratio
            }
        }
    }

    // Rule 5: Handle common location prefixes
    for prefix in &LOCATION_PREFIXES {
        if normalized_name1.starts_with(prefix) && normalized_name2.starts_with(prefix) {
            let suffix1 = normalized_name1[prefix.len()..].trim();
            let suffix2 = normalized_name2[prefix.len()..].trim();

            if !suffix1.is_empty() && !suffix2.is_empty() && jaro_winkler(suffix1, suffix2) < 0.8 {
                return pre_rl_score * 0.8; // 20% penalty for different suffixes after common prefix
            }
        }
    }

    // Rule 6: Special handling for branches/locations in parentheses
    let location_regex = Regex::new(r"\b([a-z]+)$").unwrap_or_else(|_| Regex::new(r"").unwrap());
    if let (Some(loc1_match), Some(loc2_match)) = (
        location_regex.find(normalized_name1),
        location_regex.find(normalized_name2),
    ) {
        let loc1 = loc1_match.as_str();
        let loc2 = loc2_match.as_str();

        // Check if names differ only by location suffix
        let name1_without_loc = &normalized_name1[0..loc1_match.start()].trim();
        let name2_without_loc = &normalized_name2[0..loc2_match.start()].trim();

        if name1_without_loc == name2_without_loc && loc1 != loc2 {
            // Same organization but different locations
            return pre_rl_score * 0.7; // 30% penalty
        }
    }

    // If no rules triggered, return the original score
    pre_rl_score
}

/// Calculate Levenshtein edit distance for better city name comparison
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();

    let len1 = s1_chars.len();
    let len2 = s2_chars.len();

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

            matrix[i][j] = std::cmp::min(
                std::cmp::min(
                    matrix[i - 1][j] + 1, // deletion
                    matrix[i][j - 1] + 1, // insertion
                ),
                matrix[i - 1][j - 1] + cost, // substitution
            );
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
        let idf = (total_docs / (freq as f32 + 1.0)).ln(); // Add 1 to avoid ln(0) if freq is total_docs
        token_stats_map.insert(
            token.clone(),
            TokenStats {
                token,
                frequency: freq,
                idf,
            },
        );
    }

    let max_common_token_freq = (entity_data_vec.len() as f32 * 0.05).max(10.0) as usize; // Heuristic
    let mut token_to_entities_map: HashMap<String, Vec<usize>> = HashMap::new();

    for (idx, data) in entity_data_vec.iter().enumerate() {
        let mut scored_tokens: Vec<(String, f32)> = data
            .tokens
            .iter()
            .filter_map(|token| {
                token_stats_map.get(token).and_then(|stats| {
                    if stats.frequency <= max_common_token_freq && token.len() >= MIN_TOKEN_LENGTH {
                        Some((token.clone(), stats.idf)) // Using IDF as score for simplicity here
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

async fn generate_candidate_pairs(
    entity_data: &[EntityNameData],
    token_to_entities: &HashMap<String, Vec<usize>>,
) -> Vec<(usize, usize)> {
    let mut candidate_pairs_set = HashSet::new();
    for (i, data_i) in entity_data.iter().enumerate() {
        let mut potential_matches_for_i: HashMap<usize, usize> = HashMap::new(); // entity_idx -> overlap_count
        for token in &data_i.tokens {
            if let Some(posting_list) = token_to_entities.get(token) {
                for &j_idx in posting_list {
                    if i < j_idx {
                        // Avoid self-match and duplicate pairs (i,j) vs (j,i)
                        *potential_matches_for_i.entry(j_idx).or_insert(0) += 1;
                    }
                }
            }
        }
        for (j_idx, overlap) in potential_matches_for_i {
            if overlap >= MIN_TOKEN_OVERLAP {
                candidate_pairs_set.insert((i, j_idx));
            }
        }
    }
    candidate_pairs_set.into_iter().collect()
}

/// Token statistics for efficient filtering
#[derive(Debug)]
struct TokenStats {
    token: String,
    frequency: usize, // Number of entities containing this token
    idf: f32,         // Inverse Document Frequency
}