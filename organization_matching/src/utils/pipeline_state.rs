// src/utils/pipeline_state.rs
use crate::models::stats_models::MatchMethodType;
use crate::utils::db_connect::PgPool;
use anyhow::{Context, Result};
use log::{debug, error, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
// Changed to `tokio_postgres::types::{Json, ToSql}` to ensure `Json` is brought into scope
// and can be properly boxed as `dyn ToSql + Sync + Send`.
use tokio_postgres::types::{Json, ToSql};

#[derive(Debug, Clone)]
pub struct EntitySignature {
    // Made public
    pub entity_id: String,
    pub signature: String,
    // pub source_data_last_updated_at: Option<chrono::NaiveDateTime>, // Uncomment if needed and ensure chrono type aligns with DB
}

#[derive(Debug)]
pub struct CachedComparisonResult {
    pub comparison_result: String,
    pub similarity_score: Option<f64>,
}

/// Represents a cached comparison entry, used for both reading and writing to comparison_cache.
#[derive(Debug, Clone)]
pub struct ComparisonCacheEntry {
    pub entity_id_1: String,
    pub entity_id_2: String,
    pub signature_1: String,
    pub signature_2: String,
    pub method_type: MatchMethodType, // Maps to 'method_type' column, and used to form 'comparison_type'
    pub pipeline_run_id: String,
    pub comparison_result: String,
    pub similarity_score: Option<f64>,
    pub features_snapshot: Option<JsonValue>,
}

/// Represents an entity's completion status for a specific method type.
#[derive(Debug, Clone)]
pub struct EntityCompletionStatus {
    pub entity_id: String,
    pub method_type: MatchMethodType,
    pub pipeline_run_id: String,
    pub signature_at_completion: String,
    pub completed_at: chrono::DateTime<chrono::Utc>,
    pub total_comparisons_completed: i32,
}

/// Represents the result of checking an entity's completion status.
#[derive(Debug, Clone)]
pub struct EntityCompletionCheck {
    pub entity_id: String,
    pub is_complete: bool,
    pub current_signature: Option<String>,
    pub last_completion_signature: Option<String>,
    pub total_comparisons_completed: Option<i32>,
}

// Constant to prefix comparison types in the cache for entity-entity comparisons
const COMPARISON_TYPE_ENTITY_PREFIX: &str = "ENTITY_";

/// Helper to get the `comparison_type` string based on `MatchMethodType`.
fn get_comparison_type_string(method_type: &MatchMethodType) -> String {
    format!(
        "{}{}",
        COMPARISON_TYPE_ENTITY_PREFIX,
        method_type.as_str().to_uppercase()
    )
}

// =============================================================================
// ENTITY COMPLETION TRACKING FUNCTIONS
// =============================================================================

/// Checks which entities have completed all possible comparisons for a given method type.
/// Returns a HashMap where the key is entity_id and the value contains completion status.
pub async fn batch_check_entity_completion_status(
    pool: &PgPool,
    entity_ids: &[String],
    method_type: &MatchMethodType,
) -> Result<HashMap<String, EntityCompletionCheck>> {
    if entity_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_check_entity_completion_status")?;

    // First get current signatures for all entities
    let sig_query = "SELECT entity_id, signature FROM pipeline_state.entity_data_signatures WHERE entity_id = ANY($1)";
    let sig_rows = conn.query(sig_query, &[&entity_ids]).await.context(
        "Failed to query current signatures from pipeline_state.entity_data_signatures",
    )?;
    
    let mut current_signatures: HashMap<String, String> = HashMap::new();
    for row in sig_rows {
        current_signatures.insert(row.get("entity_id"), row.get("signature"));
    }

    // Then check completion status for the specific method type
    let completion_query = "
        SELECT entity_id, signature_at_completion, completed_at, total_comparisons_completed
        FROM pipeline_state.entity_completion_status 
        WHERE entity_id = ANY($1) AND method_type = $2
        ORDER BY completed_at DESC";
    
    let completion_rows = conn.query(completion_query, &[&entity_ids, &method_type.as_str()]).await.context(
        "Failed to query completion status from pipeline_state.entity_completion_status",
    )?;
    
    // Use the most recent completion record per entity
    let mut completion_map: HashMap<String, (String, i32)> = HashMap::new();
    for row in completion_rows {
        let entity_id: String = row.get("entity_id");
        if !completion_map.contains_key(&entity_id) {
            let signature_at_completion: String = row.get("signature_at_completion");
            let total_comparisons: i32 = row.get("total_comparisons_completed");
            completion_map.insert(entity_id, (signature_at_completion, total_comparisons));
        }
    }

    // Build result map for all requested entities
    let mut result = HashMap::new();
    for entity_id in entity_ids {
        let current_sig = current_signatures.get(entity_id);
        let completion_info = completion_map.get(entity_id);
        
        let (is_complete, last_completion_signature, total_comparisons_completed) = match (current_sig, completion_info) {
            (Some(current), Some((completion_sig, total_comparisons))) => {
                (current == completion_sig, Some(completion_sig.clone()), Some(*total_comparisons))
            },
            _ => (false, completion_info.as_ref().map(|(sig, _)| sig.clone()), completion_info.as_ref().map(|(_, total)| *total)),
        };

        result.insert(entity_id.clone(), EntityCompletionCheck {
            entity_id: entity_id.clone(),
            is_complete,
            current_signature: current_sig.cloned(),
            last_completion_signature,
            total_comparisons_completed,
        });
    }

    debug!(
        "Checked completion status for {} entities, {} are complete for method {:?}",
        entity_ids.len(),
        result.values().filter(|check| check.is_complete).count(),
        method_type
    );

    Ok(result)
}

/// Marks a single entity as having completed all possible comparisons for a method type.
pub async fn mark_entity_completion(
    pool: &PgPool,
    entity_id: &str,
    method_type: &MatchMethodType,
    pipeline_run_id: &str,
    signature: &str,
    total_comparisons: i32,
) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for mark_entity_completion")?;

    let query = "
        INSERT INTO pipeline_state.entity_completion_status 
        (entity_id, method_type, pipeline_run_id, signature_at_completion, total_comparisons_completed)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (entity_id, method_type, signature_at_completion)
        DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            total_comparisons_completed = EXCLUDED.total_comparisons_completed,
            completed_at = CURRENT_TIMESTAMP";

    conn.execute(query, &[
        &entity_id,
        &method_type.as_str(),
        &pipeline_run_id,
        &signature,
        &total_comparisons,
    ]).await.context(
        "Failed to insert/update entity completion status in pipeline_state.entity_completion_status"
    )?;

    debug!("Marked entity {} as complete for {:?} with {} comparisons (signature: {})", 
           entity_id, method_type, total_comparisons, &signature[..8.min(signature.len())]);
    Ok(())
}

/// Marks multiple entities as having completed all possible comparisons for a method type.
/// This is more efficient for batch operations than calling mark_entity_completion multiple times.
pub async fn batch_mark_entity_completion(
    pool: &PgPool,
    completions: &[(String, MatchMethodType, String, String, i32)], // (entity_id, method_type, pipeline_run_id, signature, total_comparisons)
) -> Result<()> {
    if completions.is_empty() {
        return Ok(());
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_mark_entity_completion")?;

    let mut values_clause_parts = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut param_idx = 1;

    for (entity_id, method_type, pipeline_run_id, signature, total_comparisons) in completions {
        values_clause_parts.push(format!(
            "(${}, ${}, ${}, ${}, ${})",
            param_idx,
            param_idx + 1,
            param_idx + 2,
            param_idx + 3,
            param_idx + 4
        ));
        params.push(Box::new(entity_id.clone()));
        params.push(Box::new(method_type.as_str().to_string()));
        params.push(Box::new(pipeline_run_id.clone()));
        params.push(Box::new(signature.clone()));
        params.push(Box::new(*total_comparisons));
        param_idx += 5;
    }

    let values_clause = values_clause_parts.join(", ");

    let upsert_sql = format!(
        "INSERT INTO pipeline_state.entity_completion_status 
         (entity_id, method_type, pipeline_run_id, signature_at_completion, total_comparisons_completed)
         VALUES {}
         ON CONFLICT (entity_id, method_type, signature_at_completion)
         DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            total_comparisons_completed = EXCLUDED.total_comparisons_completed,
            completed_at = CURRENT_TIMESTAMP",
        values_clause
    );

    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    let rows_affected = conn.execute(upsert_sql.as_str(), params_slice.as_slice())
        .await
        .context("Failed to batch mark entity completions in pipeline_state.entity_completion_status")?;

    debug!(
        "Batch marked {} entity completions (affected {} rows)",
        completions.len(),
        rows_affected
    );

    Ok(())
}

/// Gets completion statistics for reporting and monitoring.
pub async fn get_completion_statistics(
    pool: &PgPool,
    method_type: Option<&MatchMethodType>,
    pipeline_run_id: Option<&str>,
) -> Result<HashMap<String, i64>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for get_completion_statistics")?;

    let mut query = "
        SELECT 
            method_type,
            COUNT(*) as total_completions,
            COUNT(DISTINCT entity_id) as unique_entities,
            AVG(total_comparisons_completed) as avg_comparisons,
            MAX(completed_at) as latest_completion
        FROM pipeline_state.entity_completion_status
        WHERE 1=1".to_string();

    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut param_idx = 1;

    if let Some(mt) = method_type {
        query.push_str(&format!(" AND method_type = ${}", param_idx));
        params.push(Box::new(mt.as_str().to_string()));
        param_idx += 1;
    }

    if let Some(run_id) = pipeline_run_id {
        query.push_str(&format!(" AND pipeline_run_id = ${}", param_idx));
        params.push(Box::new(run_id.to_string()));
    }

    query.push_str(" GROUP BY method_type ORDER BY method_type");

    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    let rows = conn.query(query.as_str(), params_slice.as_slice()).await.context(
        "Failed to query completion statistics from pipeline_state.entity_completion_status"
    )?;

    let mut result = HashMap::new();
    for row in rows {
        let method_type_str: String = row.get("method_type");
        let total_completions: i64 = row.get("total_completions");
        let unique_entities: i64 = row.get("unique_entities");
        
        result.insert(format!("{}_total_completions", method_type_str), total_completions);
        result.insert(format!("{}_unique_entities", method_type_str), unique_entities);
    }

    Ok(result)
}

// =============================================================================
// SIGNATURE FUNCTIONS
// =============================================================================

/// Retrieves the current signatures for a *batch* of entities.
/// Targets `pipeline_state.entity_data_signatures`.
/// Returns a HashMap where the key is entity_id and the value is EntitySignature.
pub async fn batch_get_current_signatures_for_pairs(
    pool: &PgPool,
    pairs: &[(String, String)], // Expects ordered pairs (e1_id, e2_id)
) -> Result<HashMap<(String, String), (EntitySignature, EntitySignature)>> {
    if pairs.is_empty() {
        return Ok(HashMap::new());
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_get_current_signatures_for_pairs")?;

    let mut result_map = HashMap::new();
    let mut all_entity_ids = Vec::new();
    let mut unique_entity_ids_set = std::collections::HashSet::new(); // Use a set to collect unique IDs

    for (e1, e2) in pairs {
        if unique_entity_ids_set.insert(e1.clone()) {
            all_entity_ids.push(e1.clone());
        }
        if unique_entity_ids_set.insert(e2.clone()) {
            all_entity_ids.push(e2.clone());
        }
    }

    // Correct table name: pipeline_state.entity_data_signatures
    let query = "SELECT entity_id, signature FROM pipeline_state.entity_data_signatures WHERE entity_id = ANY($1)";
    let rows = conn.query(query, &[&all_entity_ids]).await.context(
        "Failed to batch query current signatures from pipeline_state.entity_data_signatures",
    )?;

    let mut signature_lookup_map: HashMap<String, String> = HashMap::new();
    for row in rows {
        let entity_id_str: String = row.get("entity_id");
        let sig: String = row.get("signature");
        signature_lookup_map.insert(entity_id_str, sig);
    }

    for (e1, e2) in pairs {
        if let (Some(sig1), Some(sig2)) =
            (signature_lookup_map.get(e1), signature_lookup_map.get(e2))
        {
            result_map.insert(
                (e1.clone(), e2.clone()),
                (
                    EntitySignature {
                        entity_id: e1.clone(),
                        signature: sig1.clone(),
                    },
                    EntitySignature {
                        entity_id: e2.clone(),
                        signature: sig2.clone(),
                    },
                ),
            );
        } else {
            // Log which specific entity ID's signature was not found
            if signature_lookup_map.get(e1).is_none() {
                warn!("Signature not found for entity: {}", e1);
            }
            if signature_lookup_map.get(e2).is_none() {
                warn!("Signature not found for entity: {}", e2);
            }
        }
    }
    Ok(result_map)
}

/// Fetches current signatures for a pair of entities
/// Targets `pipeline_state.entity_data_signatures`.
pub async fn get_current_signatures_for_pair(
    pool: &PgPool,
    entity_id1: &String,
    entity_id2: &String,
) -> Result<Option<(EntitySignature, EntitySignature)>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for signatures")?;
    // Correct table name: pipeline_state.entity_data_signatures
    let query = "SELECT entity_id, signature FROM pipeline_state.entity_data_signatures WHERE entity_id = $1 OR entity_id = $2";

    let rows = conn
        .query(query, &[&entity_id1, &entity_id2])
        .await
        .context("Failed to query entity signatures")?;

    let mut sig_map: HashMap<String, String> = HashMap::new();
    for row in rows {
        sig_map.insert(row.get("entity_id"), row.get("signature"));
    }

    match (sig_map.get(entity_id1), sig_map.get(entity_id2)) {
        (Some(sig1), Some(sig2)) => Ok(Some((
            EntitySignature {
                entity_id: entity_id1.clone(),
                signature: sig1.clone(),
            },
            EntitySignature {
                entity_id: entity_id2.clone(),
                signature: sig2.clone(),
            },
        ))),
        _ => {
            debug!(
                "Signatures not found for one or both entities: {} (found: {}), {} (found: {})",
                entity_id1,
                sig_map.contains_key(entity_id1),
                entity_id2,
                sig_map.contains_key(entity_id2)
            );
            Ok(None)
        }
    }
}

// =============================================================================
// COMPARISON CACHE FUNCTIONS
// =============================================================================

/// Checks the comparison cache for a batch of pairs.
/// `pairs_with_signatures` should contain (ordered_e1_id, ordered_e2_id, sig1, sig2, method_type).
/// Targets `pipeline_state.comparison_cache`.
/// Returns a HashMap where the key is (e1_id, e2_id) (ordered) and the value is the ComparisonCacheEntry.
pub async fn batch_check_comparison_cache(
    pool: &PgPool,
    pairs_with_signatures: &[(String, String, String, String, MatchMethodType)],
) -> Result<HashMap<(String, String), ComparisonCacheEntry>> {
    if pairs_with_signatures.is_empty() {
        return Ok(HashMap::new());
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_check_comparison_cache")?;

    let mut result_map = HashMap::new();
    let mut where_clauses = Vec::new();
    // Changed to `dyn ToSql + Sync + Send`
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new(); // Use Box to own the values and ensure Send
    let mut param_idx = 1;

    for (e1, e2, sig1, sig2, method_type) in pairs_with_signatures {
        let comparison_type_str = get_comparison_type_string(method_type);
        where_clauses.push(format!(
            "(item_id_1 = ${} AND item_id_2 = ${} AND item_1_signature_at_comparison = ${} AND item_2_signature_at_comparison = ${} AND comparison_type = ${})",
            param_idx, param_idx + 1, param_idx + 2, param_idx + 3, param_idx + 4
        ));
        params.push(Box::new(e1.clone()));
        params.push(Box::new(e2.clone()));
        params.push(Box::new(sig1.clone()));
        params.push(Box::new(sig2.clone()));
        params.push(Box::new(comparison_type_str));
        param_idx += 5;
    }

    // Correct table name: pipeline_state.comparison_cache
    let query = format!(
        "SELECT item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison, method_type,
                pipeline_run_id, comparison_result, similarity_score, snapshotted_features
         FROM pipeline_state.comparison_cache
         WHERE {}",
        where_clauses.join(" OR ")
    );

    // Convert params Vec to Vec<&(dyn ToSql + Sync)> for query
    // The slice now correctly holds references to `dyn ToSql + Sync + Send`
    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();
    let rows = conn
        .query(query.as_str(), params_slice.as_slice())
        .await
        .context("Failed to batch query comparison cache from pipeline_state.comparison_cache")?;

    for row in rows {
        let entry = ComparisonCacheEntry {
            entity_id_1: row.get("item_id_1"),
            entity_id_2: row.get("item_id_2"),
            signature_1: row.get("item_1_signature_at_comparison"),
            signature_2: row.get("item_2_signature_at_comparison"),
            method_type: MatchMethodType::from_str(row.get("method_type")), // from_str returns Self, no unwrap_or
            pipeline_run_id: row.get("pipeline_run_id"),
            comparison_result: row.get("comparison_result"),
            similarity_score: row.get("similarity_score"),
            features_snapshot: row.get("snapshotted_features"),
        };
        result_map.insert(
            (entry.entity_id_1.clone(), entry.entity_id_2.clone()),
            entry,
        );
    }
    Ok(result_map)
}

/// Stores multiple comparison cache entries in a batch.
/// Targets `pipeline_state.comparison_cache`.
pub async fn batch_store_in_comparison_cache(
    pool: &PgPool,
    batch_data: Vec<ComparisonCacheEntry>,
) -> Result<()> {
    if batch_data.is_empty() {
        return Ok(());
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_store_in_comparison_cache")?;

    let mut values_clause_parts = Vec::new();
    // Changed to `dyn ToSql + Sync + Send`
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new(); // Use Box to own the values and ensure Send
    let mut param_idx = 1;

    for data in &batch_data {
        // Iterate by value to move ownership
        let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) =
            if data.entity_id_1 < data.entity_id_2 {
                (
                    data.entity_id_1.clone(),
                    data.entity_id_2.clone(),
                    data.signature_1.clone(),
                    data.signature_2.clone(),
                )
            } else {
                (
                    data.entity_id_2.clone(),
                    data.entity_id_1.clone(),
                    data.signature_2.clone(),
                    data.signature_1.clone(),
                )
            };
        let comparison_type_str = get_comparison_type_string(&data.method_type);
        let method_type_cache_str = data.method_type.as_str().to_string(); // Convert to owned String

        values_clause_parts.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, CURRENT_TIMESTAMP)",
            param_idx,
            param_idx + 1,
            param_idx + 2,
            param_idx + 3,
            param_idx + 4,
            param_idx + 5,
            param_idx + 6,
            param_idx + 7,
            param_idx + 8,
            param_idx + 9
        ));
        params.push(Box::new(item_id_1_ordered));
        params.push(Box::new(item_id_2_ordered));
        params.push(Box::new(item_1_sig_ordered));
        params.push(Box::new(item_2_sig_ordered));
        params.push(Box::new(comparison_type_str));
        params.push(Box::new(data.pipeline_run_id.clone()));
        params.push(Box::new(method_type_cache_str));
        params.push(Box::new(data.comparison_result.clone()));
        params.push(Box::new(data.similarity_score));
        // Directly pass the Option<JsonValue>, it will be handled correctly by the driver
        // Ensure Json wrapper is used for Option<JsonValue> to be correctly handled by ToSql
        params.push(Box::new(data.features_snapshot.clone().map(Json)));
        param_idx += 10;
    }

    let values_clause = values_clause_parts.join(", ");

    // Correct table name: pipeline_state.comparison_cache
    let upsert_sql = format!(
        "INSERT INTO pipeline_state.comparison_cache (
            item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison, comparison_type,
            pipeline_run_id, method_type, comparison_result, similarity_score, snapshotted_features, cached_at
         ) VALUES {}
         ON CONFLICT (item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison, comparison_type)
         DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            method_type = EXCLUDED.method_type,
            comparison_result = EXCLUDED.comparison_result,
            similarity_score = EXCLUDED.similarity_score,
            snapshotted_features = EXCLUDED.snapshotted_features,
            cached_at = CURRENT_TIMESTAMP", // Update timestamp on conflict as well
        values_clause
    );

    // Convert params Vec to Vec<&(dyn ToSql + Sync)> for query
    // The slice now correctly holds references to `dyn ToSql + Sync + Send`
    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();
    conn.execute(upsert_sql.as_str(), params_slice.as_slice())
        .await
        .context(
            "Failed to batch store comparison cache entries to pipeline_state.comparison_cache",
        )?;

    debug!(
        "Batch stored {} comparison cache entries.",
        batch_data.len()
    );
    Ok(())
}

/// Stores a comparison result in `pipeline_state.comparison_cache`.
pub async fn store_in_comparison_cache(
    pool: &PgPool,
    id1: &String,
    id2: &String,
    sig1_at_comparison: &str,
    sig2_at_comparison: &str,
    method_type_enum: &MatchMethodType,
    pipeline_run_id_str: &str,
    comparison_outcome: &str,
    final_similarity_score: Option<f64>,
    features_snapshot: Option<&JsonValue>,
) -> Result<()> {
    let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) =
        if id1 < id2 {
            (
                id1.clone(),
                id2.clone(),
                sig1_at_comparison,
                sig2_at_comparison,
            )
        } else {
            (
                id2.clone(),
                id1.clone(),
                sig2_at_comparison,
                sig1_at_comparison,
            )
        };
    let comparison_type_str = get_comparison_type_string(method_type_enum);
    let method_type_cache_str = method_type_enum.as_str();

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for cache store")?;
    // Correct table name: pipeline_state.comparison_cache
    let query = "
        INSERT INTO pipeline_state.comparison_cache
        (item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison,
         comparison_type, pipeline_run_id, method_type,
         comparison_result, similarity_score, snapshotted_features, cached_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
        ON CONFLICT (item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison, comparison_type)
        DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            method_type = EXCLUDED.method_type,
            comparison_result = EXCLUDED.comparison_result,
            similarity_score = EXCLUDED.similarity_score,
            snapshotted_features = EXCLUDED.snapshotted_features,
            cached_at = CURRENT_TIMESTAMP";

    let features_jsonb = features_snapshot.map(|jf| Json(jf.clone()));

    conn.execute(
        query,
        &[
            &item_id_1_ordered,
            &item_id_2_ordered,
            &item_1_sig_ordered,
            &item_2_sig_ordered,
            &comparison_type_str,
            &pipeline_run_id_str,
            &method_type_cache_str,
            &comparison_outcome,
            &final_similarity_score,
            &features_jsonb,
        ],
    )
    .await
    .context("Failed to insert/update comparison_cache")?;

    Ok(())
}

/// Checks the comparison cache for a single pair.
/// Targets `pipeline_state.comparison_cache`.
pub async fn check_comparison_cache(
    pool: &PgPool,
    id1: &String,
    id2: &String,
    sig1_at_comparison: &str,
    sig2_at_comparison: &str,
    method_type: &MatchMethodType,
) -> Result<Option<CachedComparisonResult>> {
    let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) =
        if id1 < id2 {
            (
                id1.clone(),
                id2.clone(),
                sig1_at_comparison,
                sig2_at_comparison,
            )
        } else {
            (
                id2.clone(),
                id1.clone(),
                sig2_at_comparison,
                sig1_at_comparison,
            )
        };
    let comparison_type_str = get_comparison_type_string(method_type);

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for cache check")?;
    // Correct table name: pipeline_state.comparison_cache
    let query = "
        SELECT comparison_result, similarity_score
        FROM pipeline_state.comparison_cache
        WHERE item_id_1 = $1 AND item_id_2 = $2
          AND item_1_signature_at_comparison = $3
          AND item_2_signature_at_comparison = $4
          AND comparison_type = $5";

    let row_opt = conn
        .query_opt(
            query,
            &[
                &item_id_1_ordered,
                &item_id_2_ordered,
                &item_1_sig_ordered,
                &item_2_sig_ordered,
                &comparison_type_str,
            ],
        )
        .await
        .context("Failed to query comparison_cache")?;

    if let Some(row) = row_opt {
        Ok(Some(CachedComparisonResult {
            comparison_result: row.get("comparison_result"),
            similarity_score: row.get("similarity_score"),
        }))
    } else {
        Ok(None)
    }
}