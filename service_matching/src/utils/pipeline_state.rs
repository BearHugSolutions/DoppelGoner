// src/utils/pipeline_state.rs
use crate::models::matching::MatchMethodType;
use crate::utils::db_connect::PgPool;
use anyhow::{Context, Result};
use log::{debug, warn};
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
            snapshotted_features = EXCLUDED.features_snapshot,
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

// --- Keep old functions if they are used elsewhere, or remove them ---
// (These original functions should ideally not be called directly in the refactored matching pipeline)

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
            snapshotted_features = EXCLUDED.features_snapshot,
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

// --- check_comparison_cache remains unchanged from its original correct form ---
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
