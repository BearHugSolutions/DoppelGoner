// src/pipeline_state_utils.rs
use crate::{
    db::PgPool,
    models::{EntityId, MatchMethodType},
};
use anyhow::{Context, Result};
use log::{debug, error, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tokio_postgres::types::{Json, ToSql}; // Added error and warn

#[derive(Debug, Clone)]
pub struct EntitySignature {
    pub entity_id: EntityId,
    pub signature: String,
    // pub source_data_last_updated_at: Option<chrono::NaiveDateTime>,
}

#[derive(Debug)]
pub struct CachedComparisonResult {
    pub comparison_result: String,
    pub similarity_score: Option<f64>,
}

/// Represents the data needed to insert one row into the comparison_cache.
#[derive(Debug, Clone)]
pub struct CacheEntryData {
    pub id1: EntityId,
    pub id2: EntityId,
    pub sig1: String,
    pub sig2: String,
    pub method_type: MatchMethodType,
    pub pipeline_run_id: String,
    pub comparison_outcome: String,
    pub similarity_score: Option<f64>,
    pub features_snapshot: Option<JsonValue>,
}

// Constant to prefix comparison types in the cache for entity-entity comparisons
const COMPARISON_TYPE_ENTITY_PREFIX: &str = "ENTITY_";

// Configuration for the backfill process
pub const BATCH_SIZE_PAIRS: usize = 1000; // How many pairs to process in one logical batch
pub const MAX_CONCURRENT_DB_OPS: usize = 5; // Max parallel BATCH tasks

/// Fetches current signatures for a *batch* of entities from `pipeline_state.entity_data_signatures`.
pub async fn get_signatures_for_batch(
    pool: &PgPool,
    entity_ids: &[EntityId],
) -> Result<HashMap<EntityId, EntitySignature>> {
    if entity_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch signatures")?;
    let query = "SELECT entity_id, signature FROM pipeline_state.entity_data_signatures WHERE entity_id = ANY($1)";

    // Convert EntityId Vec to Vec<String> for the query parameter
    let id_strings: Vec<String> = entity_ids.iter().map(|id| id.0.clone()).collect();

    let rows = conn
        .query(query, &[&id_strings])
        .await
        .context("Failed to query batch entity signatures")?;

    let mut signatures = HashMap::with_capacity(rows.len());
    for row in rows {
        let entity_id_str: String = row.get("entity_id");
        let sig: String = row.get("signature");
        let entity_id = EntityId(entity_id_str);
        signatures.insert(
            entity_id.clone(),
            EntitySignature {
                entity_id,
                signature: sig,
            },
        );
    }

    // Warn about missing signatures (optional but helpful)
    if signatures.len() != entity_ids.len() {
        for id in entity_ids {
            if !signatures.contains_key(id) {
                warn!("Signature not found for entity: {}", id.0);
            }
        }
    }

    Ok(signatures)
}

/// Stores a *batch* of comparison results in `pipeline_state.comparison_cache` using UNNEST.
/// Ensures item_id_1 < item_id_2 when inserting.
pub async fn store_batch_in_comparison_cache(
    pool: &PgPool,
    batch_data: &[CacheEntryData],
) -> Result<()> {
    if batch_data.is_empty() {
        return Ok(());
    }

    let mut item_id_1_vec = Vec::with_capacity(batch_data.len());
    let mut item_id_2_vec = Vec::with_capacity(batch_data.len());
    let mut item_1_sig_vec = Vec::with_capacity(batch_data.len());
    let mut item_2_sig_vec = Vec::with_capacity(batch_data.len());
    let mut comparison_type_vec = Vec::with_capacity(batch_data.len());
    let mut pipeline_run_id_vec = Vec::with_capacity(batch_data.len());
    let mut method_type_vec = Vec::with_capacity(batch_data.len());
    let mut comparison_result_vec = Vec::with_capacity(batch_data.len());
    let mut similarity_score_vec = Vec::with_capacity(batch_data.len());
    let mut features_snapshot_vec = Vec::with_capacity(batch_data.len());

    for entry in batch_data {
        let (id1, id2, sig1, sig2) = if entry.id1.0 < entry.id2.0 {
            (&entry.id1.0, &entry.id2.0, &entry.sig1, &entry.sig2)
        } else {
            (&entry.id2.0, &entry.id1.0, &entry.sig2, &entry.sig1)
        };
        let comparison_type_str = format!(
            "{}{}",
            COMPARISON_TYPE_ENTITY_PREFIX,
            entry.method_type.as_str().to_uppercase()
        );

        item_id_1_vec.push(id1.clone());
        item_id_2_vec.push(id2.clone());
        item_1_sig_vec.push(sig1.clone());
        item_2_sig_vec.push(sig2.clone());
        comparison_type_vec.push(comparison_type_str);
        pipeline_run_id_vec.push(entry.pipeline_run_id.clone());
        method_type_vec.push(entry.method_type.as_str().to_string());
        comparison_result_vec.push(entry.comparison_outcome.clone());
        similarity_score_vec.push(entry.similarity_score);
        features_snapshot_vec.push(entry.features_snapshot.as_ref().map(|jf| Json(jf.clone())));
    }

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch cache store")?;

    // NOTE: This query requires PostgreSQL 9.4+ for UNNEST with multiple arrays.
    // It also assumes a unique constraint exists on:
    // (item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison, comparison_type)
    let query = "
        INSERT INTO pipeline_state.comparison_cache
        (item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison,
         comparison_type, pipeline_run_id, method_type,
         comparison_result, similarity_score, snapshotted_features, cached_at)
        SELECT
            unnest($1::TEXT[]), unnest($2::TEXT[]), unnest($3::TEXT[]), unnest($4::TEXT[]),
            unnest($5::TEXT[]), unnest($6::TEXT[]), unnest($7::TEXT[]),
            unnest($8::TEXT[]), unnest($9::FLOAT8[]), unnest($10::JSONB[]), CURRENT_TIMESTAMP
        ON CONFLICT (item_id_1, item_id_2, item_1_signature_at_comparison, item_2_signature_at_comparison, comparison_type)
        DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            method_type = EXCLUDED.method_type,
            comparison_result = EXCLUDED.comparison_result,
            similarity_score = EXCLUDED.similarity_score,
            snapshotted_features = EXCLUDED.snapshotted_features,
            cached_at = CURRENT_TIMESTAMP";

    let params: &[&(dyn ToSql + Sync)] = &[
        &item_id_1_vec,
        &item_id_2_vec,
        &item_1_sig_vec,
        &item_2_sig_vec,
        &comparison_type_vec,
        &pipeline_run_id_vec,
        &method_type_vec,
        &comparison_result_vec,
        &similarity_score_vec,
        &features_snapshot_vec,
    ];

    match conn.execute(query, params).await {
        Ok(count) => {
            debug!("Stored/Updated {} rows in comparison_cache batch.", count);
            Ok(())
        }
        Err(e) => {
            error!("Failed to insert/update comparison_cache batch: {}", e);
            // Optionally, try inserting one-by-one here for debugging,
            // or just return the error.
            Err(e).context("Failed to execute batch insert/update")
        }
    }
}

// --- Keep old functions if they are used elsewhere, or remove them ---

/// Fetches current signatures for a pair of entities
pub async fn get_current_signatures_for_pair(
    pool: &PgPool,
    entity_id1: &EntityId,
    entity_id2: &EntityId,
) -> Result<Option<(EntitySignature, EntitySignature)>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for signatures")?;
    let query = "SELECT entity_id, signature FROM pipeline_state.entity_data_signatures WHERE entity_id = $1 OR entity_id = $2";

    let rows = conn
        .query(query, &[&entity_id1.0, &entity_id2.0])
        .await
        .context("Failed to query entity signatures")?;

    let mut sig_map: HashMap<String, String> = HashMap::new();
    for row in rows {
        sig_map.insert(row.get("entity_id"), row.get("signature"));
    }

    match (sig_map.get(&entity_id1.0), sig_map.get(&entity_id2.0)) {
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
                entity_id1.0,
                sig_map.contains_key(&entity_id1.0),
                entity_id2.0,
                sig_map.contains_key(&entity_id2.0)
            );
            Ok(None)
        }
    }
}

/// Stores a comparison result in `pipeline_state.comparison_cache`.
pub async fn store_in_comparison_cache(
    pool: &PgPool,
    id1: &EntityId,
    id2: &EntityId,
    sig1_at_comparison: &str,
    sig2_at_comparison: &str,
    method_type_enum: &MatchMethodType,
    pipeline_run_id_str: &str,
    comparison_outcome: &str,
    final_similarity_score: Option<f64>,
    features_snapshot: Option<&JsonValue>,
) -> Result<()> {
    let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) =
        if id1.0 < id2.0 {
            (&id1.0, &id2.0, sig1_at_comparison, sig2_at_comparison)
        } else {
            (&id2.0, &id1.0, sig2_at_comparison, sig1_at_comparison)
        };
    let comparison_type_str = format!(
        "{}{}",
        COMPARISON_TYPE_ENTITY_PREFIX,
        method_type_enum.as_str().to_uppercase()
    );
    let method_type_cache_str = method_type_enum.as_str();

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for cache store")?;
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

// --- check_comparison_cache remains unchanged ---
pub async fn check_comparison_cache(
    pool: &PgPool,
    id1: &EntityId,
    id2: &EntityId,
    sig1_at_comparison: &str,
    sig2_at_comparison: &str,
    method_type: &MatchMethodType,
) -> Result<Option<CachedComparisonResult>> {
    let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) =
        if id1.0 < id2.0 {
            (&id1.0, &id2.0, sig1_at_comparison, sig2_at_comparison)
        } else {
            (&id2.0, &id1.0, sig2_at_comparison, sig1_at_comparison)
        };
    let comparison_type_str = format!(
        "{}{}",
        COMPARISON_TYPE_ENTITY_PREFIX,
        method_type.as_str().to_uppercase()
    );

    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for cache check")?;
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
