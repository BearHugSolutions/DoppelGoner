// src/pipeline_state_utils.rs
use anyhow::{Context, Result};
use std::collections::HashMap;
use tokio_postgres::types::Json;
use crate::{
    db::PgPool,
    models::{EntityId, MatchMethodType},
};
use serde_json::Value as JsonValue;
use log::debug; // Added for logging

#[derive(Debug, Clone)]
pub struct EntitySignature {
    pub entity_id: EntityId,
    pub signature: String,
    // pub source_data_last_updated_at: Option<chrono::NaiveDateTime>, // Or appropriate DateTime type
}

#[derive(Debug)]
pub struct CachedComparisonResult {
    pub comparison_result: String, // e.g., "MATCH", "NON_MATCH"
    pub similarity_score: Option<f64>,
    // pub snapshotted_features: Option<JsonValue>, // If needed later
}

// Constant to prefix comparison types in the cache for entity-entity comparisons
const COMPARISON_TYPE_ENTITY_PREFIX: &str = "ENTITY_";

/// Fetches current signatures for a pair of entities from `pipeline_state.entity_data_signatures`.
pub async fn get_current_signatures_for_pair(
    pool: &PgPool,
    entity_id1: &EntityId,
    entity_id2: &EntityId,
) -> Result<Option<(EntitySignature, EntitySignature)>> {
    let conn = pool.get().await.context("Failed to get DB connection for signatures")?;
    let query = "SELECT entity_id, signature FROM pipeline_state.entity_data_signatures WHERE entity_id = $1 OR entity_id = $2";
    
    let rows = conn.query(query, &[&entity_id1.0, &entity_id2.0]).await
        .context("Failed to query entity signatures")?;

    let mut sig_map: HashMap<String, String> = HashMap::new();
    for row in rows {
        let id: String = row.get("entity_id");
        let sig: String = row.get("signature");
        sig_map.insert(id, sig);
    }

    match (sig_map.get(&entity_id1.0), sig_map.get(&entity_id2.0)) {
        (Some(sig1), Some(sig2)) => Ok(Some((
            EntitySignature { entity_id: entity_id1.clone(), signature: sig1.clone() },
            EntitySignature { entity_id: entity_id2.clone(), signature: sig2.clone() },
        ))),
        _ => {
            debug!("Signatures not found for one or both entities: {} (found: {}), {} (found: {})", 
                   entity_id1.0, sig_map.contains_key(&entity_id1.0), 
                   entity_id2.0, sig_map.contains_key(&entity_id2.0));
            Ok(None) // One or both signatures not found
        }
    }
}

/// Checks `pipeline_state.comparison_cache` for a result.
/// Ensures item_id_1 < item_id_2 when querying.
pub async fn check_comparison_cache(
    pool: &PgPool,
    id1: &EntityId,
    id2: &EntityId,
    sig1_at_comparison: &str,
    sig2_at_comparison: &str,
    method_type: &MatchMethodType, // Used to construct comparison_type
) -> Result<Option<CachedComparisonResult>> {
    let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) = if id1.0 < id2.0 {
        (&id1.0, &id2.0, sig1_at_comparison, sig2_at_comparison)
    } else {
        (&id2.0, &id1.0, sig2_at_comparison, sig1_at_comparison)
    };
    let comparison_type_str = format!("{}{}", COMPARISON_TYPE_ENTITY_PREFIX, method_type.as_str().to_uppercase());

    let conn = pool.get().await.context("Failed to get DB connection for cache check")?;
    let query = "
        SELECT comparison_result, similarity_score
        FROM pipeline_state.comparison_cache
        WHERE item_id_1 = $1 AND item_id_2 = $2
          AND item_1_signature_at_comparison = $3
          AND item_2_signature_at_comparison = $4
          AND comparison_type = $5";
    
    let row_opt = conn.query_opt(query, &[
        &item_id_1_ordered, 
        &item_id_2_ordered, 
        &item_1_sig_ordered, 
        &item_2_sig_ordered, 
        &comparison_type_str
    ]).await.context("Failed to query comparison_cache")?;

    if let Some(row) = row_opt {
        Ok(Some(CachedComparisonResult {
            comparison_result: row.get("comparison_result"),
            similarity_score: row.get("similarity_score"),
        }))
    } else {
        Ok(None)
    }
}

/// Stores a comparison result in `pipeline_state.comparison_cache`.
/// Ensures item_id_1 < item_id_2 when inserting.
pub async fn store_in_comparison_cache(
    pool: &PgPool,
    id1: &EntityId,
    id2: &EntityId,
    sig1_at_comparison: &str, 
    sig2_at_comparison: &str, 
    method_type_enum: &MatchMethodType, // The specific matching method (e.g., Address, Email)
    pipeline_run_id_str: &str,
    comparison_outcome: &str, // "MATCH" or "NON_MATCH"
    final_similarity_score: Option<f64>,
    features_snapshot: Option<&JsonValue>, // Snapshotted features from match_decision_details
) -> Result<()> {
    let (item_id_1_ordered, item_id_2_ordered, item_1_sig_ordered, item_2_sig_ordered) = if id1.0 < id2.0 {
        (&id1.0, &id2.0, sig1_at_comparison, sig2_at_comparison)
    } else {
        (&id2.0, &id1.0, sig2_at_comparison, sig1_at_comparison)
    };
    let comparison_type_str = format!("{}{}", COMPARISON_TYPE_ENTITY_PREFIX, method_type_enum.as_str().to_uppercase());
    let method_type_cache_str = method_type_enum.as_str(); // Store the original method type string

    let conn = pool.get().await.context("Failed to get DB connection for cache store")?;
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

    conn.execute(query, &[
        &item_id_1_ordered, &item_id_2_ordered, &item_1_sig_ordered, &item_2_sig_ordered,
        &comparison_type_str, &pipeline_run_id_str, &method_type_cache_str,
        &comparison_outcome, &final_similarity_score, &features_jsonb
    ]).await.context("Failed to insert/update comparison_cache")?;

    Ok(())
}