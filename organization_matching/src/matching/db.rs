// src/matching/db.rs - Enhanced version with better error handling and logging
use anyhow::{Context, Result};
use log::{debug, warn, error, info};
use tokio_postgres::types::{Json, ToSql};
use uuid::Uuid;
use std::collections::HashMap;

use crate::{
    models::matching::MatchValues,
    models::stats_models::MatchMethodType,
    utils::db_connect::PgPool,
};

/// Data structure for a single entity group upsert operation in a batch.
pub struct EntityGroupBatchData {
    pub proposed_id: String,
    pub entity_id_1: String,
    pub entity_id_2: String,
    pub confidence_score: f64,
    pub pre_rl_confidence_score: f64,
    pub method_type: MatchMethodType,
    pub match_values: MatchValues,
}

/// Data structure for a single match decision detail insert operation in a batch.
pub struct MatchDecisionDetailBatchData {
    pub entity_group_id: String,
    pub pipeline_run_id: String,
    pub snapshotted_features: serde_json::Value,
    pub method_type_at_decision: String,
    pub pre_rl_confidence_at_decision: f64,
    pub tuned_confidence_at_decision: f64,
    pub confidence_tuner_version_at_decision: Option<i32>,
}

/// Upserts an entity group. If it exists, it's updated. If not, it's inserted.
/// Returns the ID of the entity group and a boolean indicating if a new group was inserted.
pub async fn upsert_entity_group(
    pool: &PgPool,
    proposed_id: &str,
    entity_id_1: &str,
    entity_id_2: &str,
    confidence_score: f64,
    pre_rl_confidence_score: f64,
    method_type: MatchMethodType,
    match_values: MatchValues,
) -> Result<(String, bool), anyhow::Error> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for upsert_entity_group")?;

    let (e1, e2) = if entity_id_1 < entity_id_2 {
        (entity_id_1, entity_id_2)
    } else {
        (entity_id_2, entity_id_1)
    };

    let match_values_json = serde_json::to_value(match_values)
        .context("Failed to serialize match values for entity_group")?;

    debug!(
        "[{:?}] Attempting upsert for pair ({}, {})",
        method_type, e1, e2
    );

    let existing_row = conn
        .query_opt(
            "SELECT id, confirmed_status FROM public.entity_group
             WHERE entity_id_1 = $1 AND entity_id_2 = $2 AND method_type = $3",
            &[&e1, &e2, &method_type.as_str()],
        )
        .await
        .context("Failed to check existing entity_group")?;

    if let Some(row) = existing_row {
        let existing_id: String = row.get("id");
        let existing_status: Option<String> = row.get("confirmed_status");

        let new_status = match existing_status.as_deref() {
            Some("CONFIRMED") | Some("REJECTED") => existing_status.unwrap(),
            _ => "PENDING_REVIEW".to_string(),
        };

        conn.execute(
            "UPDATE public.entity_group
             SET confidence_score = $1, match_values = $2, pre_rl_confidence_score = $3,
                 confirmed_status = $4, updated_at = CURRENT_TIMESTAMP
             WHERE id = $5",
            &[
                &confidence_score,
                &match_values_json,
                &pre_rl_confidence_score,
                &new_status,
                &existing_id,
            ],
        )
        .await
        .context(format!("Failed to update entity_group id {}", existing_id))?;
        
        debug!("[{:?}] Updated existing entity_group id {}", method_type, existing_id);
        Ok((existing_id, false))
    } else {
        conn.execute(
            "INSERT INTO public.entity_group
             (id, entity_id_1, entity_id_2, confidence_score, method_type, match_values,
              pre_rl_confidence_score, confirmed_status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            &[
                &proposed_id,
                &e1,
                &e2,
                &confidence_score,
                &method_type.as_str(),
                &match_values_json,
                &pre_rl_confidence_score,
            ],
        )
        .await
        .context(format!("Failed to insert entity_group id {}", proposed_id))?;
        
        info!("[{:?}] Inserted new entity_group id {}", method_type, proposed_id);
        Ok((proposed_id.to_string(), true))
    }
}

/// Performs a batch upsert for multiple entity groups.
/// Uses INSERT ... ON CONFLICT (entity_id_1, entity_id_2, method_type) DO UPDATE.
/// Returns a HashMap where the key is (entity_id_1, entity_id_2) (ordered)
/// and the value is (group_id, was_newly_inserted: bool).
pub async fn batch_upsert_entity_groups(
    pool: &PgPool,
    batch_data: Vec<EntityGroupBatchData>,
) -> Result<HashMap<(String, String), (String, bool)>, anyhow::Error> {
    if batch_data.is_empty() {
        return Ok(HashMap::new());
    }

    let method_types: Vec<_> = batch_data.iter().map(|d| d.method_type.clone()).collect();
    let unique_methods: std::collections::HashSet<_> = method_types.iter().cloned().collect();
    
    info!(
        "Batch upsert for {} entity groups with methods: {:?}",
        batch_data.len(),
        unique_methods
    );

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_upsert_entity_groups")?;

    // Start a transaction for atomicity
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch upsert")?;

    let mut values_clause_parts = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut param_idx = 1;

    // Map to keep track of the original ordered pair for mapping results
    let mut ordered_pair_to_proposed_id = HashMap::new();

    for data in &batch_data {
        let (e1_ordered, e2_ordered) = if data.entity_id_1 < data.entity_id_2 {
            (data.entity_id_1.clone(), data.entity_id_2.clone())
        } else {
            (data.entity_id_2.clone(), data.entity_id_1.clone())
        };

        let match_values_json = serde_json::to_value(&data.match_values)
            .context("Failed to serialize match values for batch entity_group")?;

        values_clause_parts.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, 'PENDING_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            param_idx,
            param_idx + 1,
            param_idx + 2,
            param_idx + 3,
            param_idx + 4,
            param_idx + 5,
            param_idx + 6
        ));

        params.push(Box::new(data.proposed_id.clone()));
        params.push(Box::new(e1_ordered.clone()));
        params.push(Box::new(e2_ordered.clone()));
        params.push(Box::new(data.confidence_score));
        params.push(Box::new(data.method_type.as_str().to_string()));
        params.push(Box::new(match_values_json));
        params.push(Box::new(data.pre_rl_confidence_score));

        ordered_pair_to_proposed_id.insert((e1_ordered, e2_ordered), data.proposed_id.clone());
        param_idx += 7;
    }

    let values_clause = values_clause_parts.join(", ");

    let upsert_sql = format!(
        "INSERT INTO public.entity_group (
            id, entity_id_1, entity_id_2, confidence_score, method_type, match_values,
            pre_rl_confidence_score, confirmed_status, created_at, updated_at
         ) VALUES {}
         ON CONFLICT (entity_id_1, entity_id_2, method_type) DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            match_values = EXCLUDED.match_values,
            pre_rl_confidence_score = EXCLUDED.pre_rl_confidence_score,
            confirmed_status = CASE 
                WHEN entity_group.confirmed_status IN ('CONFIRMED', 'REJECTED') 
                THEN entity_group.confirmed_status 
                ELSE 'PENDING_REVIEW' 
            END,
            updated_at = CURRENT_TIMESTAMP
         RETURNING id, entity_id_1, entity_id_2, method_type, (xmax = 0) as was_inserted",
        values_clause
    );

    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    debug!("Executing batch upsert with {} parameters", params_slice.len());

    let rows = transaction
        .query(upsert_sql.as_str(), params_slice.as_slice())
        .await
        .map_err(|e| {
            error!("Batch upsert SQL error: {}", e);
            e
        })
        .context("Failed to execute batch upsert for entity_group")?;

    let mut results_map = HashMap::new();
    let mut insert_count = 0;
    let mut update_count = 0;

    for row in rows {
        let group_id: String = row.get("id");
        let e1: String = row.get("entity_id_1");
        let e2: String = row.get("entity_id_2");
        let method: String = row.get("method_type");
        let was_inserted: bool = row.get("was_inserted");
        
        if was_inserted {
            insert_count += 1;
            debug!("[{}] Inserted new group: {} <-> {}", method, &e1[..8], &e2[..8]);
        } else {
            update_count += 1;
            debug!("[{}] Updated existing group: {} <-> {}", method, &e1[..8], &e2[..8]);
        }
        
        results_map.insert((e1, e2), (group_id, was_inserted));
    }

    // Commit the transaction
    transaction
        .commit()
        .await
        .context("Failed to commit batch upsert transaction")?;

    info!(
        "Batch upsert completed: {} inserts, {} updates (total: {})",
        insert_count,
        update_count,
        batch_data.len()
    );

    // Verify that all expected pairs were processed
    if results_map.len() != batch_data.len() {
        warn!(
            "Batch upsert result count mismatch: expected {}, got {}",
            batch_data.len(),
            results_map.len()
        );
        
        // Log which pairs might be missing
        for data in &batch_data {
            let (e1, e2) = if data.entity_id_1 < data.entity_id_2 {
                (&data.entity_id_1, &data.entity_id_2)
            } else {
                (&data.entity_id_2, &data.entity_id_1)
            };
            
            if !results_map.contains_key(&(e1.clone(), e2.clone())) {
                warn!(
                    "[{:?}] Missing result for pair: {} <-> {}",
                    data.method_type, e1, e2
                );
            }
        }
    }

    Ok(results_map)
}

/// Inserts an entity match decision detail record directly.
pub async fn insert_match_decision_detail_direct(
    pool: &PgPool,
    entity_group_id: &str,
    pipeline_run_id: &str,
    snapshotted_features: serde_json::Value,
    method_type_at_decision: &str,
    pre_rl_confidence_at_decision: f64,
    tuned_confidence_at_decision: f64,
    confidence_tuner_version_at_decision: Option<i32>,
) -> Result<Uuid, anyhow::Error> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for insert_match_decision_detail_direct")?;
    
    const INSERT_SQL: &str = "
        INSERT INTO clustering_metadata.match_decision_details (
            entity_group_id, pipeline_run_id, snapshotted_features,
            method_type_at_decision, pre_rl_confidence_at_decision,
            tuned_confidence_at_decision, confidence_tuner_version_at_decision
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id";

    let row = conn
        .query_one(
            INSERT_SQL,
            &[
                &entity_group_id,
                &pipeline_run_id,
                &snapshotted_features,
                &method_type_at_decision,
                &pre_rl_confidence_at_decision,
                &tuned_confidence_at_decision,
                &confidence_tuner_version_at_decision,
            ],
        )
        .await
        .context("Failed to insert match_decision_detail")?;

    let id_str: String = row.get(0);
    let id = Uuid::parse_str(&id_str)
        .context("Failed to parse returned ID string as UUID for match_decision_detail")?;
    
    debug!(
        "[{}] Inserted match_decision_detail id {} for group {}",
        method_type_at_decision, id, entity_group_id
    );
    
    Ok(id)
}

/// Performs a batch insert for multiple match decision detail records.
pub async fn batch_insert_match_decision_details(
    pool: &PgPool,
    batch_data: Vec<MatchDecisionDetailBatchData>,
) -> Result<(), anyhow::Error> {
    if batch_data.is_empty() {
        return Ok(());
    }

    let method_types: Vec<_> = batch_data
        .iter()
        .map(|d| d.method_type_at_decision.clone())
        .collect();
    let unique_methods: std::collections::HashSet<_> = method_types.iter().cloned().collect();
    
    info!(
        "Batch insert for {} match decision details with methods: {:?}",
        batch_data.len(),
        unique_methods
    );

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch_insert_match_decision_details")?;

    // Start a transaction
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch insert decision details")?;

    let mut values_clause_parts = Vec::new();
    let mut params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
    let mut param_idx = 1;

    for data in &batch_data {
        values_clause_parts.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
            param_idx,
            param_idx + 1,
            param_idx + 2,
            param_idx + 3,
            param_idx + 4,
            param_idx + 5,
            param_idx + 6
        ));

        params.push(Box::new(data.entity_group_id.clone()));
        params.push(Box::new(data.pipeline_run_id.clone()));
        params.push(Box::new(data.snapshotted_features.clone()));
        params.push(Box::new(data.method_type_at_decision.clone()));
        params.push(Box::new(data.pre_rl_confidence_at_decision));
        params.push(Box::new(data.tuned_confidence_at_decision));
        params.push(Box::new(data.confidence_tuner_version_at_decision));
        param_idx += 7;
    }

    let values_clause = values_clause_parts.join(", ");

    let insert_sql = format!(
        "INSERT INTO clustering_metadata.match_decision_details (
            entity_group_id, pipeline_run_id, snapshotted_features,
            method_type_at_decision, pre_rl_confidence_at_decision,
            tuned_confidence_at_decision, confidence_tuner_version_at_decision
         ) VALUES {}",
        values_clause
    );

    let params_slice: Vec<&(dyn ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn ToSql + Sync))
        .collect();

    debug!(
        "Executing batch insert for match_decision_details with {} parameters",
        params_slice.len()
    );

    let rows_affected = transaction
        .execute(insert_sql.as_str(), params_slice.as_slice())
        .await
        .map_err(|e| {
            error!("Batch insert match_decision_details SQL error: {}", e);
            e
        })
        .context("Failed to execute batch insert for match_decision_details")?;

    // Commit the transaction
    transaction
        .commit()
        .await
        .context("Failed to commit batch insert decision details transaction")?;

    info!(
        "Batch inserted {} match decision details (expected: {})",
        rows_affected, batch_data.len()
    );

    if rows_affected as usize != batch_data.len() {
        warn!(
            "Batch insert row count mismatch: expected {}, inserted {}",
            batch_data.len(),
            rows_affected
        );
    }

    Ok(())
}