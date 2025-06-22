use anyhow::Context;
use log::{debug, info};
use postgres_types::ToSql;
use uuid::Uuid;

use crate::models::matching::NewServiceGroup;
use crate::utils::db_connect::PgPool;
use crate::models::matching::ServiceMatch;
use serde_json::json;
use indicatif::{ProgressBar, ProgressStyle};

/// NEW: Batch data structure for service match decision details (RL logging)
#[derive(Debug, Clone)]
pub struct ServiceMatchDecisionDetailBatchData {
    pub service_group_id: String,
    pub pipeline_run_id: String,
    pub snapshotted_features: serde_json::Value,
    pub method_type_at_decision: String,
    pub pre_rl_confidence_at_decision: f64,
    pub tuned_confidence_at_decision: f64,
    pub confidence_tuner_version_at_decision: Option<i32>,
}

/// Phase 4: Batch insert service groups using optimized batch upsert with RL decision logging
pub async fn insert_service_groups_batch(
    pool: &PgPool,
    matches: Vec<ServiceMatch>,
    pipeline_run_id: &str,
    // NEW: RL-related data for decision logging
    rl_decision_data: Option<Vec<ServiceMatchDecisionDetailBatchData>>,
) -> Result<(), anyhow::Error> {
    if matches.is_empty() {
        info!("No service matches to insert.");
        return Ok(());
    }

    info!("Inserting {} service matches using batch upsert (with RL decision logging: {})...", 
          matches.len(), rl_decision_data.is_some());
    let pb = ProgressBar::new(matches.len() as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .context("Failed to set progress bar style for insert")?
        .progress_chars("#>-"));
    pb.set_message("Batch upserting matches...");

    // Convert ServiceMatch to NewServiceGroup for batch upsert
    let new_service_groups: Vec<NewServiceGroup> = matches.iter().map(|service_match| {
        NewServiceGroup {
            proposed_id: Uuid::new_v4().to_string(),
            service_id_1: service_match.service_id_1.clone(),
            service_id_2: service_match.service_id_2.clone(),
            confidence_score: service_match.confidence_score,
            pre_rl_confidence_score: service_match.confidence_score, // Using same value for now, will be overridden by RL data
            method_type: service_match.match_method.clone(),
            match_values: service_match.match_values.clone(),
        }
    }).collect();

    // Batch upsert service groups
    let inserted_ids = upsert_service_groups_batch(pool, &new_service_groups).await
        .context("Failed to batch upsert service groups")?;

    pb.set_length((matches.len() + inserted_ids.len()) as u64);
    pb.set_position(matches.len() as u64);
    pb.set_message("Creating decision details...");

    let had_rl_data = rl_decision_data.is_some();

    // NEW: Prepare service match decision details with RL data if available
    let decision_details: Vec<(String, String, serde_json::Value, String, f64, f64, Option<i32>)> = 
        if let Some(rl_data) = rl_decision_data {
            // Use RL decision data if available
            inserted_ids.iter().zip(rl_data.iter()).map(|(group_id, rl_decision)| {
                (
                    group_id.clone(),
                    rl_decision.pipeline_run_id.clone(),
                    rl_decision.snapshotted_features.clone(),
                    rl_decision.method_type_at_decision.clone(),
                    rl_decision.pre_rl_confidence_at_decision,
                    rl_decision.tuned_confidence_at_decision,
                    rl_decision.confidence_tuner_version_at_decision
                )
            }).collect()
        } else {
            // Fallback to basic decision details without RL features
            inserted_ids.iter().zip(matches.iter()).map(|(group_id, service_match)| {
                let features_json = json!({
                    "method_type": service_match.match_method.as_str(),
                    "pipeline_run_id": pipeline_run_id,
                    "service_id_1": service_match.service_id_1,
                    "service_id_2": service_match.service_id_2,
                    "note": "No RL features available"
                });
                (
                    group_id.clone(),
                    pipeline_run_id.to_string(),
                    features_json,
                    service_match.match_method.as_str().to_string(),
                    service_match.confidence_score,
                    service_match.confidence_score,
                    None
                )
            }).collect()
        };

    // Batch insert decision details
    let _detail_ids = insert_service_match_decision_details_batch(pool, &decision_details).await
        .context("Failed to batch insert service match decision details")?;

    pb.set_position((matches.len() + inserted_ids.len()) as u64);
    pb.finish_with_message("Batch insert complete.");
    
    info!("Successfully inserted {} service groups and {} decision details (RL enabled: {})", 
          inserted_ids.len(), _detail_ids.len(), had_rl_data);
    Ok(())
}

/// Batch upsert service groups. If they exist, they're updated. If not, they're inserted.
/// Returns a vector of the service group IDs (either existing or newly inserted).
/// Processes the batch using PostgreSQL's ON CONFLICT functionality for efficient upserts.
pub async fn upsert_service_groups_batch(
    pool: &PgPool,
    service_groups: &[NewServiceGroup],
) -> Result<Vec<String>, anyhow::Error> {
    if service_groups.is_empty() {
        return Ok(Vec::new());
    }

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch upsert service groups")?;
    
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch upsert service groups")?;

    let mut all_ids = Vec::with_capacity(service_groups.len());

    // Process in chunks to avoid exceeding parameter limits
    const CHUNK_SIZE: usize = 1000;
    
    for chunk in service_groups.chunks(CHUNK_SIZE) {
        let mut query = String::from(
            "INSERT INTO public.service_group (
                id, service_id_1, service_id_2, confidence_score, method_type, match_values,
                pre_rl_confidence_score, confirmed_status, created_at, updated_at
            ) VALUES "
        );

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let mut param_groups = Vec::new();
        let mut ordered_groups: Vec<(
            &String,        // proposed_id
            &String,     // service_id_1
            &String,     // service_id_2  
            &f64,           // confidence_score
            String,         // method_type (now owned String)
            serde_json::Value, // match_values_json
            &f64,           // pre_rl_confidence_score
        )> = Vec::new();

        // Prepare data with proper ordering and serialization
        for (i, group) in chunk.iter().enumerate() {
            let base_idx = i * 7; // Fixed: only 7 parameters
            param_groups.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, 'PENDING_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                base_idx + 1,
                base_idx + 2,
                base_idx + 3,
                base_idx + 4,
                base_idx + 5,
                base_idx + 6,
                base_idx + 7
            ));

            // Ensure consistent ordering of service IDs
            let (s1, s2) = if group.service_id_1 < group.service_id_2 {
                (&group.service_id_1, &group.service_id_2)
            } else {
                (&group.service_id_2, &group.service_id_1)
            };

            let match_values_json = serde_json::to_value(&group.match_values)
                .context("Failed to serialize match values for batch upsert")?;

            // Store method_type string to avoid temporary value issues
            let method_type_str = group.method_type.as_str().to_string();

            ordered_groups.push((
                &group.proposed_id,
                s1,
                s2,
                &group.confidence_score,
                method_type_str,
                match_values_json,
                &group.pre_rl_confidence_score,
            ));
        }

        // Add parameters in the same order as the query
        for (proposed_id, s1, s2, confidence_score, method_type_str, match_values_json, pre_rl_confidence_score) in &ordered_groups {
            params.push(proposed_id);
            params.push(s1);
            params.push(s2);
            params.push(confidence_score);
            params.push(method_type_str);
            params.push(match_values_json);
            params.push(pre_rl_confidence_score);
        }

        query.push_str(&param_groups.join(", "));
        
        // Add ON CONFLICT clause to handle upserts
        query.push_str("
            ON CONFLICT (service_id_1, service_id_2, method_type)
            DO UPDATE SET
                confidence_score = EXCLUDED.confidence_score,
                match_values = EXCLUDED.match_values,
                pre_rl_confidence_score = EXCLUDED.pre_rl_confidence_score,
                confirmed_status = CASE 
                    WHEN public.service_group.confirmed_status IN ('CONFIRMED', 'REJECTED') 
                    THEN public.service_group.confirmed_status 
                    ELSE 'PENDING_REVIEW' 
                END,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id"
        );

        let rows = transaction
            .query(&query, &params[..])
            .await
            .context("Failed to batch upsert service_groups")?;

        for row in rows {
            let id: String = row.get(0);
            all_ids.push(id);
        }

        debug!("Batch upserted {} service groups in chunk", chunk.len());
    }

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for batch upsert service groups")?;

    info!("Successfully batch upserted {} service groups", service_groups.len());
    Ok(all_ids)
}

/// Batch insert service match decision details for the upserted service groups
/// This is a companion function to use after upsert_service_groups_batch
pub async fn insert_service_match_decision_details_batch(
    pool: &PgPool,
    details: &[(String, String, serde_json::Value, String, f64, f64, Option<i32>)], // (service_group_id, pipeline_run_id, features, method_type, pre_rl_conf, tuned_conf, tuner_version)
) -> Result<Vec<String>, anyhow::Error> {
    if details.is_empty() {
        return Ok(Vec::new());
    }

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch insert service match decision details")?;
    
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch insert service match decision details")?;

    let mut all_ids = Vec::with_capacity(details.len());

    // Process in chunks to avoid parameter limits
    const CHUNK_SIZE: usize = 1000;
    
    for chunk in details.chunks(CHUNK_SIZE) {
        let mut query = String::from(
            "INSERT INTO clustering_metadata.service_match_decision_details (
                service_group_id, pipeline_run_id, snapshotted_features,
                method_type_at_decision, pre_rl_confidence_at_decision,
                tuned_confidence_at_decision, confidence_tuner_version_at_decision
            ) VALUES "
        );

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let mut param_groups = Vec::new();

        for (i, (service_group_id, pipeline_run_id, features, method_type, pre_rl_conf, tuned_conf, tuner_version)) in chunk.iter().enumerate() {
            let base_idx = i * 7;
            param_groups.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${})",
                base_idx + 1,
                base_idx + 2,
                base_idx + 3,
                base_idx + 4,
                base_idx + 5,
                base_idx + 6,
                base_idx + 7
            ));

            params.push(service_group_id);
            params.push(pipeline_run_id);
            params.push(features);
            params.push(method_type);
            params.push(pre_rl_conf);
            params.push(tuned_conf);
            params.push(tuner_version);
        }

        query.push_str(&param_groups.join(", "));
        query.push_str(" RETURNING id");

        let rows = transaction
            .query(&query, &params[..])
            .await
            .context("Failed to batch insert service_match_decision_details")?;

        for row in rows {
            let id_str: String = row.get(0);
            all_ids.push(id_str);
        }

        debug!("Batch inserted {} service match decision details in chunk", chunk.len());
    }

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for batch insert service match decision details")?;

    info!("Successfully batch inserted {} service match decision details", details.len());
    Ok(all_ids)
}

/// NEW: Helper function to create RL decision batch data from service matches
pub fn create_rl_decision_batch_data(
    service_matches: &[ServiceMatch],
    pipeline_run_id: &str,
    features_data: &[(String, String, Vec<f64>)], // (service_id_1, service_id_2, features)
    tuner_version: Option<i32>,
) -> Vec<ServiceMatchDecisionDetailBatchData> {
    let mut decision_data = Vec::new();
    
    for (match_data, (s1, s2, features)) in service_matches.iter().zip(features_data.iter()) {
        // Verify the service IDs match
        let (match_s1, match_s2) = if match_data.service_id_1 <= match_data.service_id_2 {
            (&match_data.service_id_1, &match_data.service_id_2)
        } else {
            (&match_data.service_id_2, &match_data.service_id_1)
        };
        
        if match_s1 == s1 && match_s2 == s2 {
            let features_json = json!({
                "features": features,
                "method_type": match_data.match_method.as_str(),
                "service_id_1": s1,
                "service_id_2": s2,
                "pipeline_run_id": pipeline_run_id,
                "feature_extraction_timestamp": chrono::Utc::now().to_rfc3339()
            });
            
            decision_data.push(ServiceMatchDecisionDetailBatchData {
                service_group_id: format!("temp-{}-{}", s1, s2), // Will be replaced with actual group ID
                pipeline_run_id: pipeline_run_id.to_string(),
                snapshotted_features: features_json,
                method_type_at_decision: match_data.match_method.as_str().to_string(),
                pre_rl_confidence_at_decision: match_data.confidence_score, // Assuming this is pre-RL for now
                tuned_confidence_at_decision: match_data.confidence_score,
                confidence_tuner_version_at_decision: tuner_version,
            });
        }
    }
    
    decision_data
}