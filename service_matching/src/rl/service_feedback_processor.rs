// src/rl/service_feedback_processor.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde_json::Value as JsonValue;
use tokio_postgres::Client as PgClient;
use uuid::Uuid;

use super::service_confidence_tuner::ServiceConfidenceTuner;
use crate::utils::db_connect::PgPool;

// Internal structs for service feedback processing
#[derive(Debug)]
struct ServiceHumanFeedbackRecord {
    id: Uuid,
    service_group_id: String,
    is_match_correct: bool,
    reviewer_id: String,
}

#[derive(Debug)]
struct ServiceMatchDecisionDetailsRecord {
    snapshotted_features_json: JsonValue,
    method_type_at_decision: String,
    tuned_confidence_at_decision: f64,
}

// DB fetch functions for service feedback
async fn fetch_unprocessed_service_human_feedback(client: &PgClient) -> Result<Vec<ServiceHumanFeedbackRecord>> {
    let rows = client.query(
        "SELECT id, service_group_id, is_match_correct, reviewer_id 
         FROM clustering_metadata.service_match_human_feedback 
         WHERE processed_for_tuner_update_at IS NULL 
         ORDER BY feedback_timestamp ASC 
         LIMIT 1000",
        &[],
    ).await?;
    
    Ok(rows
        .into_iter()
        .map(|row| ServiceHumanFeedbackRecord {
            id: row.get("id"),
            service_group_id: row.get("service_group_id"),
            is_match_correct: row.get("is_match_correct"),
            reviewer_id: row.get("reviewer_id"),
        })
        .collect())
}

async fn fetch_service_match_decision_details(
    client: &PgClient,
    service_group_id: &str,
) -> Result<Option<ServiceMatchDecisionDetailsRecord>> {
    let row_opt = client.query_opt(
        "SELECT snapshotted_features, method_type_at_decision, tuned_confidence_at_decision 
         FROM clustering_metadata.service_match_decision_details 
         WHERE service_group_id = $1 
         ORDER BY created_at DESC 
         LIMIT 1",
        &[&service_group_id],
    ).await?;
    
    Ok(row_opt.map(|row| ServiceMatchDecisionDetailsRecord {
        snapshotted_features_json: row.get("snapshotted_features"),
        method_type_at_decision: row.get("method_type_at_decision"),
        tuned_confidence_at_decision: row.get("tuned_confidence_at_decision"),
    }))
}

async fn mark_service_human_feedback_as_processed(client: &PgClient, feedback_id: String) -> Result<()> {
    client.execute(
        "UPDATE clustering_metadata.service_match_human_feedback 
         SET processed_for_tuner_update_at = CURRENT_TIMESTAMP 
         WHERE id = $1",
        &[&feedback_id],
    ).await?;
    Ok(())
}

/// Processes new human feedback and updates the ServiceConfidenceTuner.
pub async fn process_service_human_feedback_for_tuner(
    pool: &PgPool,
    confidence_tuner: &mut ServiceConfidenceTuner,
) -> Result<()> {
    info!(
        "Starting service human feedback processing cycle for Service Contextual Bandit v{}",
        confidence_tuner.version
    );
    let client = pool.get().await.context("Failed to get DB connection")?;
    let feedback_items = fetch_unprocessed_service_human_feedback(&client).await?;

    if feedback_items.is_empty() {
        info!("No new service human feedback to process.");
        return Ok(());
    }

    let mut processed_count = 0;
    let mut error_count = 0;

    for feedback_record in feedback_items {
        match fetch_service_match_decision_details(&client, &feedback_record.service_group_id).await {
            Ok(Some(decision_details)) => {
                let snapshotted_features: Vec<f64> = match serde_json::from_value(
                    decision_details.snapshotted_features_json.clone(),
                ) {
                    Ok(features) => features,
                    Err(e) => {
                        warn!(
                            "Failed to deserialize service features for group {}: {}. Skipping.",
                            feedback_record.service_group_id, e
                        );
                        error_count += 1;
                        continue;
                    }
                };

                if snapshotted_features.is_empty() {
                    warn!(
                        "Empty service features for group {}. Skipping.",
                        feedback_record.service_group_id
                    );
                    error_count += 1;
                    continue;
                }

                let reward = if feedback_record.is_match_correct {
                    1.0
                } else {
                    0.0
                };

                // Update the service confidence tuner with the feedback
                if let Err(e) = confidence_tuner.update(
                    &decision_details.method_type_at_decision,
                    decision_details.tuned_confidence_at_decision,
                    reward,
                    &snapshotted_features, // Pass service features here
                ) {
                    warn!(
                        "Failed to update ServiceConfidenceTuner for feedback {}: {}",
                        feedback_record.id, e
                    );
                    error_count += 1;
                    continue;
                }

                if let Err(e) =
                    mark_service_human_feedback_as_processed(&client, feedback_record.id.to_string()).await
                {
                    warn!(
                        "Failed to mark service feedback {} as processed: {}",
                        feedback_record.id, e
                    );
                    error_count += 1;
                } else {
                    processed_count += 1;
                    debug!(
                        "Successfully processed service feedback {} for method '{}' with reward {}",
                        feedback_record.id, 
                        decision_details.method_type_at_decision,
                        reward
                    );
                }
            }
            Ok(None) => {
                warn!(
                    "No service decision details for group {}. Skipping feedback {}.",
                    feedback_record.service_group_id, feedback_record.id
                );
                error_count += 1;
            }
            Err(e) => {
                warn!(
                    "Error fetching service decision details for feedback {}: {}",
                    feedback_record.id, e
                );
                error_count += 1;
            }
        }
    }

    info!(
        "Service feedback processing complete. Processed: {}, Errors/Skipped: {}.",
        processed_count, error_count
    );
    
    if processed_count > 0 {
        info!(
            "ServiceConfidenceTuner updated with {} new training examples. Current version: {}",
            processed_count, confidence_tuner.version
        );
    }
    
    Ok(())
}