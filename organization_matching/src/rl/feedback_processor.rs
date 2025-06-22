// src/rl/feedback_processor.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde_json::Value as JsonValue;
use tokio_postgres::Client as PgClient;
use uuid::Uuid;

use super::confidence_tuner::ConfidenceTuner;
use crate::utils::db_connect::PgPool;

// Internal structs remain the same
#[derive(Debug)]
struct HumanFeedbackRecord {
    id: Uuid,
    entity_group_id: String,
    is_match_correct: bool,
    reviewer_id: String,
}

#[derive(Debug)]
struct MatchDecisionDetailsRecord {
    snapshotted_features_json: JsonValue,
    method_type_at_decision: String,
    tuned_confidence_at_decision: f64,
}

// DB fetch functions remain the same
async fn fetch_unprocessed_human_feedback(client: &PgClient) -> Result<Vec<HumanFeedbackRecord>> {
    let rows = client.query(
        "SELECT id, entity_group_id, is_match_correct, reviewer_id FROM clustering_metadata.human_feedback WHERE processed_for_tuner_update_at IS NULL ORDER BY feedback_timestamp ASC LIMIT 1000",
        &[],
    ).await?;
    Ok(rows
        .into_iter()
        .map(|row| HumanFeedbackRecord {
            id: row.get("id"),
            entity_group_id: row.get("entity_group_id"),
            is_match_correct: row.get("is_match_correct"),
            reviewer_id: row.get("reviewer_id"),
        })
        .collect())
}

async fn fetch_match_decision_details(
    client: &PgClient,
    entity_group_id: &str,
) -> Result<Option<MatchDecisionDetailsRecord>> {
    let row_opt = client.query_opt(
        "SELECT snapshotted_features, method_type_at_decision, tuned_confidence_at_decision FROM clustering_metadata.match_decision_details WHERE entity_group_id = $1 ORDER BY created_at DESC LIMIT 1",
        &[&entity_group_id],
    ).await?;
    Ok(row_opt.map(|row| MatchDecisionDetailsRecord {
        snapshotted_features_json: row.get("snapshotted_features"),
        method_type_at_decision: row.get("method_type_at_decision"),
        tuned_confidence_at_decision: row.get("tuned_confidence_at_decision"),
    }))
}

async fn mark_human_feedback_as_processed(client: &PgClient, feedback_id: String) -> Result<()> {
    client.execute(
        "UPDATE clustering_metadata.human_feedback SET processed_for_tuner_update_at = CURRENT_TIMESTAMP WHERE id = $1",
        &[&feedback_id],
    ).await?;
    Ok(())
}

/// Processes new human feedback and updates the ConfidenceTuner.
pub async fn process_human_feedback_for_tuner(
    pool: &PgPool,
    confidence_tuner: &mut ConfidenceTuner,
) -> Result<()> {
    info!(
        "Starting human feedback processing cycle for Contextual Bandit v{}",
        confidence_tuner.version
    );
    let client = pool.get().await.context("Failed to get DB connection")?;
    let feedback_items = fetch_unprocessed_human_feedback(&client).await?;

    if feedback_items.is_empty() {
        info!("No new human feedback to process.");
        return Ok(());
    }

    let mut processed_count = 0;
    let mut error_count = 0;

    for feedback_record in feedback_items {
        match fetch_match_decision_details(&client, &feedback_record.entity_group_id).await {
            Ok(Some(decision_details)) => {
                let snapshotted_features: Vec<f64> = match serde_json::from_value(
                    decision_details.snapshotted_features_json.clone(),
                ) {
                    Ok(features) => features,
                    Err(e) => {
                        warn!(
                            "Failed to deserialize features for group {}: {}. Skipping.",
                            feedback_record.entity_group_id, e
                        );
                        error_count += 1;
                        continue;
                    }
                };

                if snapshotted_features.is_empty() {
                    warn!(
                        "Empty features for group {}. Skipping.",
                        feedback_record.entity_group_id
                    );
                    error_count += 1;
                    continue;
                }

                let reward = if feedback_record.is_match_correct {
                    1.0
                } else {
                    0.0
                };

                // **MODIFICATION**: Pass the contextual features to the update function.
                if let Err(e) = confidence_tuner.update(
                    &decision_details.method_type_at_decision,
                    decision_details.tuned_confidence_at_decision,
                    reward,
                    &snapshotted_features, // Pass features here
                ) {
                    warn!(
                        "Failed to update ConfidenceTuner for feedback {}: {}",
                        feedback_record.id, e
                    );
                    error_count += 1;
                    continue;
                }

                if let Err(e) =
                    mark_human_feedback_as_processed(&client, feedback_record.id.to_string()).await
                {
                    warn!(
                        "Failed to mark feedback {} as processed: {}",
                        feedback_record.id, e
                    );
                    error_count += 1;
                } else {
                    processed_count += 1;
                }
            }
            Ok(None) => {
                warn!(
                    "No decision details for group {}. Skipping feedback {}.",
                    feedback_record.entity_group_id, feedback_record.id
                );
                error_count += 1;
            }
            Err(e) => {
                warn!(
                    "Error fetching decision details for feedback {}: {}",
                    feedback_record.id, e
                );
                error_count += 1;
            }
        }
    }

    info!(
        "Feedback processing complete. Processed: {}, Errors/Skipped: {}.",
        processed_count, error_count
    );
    Ok(())
}
