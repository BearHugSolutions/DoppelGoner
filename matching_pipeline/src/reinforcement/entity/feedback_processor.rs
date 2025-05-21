// src/reinforcement/entity/feedback_processor.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde_json::Value as JsonValue; // For parsing JSONB features
use tokio_postgres::Client as PgClient; // Direct use of PgClient
use uuid::Uuid;

use crate::db::PgPool; // Assuming this is the main pool type
use crate::reinforcement::types::HumanFeedbackDataForTuner;

use super::confidence_tuner::ConfidenceTuner; // New type for this process

// This struct will represent a row fetched from `clustering_metadata.human_feedback`
// It's an internal representation for this module.
#[derive(Debug)]
struct HumanFeedbackRecord {
    id: Uuid,
    entity_group_id: String,
    is_match_correct: bool,
    // reviewer_id: String, // Not strictly needed for tuner update logic itself
    // feedback_timestamp: chrono::NaiveDateTime, // Also not strictly needed for tuner logic
}

// This struct will represent a row fetched from `clustering_metadata.match_decision_details`
#[derive(Debug)]
struct MatchDecisionDetailsRecord {
    // entity_group_id: String, // Keyed by this already
    snapshotted_features_json: JsonValue, // JSONB from DB
    method_type_at_decision: String,
    tuned_confidence_at_decision: f64,
}

/// Fetches unprocessed human feedback from the database.
async fn fetch_unprocessed_human_feedback(
    client: &PgClient, // Takes a direct client/connection
) -> Result<Vec<HumanFeedbackRecord>> {
    let rows = client
        .query(
            "SELECT id, entity_group_id, is_match_correct
             FROM clustering_metadata.human_feedback
             WHERE processed_for_tuner_update_at IS NULL
             ORDER BY feedback_timestamp ASC -- Process older feedback first
             LIMIT 1000", // Process in batches
            &[],
        )
        .await
        .context("Failed to fetch unprocessed human feedback items from DB")?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(HumanFeedbackRecord {
            id: row.get("id"),
            entity_group_id: row.get("entity_group_id"),
            is_match_correct: row.get("is_match_correct"),
        });
    }
    debug!("Fetched {} unprocessed human feedback items.", items.len());
    Ok(items)
}

/// Fetches the decision details for a given entity_group_id.
async fn fetch_match_decision_details(
    client: &PgClient, // Takes a direct client/connection
    entity_group_id: &str,
) -> Result<Option<MatchDecisionDetailsRecord>> {
    let row_opt = client
        .query_opt(
            "SELECT snapshotted_features, method_type_at_decision, tuned_confidence_at_decision
             FROM clustering_metadata.match_decision_details
             WHERE entity_group_id = $1
             ORDER BY created_at DESC -- Get the latest snapshot if multiple (should be rare)
             LIMIT 1",
            &[&entity_group_id],
        )
        .await
        .context(format!(
            "Failed to fetch match decision details for entity_group_id: {}",
            entity_group_id
        ))?;

    if let Some(row) = row_opt {
        Ok(Some(MatchDecisionDetailsRecord {
            snapshotted_features_json: row.get("snapshotted_features"),
            method_type_at_decision: row.get("method_type_at_decision"),
            tuned_confidence_at_decision: row.get("tuned_confidence_at_decision"),
        }))
    } else {
        Ok(None)
    }
}

/// Marks a human feedback item as processed in the database.
async fn mark_human_feedback_as_processed(
    client: &PgClient, // Takes a direct client/connection
    feedback_id: String,
) -> Result<()> {
    let rows_affected = client
        .execute(
            "UPDATE clustering_metadata.human_feedback
             SET processed_for_tuner_update_at = CURRENT_TIMESTAMP
             WHERE id = $1",
            &[&feedback_id],
        )
        .await
        .context(format!(
            "Failed to mark human feedback ID {} as processed",
            feedback_id
        ))?;

    if rows_affected == 1 {
        debug!("Marked human feedback ID {} as processed.", feedback_id);
    } else {
        warn!(
            "Attempted to mark human feedback ID {} as processed, but {} rows were affected (expected 1).",
            feedback_id, rows_affected
        );
    }
    Ok(())
}

/// Processes new human feedback and updates the ConfidenceTuner.
pub async fn process_human_feedback_for_tuner(
    pool: &PgPool,
    confidence_tuner: &mut ConfidenceTuner,
) -> Result<()> {
    info!(
        "Starting human feedback processing cycle for ConfidenceTuner v{}",
        confidence_tuner.version
    );
    let client = pool
        .get()
        .await
        .context("Failed to get DB connection for feedback processing")?;

    let feedback_items_to_process = fetch_unprocessed_human_feedback(&client).await?;

    if feedback_items_to_process.is_empty() {
        info!("No new human feedback to process for ConfidenceTuner.");
        return Ok(());
    }

    let mut processed_count = 0;
    let mut error_count = 0;

    for feedback_record in feedback_items_to_process {
        match fetch_match_decision_details(&client, &feedback_record.entity_group_id).await {
            Ok(Some(decision_details)) => {
                // Deserialize snapshotted_features from JSONB to Vec<f64>
                let snapshotted_features: Vec<f64> = match serde_json::from_value(
                    decision_details.snapshotted_features_json.clone(),
                ) {
                    Ok(features) => features,
                    Err(e) => {
                        warn!("Failed to deserialize snapshotted_features for entity_group_id {}: {}. Skipping feedback item {}.", feedback_record.entity_group_id, e, feedback_record.id);
                        error_count += 1;
                        continue;
                    }
                };

                // Ensure features vector is not empty if it's required by the tuner's update logic implicitly
                if snapshotted_features.is_empty() {
                    warn!("Snapshotted features are empty for entity_group_id {}. Skipping feedback item {}.", feedback_record.entity_group_id, feedback_record.id);
                    error_count += 1;
                    continue;
                }

                let reward = if feedback_record.is_match_correct {
                    1.0
                } else {
                    0.0
                }; // Or -1.0 for incorrect

                // The `tuned_confidence_at_decision` is the confidence score that the tuner *outputted*
                // for this specific method and feature context when the match was made.
                // This is what the tuner needs to associate the reward with the correct arm/state.
                if let Err(e) = confidence_tuner.update(
                    &decision_details.method_type_at_decision,
                    decision_details.tuned_confidence_at_decision,
                    reward,
                ) {
                    warn!("Failed to update ConfidenceTuner for feedback item {}: {}. Entity Group ID: {}", feedback_record.id, e, feedback_record.entity_group_id);
                    error_count += 1;
                    continue; // Skip marking as processed if tuner update fails
                }

                if let Err(e) =
                    mark_human_feedback_as_processed(&client, feedback_record.id.to_string()).await
                {
                    warn!(
                        "Failed to mark feedback item {} as processed after tuner update: {}",
                        feedback_record.id, e
                    );
                    error_count += 1;
                    // Consider if we should proceed or halt if marking fails.
                } else {
                    processed_count += 1;
                }
            }
            Ok(None) => {
                warn!("No match decision details found for entity_group_id: {}. Cannot process feedback item {}.", feedback_record.entity_group_id, feedback_record.id);
                // Optionally, mark this feedback as "unactionable" or retry later. For now, just log.
                error_count += 1;
            }
            Err(e) => {
                warn!(
                    "Error fetching decision details for feedback item {}: {}. Entity Group ID: {}",
                    feedback_record.id, e, feedback_record.entity_group_id
                );
                error_count += 1;
            }
        }
    }

    info!(
        "Human feedback processing cycle complete. Processed: {}, Errors/Skipped: {}. ConfidenceTuner version: {}",
        processed_count, error_count, confidence_tuner.version
    );
    Ok(())
}
