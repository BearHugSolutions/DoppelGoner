// src/reinforcement/service_feedback_processor.rs
use anyhow::{Context, Result};
use log::{debug, info, warn};
use serde_json::Value as JsonValue;
use tokio_postgres::Client as PgClient;
use uuid::Uuid;

use crate::db::PgPool;

use super::service_confidence_tuner::ServiceConfidenceTuner;

// This struct will represent a row fetched from `clustering_metadata.service_match_human_feedback`
#[derive(Debug)]
struct ServiceHumanFeedbackRecord {
    id: Uuid,
    service_group_id: String,
    is_match_correct: bool,
}

// This struct will represent a row fetched from `clustering_metadata.service_match_decision_details`
#[derive(Debug)]
struct ServiceMatchDecisionDetailsRecord {
    snapshotted_features_json: JsonValue,
    method_type_at_decision: String,
    tuned_confidence_at_decision: f64,
}

/// Fetches unprocessed human feedback from the database.
async fn fetch_unprocessed_service_match_feedback(
    client: &PgClient,
) -> Result<Vec<ServiceHumanFeedbackRecord>> {
    let rows = client
        .query(
            "SELECT id, service_group_id, is_match_correct
             FROM clustering_metadata.service_match_human_feedback
             WHERE processed_for_tuner_update_at IS NULL
             ORDER BY feedback_timestamp ASC -- Process older feedback first
             LIMIT 1000", // Process in batches
            &[],
        )
        .await
        .context("Failed to fetch unprocessed service match human feedback items from DB")?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(ServiceHumanFeedbackRecord {
            id: row.get("id"),
            service_group_id: row.get("service_group_id"),
            is_match_correct: row.get("is_match_correct"),
        });
    }
    debug!("Fetched {} unprocessed service match human feedback items.", items.len());
    Ok(items)
}

/// Fetches the decision details for a given service_group_id.
async fn fetch_service_match_decision_details(
    client: &PgClient,
    service_group_id: &str,
) -> Result<Option<ServiceMatchDecisionDetailsRecord>> {
    let row_opt = client
        .query_opt(
            "SELECT snapshotted_features, method_type_at_decision, tuned_confidence_at_decision
             FROM clustering_metadata.service_match_decision_details
             WHERE service_group_id = $1
             ORDER BY created_at DESC -- Get the latest snapshot if multiple (should be rare)
             LIMIT 1",
            &[&service_group_id],
        )
        .await
        .context(format!(
            "Failed to fetch service match decision details for service_group_id: {}",
            service_group_id
        ))?;

    if let Some(row) = row_opt {
        Ok(Some(ServiceMatchDecisionDetailsRecord {
            snapshotted_features_json: row.get("snapshotted_features"),
            method_type_at_decision: row.get("method_type_at_decision"),
            tuned_confidence_at_decision: row.get("tuned_confidence_at_decision"),
        }))
    } else {
        Ok(None)
    }
}

/// Marks a human feedback item as processed in the database.
async fn mark_service_match_feedback_as_processed(
    client: &PgClient,
    feedback_id: String,
) -> Result<()> {
    let rows_affected = client
        .execute(
            "UPDATE clustering_metadata.service_match_human_feedback
             SET processed_for_tuner_update_at = CURRENT_TIMESTAMP
             WHERE id = $1",
            &[&feedback_id],
        )
        .await
        .context(format!(
            "Failed to mark service match human feedback ID {} as processed",
            feedback_id
        ))?;

    if rows_affected == 1 {
        debug!("Marked service match human feedback ID {} as processed.", feedback_id);
    } else {
        warn!(
            "Attempted to mark service match human feedback ID {} as processed, but {} rows were affected (expected 1).",
            feedback_id, rows_affected
        );
    }
    Ok(())
}

/// Processes new human feedback and updates the ServiceConfidenceTuner.
pub async fn process_service_feedback_for_tuner(
    pool: &PgPool,
    confidence_tuner: &mut ServiceConfidenceTuner,
) -> Result<()> {
    info!(
        "Starting service human feedback processing cycle for ServiceConfidenceTuner v{}",
        confidence_tuner.version
    );
    let client = pool
        .get()
        .await
        .context("Failed to get DB connection for service feedback processing")?;

    let feedback_items_to_process = fetch_unprocessed_service_match_feedback(&client).await?;

    if feedback_items_to_process.is_empty() {
        info!("No new service human feedback to process for ServiceConfidenceTuner.");
        return Ok(());
    }

    let mut processed_count = 0;
    let mut error_count = 0;

    for feedback_record in feedback_items_to_process {
        match fetch_service_match_decision_details(&client, &feedback_record.service_group_id).await {
            Ok(Some(decision_details)) => {
                // Deserialize snapshotted_features from JSONB to Vec<f64>
                let snapshotted_features: Vec<f64> = match serde_json::from_value(
                    decision_details.snapshotted_features_json.clone(),
                ) {
                    Ok(features) => features,
                    Err(e) => {
                        warn!("Failed to deserialize snapshotted_features for service_group_id {}: {}. Skipping feedback item {}.", feedback_record.service_group_id, e, feedback_record.id);
                        error_count += 1;
                        continue;
                    }
                };

                // Ensure features vector is not empty if it's required by the tuner's update logic implicitly
                if snapshotted_features.is_empty() {
                    warn!("Snapshotted features are empty for service_group_id {}. Skipping feedback item {}.", feedback_record.service_group_id, feedback_record.id);
                    error_count += 1;
                    continue;
                }

                // Map the standard MatchMethodType string to service-specific method name
                let service_method_name = match decision_details.method_type_at_decision.as_str() {
                    "service_name" => "service_name",
                    "service_url" => "service_url",
                    "service_email" => "service_email",
                    "service_embedding" => "service_embedding",
                    other => {
                        // Try to extract a service method name from the type if it's not directly one of our expected types
                        if other.contains("Name") {
                            "service_name"
                        } else if other.contains("Url") || other.contains("URL") {
                            "service_url"
                        } else if other.contains("Email") {
                            "service_email"
                        } else if other.contains("Embedding") {
                            "service_embedding"
                        } else {
                            warn!("Unknown method type '{}' for service_group_id {}. Using 'default'.", other, feedback_record.service_group_id);
                            "default"
                        }
                    }
                };

                let reward = if feedback_record.is_match_correct {
                    1.0
                } else {
                    0.0
                }; // Or -1.0 for incorrect

                // The `tuned_confidence_at_decision` is the confidence score that the tuner *outputted*
                // for this specific method and feature context when the match was made.
                // This is what the tuner needs to associate the reward with the correct arm/state.
                if let Err(e) = confidence_tuner.update(
                    service_method_name,
                    decision_details.tuned_confidence_at_decision,
                    reward,
                ) {
                    warn!("Failed to update ServiceConfidenceTuner for feedback item {}: {}. Service Group ID: {}", feedback_record.id, e, feedback_record.service_group_id);
                    error_count += 1;
                    continue; // Skip marking as processed if tuner update fails
                }

                if let Err(e) =
                    mark_service_match_feedback_as_processed(&client, feedback_record.id.to_string()).await
                {
                    warn!(
                        "Failed to mark service feedback item {} as processed after tuner update: {}",
                        feedback_record.id, e
                    );
                    error_count += 1;
                    // Consider if we should proceed or halt if marking fails.
                } else {
                    processed_count += 1;
                }
            }
            Ok(None) => {
                warn!("No service match decision details found for service_group_id: {}. Cannot process feedback item {}.", feedback_record.service_group_id, feedback_record.id);
                // Optionally, mark this feedback as "unactionable" or retry later. For now, just log.
                error_count += 1;
            }
            Err(e) => {
                warn!(
                    "Error fetching service decision details for feedback item {}: {}. Service Group ID: {}",
                    feedback_record.id, e, feedback_record.service_group_id
                );
                error_count += 1;
            }
        }
    }

    info!(
        "Service human feedback processing cycle complete. Processed: {}, Errors/Skipped: {}. ServiceConfidenceTuner version: {}",
        processed_count, error_count, confidence_tuner.version
    );
    Ok(())
}