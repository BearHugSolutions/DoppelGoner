// src/bin/train_reviewer_model.rs
use anyhow::{Context, Result};
use dedupe_lib::{
    rl::confidence_tuner::ConfidenceTuner,
    utils::{
        db_connect::{connect, PgPool},
        env::load_env,
    },
};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};
use tokio_postgres::Client as PgClient;

// Structs for records remain the same
#[derive(Debug, Clone)]
struct HumanFeedbackRecord {
    id: String,
    entity_group_id: String,
    is_match_correct: bool,
    reviewer_id: String,
}

#[derive(Debug, Clone)]
struct MatchDecisionDetailsRecord {
    snapshotted_features_json: JsonValue,
    method_type_at_decision: String,
    tuned_confidence_at_decision: f64,
}

// Error enum remains the same
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
enum ProcessingError {
    DbConnection,
    TransactionStart,
    DecisionDetailsFetch,
    NoDecisionDetails,
    FeatureDeserialization,
    EmptyFeatures,
    TunerUpdate,
    MarkProcessed,
    Commit,
    TaskPanic,
}

impl std::fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessingError::DbConnection => write!(f, "Failed to get DB connection from pool"),
            ProcessingError::TransactionStart => {
                write!(f, "Failed to start a database transaction")
            }
            ProcessingError::DecisionDetailsFetch => {
                write!(f, "Error fetching match decision details from DB")
            }
            ProcessingError::NoDecisionDetails => {
                write!(f, "No match decision details found for the entity group")
            }
            ProcessingError::FeatureDeserialization => {
                write!(f, "Failed to deserialize snapshotted features JSON")
            }
            ProcessingError::EmptyFeatures => write!(f, "Snapshotted features vector was empty"),
            ProcessingError::TunerUpdate => write!(f, "Failed to update the ConfidenceTuner model"),
            ProcessingError::MarkProcessed => {
                write!(f, "Failed to mark feedback item as processed in DB")
            }
            ProcessingError::Commit => write!(f, "Failed to commit the database transaction"),
            ProcessingError::TaskPanic => write!(f, "A concurrent task panicked"),
        }
    }
}

// DB fetch functions remain the same
async fn fetch_reviewer_specific_feedback(
    client: &PgClient,
    reviewer_ids: &[&str],
) -> Result<Vec<HumanFeedbackRecord>> {
    let reviewer_ids_vec: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = reviewer_ids
        .iter()
        .map(|id| &*id as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();
    let placeholders: String = (1..=reviewer_ids.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT id, entity_group_id, is_match_correct, reviewer_id
         FROM clustering_metadata.human_feedback
         WHERE reviewer_id IN ({}) AND processed_for_tuner_update_at IS NULL
         ORDER BY feedback_timestamp ASC LIMIT 10000",
        placeholders
    );
    let rows = client.query(&query, &reviewer_ids_vec).await?;
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

async fn fetch_match_decision_details<C: tokio_postgres::GenericClient + Send + Sync>(
    client: &C,
    entity_group_id: &str,
) -> Result<Option<MatchDecisionDetailsRecord>> {
    let row_opt = client
        .query_opt(
            "SELECT snapshotted_features, method_type_at_decision, tuned_confidence_at_decision
         FROM clustering_metadata.match_decision_details
         WHERE entity_group_id = $1 ORDER BY created_at DESC LIMIT 1",
            &[&entity_group_id],
        )
        .await?;
    Ok(row_opt.map(|row| MatchDecisionDetailsRecord {
        snapshotted_features_json: row.get("snapshotted_features"),
        method_type_at_decision: row.get("method_type_at_decision"),
        tuned_confidence_at_decision: row.get("tuned_confidence_at_decision"),
    }))
}

async fn mark_human_feedback_as_processed<C: tokio_postgres::GenericClient + Send + Sync>(
    client: &C,
    feedback_id: &str,
) -> Result<()> {
    client.execute(
        "UPDATE clustering_metadata.human_feedback SET processed_for_tuner_update_at = CURRENT_TIMESTAMP WHERE id = $1",
        &[&feedback_id],
    ).await?;
    Ok(())
}

// The core processing logic is updated to pass features to the tuner.
async fn process_single_feedback_item(
    pool: PgPool,
    tuner: Arc<Mutex<ConfidenceTuner>>,
    feedback_record: HumanFeedbackRecord,
    dry_run: bool,
) -> Result<(), (ProcessingError, String)> {
    let mut client = pool
        .get()
        .await
        .map_err(|e| (ProcessingError::DbConnection, e.to_string()))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|e| (ProcessingError::TransactionStart, e.to_string()))?;

    match fetch_match_decision_details(&transaction, &feedback_record.entity_group_id).await {
        Ok(Some(decision_details)) => {
            let features_json_value = decision_details.snapshotted_features_json.clone();
            let snapshotted_features: Vec<f64> =
                serde_json::from_value(features_json_value.clone())
                    .ok()
                    .or_else(|| {
                        features_json_value.as_object().and_then(|obj| {
                            obj.get("features")
                                .and_then(|val| serde_json::from_value(val.clone()).ok())
                        })
                    })
                    .ok_or_else(|| {
                        (
                            ProcessingError::FeatureDeserialization,
                            format!(
                                "For feedback_id {}: JSON is not a valid feature array or object.",
                                feedback_record.id
                            ),
                        )
                    })?;

            if snapshotted_features.is_empty() {
                return Err((
                    ProcessingError::EmptyFeatures,
                    format!("For feedback_id {}", feedback_record.id),
                ));
            }

            let reward = if feedback_record.is_match_correct {
                1.0
            } else {
                0.0
            };

            // **MODIFICATION**: The `update` call now includes the contextual features.
            {
                let mut tuner_guard = tuner.lock().unwrap();
                if let Err(e) = tuner_guard.update(
                    &decision_details.method_type_at_decision,
                    decision_details.tuned_confidence_at_decision,
                    reward,
                    &snapshotted_features, // Pass the features here
                ) {
                    return Err((
                        ProcessingError::TunerUpdate,
                        format!("For feedback_id {}: {}", feedback_record.id, e),
                    ));
                }
            }

            if !dry_run {
                mark_human_feedback_as_processed(&transaction, &feedback_record.id)
                    .await
                    .map_err(|e| (ProcessingError::MarkProcessed, e.to_string()))?;
            }
            transaction
                .commit()
                .await
                .map_err(|e| (ProcessingError::Commit, e.to_string()))?;
            Ok(())
        }
        Ok(None) => Err((
            ProcessingError::NoDecisionDetails,
            format!("For feedback_id {}", feedback_record.id),
        )),
        Err(e) => Err((ProcessingError::DecisionDetailsFetch, e.to_string())),
    }
}

// The rest of the file (processing loop, main function) remains largely the same
// as it correctly orchestrates the calls to the updated process_single_feedback_item.

async fn process_reviewer_feedback_for_tuner(
    pool: &PgPool,
    confidence_tuner: Arc<Mutex<ConfidenceTuner>>,
    reviewer_ids: &[&str],
    dry_run: bool,
) -> Result<(usize, usize)> {
    let tuner_version = confidence_tuner.lock().unwrap().version;
    info!(
        "Starting Contextual Bandit (v{}) training with reviewers: {:?}",
        tuner_version, reviewer_ids
    );
    let client = pool.get().await.context("Failed to get DB connection")?;
    let feedback_items = fetch_reviewer_specific_feedback(&client, reviewer_ids).await?;
    let total_items = feedback_items.len();
    if total_items == 0 {
        info!("No feedback to process.");
        return Ok((0, 0));
    }
    let pb = ProgressBar::new(total_items as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap(),
    );

    let results = stream::iter(feedback_items)
        .map(|item| {
            let pool = pool.clone();
            let tuner = Arc::clone(&confidence_tuner);
            let pb = pb.clone();
            tokio::spawn(async move {
                let result = process_single_feedback_item(pool, tuner, item, dry_run).await;
                pb.inc(1);
                result
            })
        })
        .buffer_unordered(num_cpus::get())
        .collect::<Vec<_>>()
        .await;

    let mut processed_count = 0;
    let mut error_counts: HashMap<ProcessingError, usize> = HashMap::new();
    results.iter().for_each(|res| match res {
        Ok(Ok(_)) => processed_count += 1,
        Ok(Err((e_type, _))) => *error_counts.entry(e_type.clone()).or_insert(0) += 1,
        Err(_) => *error_counts.entry(ProcessingError::TaskPanic).or_insert(0) += 1,
    });

    pb.finish_with_message("Training complete.");
    let total_errors: usize = error_counts.values().sum();
    info!(
        "Training complete. Processed: {}, Errors/Skipped: {}.",
        processed_count, total_errors
    );
    if total_errors > 0 {
        warn!("Error summary: {:?}", error_counts);
    }
    Ok((processed_count, total_errors))
}

async fn save_trained_model(
    pool: &PgPool,
    confidence_tuner: Arc<Mutex<ConfidenceTuner>>,
    reviewer_ids: &[&str],
    processed_count: usize,
) -> Result<(String, u32)> {
    let mut tuner_guard = confidence_tuner.lock().unwrap();
    let model_id = tuner_guard.save_to_db(pool).await?;
    info!(
        "Saved new model version {}. Description: Trained on {} items from reviewers {:?}.",
        tuner_guard.version, processed_count, reviewer_ids
    );
    Ok((model_id, tuner_guard.version))
}

fn print_training_summary(
    reviewer_ids: &[&str],
    processed_count: usize,
    error_count: usize,
    model_id: &str,
    model_version: u32,
) {
    println!("\n=== CONTEXTUAL BANDIT TRAINING SUMMARY ===");
    println!("Model Version: {}", model_version);
    println!("Model ID: {}", model_id);
    println!("Reviewers: {:?}", reviewer_ids);
    println!("Feedback Processed: {}", processed_count);
    println!("Errors/Skipped: {}", error_count);
    println!("\nTo use this model, ensure your orchestrator loads the latest version.");
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    load_env();
    let args: Vec<String> = env::args().collect();
    let dry_run = args.contains(&"--dry-run".to_string());
    if dry_run {
        warn!("DRY RUN MODE: No changes will be saved to the database.");
    }
    let reviewer_ids = vec![
        "009fcfeb-64e7-4400-9ada-c8eb6c68455c",
        "c5adbcbc-562d-43e2-8bea-90aa3d58cd33",
    ];
    let pool = connect().await?;
    let confidence_tuner = Arc::new(Mutex::new(ConfidenceTuner::load_from_db(&pool).await?));
    let (processed, errors) = process_reviewer_feedback_for_tuner(
        &pool,
        confidence_tuner.clone(),
        &reviewer_ids,
        dry_run,
    )
    .await?;
    if processed > 0 && !dry_run {
        let (model_id, version) =
            save_trained_model(&pool, confidence_tuner.clone(), &reviewer_ids, processed).await?;
        print_training_summary(&reviewer_ids, processed, errors, &model_id, version);
        println!("\n=== MODEL STATISTICS ===");
        println!("{}", confidence_tuner.lock().unwrap().get_stats_display());
    } else {
        info!("No data processed or dry run enabled. Model was not saved.");
    }
    Ok(())
}
