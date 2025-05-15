// src/db.rs

use anyhow::{Context, Result};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::NaiveDateTime; // For human_feedback table
use log::{debug, info, warn};
use std::time::Duration;
use tokio_postgres::{Config, GenericClient, NoTls, Row as PgRow};
use uuid::Uuid;

// Assuming these structs are defined in your main models.rs or a shared location.
// If not, they would need to be defined or imported appropriately.
use crate::models::{
    EntityId, // Assuming EntityId is defined in models
    NewClusterFormationEdge,
    NewSuggestedAction,
    // Add other necessary model imports here
};
// For RL specific data structures that might be passed to/from these DB functions.
// We'll define simple structs here for clarity if they aren't part of the main models.

pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;

// Represents a row fetched from clustering_metadata.human_feedback for processing
#[derive(Debug)]
pub struct HumanFeedbackDbRecord {
    pub id: String,
    pub entity_group_id: String,
    pub is_match_correct: bool,
    // Add other fields if needed by the processing logic, e.g., reviewer_id, feedback_timestamp
}

// Struct for inserting a new human feedback record
// This would typically be called by the part of your system handling HITL frontend interactions.
#[derive(Debug)]
pub struct NewHumanFeedback<'a> {
    pub entity_group_id: &'a str,
    pub reviewer_id: &'a str,
    pub is_match_correct: bool,
    pub notes: Option<&'a str>,
    // feedback_timestamp is defaulted by DB
}

/// Reads environment variables and constructs a PostgreSQL config.
fn build_pg_config() -> Config {
    let mut config = Config::new();
    let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port_str = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
    let port = port_str.parse::<u16>().unwrap_or(5432);
    let dbname = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "dataplatform".to_string());
    let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("POSTGRES_PASSWORD").unwrap_or_default();

    info!(
        "DB Config: Host={}, Port={}, DB={}, User={}",
        host, port, dbname, user
    );
    config
        .host(&host)
        .port(port)
        .dbname(&dbname)
        .user(&user)
        .password(&password);
    config.application_name("deduplication_pipeline");
    config.connect_timeout(Duration::from_secs(10));
    config
}

/// Initializes the database connection pool.
pub async fn connect() -> Result<PgPool> {
    let config = build_pg_config();
    info!("Connecting to PostgreSQL database...");
    let manager = PostgresConnectionManager::new(config, NoTls);

    let pool = Pool::builder()
        .max_size(60)
        .min_idle(Some(2))
        .idle_timeout(Some(Duration::from_secs(180))) // e.g., 3 minutes
        .connection_timeout(Duration::from_secs(15))
        .build(manager)
        .await
        .context("Failed to build database connection pool")?;

    // Test connection
    let conn = pool
        .get()
        .await
        .context("Failed to get test connection from pool")?;
    conn.query_one("SELECT 1", &[])
        .await
        .context("Test query 'SELECT 1' failed")?;
    info!("Database connection pool initialized successfully.");
    Ok(pool.clone())
}

/// Loads environment variables from a .env file.
pub fn load_env_from_file(file_path: &str) -> Result<()> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    info!(
        "Attempting to load environment variables from: {}",
        file_path
    );
    match File::open(file_path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line.context("Failed to read line from env file")?;
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }
                if let Some(idx) = line.find('=') {
                    let key = line[..idx].trim();
                    let value = line[idx + 1..].trim().trim_matches('"'); // Trim quotes
                    if std::env::var(key).is_err() {
                        // Set only if not already set
                        std::env::set_var(key, value);
                        debug!(
                            "Set env var from file: {} = {}",
                            key,
                            if key == "POSTGRES_PASSWORD" {
                                "[hidden]"
                            } else {
                                value
                            }
                        );
                    }
                }
            }
            info!("Successfully processed env file: {}", file_path);
        }
        Err(e) => {
            warn!(
                "Could not open env file '{}': {}. Proceeding with system environment variables.",
                file_path, e
            );
            // Not returning an error, as .env file is optional.
        }
    }
    Ok(())
}

/// Creates an initial pipeline_run record with default values.
/// This ensures the pipeline_run record exists before any references are made to it.
pub async fn create_initial_pipeline_run(
    pool: &PgPool,
    run_id: &str,
    run_timestamp: NaiveDateTime,
    description: Option<&str>,
) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for create_initial_pipeline_run")?;

    // Initial record with zero counts and timing
    // This will be updated at the end of the pipeline
    const INSERT_SQL: &str = "
        INSERT INTO clustering_metadata.pipeline_run (
            id, run_timestamp, description,
            total_entities, total_groups, total_clusters, total_service_matches, total_visualization_edges,
            entity_processing_time, context_feature_extraction_time, matching_time,
            clustering_time, visualization_edge_calculation_time, service_matching_time, total_processing_time
        )
        VALUES ($1, $2, $3, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    ";

    conn.execute(INSERT_SQL, &[&run_id, &run_timestamp, &description])
        .await
        .context("Failed to insert initial pipeline_run record")?;

    info!("Created initial pipeline_run record with ID: {}", run_id);
    Ok(())
}

pub async fn insert_suggestion(
    conn: &impl GenericClient,
    suggestion: &NewSuggestedAction,
) -> Result<Uuid> {
    const INSERT_SUGGESTION_SQL: &str = "
        INSERT INTO clustering_metadata.suggested_actions (
            pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
            triggering_confidence, details, reason_code, reason_message, priority, status,
            reviewer_id, reviewed_at, review_notes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id";
    let row = conn
        .query_one(
            INSERT_SUGGESTION_SQL,
            &[
                &suggestion.pipeline_run_id,
                &suggestion.action_type,
                &suggestion.entity_id,
                &suggestion.group_id_1,
                &suggestion.group_id_2,
                &suggestion.cluster_id,
                &suggestion.triggering_confidence,
                &suggestion.details,
                &suggestion.reason_code,
                &suggestion.reason_message,
                &suggestion.priority,
                &suggestion.status,
                &suggestion.reviewer_id,
                &suggestion.reviewed_at,
                &suggestion.review_notes,
            ],
        )
        .await
        .context("Failed to insert suggested_action")?;
    
    // Get the ID as a string first
    let id_str: String = row.get(0);
    
    // Parse the string into a Uuid
    let id = Uuid::parse_str(&id_str)
        .context("Failed to parse returned ID string as UUID")?;
    
    Ok(id)
}

pub async fn update_suggestion_review(
    pool: &PgPool,
    suggestion_id: Uuid,
    reviewer_id: String,
    new_status: String,
    review_notes: Option<String>,
) -> Result<u64> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for update_suggestion_review")?;
    const UPDATE_SUGGESTION_SQL: &str = "
        UPDATE clustering_metadata.suggested_actions
         SET status = $1, reviewer_id = $2, reviewed_at = CURRENT_TIMESTAMP, review_notes = $3
         WHERE id = $4";
    
    // Convert UUID to string for the query
    let suggestion_id_str = suggestion_id.to_string();
    
    conn.execute(
        UPDATE_SUGGESTION_SQL,
        &[
            &new_status,
            &reviewer_id,
            &review_notes,
            &suggestion_id_str,
        ],
    )
    .await
    .context("Failed to update suggested_action review")
}

pub async fn insert_cluster_formation_edge(
    conn: &impl GenericClient,
    edge: &NewClusterFormationEdge,
) -> Result<Uuid> {
    const INSERT_EDGE_SQL: &str = "
        INSERT INTO clustering_metadata.cluster_formation_edges (
            pipeline_run_id, source_group_id, target_group_id,
            calculated_edge_weight, contributing_shared_entities
        ) VALUES ($1, $2, $3, $4, $5)
        RETURNING id";
    let row = conn
        .query_one(
            INSERT_EDGE_SQL,
            &[
                &edge.pipeline_run_id,
                &edge.source_group_id,
                &edge.target_group_id,
                &edge.calculated_edge_weight,
                &edge.contributing_shared_entities,
            ],
        )
        .await
        .context("Failed to insert cluster_formation_edge")?;
    
    // Get the ID as a string first
    let id_str: String = row.get(0);
    
    // Parse the string into a Uuid
    let id = Uuid::parse_str(&id_str)
        .context("Failed to parse returned ID string as UUID")?;
    
    Ok(id)
}

pub async fn get_confidence_for_entity_in_group(
    conn: &impl GenericClient,
    _entity_id_to_check: &EntityId, // Parameter not used in the simplified query
    pair_group_id: &str,
) -> Result<Option<f64>> {
    const SELECT_CONFIDENCE_SQL: &str = "
        SELECT confidence_score FROM public.entity_group WHERE id = $1";
    match conn
        .query_opt(SELECT_CONFIDENCE_SQL, &[&pair_group_id])
        .await
    {
        Ok(Some(row)) => Ok(row.get("confidence_score")),
        Ok(None) => {
            warn!("No entity_group found with id: {}", pair_group_id);
            Ok(None)
        }
        Err(e) => Err(anyhow::anyhow!(e).context(format!(
            "Failed to query entity_group for pair_id: {}",
            pair_group_id
        ))),
    }
}

// --- New/Refactored DB Functions for V1 RL System ---

/// Inserts a snapshot of the match decision details.
// Modified insert_match_decision_detail function in db.rs
pub async fn insert_match_decision_detail(
    pool: &PgPool,
    entity_group_id: &str,
    pipeline_run_id: &str,
    snapshotted_features: serde_json::Value,
    method_type_at_decision: &str,
    pre_rl_confidence_at_decision: f64,
    tuned_confidence_at_decision: f64,
    confidence_tuner_version_at_decision: u32,
) -> Result<Uuid> {
    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for insert_match_decision_detail")?;

    // Start a transaction for better error handling
    let tx = conn
        .transaction()
        .await
        .context("Failed to start transaction for match_decision_detail")?;

    // Verify that the entity_group exists before attempting to insert
    let entity_group_check = tx
        .query_one(
            "SELECT 1 FROM public.entity_group WHERE id = $1",
            &[&entity_group_id],
        )
        .await;

    match entity_group_check {
        Ok(_) => {
            // Entity group exists, proceed with insertion
            const INSERT_SQL: &str = "
                INSERT INTO clustering_metadata.match_decision_details (
                    entity_group_id, pipeline_run_id, snapshotted_features,
                    method_type_at_decision, pre_rl_confidence_at_decision,
                    tuned_confidence_at_decision, confidence_tuner_version_at_decision
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id";

            match tx
                .query_one(
                    INSERT_SQL,
                    &[
                        &entity_group_id,
                        &pipeline_run_id,
                        &snapshotted_features,
                        &method_type_at_decision,
                        &pre_rl_confidence_at_decision,
                        &tuned_confidence_at_decision,
                        &(confidence_tuner_version_at_decision as i32), // Cast u32 to i32 for DB
                    ],
                )
                .await
            {
                Ok(row) => {
                    // Commit the transaction
                    tx.commit()
                        .await
                        .context("Failed to commit match_decision_detail transaction")?;
                    
                    // Get the ID as a string first
                    let id_str: String = row.get(0);
                    
                    // Parse the string into a Uuid
                    let id = Uuid::parse_str(&id_str)
                        .context("Failed to parse returned ID string as UUID")?;
                    
                    Ok(id)
                }
                Err(e) => {
                    // Rollback the transaction
                    let _rollback_result = tx.rollback().await;
                    Err(anyhow::anyhow!(
                        "Failed to insert match_decision_detail for entity_group_id {}, method {}: {}", 
                        entity_group_id, method_type_at_decision, e
                    ))
                }
            }
        }
        Err(e) => {
            // Entity group doesn't exist or couldn't be verified
            let _rollback_result = tx.rollback().await;
            Err(anyhow::anyhow!(
                "Failed to verify entity_group_id {} exists before inserting match_decision_detail: {}", 
                entity_group_id, e
            ))
        }
    }
}

/// Inserts a human feedback record.
/// This function would typically be called by the backend service handling HITL frontend requests.
pub async fn insert_human_feedback(
    conn: &impl GenericClient,
    feedback: &NewHumanFeedback<'_>,
) -> Result<Uuid> {
    const INSERT_SQL: &str = "
        INSERT INTO clustering_metadata.human_feedback
            (entity_group_id, reviewer_id, is_match_correct, notes)
        VALUES ($1, $2, $3, $4)
        RETURNING id";
    let row = conn
        .query_one(
            INSERT_SQL,
            &[
                &feedback.entity_group_id,
                &feedback.reviewer_id,
                &feedback.is_match_correct,
                &feedback.notes,
            ],
        )
        .await
        .context("Failed to insert human_feedback")?;
    
    // Get the ID as a string first
    let id_str: String = row.get(0);
    
    // Parse the string into a Uuid
    let id = Uuid::parse_str(&id_str)
        .context("Failed to parse returned ID string as UUID")?;
    
    Ok(id)
}

/// Fetches unprocessed human feedback items for the ConfidenceTuner.
pub async fn fetch_unprocessed_human_feedback(
    client: &impl GenericClient, // Can be a direct connection or transaction
    batch_size: u32,
) -> Result<Vec<HumanFeedbackDbRecord>> {
    let rows = client
        .query(
            "SELECT id, entity_group_id, is_match_correct
             FROM clustering_metadata.human_feedback
             WHERE processed_for_tuner_update_at IS NULL
             ORDER BY feedback_timestamp ASC
             LIMIT $1",
            &[&(batch_size as i64)], // LIMIT expects i64
        )
        .await
        .context("Failed to fetch unprocessed human feedback items from DB")?;

    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        items.push(HumanFeedbackDbRecord {
            id: row.get("id"),
            entity_group_id: row.get("entity_group_id"),
            is_match_correct: row.get("is_match_correct"),
        });
    }
    debug!(
        "Fetched {} unprocessed human feedback items (batch size: {}).",
        items.len(),
        batch_size
    );
    Ok(items)
}

/// Marks a human feedback item as processed by the ConfidenceTuner.
pub async fn mark_human_feedback_as_processed(
    client: &impl GenericClient,
    feedback_id: String,
) -> Result<u64> {
    // Convert UUID to string for the query
    let feedback_id_str = feedback_id.to_string();
    
    let rows_affected = client
        .execute(
            "UPDATE clustering_metadata.human_feedback
             SET processed_for_tuner_update_at = CURRENT_TIMESTAMP
             WHERE id = $1 AND processed_for_tuner_update_at IS NULL",
            &[&feedback_id_str],
        )
        .await
        .context(format!(
            "Failed to mark human feedback ID {} as processed",
            feedback_id_str
        ))?;

    if rows_affected == 1 {
        debug!(
            "Marked human feedback ID {} as processed.",
            feedback_id_str
        );
    } else if rows_affected == 0 {
        warn!(
            "Human feedback ID {} was already processed or does not exist.",
            feedback_id_str
        );
    } else {
        warn!("Attempted to mark human feedback ID {} as processed, but {} rows were affected (expected 0 or 1).", feedback_id_str, rows_affected);
    }
    Ok(rows_affected)
}

// The old `fetch_recent_feedback_items` that used `FeedbackItem` and `view_rl_feedback_items`
// is now replaced by `fetch_unprocessed_human_feedback` which uses the new tables and a simpler struct.
// If `FeedbackItem` from `reinforcement::types` is still needed elsewhere, it should be adjusted or
// the new `HumanFeedbackDataForTuner` should be used.
