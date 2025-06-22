// src/db.rs

use anyhow::{Context, Result};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, NaiveDateTime, Utc};
use log::{debug, error, info, warn};
use postgres_types::ToSql;
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio_postgres::{Config, GenericClient, NoTls, Row as PgRow, Transaction}; // Keep Transaction for other uses
use uuid::Uuid;

use crate::{
    models::{EntityId, MatchValues, NewClusterFormationEdge, NewSuggestedAction, ServiceId, VisualizationEdgeData},
    MatchMethodType,
};

pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;

#[derive(Debug)]
pub struct HumanFeedbackDbRecord {
    pub id: String,
    pub entity_group_id: String,
    pub is_match_correct: bool,
}

#[derive(Debug)]
pub struct NewHumanFeedback<'a> {
    pub entity_group_id: &'a str,
    pub reviewer_id: &'a str,
    pub is_match_correct: bool,
    pub notes: Option<&'a str>,
    pub match_decision_id: &'a str,
}

#[derive(Debug, Clone)]
pub struct ServiceComparisonCacheEntry {
    pub service_id_1: String,
    pub service_id_2: String,
    pub signature_1: String,
    pub signature_2: String,
    pub method_type: String,
    pub pipeline_run_id: Option<String>,
    pub comparison_result: String,
    pub confidence_score: Option<f64>,
    pub snapshotted_features: Option<JsonValue>,
    pub cached_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewServiceComparisonCacheEntry {
    pub service_id_1: String,
    pub service_id_2: String,
    pub signature_1: String,
    pub signature_2: String,
    pub method_type: String,
    pub pipeline_run_id: Option<String>,
    pub comparison_result: String,
    pub confidence_score: Option<f64>,
    pub snapshotted_features: Option<JsonValue>,
}

#[derive(Debug, Clone)]
pub struct ServiceDataSignature {
    pub service_id: String,
    pub signature: String,
    pub relevant_attributes_snapshot: Option<JsonValue>,
    pub source_data_last_updated_at: Option<DateTime<Utc>>,
    pub signature_calculated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewServiceGroup {
    pub proposed_id: String,
    pub service_id_1: ServiceId,
    pub service_id_2: ServiceId,
    pub confidence_score: f64,
    pub pre_rl_confidence_score: f64,
    pub method_type: MatchMethodType,
    pub match_values: MatchValues,
}

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

pub async fn connect() -> Result<PgPool> {
    let config = build_pg_config();
    info!("Connecting to PostgreSQL database...");
    let manager = PostgresConnectionManager::new(config, NoTls);

    let pool = Pool::builder()
        .max_size(90)
        .min_idle(Some(2))
        .idle_timeout(Some(Duration::from_secs(180)))
        .connection_timeout(Duration::from_secs(40))
        .build(manager)
        .await
        .context("Failed to build database connection pool")?;

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
                    let value = line[idx + 1..].trim().trim_matches('"');
                    if std::env::var(key).is_err() {
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
        }
    }
    Ok(())
}

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

/// Upserts a service group. If it exists, it's updated. If not, it's inserted.
/// Returns the ID of the service group (either existing or newly inserted).
pub async fn upsert_service_group(
    pool: &PgPool,
    proposed_id: &str, // ID to use if inserting a new record
    service_id_1: &ServiceId,
    service_id_2: &ServiceId,
    confidence_score: f64,
    pre_rl_confidence_score: f64,
    method_type: MatchMethodType,
    match_values: MatchValues,
) -> Result<String> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for upsert_service_group")?;

    let (s1, s2) = if service_id_1.0 < service_id_2.0 {
        (service_id_1, service_id_2)
    } else {
        (service_id_2, service_id_1)
    };

    let match_values_json =
        serde_json::to_value(match_values).context("Failed to serialize match values")?;

    // Check for existing group
    let existing_row = conn
        .query_opt(
            "SELECT id, confirmed_status FROM public.service_group
             WHERE service_id_1 = $1 AND service_id_2 = $2 AND method_type = $3",
            &[&s1.0, &s2.0, &method_type.as_str()],
        )
        .await
        .context("Failed to check existing service_group")?;

    if let Some(row) = existing_row {
        let existing_id: String = row.get("id");
        let existing_status: String = row.get("confirmed_status");

        let new_status = match existing_status.as_str() {
            "CONFIRMED" | "REJECTED" => existing_status,
            _ => "PENDING_REVIEW".to_string(),
        };

        conn.execute(
            "UPDATE public.service_group
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
        .context(format!("Failed to update service_group id {}", existing_id))?;
        debug!("Updated service_group id {}.", existing_id);
        Ok(existing_id)
    } else {
        conn.execute(
            "INSERT INTO public.service_group
             (id, service_id_1, service_id_2, confidence_score, method_type, match_values,
              pre_rl_confidence_score, confirmed_status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            &[
                &proposed_id,
                &s1.0,
                &s2.0,
                &confidence_score,
                &method_type.as_str(),
                &match_values_json,
                &pre_rl_confidence_score,
            ],
        )
        .await
        .context(format!("Failed to insert service_group id {}", proposed_id))?;
        debug!("Inserted service_group id {}.", proposed_id);
        Ok(proposed_id.to_string())
    }
}

/// Inserts a service match decision detail record directly.
pub async fn insert_service_match_decision_detail_direct(
    pool: &PgPool,
    service_group_id: &str,
    pipeline_run_id: &str,
    snapshotted_features: serde_json::Value,
    method_type_at_decision: &str,
    pre_rl_confidence_at_decision: f64,
    tuned_confidence_at_decision: f64,
    confidence_tuner_version_at_decision: Option<i32>,
) -> Result<Uuid> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for insert_service_match_decision_detail_direct")?;

    const INSERT_SQL: &str = "
        INSERT INTO clustering_metadata.service_match_decision_details (
            service_group_id, pipeline_run_id, snapshotted_features,
            method_type_at_decision, pre_rl_confidence_at_decision,
            tuned_confidence_at_decision, confidence_tuner_version_at_decision
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id";

    let row = conn
        .query_one(
            INSERT_SQL,
            &[
                &service_group_id,
                &pipeline_run_id,
                &snapshotted_features,
                &method_type_at_decision,
                &pre_rl_confidence_at_decision,
                &tuned_confidence_at_decision,
                &confidence_tuner_version_at_decision,
            ],
        )
        .await
        .context("Failed to insert service_match_decision_detail")?;

    let id_str: String = row.get(0);
    let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID for service_match_decision_detail")?;
    debug!(
        "Inserted service_match_decision_detail id {} for group {}.",
        id, service_group_id
    );
    Ok(id)
}

/// Batch upsert service groups. If they exist, they're updated. If not, they're inserted.
/// Returns a vector of the service group IDs (either existing or newly inserted).
/// Processes the batch using PostgreSQL's ON CONFLICT functionality for efficient upserts.
pub async fn upsert_service_groups_batch(
    pool: &PgPool,
    service_groups: &[NewServiceGroup],
) -> Result<Vec<String>> {
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
            &ServiceId,     // service_id_1
            &ServiceId,     // service_id_2  
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
            let (s1, s2) = if group.service_id_1.0 < group.service_id_2.0 {
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
            params.push(&s1.0);
            params.push(&s2.0);
            params.push(*confidence_score);
            params.push(method_type_str); // No need for & since it's already a String
            params.push(match_values_json);
            params.push(*pre_rl_confidence_score);
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
) -> Result<Vec<Uuid>> {
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
            let id = Uuid::parse_str(&id_str)
                .context("Failed to parse returned ID string as UUID for service_match_decision_detail")?;
            all_ids.push(id);
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

/// Upserts an entity group. If it exists, it's updated. If not, it's inserted.
/// Returns the ID of the entity group and a boolean indicating if a new group was inserted.
pub async fn upsert_entity_group(
    pool: &PgPool,
    proposed_id: &str, // ID to use if inserting a new record
    entity_id_1: &EntityId,
    entity_id_2: &EntityId,
    confidence_score: f64,
    pre_rl_confidence_score: f64,
    method_type: MatchMethodType,
    match_values: MatchValues,
) -> Result<(String, bool)> { // Returns (group_id, was_inserted)
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for upsert_entity_group")?;

    let (e1, e2) = if entity_id_1.0 < entity_id_2.0 {
        (entity_id_1, entity_id_2)
    } else {
        (entity_id_2, entity_id_1)
    };

    let match_values_json = serde_json::to_value(match_values)
        .context("Failed to serialize match values for entity_group")?;

    let existing_row = conn
        .query_opt(
            "SELECT id, confirmed_status FROM public.entity_group
             WHERE entity_id_1 = $1 AND entity_id_2 = $2 AND method_type = $3",
            &[&e1.0, &e2.0, &method_type.as_str()],
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
        debug!("Updated entity_group id {}.", existing_id);
        Ok((existing_id, false)) // false because it was an update
    } else {
        conn.execute(
            "INSERT INTO public.entity_group
             (id, entity_id_1, entity_id_2, confidence_score, method_type, match_values,
              pre_rl_confidence_score, confirmed_status, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING_REVIEW', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            &[
                &proposed_id,
                &e1.0,
                &e2.0,
                &confidence_score,
                &method_type.as_str(),
                &match_values_json,
                &pre_rl_confidence_score,
            ],
        )
        .await
        .context(format!("Failed to insert entity_group id {}", proposed_id))?;
        debug!("Inserted entity_group id {}.", proposed_id);
        Ok((proposed_id.to_string(), true)) // true because it was an insert
    }
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
) -> Result<Uuid> {
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
    let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID for match_decision_detail")?;
    debug!(
        "Inserted match_decision_detail id {} for group {}.",
        id, entity_group_id
    );
    Ok(id)
}


// --- Other existing functions from db.rs (shortened for brevity) ---

pub async fn insert_suggestion(pool: &PgPool, suggestion: &NewSuggestedAction) -> Result<Uuid> {
    // This function already uses a single transaction for a single insert.
    // For consistency with the request to move away from explicit transactions
    // for single operations where not strictly needed for atomicity of multiple steps,
    // this could also be refactored to use a direct connection.
    // However, since it's one operation, the benefit is smaller.
    // For now, leaving as is unless specifically requested to change.
    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for insert_suggestion")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for insert_suggestion")?;

    const INSERT_SUGGESTION_SQL: &str = "
        INSERT INTO clustering_metadata.suggested_actions (
            pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
            triggering_confidence, details, reason_code, reason_message, priority, status,
            reviewer_id, reviewed_at, review_notes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        RETURNING id";
    let row = transaction
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

    let id_str: String = row.get(0);
    let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID")?;

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for insert_suggestion")?;
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

    let suggestion_id_str = suggestion_id.to_string();

    conn.execute(
        UPDATE_SUGGESTION_SQL,
        &[&new_status, &reviewer_id, &review_notes, &suggestion_id_str],
    )
    .await
    .context("Failed to update suggested_action review")
}

pub async fn insert_cluster_formation_edge(
    pool: &PgPool,
    edge: &NewClusterFormationEdge,
) -> Result<Uuid> {
    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for insert_cluster_formation_edge")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for insert_cluster_formation_edge")?;

    const INSERT_EDGE_SQL: &str = "
        INSERT INTO clustering_metadata.cluster_formation_edges (
            pipeline_run_id, source_group_id, target_group_id,
            calculated_edge_weight, contributing_shared_entities
        ) VALUES ($1, $2, $3, $4, $5)
        RETURNING id";
    let row = transaction
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

    let id_str: String = row.get(0);
    let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID")?;

    transaction
        .commit()
        .await
        .context("Failed to commit transaction for insert_cluster_formation_edge")?;
    Ok(id)
}

pub async fn get_confidence_for_entity_in_group(
    conn: &impl GenericClient,
    _entity_id_to_check: &EntityId,
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

pub async fn insert_human_feedback(
    conn: &impl GenericClient, // Note: This takes a GenericClient, not a pool directly
    feedback: &NewHumanFeedback<'_>,
) -> Result<Uuid> {
    // This function already operates on a GenericClient, so it's not managing its own connection from the pool.
    // It's suitable for use within an existing transaction or with a direct connection.
    // No change needed here based on the primary request, but be mindful of how it's called.
    const INSERT_SQL: &str = "
        INSERT INTO clustering_metadata.human_feedback
            (entity_group_id, reviewer_id, is_match_correct, notes, match_decision_id)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id";
    let row = conn
        .query_one(
            INSERT_SQL,
            &[
                &feedback.entity_group_id,
                &feedback.reviewer_id,
                &feedback.is_match_correct,
                &feedback.notes,
                &feedback.match_decision_id,
            ],
        )
        .await
        .context("Failed to insert human_feedback")?;

    let id_str: String = row.get(0);
    let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID")?;
    Ok(id)
}

pub async fn fetch_unprocessed_human_feedback(
    client: &impl GenericClient,
    batch_size: u32,
) -> Result<Vec<HumanFeedbackDbRecord>> {
    let rows = client
        .query(
            "SELECT id, entity_group_id, is_match_correct
             FROM clustering_metadata.human_feedback
             WHERE processed_for_tuner_update_at IS NULL
             ORDER BY feedback_timestamp ASC
             LIMIT $1",
            &[&(batch_size as i64)],
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

pub async fn mark_human_feedback_as_processed(
    client: &impl GenericClient,
    feedback_id: String,
) -> Result<u64> {
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
        debug!("Marked human feedback ID {} as processed.", feedback_id_str);
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

pub async fn insert_cluster_formation_edges_batch(
    pool: &PgPool,
    edges: &[NewClusterFormationEdge],
) -> Result<Vec<Uuid>> {
    if edges.is_empty() {
        return Ok(Vec::new());
    }

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch insert")?;
    // This function correctly uses a transaction for a BATCH insert, which is appropriate.
    // No change needed here based on the request.
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch insert")?;

    let mut query = String::from(
        "INSERT INTO clustering_metadata.cluster_formation_edges (
            pipeline_run_id, source_group_id, target_group_id,
            calculated_edge_weight, contributing_shared_entities
        ) VALUES ",
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let mut param_groups = Vec::new();

    for (i, edge) in edges.iter().enumerate() {
        let base_idx = i * 5;
        param_groups.push(format!(
            "(${}, ${}, ${}, ${}, ${})",
            base_idx + 1,
            base_idx + 2,
            base_idx + 3,
            base_idx + 4,
            base_idx + 5
        ));

        params.push(&edge.pipeline_run_id);
        params.push(&edge.source_group_id);
        params.push(&edge.target_group_id);
        params.push(&edge.calculated_edge_weight);
        params.push(&edge.contributing_shared_entities);
    }

    query.push_str(&param_groups.join(", "));
    query.push_str(" RETURNING id");

    let rows = transaction
        .query(&query, &params[..])
        .await
        .context("Failed to batch insert cluster_formation_edges")?;

    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        let id_str: String = row.get(0);
        let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID")?;
        ids.push(id);
    }
    transaction
        .commit()
        .await
        .context("Failed to commit transaction for batch insert")?;

    Ok(ids)
}

pub async fn insert_suggestions_batch(
    pool: &PgPool,
    suggestions: &[NewSuggestedAction],
) -> Result<Vec<Uuid>> {
    if suggestions.is_empty() {
        return Ok(Vec::new());
    }

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch insert suggestions")?;
    // This function correctly uses a transaction for a BATCH insert.
    // No change needed here.
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch insert suggestions")?;

    let mut query = String::from(
        "INSERT INTO clustering_metadata.suggested_actions (
            pipeline_run_id, action_type, entity_id, group_id_1, group_id_2, cluster_id,
            triggering_confidence, details, reason_code, reason_message, priority, status,
            reviewer_id, reviewed_at, review_notes
        ) VALUES ",
    );

    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
    let mut param_groups = Vec::new();

    for (i, suggestion) in suggestions.iter().enumerate() {
        let base_idx = i * 15;
        param_groups.push(format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
            base_idx + 1,
            base_idx + 2,
            base_idx + 3,
            base_idx + 4,
            base_idx + 5,
            base_idx + 6,
            base_idx + 7,
            base_idx + 8,
            base_idx + 9,
            base_idx + 10,
            base_idx + 11,
            base_idx + 12,
            base_idx + 13,
            base_idx + 14,
            base_idx + 15
        ));

        params.push(&suggestion.pipeline_run_id);
        params.push(&suggestion.action_type);
        params.push(&suggestion.entity_id);
        params.push(&suggestion.group_id_1);
        params.push(&suggestion.group_id_2);
        params.push(&suggestion.cluster_id);
        params.push(&suggestion.triggering_confidence);
        params.push(&suggestion.details);
        params.push(&suggestion.reason_code);
        params.push(&suggestion.reason_message);
        params.push(&suggestion.priority);
        params.push(&suggestion.status);
        params.push(&suggestion.reviewer_id);
        params.push(&suggestion.reviewed_at);
        params.push(&suggestion.review_notes);
    }

    query.push_str(&param_groups.join(", "));
    query.push_str(" RETURNING id");

    let rows = transaction
        .query(&query, &params[..])
        .await
        .context("Failed to batch insert suggested_actions")?;

    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        let id_str: String = row.get(0);
        let id = Uuid::parse_str(&id_str).context("Failed to parse returned ID string as UUID")?;
        ids.push(id);
    }
    transaction
        .commit()
        .await
        .context("Failed to commit transaction for batch insert suggestions")?;

    Ok(ids)
}

pub async fn get_service_comparison_cache_entry(
    pool: &PgPool,
    service_id_1: &str,
    service_id_2: &str,
    signature_1: &str,
    signature_2: &str,
    method_type: &str,
) -> Result<Option<ServiceComparisonCacheEntry>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for service comparison cache lookup")?;

    let (s1, s2, sig1, sig2) = if service_id_1 < service_id_2 {
        (service_id_1, service_id_2, signature_1, signature_2)
    } else {
        (service_id_2, service_id_1, signature_2, signature_1)
    };

    let row_opt = conn
        .query_opt(
            "SELECT service_id_1, service_id_2, signature_1, signature_2, method_type,
                pipeline_run_id, comparison_result, confidence_score,
                snapshotted_features, cached_at
         FROM pipeline_state.service_comparison_cache
         WHERE service_id_1 = $1 AND service_id_2 = $2
           AND signature_1 = $3 AND signature_2 = $4
           AND method_type = $5",
            &[&s1, &s2, &sig1, &sig2, &method_type],
        )
        .await
        .context("Failed to query service_comparison_cache")?;

    if let Some(row) = row_opt {
        Ok(Some(ServiceComparisonCacheEntry {
            service_id_1: row.get("service_id_1"),
            service_id_2: row.get("service_id_2"),
            signature_1: row.get("signature_1"),
            signature_2: row.get("signature_2"),
            method_type: row.get("method_type"),
            pipeline_run_id: row.get("pipeline_run_id"),
            comparison_result: row.get("comparison_result"),
            confidence_score: row.get("confidence_score"),
            snapshotted_features: row.get("snapshotted_features"),
            cached_at: row.get("cached_at"),
        }))
    } else {
        Ok(None)
    }
}

pub async fn insert_service_comparison_cache_entry(
    pool: &PgPool,
    entry: NewServiceComparisonCacheEntry,
) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for inserting service comparison cache entry")?;

    let (s1, s2, sig1, sig2) = if entry.service_id_1 < entry.service_id_2 {
        (
            entry.service_id_1,
            entry.service_id_2,
            entry.signature_1,
            entry.signature_2,
        )
    } else {
        (
            entry.service_id_2,
            entry.service_id_1,
            entry.signature_2,
            entry.signature_1,
        )
    };

    conn.execute(
        "INSERT INTO pipeline_state.service_comparison_cache
         (service_id_1, service_id_2, signature_1, signature_2, method_type,
          pipeline_run_id, comparison_result, confidence_score, snapshotted_features, cached_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
         ON CONFLICT (service_id_1, service_id_2, signature_1, signature_2, method_type)
         DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            comparison_result = EXCLUDED.comparison_result,
            confidence_score = EXCLUDED.confidence_score,
            snapshotted_features = EXCLUDED.snapshotted_features,
            cached_at = CURRENT_TIMESTAMP",
        &[
            &s1,
            &s2,
            &sig1,
            &sig2,
            &entry.method_type,
            &entry.pipeline_run_id,
            &entry.comparison_result,
            &entry.confidence_score,
            &entry.snapshotted_features,
        ],
    )
    .await
    .context("Failed to insert or update service_comparison_cache entry")?;
    debug!(
        "Inserted/Updated service comparison cache entry for ({}, {}) method {}",
        s1, s2, entry.method_type
    );
    Ok(())
}

pub async fn get_service_signature(
    pool: &PgPool,
    service_id: &str,
) -> Result<Option<ServiceDataSignature>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for getting service signature")?;
    let row_opt = conn
        .query_opt(
            "SELECT service_id, signature, relevant_attributes_snapshot,
                source_data_last_updated_at, signature_calculated_at
         FROM pipeline_state.service_data_signatures
         WHERE service_id = $1",
            &[&service_id],
        )
        .await
        .context("Failed to query service_data_signatures")?;

    if let Some(row) = row_opt {
        Ok(Some(ServiceDataSignature {
            service_id: row.get("service_id"),
            signature: row.get("signature"),
            relevant_attributes_snapshot: row.get("relevant_attributes_snapshot"),
            source_data_last_updated_at: row.get("source_data_last_updated_at"),
            signature_calculated_at: row.get("signature_calculated_at"),
        }))
    } else {
        Ok(None)
    }
}

pub async fn upsert_service_signature(
    pool: &PgPool,
    service_id: &str,
    signature: &str,
    relevant_attributes_snapshot: Option<JsonValue>,
    source_data_last_updated_at: Option<DateTime<Utc>>,
) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for upserting service signature")?;
    conn.execute(
        "INSERT INTO pipeline_state.service_data_signatures
         (service_id, signature, relevant_attributes_snapshot, source_data_last_updated_at, signature_calculated_at)
         VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
         ON CONFLICT (service_id)
         DO UPDATE SET
            signature = EXCLUDED.signature,
            relevant_attributes_snapshot = EXCLUDED.relevant_attributes_snapshot,
            source_data_last_updated_at = EXCLUDED.source_data_last_updated_at,
            signature_calculated_at = CURRENT_TIMESTAMP",
        &[&service_id, &signature, &relevant_attributes_snapshot, &source_data_last_updated_at],
    ).await.context("Failed to upsert service_data_signature")?;
    debug!("Upserted service signature for service_id: {}", service_id);
    Ok(())
}

/// This function is moved from `cluster_visualization.rs` and generalized to be
/// part of the shared DB module. It efficiently inserts visualization edges in batches.
pub async fn bulk_insert_visualization_edges(pool: &PgPool, edges: &[VisualizationEdgeData]) -> Result<()> {
    if edges.is_empty() {
        return Ok(());
    }

    const BATCH_SIZE: usize = 1000;
    let client = pool.get().await.context("Failed to get DB connection for bulk insert")?;

    for chunk in edges.chunks(BATCH_SIZE) {
        let mut id_vec = Vec::with_capacity(chunk.len());
        let mut cluster_id_vec = Vec::with_capacity(chunk.len());
        let mut entity_id_1_vec = Vec::with_capacity(chunk.len());
        let mut entity_id_2_vec = Vec::with_capacity(chunk.len());
        let mut edge_weight_vec = Vec::with_capacity(chunk.len());
        let mut details_vec = Vec::with_capacity(chunk.len());
        let mut pipeline_run_id_vec = Vec::with_capacity(chunk.len());

        for edge in chunk {
            id_vec.push(Uuid::new_v4().to_string());
            cluster_id_vec.push(edge.cluster_id.clone());
            entity_id_1_vec.push(edge.entity_id_1.clone());
            entity_id_2_vec.push(edge.entity_id_2.clone());
            edge_weight_vec.push(edge.edge_weight);
            details_vec.push(edge.details.clone());
            pipeline_run_id_vec.push(edge.pipeline_run_id.clone());
        }

        client.execute(
            "INSERT INTO public.entity_edge_visualization
             (id, cluster_id, entity_id_1, entity_id_2, edge_weight, details, pipeline_run_id)
             SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[], $6::jsonb[], $7::text[])",
            &[
                &id_vec,
                &cluster_id_vec,
                &entity_id_1_vec,
                &entity_id_2_vec,
                &edge_weight_vec,
                &details_vec,
                &pipeline_run_id_vec,
            ],
        )
        .await
        .context("Failed to insert visualization entity edges in batch")?;
    }

    Ok(())
}

/// Counts the number of visualization edges for a given pipeline run.
pub async fn count_visualization_edges(pool: &PgPool, pipeline_run_id: &str) -> Result<i64> {
    let client = pool.get().await.context("Failed to get DB connection")?;
    let row = client
        .query_one(
            "SELECT COUNT(*) FROM public.entity_edge_visualization WHERE pipeline_run_id = $1",
            &[&pipeline_run_id],
        )
        .await
        .context("Failed to count visualization edges")?;
    Ok(row.get(0))
}

/// Determine the proper weighting between RL and pre-RL confidence scores
/// based on historical human feedback. This is moved here to be a shared utility.
pub async fn get_rl_weight_from_feedback(pool: &PgPool) -> Result<f64> {
    let client = pool.get().await.context("DB connection failed for RL weight")?;
    let query = "
        SELECT
            COUNT(CASE WHEN is_match_correct = true THEN 1 END) as correct_count,
            COUNT(CASE WHEN is_match_correct = false THEN 1 END) as incorrect_count
        FROM clustering_metadata.human_feedback
        WHERE reviewer_id != 'ml_system'
    ";

    let result = client.query_opt(query, &[]).await;
    match result {
        Ok(Some(row)) => {
            let correct_count: i64 = row.get("correct_count");
            let incorrect_count: i64 = row.get("incorrect_count");

            if correct_count + incorrect_count > 0 {
                let accuracy = correct_count as f64 / (correct_count + incorrect_count) as f64;
                // Give more weight to RL if it has proven to be accurate
                let rl_weight = 0.5 + (0.4 * accuracy); // Scale from 0.5 to 0.9
                Ok(rl_weight.min(0.9))
            } else {
                Ok(0.6) // Default weight if no feedback
            }
        }
        _ => {
            debug!("Could not query human feedback, using default RL weight of 0.6");
            Ok(0.6)
        }
    }
}