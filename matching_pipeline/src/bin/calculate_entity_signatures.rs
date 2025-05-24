use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use dedupe_lib::{
    db::{self, PgPool},
    models::{EntityId, ServiceId},
};
use futures::stream::{self, StreamExt};
use log::{debug, error, info, warn};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;
use tokio_postgres::types::ToSql;

// Constants for batching and CPU utilization
const MIN_BATCH_SIZE: usize = 50;
const MAX_BATCH_SIZE: usize = 500;
const ENTITIES_PER_CPU: usize = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    // Load environment variables
    let env_paths = [".env", ".env.local", "../.env"];
    for path in env_paths.iter() {
        if std::path::Path::new(path).exists() {
            if let Err(e) = db::load_env_from_file(path) {
                warn!("Failed to load environment from {}: {}", path, e);
            } else {
                info!("Loaded environment variables from {}", path);
                break;
            }
        }
    }

    info!("Starting ENTITY signature calculation process...");
    let start_time = Instant::now();

    // Connect to the database
    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;

    // Run validation (optional but recommended)
    if let Err(e) = validate_data_integrity(&pool).await {
        error!("Data integrity validation failed: {}", e);
    }

    // Determine the calculation mode
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "incremental".to_string());

    match mode.as_str() {
        "full" => {
            info!("Running FULL entity signature calculation...");
            calculate_entity_signatures_parallel(&pool, None).await?;
        }
        "incremental" => {
            info!("Running INCREMENTAL entity signature calculation...");
            calculate_changed_entity_signatures_parallel(&pool).await?;
        }
        "check-affected" => {
            info!("Checking and recalculating affected entity signatures...");
            recalculate_affected_entity_signatures_parallel(&pool).await?;
        }
        _ => {
            error!("Invalid mode. Use 'full', 'incremental', or 'check-affected'");
            std::process::exit(1);
        }
    }

    let elapsed = start_time.elapsed();
    info!("Entity signature calculation completed in {:.2?}", elapsed);
    Ok(())
}

async fn calculate_changed_entity_signatures_parallel(pool: &PgPool) -> Result<()> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Query for entities with changed data since the last signature calculation
    let changed_entities_query = r#"
        WITH entity_last_modified AS (
            SELECT
                e.id as entity_id,
                GREATEST(
                    e.updated_at,
                    COALESCE(MAX(o.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(l.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(a.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(p.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(s.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(c.last_modified), '1900-01-01'::timestamp)
                ) as last_data_update
            FROM public.entity e
            LEFT JOIN public.organization o ON e.organization_id = o.id
            LEFT JOIN public.entity_feature ef ON e.id = ef.entity_id
            LEFT JOIN public.location l ON ef.table_id = l.id AND ef.table_name = 'location'
            LEFT JOIN public.address a ON l.id = a.location_id
            LEFT JOIN public.phone p ON ef.table_id = p.id AND ef.table_name = 'phone'
            LEFT JOIN public.service s ON ef.table_id = s.id AND ef.table_name = 'service'
            LEFT JOIN public.contact c ON ef.table_id = c.id AND ef.table_name = 'contact'
            GROUP BY e.id
        )
        SELECT elm.entity_id
        FROM entity_last_modified elm
        LEFT JOIN pipeline_state.entity_data_signatures eds ON elm.entity_id = eds.entity_id
        WHERE eds.entity_id IS NULL
           OR elm.last_data_update > eds.source_data_last_updated_at
    "#;

    let rows = conn
        .query(changed_entities_query, &[])
        .await
        .context("Failed to query changed entities")?;

    let changed_entity_ids: Vec<EntityId> =
        rows.into_iter().map(|row| EntityId(row.get(0))).collect();

    if !changed_entity_ids.is_empty() {
        info!("Found {} entities with changes", changed_entity_ids.len());
        calculate_entity_signatures_parallel(pool, Some(changed_entity_ids)).await?;
    } else {
        info!("No entity changes detected");
    }
    Ok(())
}

async fn recalculate_affected_entity_signatures_parallel(pool: &PgPool) -> Result<()> {
    info!("Checking for entities affected by missing service references...");

    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Find entities referencing non-existent services
    let affected_entities_query = r#"
        SELECT DISTINCT ef.entity_id
        FROM public.entity_feature ef
        LEFT JOIN public.service s ON ef.table_id = s.id
        WHERE ef.table_name = 'service' AND s.id IS NULL
    "#;

    let rows = conn.query(affected_entities_query, &[]).await?;
    let affected_entity_ids: Vec<EntityId> =
        rows.into_iter().map(|row| EntityId(row.get(0))).collect();

    if !affected_entity_ids.is_empty() {
        info!(
            "Found {} entities with missing service references. Recalculating their signatures...",
            affected_entity_ids.len()
        );
        calculate_entity_signatures_parallel(pool, Some(affected_entity_ids)).await?;
    } else {
        info!("No entities affected by missing service references");
    }

    Ok(())
}

async fn calculate_entity_signatures_parallel(
    pool: &PgPool,
    entity_ids: Option<Vec<EntityId>>,
) -> Result<usize> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Determine which entities to process
    let entities_query = if entity_ids.is_some() {
        "SELECT id FROM public.entity WHERE id = ANY($1) ORDER BY id"
    } else {
        "SELECT id FROM public.entity ORDER BY id"
    };

    let rows = if let Some(ids) = &entity_ids {
        let id_strings: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();
        conn.query(entities_query, &[&id_strings]).await?
    } else {
        conn.query(entities_query, &[]).await?
    };

    let all_entity_ids: Vec<EntityId> = rows.into_iter().map(|row| EntityId(row.get(0))).collect();
    let total_count = all_entity_ids.len();
    info!("Processing {} entities", total_count);

    if total_count == 0 {
        return Ok(0);
    }

    // Optimize batching and parallelism based on CPU cores
    let cpu_count = num_cpus::get();
    let optimal_batch_size = calculate_optimal_batch_size(total_count, cpu_count);
    let concurrent_tasks = std::cmp::min(cpu_count * 2, total_count.div_ceil(optimal_batch_size));
    info!(
        "Using {} concurrent tasks with batch size {} (detected {} CPUs)",
        concurrent_tasks, optimal_batch_size, cpu_count
    );

    let batches: Vec<Vec<EntityId>> = all_entity_ids
        .chunks(optimal_batch_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    let processed = Arc::new(AtomicUsize::new(0));
    let mut join_set = JoinSet::new();

    // Process batches in parallel
    for batch in batches {
        let pool_clone = pool.clone();
        let processed_clone = processed.clone();

        join_set.spawn(async move {
            match process_entity_batch_optimized(&pool_clone, batch).await {
                Ok(count) => {
                    let current_processed =
                        processed_clone.fetch_add(count, Ordering::Relaxed) + count;
                    if current_processed % 1000 == 0 || current_processed >= total_count {
                        info!("Processed {}/{} entities", current_processed, total_count);
                    }
                    Ok(count)
                }
                Err(e) => {
                    error!("Batch processing error: {}", e);
                    Err(e)
                }
            }
        });
    }

    // Wait for all tasks to finish
    let mut total_processed = 0;
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(count)) => total_processed += count,
            Ok(Err(e)) => error!("Task failed: {}", e),
            Err(e) => error!("Join error: {}", e),
        }
    }

    info!("Completed processing {} entities", total_processed);
    Ok(total_processed)
}

fn calculate_optimal_batch_size(total_items: usize, cpu_count: usize) -> usize {
    let target_batches_per_cpu = 4;
    let ideal_batch_size = total_items / (cpu_count * target_batches_per_cpu);
    std::cmp::max(
        MIN_BATCH_SIZE,
        std::cmp::min(MAX_BATCH_SIZE, ideal_batch_size.max(1)),
    )
}

async fn process_entity_batch_optimized(pool: &PgPool, entity_ids: Vec<EntityId>) -> Result<usize> {
    let mut conn = pool.get().await.context("Failed to get DB connection")?;
    let tx = conn
        .transaction()
        .await
        .context("Failed to start transaction")?;

    let mut success_count = 0;
    let batch_size = entity_ids.len();

    // Process each entity in the batch
    for (idx, entity_id) in entity_ids.into_iter().enumerate() {
        match calculate_single_entity_signature(&tx, &entity_id).await {
            Ok((signature, last_updated, feature_snapshot)) => {
                // Upsert the calculated signature into the database
                let upsert_query = r#"
                    INSERT INTO pipeline_state.entity_data_signatures
                        (entity_id, signature, source_data_last_updated_at, signature_calculated_at, relevant_attributes_snapshot)
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)
                    ON CONFLICT (entity_id) DO UPDATE SET
                        signature = EXCLUDED.signature,
                        source_data_last_updated_at = EXCLUDED.source_data_last_updated_at,
                        signature_calculated_at = EXCLUDED.signature_calculated_at,
                        relevant_attributes_snapshot = EXCLUDED.relevant_attributes_snapshot
                "#;

                if let Err(e) = tx
                    .execute(
                        upsert_query,
                        &[&entity_id.0, &signature, &last_updated, &feature_snapshot],
                    )
                    .await
                {
                    error!(
                        "Failed to store signature for entity {}: {}",
                        entity_id.0, e
                    );
                } else {
                    success_count += 1;
                }
            }
            Err(e) => {
                error!(
                    "Failed to calculate signature for entity {}: {}",
                    entity_id.0, e
                );
            }
        }

        if batch_size > 100 && (idx + 1) % 50 == 0 {
            debug!(
                "Batch progress: {}/{} entities processed",
                idx + 1,
                batch_size
            );
        }
    }

    tx.commit().await.context("Failed to commit transaction")?;
    Ok(success_count)
}

async fn calculate_single_entity_signature(
    conn: &impl tokio_postgres::GenericClient,
    entity_id: &EntityId,
) -> Result<(String, DateTime<Utc>, serde_json::Value)> {
    let mut components = BTreeMap::new();
    let mut last_updated = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut feature_errors = Vec::new();
    let mut feature_snapshot = json!({});

    // Fetch entity core data
    let entity_query =
        "SELECT name, source_system, source_id, updated_at FROM public.entity WHERE id = $1";
    let entity_row = conn
        .query_one(entity_query, &[&entity_id.0])
        .await
        .context("Failed to fetch entity data")?;

    components.insert(
        "entity_name".to_string(),
        json!(entity_row.get::<_, Option<String>>(0)),
    );
    components.insert(
        "source_system".to_string(),
        json!(entity_row.get::<_, Option<String>>(1)),
    );
    components.insert(
        "source_id".to_string(),
        json!(entity_row.get::<_, Option<String>>(2)),
    );

    if let Ok(updated) = entity_row.try_get::<_, chrono::NaiveDateTime>(3) {
        let updated_utc = DateTime::<Utc>::from_naive_utc_and_offset(updated, Utc);
        last_updated = last_updated.max(updated_utc);
    }

    // Fetch organization data
    let org_query = r#"
        SELECT o.name, o.email, o.url, o.tax_status, o.tax_id,
               o.year_incorporated, o.legal_status, o.last_modified,
               o.embedding_updated_at
        FROM public.organization o
        JOIN public.entity e ON e.organization_id = o.id
        WHERE e.id = $1
    "#;

    if let Ok(org_row) = conn.query_one(org_query, &[&entity_id.0]).await {
        components.insert(
            "org_name".to_string(),
            json!(org_row.get::<_, Option<String>>(0)),
        );
        components.insert(
            "org_email".to_string(),
            json!(org_row.get::<_, Option<String>>(1)),
        );
        components.insert(
            "org_url".to_string(),
            json!(org_row.get::<_, Option<String>>(2)),
        );
        components.insert(
            "tax_status".to_string(),
            json!(org_row.get::<_, Option<String>>(3)),
        );
        components.insert(
            "tax_id".to_string(),
            json!(org_row.get::<_, Option<String>>(4)),
        );
        components.insert(
            "year_incorporated".to_string(),
            json!(org_row.get::<_, Option<String>>(5)),
        );
        components.insert(
            "legal_status".to_string(),
            json!(org_row.get::<_, Option<String>>(6)),
        );
        if let Ok(emb_updated) = org_row.try_get::<_, Option<DateTime<Utc>>>(8) {
            components.insert("embedding_updated_at".to_string(), json!(emb_updated));
        }
        if let Ok(modified) = org_row.try_get::<_, chrono::NaiveDateTime>(7) {
            let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
            last_updated = last_updated.max(modified_utc);
        }
    }

    // Fetch all linked features
    let features_query = r#"
        SELECT ef.table_name, ef.table_id
        FROM public.entity_feature ef
        WHERE ef.entity_id = $1
        ORDER BY ef.table_name, ef.table_id
    "#;
    let feature_rows = conn
        .query(features_query, &[&entity_id.0])
        .await
        .context("Failed to fetch entity features")?;

    for row in feature_rows {
        let table_name: String = row.get(0);
        let table_id: String = row.get(1);

        let feature_result = match table_name.as_str() {
            "location" => fetch_location_data(conn, &table_id)
                .await
                .map(|data| (format!("location_{}", table_id), data.0, data.1)),
            "phone" => fetch_phone_data(conn, &table_id)
                .await
                .map(|data| (format!("phone_{}", table_id), data.0, data.1)),
            "service" => fetch_service_feature_data_safe(conn, &table_id)
                .await
                .map(|data| (format!("service_{}", table_id), data.0, data.1)),
            "contact" => fetch_contact_data(conn, &table_id)
                .await
                .map(|data| (format!("contact_{}", table_id), data.0, data.1)),
            _ => {
                warn!(
                    "Unknown feature table: {} for entity {}",
                    table_name, entity_id.0
                );
                continue;
            }
        };

        match feature_result {
            Ok((key, value, updated)) => {
                components.insert(key.clone(), value.clone());
                feature_snapshot[&key] = value;
                last_updated = last_updated.max(updated);
            }
            Err(e) => {
                feature_errors.push(format!(
                    "Failed to fetch {} {}: {}",
                    table_name, table_id, e
                ));
            }
        }
    }

    if !feature_errors.is_empty() {
        warn!(
            "Entity {} had {} feature fetch errors: {:?}",
            entity_id.0,
            feature_errors.len(),
            feature_errors
        );
        feature_snapshot["_errors"] = json!(feature_errors);
    }

    // Generate SHA256 hash
    let mut hasher = Sha256::new();
    for (key, value) in components {
        hasher.update(format!("{}:{}", key, value.to_string()).as_bytes());
    }

    let signature = hex::encode(hasher.finalize());
    Ok((signature, last_updated, feature_snapshot))
}

// --- Data Integrity Validation (Copied) ---
async fn validate_data_integrity(pool: &PgPool) -> Result<()> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for validation")?;

    info!("Validating data integrity...");

    let orphaned_services_query = r#"
        SELECT COUNT(DISTINCT ef.table_id) as orphaned_count
        FROM public.entity_feature ef
        LEFT JOIN public.service s ON ef.table_id = s.id
        WHERE ef.table_name = 'service' AND s.id IS NULL
    "#;

    let row = conn.query_one(orphaned_services_query, &[]).await?;
    let orphaned_count: i64 = row.get(0);

    if orphaned_count > 0 {
        warn!(
            "Found {} orphaned service references in entity_feature table!",
            orphaned_count
        );
        // Add more detailed logging if needed...
    }
    // Add other checks if necessary...

    Ok(())
}


// --- Feature Fetching Functions (Copied) ---
async fn fetch_location_data(
    conn: &impl tokio_postgres::GenericClient,
    location_id: &str,
) -> Result<(serde_json::Value, DateTime<Utc>)> {
        let query = r#"
        SELECT l.name, l.alternate_name, l.description, l.latitude, l.longitude,
               l.location_type, l.last_modified,
               a.address_1, a.address_2, a.city, a.state_province, a.postal_code,
               a.country, a.last_modified
        FROM public.location l
        LEFT JOIN public.address a ON l.id = a.location_id
        WHERE l.id = $1
    "#;

    let row = conn
        .query_one(query, &[&location_id])
        .await
        .context("Failed to fetch location data")?;

    let mut last_updated = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    if let Ok(modified) = row.try_get::<_, chrono::NaiveDateTime>(6) {
        let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
        last_updated = last_updated.max(modified_utc);
    }

    if let Ok(modified) = row.try_get::<_, chrono::NaiveDateTime>(13) {
        let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
        last_updated = last_updated.max(modified_utc);
    }

    let data = json!({
        "name": row.get::<_, Option<String>>(0),
        "alternate_name": row.get::<_, Option<String>>(1),
        "description": row.get::<_, Option<String>>(2),
        "latitude": row.get::<_, Option<f64>>(3),
        "longitude": row.get::<_, Option<f64>>(4),
        "location_type": row.get::<_, Option<String>>(5),
        "address_1": row.get::<_, Option<String>>(7),
        "address_2": row.get::<_, Option<String>>(8),
        "city": row.get::<_, Option<String>>(9),
        "state_province": row.get::<_, Option<String>>(10),
        "postal_code": row.get::<_, Option<String>>(11),
        "country": row.get::<_, Option<String>>(12),
    });

    Ok((data, last_updated))
}

async fn fetch_phone_data(
    conn: &impl tokio_postgres::GenericClient,
    phone_id: &str,
) -> Result<(serde_json::Value, DateTime<Utc>)> {
    let query = "SELECT number, extension, type, last_modified FROM public.phone WHERE id = $1";
    let row = conn
        .query_one(query, &[&phone_id])
        .await
        .context("Failed to fetch phone data")?;

    let mut last_updated = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    if let Ok(modified) = row.try_get::<_, chrono::NaiveDateTime>(3) {
        let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
        last_updated = last_updated.max(modified_utc);
    }

    let data = json!({
        "number": row.get::<_, String>(0),
        "extension": row.get::<_, Option<String>>(1),
        "type": row.get::<_, Option<String>>(2),
    });

    Ok((data, last_updated))
}

async fn fetch_contact_data(
    conn: &impl tokio_postgres::GenericClient,
    contact_id: &str,
) -> Result<(serde_json::Value, DateTime<Utc>)> {
    let query =
        "SELECT name, title, department, email, last_modified FROM public.contact WHERE id = $1";
    let row = conn
        .query_one(query, &[&contact_id])
        .await
        .context("Failed to fetch contact data")?;

    let mut last_updated = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    if let Ok(modified) = row.try_get::<_, chrono::NaiveDateTime>(4) {
        let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
        last_updated = last_updated.max(modified_utc);
    }

    let data = json!({
        "name": row.get::<_, Option<String>>(0),
        "title": row.get::<_, Option<String>>(1),
        "department": row.get::<_, Option<String>>(2),
        "email": row.get::<_, Option<String>>(3),
    });

    Ok((data, last_updated))
}

fn hash_long_field(field: &Option<String>) -> Option<String> {
    field.as_ref().map(|text| {
        if text.len() > 100 {
            let mut hasher = Sha256::new();
            hasher.update(text.as_bytes());
            hex::encode(hasher.finalize())
        } else {
            text.clone()
        }
    })
}

async fn fetch_service_feature_data_safe(
    conn: &impl tokio_postgres::GenericClient,
    service_id: &str,
) -> Result<(serde_json::Value, DateTime<Utc>)> {
    let query = r#"
        SELECT name, alternate_name, description, url, email, status,
               last_modified, embedding_v2_updated_at
        FROM public.service WHERE id = $1
    "#;

    match conn.query_opt(query, &[&service_id]).await {
        Ok(Some(row)) => {
            let mut last_updated = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc);

            if let Ok(modified) = row.try_get::<_, chrono::NaiveDateTime>(6) {
                let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
                last_updated = last_updated.max(modified_utc);
            }

            let data = json!({
                "name": row.get::<_, String>(0),
                "alternate_name": row.get::<_, Option<String>>(1),
                "description_hash": hash_long_field(&row.get::<_, Option<String>>(2)),
                "url": row.get::<_, Option<String>>(3),
                "email": row.get::<_, Option<String>>(4),
                "status": row.get::<_, String>(5),
                "embedding_v2_updated_at": row.get::<_, Option<DateTime<Utc>>>(7),
            });

            Ok((data, last_updated))
        }
        Ok(None) => {
            warn!(
                "Service {} referenced in entity_feature but not found in service table",
                service_id
            );
            let empty_data = json!({
                "error": "service_not_found",
                "service_id": service_id
            });
            let default_time = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc);
            Ok((empty_data, default_time))
        }
        Err(e) => Err(anyhow::anyhow!(
            "Database error fetching service {}: {}",
            service_id,
            e
        )),
    }
}