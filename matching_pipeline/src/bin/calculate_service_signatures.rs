use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use dedupe_lib::{
    db::{self, PgPool},
    models::ServiceId,
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

    info!("Starting SERVICE signature calculation process...");
    let start_time = Instant::now();

    // Connect to the database
    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;

    // Determine the calculation mode
    let mode = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "incremental".to_string());

    match mode.as_str() {
        "full" => {
            info!("Running FULL service signature calculation...");
            calculate_service_signatures_parallel(&pool, None).await?;
        }
        "incremental" => {
            info!("Running INCREMENTAL service signature calculation...");
            calculate_changed_service_signatures_parallel(&pool).await?;
        }
        "rebuild" => {
            info!("Running REBUILD of service signatures...");
            info!("First, deleting all existing service signatures...");
            delete_all_service_signatures(&pool).await?; // New step
            info!("Existing service signatures deleted. Now running FULL calculation...");
            calculate_service_signatures_parallel(&pool, None).await?; // Run full after delete
        }
        _ => {
            error!("Invalid mode. Use 'full', 'incremental', or 'rebuild'");
            std::process::exit(1);
        }
    }

    let elapsed = start_time.elapsed();
    info!("Service signature calculation completed in {:.2?}", elapsed);
    Ok(())
}

async fn delete_all_service_signatures(pool: &PgPool) -> Result<()> {
    let conn = pool.get().await.context("Failed to get DB connection for delete")?;
    let delete_query = "DELETE FROM pipeline_state.service_data_signatures";
    let rows_affected = conn
        .execute(delete_query, &[])
        .await
        .context("Failed to delete service signatures")?;
    info!("Deleted {} existing service signatures.", rows_affected);
    Ok(())
}


async fn calculate_changed_service_signatures_parallel(pool: &PgPool) -> Result<()> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Query for services with changed data
    let changed_services_query = r#"
        WITH service_last_modified AS (
            SELECT
                s.id as service_id,
                GREATEST(
                    s.last_modified,
                    COALESCE(MAX(l.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(p.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(sa.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(sch.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(rd.last_modified), '1900-01-01'::timestamp),
                    COALESCE(MAX(lang.last_modified), '1900-01-01'::timestamp)
                ) as last_data_update
            FROM public.service s
            LEFT JOIN public.service_at_location sal ON s.id = sal.service_id
            LEFT JOIN public.location l ON sal.location_id = l.id
            LEFT JOIN public.phone p ON s.id = p.service_id
            LEFT JOIN public.service_area sa ON s.id = sa.service_id
            LEFT JOIN public.schedule sch ON s.id = sch.service_id
            LEFT JOIN public.required_document rd ON s.id = rd.service_id
            LEFT JOIN public.language lang ON s.id = lang.service_id
            GROUP BY s.id
        )
        SELECT slm.service_id
        FROM service_last_modified slm
        LEFT JOIN pipeline_state.service_data_signatures sds ON slm.service_id = sds.service_id
        WHERE sds.service_id IS NULL
           OR slm.last_data_update > sds.source_data_last_updated_at
    "#;

    let rows = conn
        .query(changed_services_query, &[])
        .await
        .context("Failed to query changed services")?;

    let changed_service_ids: Vec<ServiceId> =
        rows.into_iter().map(|row| ServiceId(row.get(0))).collect();

    if !changed_service_ids.is_empty() {
        info!("Found {} services with changes", changed_service_ids.len());
        calculate_service_signatures_parallel(pool, Some(changed_service_ids)).await?;
    } else {
        info!("No service changes detected");
    }

    Ok(())
}


async fn calculate_service_signatures_parallel(
    pool: &PgPool,
    service_ids: Option<Vec<ServiceId>>,
) -> Result<usize> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Determine which services to process
    let services_query = if service_ids.is_some() {
        "SELECT id FROM public.service WHERE id = ANY($1) ORDER BY id"
    } else {
        "SELECT id FROM public.service ORDER BY id"
    };

    let rows = if let Some(ids) = &service_ids {
        let id_strings: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();
        conn.query(services_query, &[&id_strings]).await?
    } else {
        conn.query(services_query, &[]).await?
    };

    let all_service_ids: Vec<ServiceId> =
        rows.into_iter().map(|row| ServiceId(row.get(0))).collect();

    let total_count = all_service_ids.len();
    info!("Processing {} services", total_count);

    if total_count == 0 {
        return Ok(0);
    }

    // Optimize batching and parallelism
    let cpu_count = num_cpus::get();
    let optimal_batch_size = calculate_optimal_batch_size(total_count, cpu_count);
    let concurrent_tasks = std::cmp::min(cpu_count * 2, total_count.div_ceil(optimal_batch_size));
    info!(
        "Using {} concurrent tasks with batch size {} (detected {} CPUs)",
        concurrent_tasks, optimal_batch_size, cpu_count
    );

    let batches: Vec<Vec<ServiceId>> = all_service_ids
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
            match process_service_batch_optimized(&pool_clone, batch).await {
                Ok(count) => {
                    let current_processed =
                        processed_clone.fetch_add(count, Ordering::Relaxed) + count;
                    if current_processed % 1000 == 0 || current_processed >= total_count {
                        info!("Processed {}/{} services", current_processed, total_count);
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

    info!("Completed processing {} services", total_processed);
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

async fn process_service_batch_optimized(
    pool: &PgPool,
    service_ids: Vec<ServiceId>,
) -> Result<usize> {
    let mut conn = pool.get().await.context("Failed to get DB connection")?;
    let tx = conn
        .transaction()
        .await
        .context("Failed to start transaction")?;

    let mut success_count = 0;
    let batch_size = service_ids.len();

    // Process each service in the batch
    for (idx, service_id) in service_ids.into_iter().enumerate() {
        match calculate_single_service_signature(&tx, &service_id).await {
            Ok((signature, last_updated, feature_snapshot)) => {
                // Upsert the calculated signature
                let upsert_query = r#"
                    INSERT INTO pipeline_state.service_data_signatures
                        (service_id, signature, source_data_last_updated_at, signature_calculated_at, relevant_attributes_snapshot)
                    VALUES ($1, $2, $3, CURRENT_TIMESTAMP, $4)
                    ON CONFLICT (service_id) DO UPDATE SET
                        signature = EXCLUDED.signature,
                        source_data_last_updated_at = EXCLUDED.source_data_last_updated_at,
                        signature_calculated_at = EXCLUDED.signature_calculated_at,
                        relevant_attributes_snapshot = EXCLUDED.relevant_attributes_snapshot
                "#;

                if let Err(e) = tx
                    .execute(
                        upsert_query,
                        &[&service_id.0, &signature, &last_updated, &feature_snapshot],
                    )
                    .await
                {
                    error!(
                        "Failed to store signature for service {}: {}",
                        service_id.0, e
                    );
                } else {
                    success_count += 1;
                }
            }
            Err(e) => {
                error!(
                    "Failed to calculate signature for service {}: {}",
                    service_id.0, e
                );
            }
        }

        if batch_size > 100 && (idx + 1) % 50 == 0 {
            debug!(
                "Batch progress: {}/{} services processed",
                idx + 1,
                batch_size
            );
        }
    }

    tx.commit().await.context("Failed to commit transaction")?;
    Ok(success_count)
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

async fn calculate_single_service_signature(
    conn: &impl tokio_postgres::GenericClient,
    service_id: &ServiceId,
) -> Result<(String, DateTime<Utc>, serde_json::Value)> {
    let mut components = BTreeMap::new();
    let mut last_updated = DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut feature_snapshot = json!({});

    // 1. Service core data
    let service_query = r#"
        SELECT name, alternate_name, description, url, email, status,
               interpretation_services, application_process, fees_description,
               eligibility_description, minimum_age, maximum_age,
               last_modified, embedding_v2_updated_at
        FROM public.service WHERE id = $1
    "#;

    let service_row_opt = conn
        .query_opt(service_query, &[&service_id.0])
        .await
        .context("Failed to query service data")?;

    match service_row_opt {
        Some(service_row) => {
            components.insert("name".to_string(), json!(service_row.get::<_, String>(0)));
            components.insert(
                "alternate_name".to_string(),
                json!(service_row.get::<_, Option<String>>(1)),
            );
            components.insert(
                "description_hash".to_string(),
                json!(hash_long_field(&service_row.get::<_, Option<String>>(2))),
            );
            components.insert(
                "url".to_string(),
                json!(service_row.get::<_, Option<String>>(3)),
            );
            components.insert(
                "email".to_string(),
                json!(service_row.get::<_, Option<String>>(4)),
            );
            components.insert("status".to_string(), json!(service_row.get::<_, String>(5)));
            components.insert(
                "interpretation_services".to_string(),
                json!(service_row.get::<_, Option<String>>(6)),
            );
            components.insert(
                "application_process".to_string(),
                json!(service_row.get::<_, Option<String>>(7)),
            );
            components.insert(
                "fees_description".to_string(),
                json!(service_row.get::<_, Option<String>>(8)),
            );
            components.insert(
                "eligibility_description".to_string(),
                json!(service_row.get::<_, Option<String>>(9)),
            );
            components.insert(
                "minimum_age".to_string(),
                json!(service_row.get::<_, Option<i32>>(10)),
            );
            components.insert(
                "maximum_age".to_string(),
                json!(service_row.get::<_, Option<i32>>(11)),
            );

            if let Ok(emb_updated) = service_row.try_get::<_, Option<DateTime<Utc>>>(13) {
                components.insert("embedding_v2_updated_at".to_string(), json!(emb_updated));
            }

            if let Ok(modified) = service_row.try_get::<_, chrono::NaiveDateTime>(12) {
                let modified_utc = DateTime::<Utc>::from_naive_utc_and_offset(modified, Utc);
                last_updated = last_updated.max(modified_utc);
            }

            feature_snapshot["core"] = json!({
                "name": service_row.get::<_, String>(0),
                "status": service_row.get::<_, String>(5),
                // Add other core fields to snapshot if needed
            });
        }
        None => {
            return Err(anyhow::anyhow!(
                "Service {} not found in database",
                service_id.0
            ));
        }
    }

    // 2. Linked features (Languages, Docs, Areas, Taxonomies, Locations)
    let lang_query = "SELECT language FROM public.language WHERE service_id = $1 ORDER BY language";
    let lang_rows = conn.query(lang_query, &[&service_id.0]).await?;
    let languages: Vec<String> = lang_rows.into_iter().map(|row| row.get(0)).collect();
    components.insert("languages".to_string(), json!(languages));
    feature_snapshot["languages"] = json!(languages);

    let doc_query =
        "SELECT document FROM public.required_document WHERE service_id = $1 ORDER BY document";
    let doc_rows = conn.query(doc_query, &[&service_id.0]).await?;
    let documents: Vec<String> = doc_rows.into_iter().map(|row| row.get(0)).collect();
    components.insert("required_documents".to_string(), json!(documents));
    feature_snapshot["required_documents"] = json!(documents);

    let area_query = "SELECT service_area, extent_type FROM public.service_area WHERE service_id = $1 ORDER BY service_area";
    let area_rows = conn.query(area_query, &[&service_id.0]).await?;
    let areas: Vec<(Option<String>, Option<String>)> = area_rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    components.insert("service_areas".to_string(), json!(areas));
    feature_snapshot["service_areas"] = json!(areas);

    let taxonomy_query = r#"
        SELECT tt.term, tt.taxonomy
        FROM public.service_taxonomy st
        JOIN public.taxonomy_term tt ON st.taxonomy_term_id = tt.id
        WHERE st.service_id = $1
        ORDER BY tt.taxonomy, tt.term
    "#;
    let tax_rows = conn.query(taxonomy_query, &[&service_id.0]).await?;
    let taxonomies: Vec<(String, Option<String>)> = tax_rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    components.insert("taxonomies".to_string(), json!(taxonomies));
    feature_snapshot["taxonomies"] = json!(taxonomies);

    let location_query = r#"
        SELECT l.name, l.latitude, l.longitude, a.city, a.state_province
        FROM public.service_at_location sal
        JOIN public.location l ON sal.location_id = l.id
        LEFT JOIN public.address a ON l.id = a.location_id
        WHERE sal.service_id = $1
        ORDER BY l.name
    "#;
    let loc_rows = conn.query(location_query, &[&service_id.0]).await?;
    let locations: Vec<serde_json::Value> = loc_rows
        .into_iter()
        .map(|row| {
            json!({
                "name": row.get::<_, Option<String>>(0),
                "latitude": row.get::<_, Option<f64>>(1),
                "longitude": row.get::<_, Option<f64>>(2),
                "city": row.get::<_, Option<String>>(3),
                "state": row.get::<_, Option<String>>(4),
            })
        })
        .collect();
    components.insert("locations".to_string(), json!(locations));
    feature_snapshot["locations"] = json!(locations);


    // Create hash from all components
    let mut hasher = Sha256::new();
    for (key, value) in components {
        hasher.update(format!("{}:{}", key, value.to_string()).as_bytes());
    }

    let signature = hex::encode(hasher.finalize());
    Ok((signature, last_updated, feature_snapshot))
}