// src/signatures.rs

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use log::{error, info, warn};
use serde_json::{json, Value as JsonValue};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tokio_postgres::GenericClient;

use crate::db::{self, PgPool};
use crate::models::ServiceId;

// --- Public Functions ---

/// Calculates, stores, and returns the signature and snapshot for a service.
///
/// This function is called when a signature is confirmed to be missing or
/// when an error occurs while trying to fetch an existing one. It performs
/// the calculation, saves the result to the database, and returns it.
pub async fn calculate_and_store_service_signature(
    pool: &PgPool,
    service_id: &ServiceId,
) -> Result<(JsonValue, String)> {
    info!(
        "Attempting on-demand calculation & storage for service {}",
        service_id.0
    );
    let conn = pool
        .get()
        .await
        .context("Failed to get DB conn for on-demand calculation")?;

    // Calculate using the existing logic
    let (signature, last_updated, feature_snapshot) =
        calculate_single_service_signature(&*conn, service_id)
            .await
            .context(format!(
                "Failed to calculate signature for service {}",
                service_id.0
            ))?;

    // Store/Upsert the new signature
    db::upsert_service_signature(
        pool,
        &service_id.0,
        &signature,
        Some(feature_snapshot.clone()),
        Some(last_updated),
    )
    .await
    .context(format!(
        "Failed to store signature for service {}",
        service_id.0
    ))?;

    info!(
        "Successfully calculated and stored signature for service {}",
        service_id.0
    );
    Ok((feature_snapshot, signature))
}

/// Tries to get a signature from the database, and if it fails or is missing,
/// triggers the calculation and storage process.
/// Returns only the signature string, suitable for cache checks.
pub async fn get_or_calculate_signature(pool: &PgPool, service_id: &ServiceId) -> Result<String> {
    // 1. Try fetching from the database first
    match db::get_service_signature(pool, &service_id.0).await {
        Ok(Some(sig_data)) => {
            info!("Found existing signature for service {}.", service_id.0);
            return Ok(sig_data.signature);
        }
        Ok(None) => {
            info!(
                "Signature not found for {}. Proceeding to calculate.",
                service_id.0
            );
        }
        Err(e) => {
            warn!(
                "Error fetching signature for {}: {}. Attempting calculation.",
                service_id.0, e
            );
        }
    }

    // 2. If fetch failed or returned None, calculate and store
    match calculate_and_store_service_signature(pool, service_id).await {
        Ok((_snapshot, signature)) => Ok(signature),
        Err(e) => {
            error!(
                "Failed to get or calculate signature for {}: {}",
                service_id.0, e
            );
            Err(e)
        }
    }
}

// --- Private Helper Functions ---

/// Hashes a string field if it's longer than 100 characters,
/// otherwise returns it as is. Used to keep signatures consistent
/// for long text fields like descriptions.
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

/// Fetches all relevant data for a single service and calculates its signature.
/// This includes core service data and data from linked tables like
/// language, documents, areas, taxonomy, and locations.
/// Returns the signature, the latest update timestamp, and a snapshot
/// of the features used.
async fn calculate_single_service_signature(
    conn: &impl GenericClient,
    service_id: &ServiceId,
) -> Result<(String, DateTime<Utc>, JsonValue)> {
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
                "url": service_row.get::<_, Option<String>>(3),
                "email": service_row.get::<_, Option<String>>(4),
            });
        }
        None => {
            return Err(anyhow::anyhow!(
                "Service {} not found in database during signature calculation",
                service_id.0
            ));
        }
    }

    // 2. All linked features
    // Languages
    let lang_query = "SELECT language FROM public.language WHERE service_id = $1 ORDER BY language";
    let lang_rows = conn.query(lang_query, &[&service_id.0]).await?;
    let languages: Vec<String> = lang_rows.into_iter().map(|row| row.get(0)).collect();
    components.insert("languages".to_string(), json!(languages));
    feature_snapshot["languages"] = json!(languages);

    // Required documents
    let doc_query =
        "SELECT document FROM public.required_document WHERE service_id = $1 ORDER BY document";
    let doc_rows = conn.query(doc_query, &[&service_id.0]).await?;
    let documents: Vec<String> = doc_rows.into_iter().map(|row| row.get(0)).collect();
    components.insert("required_documents".to_string(), json!(documents));
    feature_snapshot["required_documents"] = json!(documents);

    // Service areas
    let area_query = "SELECT service_area, extent_type FROM public.service_area WHERE service_id = $1 ORDER BY service_area";
    let area_rows = conn.query(area_query, &[&service_id.0]).await?;
    let areas: Vec<(Option<String>, Option<String>)> = area_rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    components.insert("service_areas".to_string(), json!(areas));
    feature_snapshot["service_areas"] = json!(areas);

    // Taxonomy terms
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

    // Locations (through service_at_location)
    let location_query = r#"
        SELECT l.name, l.latitude, l.longitude, a.city, a.state_province
        FROM public.service_at_location sal
        JOIN public.location l ON sal.location_id = l.id
        LEFT JOIN public.address a ON l.id = a.location_id
        WHERE sal.service_id = $1
        ORDER BY l.name
    "#;
    let loc_rows = conn.query(location_query, &[&service_id.0]).await?;
    let locations: Vec<JsonValue> = loc_rows
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
