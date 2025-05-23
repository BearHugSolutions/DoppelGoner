// src/bin/backfill_comparison_cache.rs
use anyhow::{Context, Result};
use dedupe_lib::db::{self, PgPool}; // Assuming dedupe_lib is your crate name
use dedupe_lib::models::{EntityId, MatchMethodType}; // Adjust path as needed
use dedupe_lib::pipeline_state_utils::{
    get_current_signatures_for_pair, store_in_comparison_cache,
};
// You'll need to import or replicate normalization and candidate generation logic
// e.g., from dedupe_lib::matching::email, dedupe_lib::matching::phone, etc.
// For simplicity, some of this logic is sketched out in generate_candidates_for_method.
use futures::future::try_join_all;
use log::{debug, error, info, warn};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_postgres::Row;
use uuid::Uuid;

// Configuration for the backfill process
const BATCH_SIZE_CACHE_OPS: usize = 200; // How many cache store operations to batch
const MAX_CONCURRENT_DB_OPS: usize = 10; // Max parallel DB tasks

/// Represents the essential data from an entity_group row for backfilling.
struct MinimalEntityGroup {
    entity_id_1: EntityId,
    entity_id_2: EntityId,
    method_type: MatchMethodType,
    confidence_score: Option<f64>,
}

/// Fetches all existing entity groups from the database.
async fn fetch_all_entity_groups_for_backfill(pool: &PgPool) -> Result<Vec<MinimalEntityGroup>> {
    info!("Fetching all entity groups for backfill...");
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for fetching entity groups")?;
    let query = "SELECT entity_id_1, entity_id_2, method_type, confidence_score FROM public.entity_group";
    let rows = conn
        .query(query, &[])
        .await
        .context("Failed to query all entity groups")?;

    let mut groups = Vec::with_capacity(rows.len());
    for row in rows {
        let e1_str: String = row.get("entity_id_1");
        let e2_str: String = row.get("entity_id_2");
        let method_str: String = row.get("method_type");
        let conf_score: Option<f64> = row.get("confidence_score");

        groups.push(MinimalEntityGroup {
            entity_id_1: EntityId(e1_str),
            entity_id_2: EntityId(e2_str),
            method_type: MatchMethodType::from_str(&method_str),
            confidence_score: conf_score,
        });
    }
    info!("Fetched {} entity groups.", groups.len());
    Ok(groups)
}

/// Generates candidate pairs for a given method type by roughly mimicking its matching logic.
/// NOTE: This is a simplified placeholder. For accurate backfilling, this function
/// needs to carefully replicate the candidate generation logic from each specific
/// matching module in your `dedupe_lib::matching` an other relevant files.
async fn generate_candidates_for_method(
    pool: &PgPool,
    method_type: &MatchMethodType,
) -> Result<Vec<(EntityId, EntityId)>> {
    let mut candidates = Vec::new();
    let conn = pool
        .get()
        .await
        .context(format!("DB conn for candidates ({})", method_type.as_str()))?;

    match method_type {
        MatchMethodType::Email => {
            // Simplified logic from `matching::email.rs`
            // Fetches entities sharing the same normalized email.
            let email_query = "
                SELECT e.id as entity_id, o.email FROM entity e JOIN organization o ON e.organization_id = o.id WHERE o.email IS NOT NULL AND o.email != ''
                UNION ALL
                SELECT e.id as entity_id, s.email FROM public.entity e JOIN public.entity_feature ef ON e.id = ef.entity_id
                JOIN public.service s ON ef.table_id = s.id AND ef.table_name = 'service' WHERE s.email IS NOT NULL AND s.email != ''";
            let rows = conn
                .query(email_query, &[])
                .await
                .context("Email candidate query failed for backfill")?;
            let mut email_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let email_val: String = row.get("email");
                // Use the actual normalization from your email.rs
                let normalized_email = dedupe_lib::matching::email::normalize_email(&email_val);
                if !normalized_email.is_empty() {
                    email_map
                        .entry(normalized_email)
                        .or_default()
                        .push(EntityId(entity_id_str));
                }
            }
            for entity_ids in email_map.values() {
                if entity_ids.len() >= 2 {
                    for i in 0..entity_ids.len() {
                        for j in (i + 1)..entity_ids.len() {
                            candidates.push((entity_ids[i].clone(), entity_ids[j].clone()));
                        }
                    }
                }
            }
        }
        MatchMethodType::Phone => {
            // Simplified logic from `matching::phone.rs`
            // Fetches entities sharing the same normalized phone number.
            let phone_query = "
                SELECT e.id as entity_id, p.number FROM public.entity e
                JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'phone'
                JOIN public.phone p ON ef.table_id = p.id WHERE p.number IS NOT NULL AND p.number != ''";
            let rows = conn
                .query(phone_query, &[])
                .await
                .context("Phone candidate query failed for backfill")?;
            let mut phone_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let phone_val: String = row.get("number");
                // Use the actual normalization from your phone.rs
                let normalized_phone = dedupe_lib::matching::phone::normalize_phone(&phone_val);
                if !normalized_phone.is_empty() {
                    phone_map
                        .entry(normalized_phone)
                        .or_default()
                        .push(EntityId(entity_id_str));
                }
            }
            for entity_ids in phone_map.values() {
                if entity_ids.len() >= 2 {
                    for i in 0..entity_ids.len() {
                        for j in (i + 1)..entity_ids.len() {
                            candidates.push((entity_ids[i].clone(), entity_ids[j].clone()));
                        }
                    }
                }
            }
        }
        MatchMethodType::Url => {
            // Simplified logic from `matching::url.rs`
            // Fetches entities sharing the same normalized domain.
            let url_query = r#"
                SELECT e.id AS entity_id, s.url AS service_url FROM public.entity e
                JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'service' JOIN public.service s ON ef.table_id = s.id
                WHERE s.url IS NOT NULL AND s.url != '' AND s.url !~ '^\s*$' AND s.url NOT LIKE 'mailto:%' AND s.url NOT LIKE 'tel:%'
                UNION
                SELECT e.id AS entity_id, o.url AS org_url FROM public.entity e
                JOIN public.organization o ON e.organization_id = o.id WHERE o.url IS NOT NULL AND o.url != '' AND o.url !~ '^\s*$' AND o.url NOT LIKE 'mailto:%' AND o.url NOT LIKE 'tel:%'
            "#;
            let rows = conn
                .query(url_query, &[])
                .await
                .context("URL candidate query failed for backfill")?;
            let mut domain_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let url_val: String = row.get(1); // "service_url" or "org_url"
                // Use actual normalization from url.rs
                if let Some(normalized_data) = dedupe_lib::matching::url::normalize_url_with_slugs(&url_val) {
                     if !dedupe_lib::matching::url::is_ignored_domain(&normalized_data.domain) {
                        domain_map
                            .entry(normalized_data.domain)
                            .or_default()
                            .push(EntityId(entity_id_str));
                     }
                }
            }
            for entity_ids in domain_map.values() {
                if entity_ids.len() >= 2 {
                    for i in 0..entity_ids.len() {
                        for j in (i + 1)..entity_ids.len() {
                            candidates.push((entity_ids[i].clone(), entity_ids[j].clone()));
                        }
                    }
                }
            }
        }
        MatchMethodType::Address => {
            // Simplified from `matching::address.rs`
            // Fetches entities sharing the same normalized address.
            let address_query = "
                SELECT e.id AS entity_id, a.address_1, a.address_2, a.city, a.state_province, a.postal_code, a.country
                FROM public.entity e
                JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
                JOIN public.location l ON ef.table_id = l.id
                JOIN public.address a ON a.location_id = l.id
                WHERE a.address_1 IS NOT NULL AND a.address_1 != '' AND a.city IS NOT NULL AND a.city != ''";
            let rows = conn
                .query(address_query, &[])
                .await
                .context("Address candidate query failed for backfill")?;
            let mut address_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                // Use actual normalization from address.rs
                let full_address = dedupe_lib::matching::address::format_full_address(&row)?;
                let normalized_address = dedupe_lib::matching::address::normalize_address(&full_address);
                if !normalized_address.is_empty() {
                    address_map
                        .entry(normalized_address)
                        .or_default()
                        .push(EntityId(entity_id_str));
                }
            }
            for entity_ids in address_map.values() {
                if entity_ids.len() >= 2 {
                    for i in 0..entity_ids.len() {
                        for j in (i + 1)..entity_ids.len() {
                            candidates.push((entity_ids[i].clone(), entity_ids[j].clone()));
                        }
                    }
                }
            }
        }
        MatchMethodType::Name => {
            // Simplified from `matching::name.rs`
            // This is complex; proper candidate generation involves tokenization, IDF, and overlap thresholds.
            // The sketch below is a very rough approximation (pairs sharing any common non-stopword token).
            info!("Name candidate generation for backfill is highly simplified and may differ from actual pipeline.");
            let name_query = "SELECT e.id, e.name FROM public.entity e WHERE e.name IS NOT NULL AND e.name != ''";
            let rows = conn
                .query(name_query, &[])
                .await
                .context("Name candidate query failed for backfill")?;
            let mut token_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            let stopwords: HashSet<String> = dedupe_lib::matching::name::STOPWORDS.iter().map(|&s| s.to_string()).collect();

            for row in rows {
                let entity_id_str: String = row.get("id");
                let name_val: String = row.get("name");
                let (normalized_name, _) = dedupe_lib::matching::name::normalize_name(&name_val);

                let entity_tokens: HashSet<String> = normalized_name
                    .split_whitespace()
                    .map(|s| s.to_lowercase())
                    .filter(|t| !stopwords.contains(t) && t.len() >= dedupe_lib::matching::name::MIN_TOKEN_LENGTH)
                    .collect();

                for token in entity_tokens {
                    token_map
                        .entry(token)
                        .or_default()
                        .push(EntityId(entity_id_str.clone()));
                }
            }

            let mut temp_candidate_pairs: HashSet<(EntityId, EntityId)> = HashSet::new();
            for entity_list_for_token in token_map.values() {
                if entity_list_for_token.len() >= 2 {
                    for i in 0..entity_list_for_token.len() {
                        for j in (i + 1)..entity_list_for_token.len() {
                            // This is a very loose candidate generation (any shared token).
                            // Actual name.rs uses TOP_TOKENS_PER_ENTITY and MIN_TOKEN_OVERLAP.
                            let e1 = entity_list_for_token[i].clone();
                            let e2 = entity_list_for_token[j].clone();
                            if e1.0 < e2.0 { temp_candidate_pairs.insert((e1, e2)); }
                            else { temp_candidate_pairs.insert((e2, e1)); }
                        }
                    }
                }
            }
            candidates.extend(temp_candidate_pairs.into_iter());
        }
        MatchMethodType::Geospatial => {
            // Logic from `matching::geospatial.rs` `Workspace_candidate_pairs_excluding_existing`
            // but without the exclusion part, and using the defined METERS_TO_CHECK.
            let geo_query = "
                WITH EntityLocations AS (
                    SELECT e.id AS entity_id, l.geom
                    FROM public.entity e
                    JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
                    JOIN public.location l ON ef.table_id = l.id
                    WHERE l.geom IS NOT NULL AND e.id IS NOT NULL
                )
                SELECT el1.entity_id AS entity_id_1_str, el2.entity_id AS entity_id_2_str
                FROM EntityLocations el1 JOIN EntityLocations el2
                ON el1.entity_id < el2.entity_id AND ST_DWithin(el1.geom, el2.geom, $1)"; // $1 is METERS_TO_CHECK
            let rows = conn
                .query(geo_query, &[&dedupe_lib::matching::geospatial::METERS_TO_CHECK])
                .await
                .context("Geospatial candidate query failed for backfill")?;
            for row in rows {
                candidates.push((
                    EntityId(row.get("entity_id_1_str")),
                    EntityId(row.get("entity_id_2_str")),
                ));
            }
        }
        _ => {
            warn!(
                "Candidate generation for backfill not implemented for method type: {}",
                method_type.as_str()
            );
        }
    }
    Ok(candidates)
}


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting comparison_cache backfill utility...");

    // --- Environment Loading ---
    let env_paths = [".env", ".env.local", "../.env"];
    let mut loaded_env = false;

    for path in env_paths.iter() {
        if Path::new(path).exists() {
            if let Err(e) = db::load_env_from_file(path) {
                warn!("Failed to load environment from {}: {}", path, e);
            } else {
                info!("Loaded environment variables from {}", path);
                loaded_env = true;
                break;
            }
        }
    }

    if !loaded_env {
        info!("No .env file found, using environment variables from system");
    }


    let pool = db::connect().await.context("Failed to connect to database")?;
    info!("Successfully connected to the database.");

    let pipeline_run_id_for_backfill = format!("backfill-cache-{}", Uuid::new_v4());
    info!("Using pipeline_run_id: {}", pipeline_run_id_for_backfill);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DB_OPS));

    // --- 1. Process existing "MATCH" cases ---
    info!("Processing existing entity groups as MATCH cases...");
    let existing_groups_data = fetch_all_entity_groups_for_backfill(&pool).await?;
    let mut match_tasks = Vec::new();

    for group in existing_groups_data {
        let pool_clone = pool.clone();
        let run_id_clone = pipeline_run_id_for_backfill.clone();
        let sem_clone = semaphore.clone();

        match_tasks.push(tokio::spawn(async move {
            let _permit = sem_clone
                .acquire()
                .await
                .context("Failed to acquire semaphore permit for MATCH task")?;
            let (e1_id, e2_id) = if group.entity_id_1.0 < group.entity_id_2.0 {
                (group.entity_id_1, group.entity_id_2)
            } else {
                (group.entity_id_2, group.entity_id_1)
            };

            match get_current_signatures_for_pair(&pool_clone, &e1_id, &e2_id).await {
                Ok(Some((sig1_data, sig2_data))) => {
                    if let Err(e) = store_in_comparison_cache(
                        &pool_clone,
                        &e1_id,
                        &e2_id,
                        &sig1_data.signature,
                        &sig2_data.signature,
                        &group.method_type,
                        &run_id_clone,
                        "MATCH",
                        group.confidence_score,
                        None, // features_snapshot - not available for backfill
                    )
                    .await
                    {
                        warn!(
                            "Failed to store MATCH in cache for pair ({}, {}), method {}: {}",
                            e1_id.0,
                            e2_id.0,
                            group.method_type.as_str(),
                            e
                        );
                    } else {
                        debug!(
                            "Stored MATCH in cache for pair ({}, {}), method {}",
                            e1_id.0,
                            e2_id.0,
                            group.method_type.as_str()
                        );
                    }
                }
                Ok(None) => {
                    warn!(
                        "Signatures not found for existing group pair ({}, {}). Cannot cache.",
                        e1_id.0, e2_id.0
                    );
                }
                Err(e) => {
                    warn!(
                        "Error fetching signatures for existing group pair ({}, {}): {}",
                        e1_id.0, e1_id.0, e // Corrected e2_id.0 to e1_id.0 in log
                    );
                }
            }
            Ok::<(), anyhow::Error>(())
        }));

        if match_tasks.len() >= BATCH_SIZE_CACHE_OPS {
            info!("Processing a batch of {} MATCH cache tasks...", match_tasks.len());
            for result in try_join_all(match_tasks).await? {
                if let Err(e) = result { error!("Error in MATCH task: {}", e); }
            }
            match_tasks = Vec::new();
        }
    }
    if !match_tasks.is_empty() {
        info!("Processing final batch of {} MATCH cache tasks...", match_tasks.len());
        for result in try_join_all(match_tasks).await? {
            if let Err(e) = result { error!("Error in final MATCH task: {}", e); }
        }
    }
    info!("Finished processing existing groups as MATCH cases.");

    // --- 2. Process "NON_MATCH" cases ---
    // Create a HashSet of existing groups for quick lookups: (e1_id_str, e2_id_str, method_type_str)
    let mut existing_groups_lookup_set = HashSet::new();
    let all_db_groups_for_non_match = fetch_all_entity_groups_for_backfill(&pool).await?;
    for group in all_db_groups_for_non_match {
        let (e1_s, e2_s) = if group.entity_id_1.0 < group.entity_id_2.0 {
            (group.entity_id_1.0, group.entity_id_2.0)
        } else {
            (group.entity_id_2.0, group.entity_id_1.0)
        };
        existing_groups_lookup_set.insert((e1_s, e2_s, group.method_type.as_str().to_string()));
    }
    let existing_groups_lookup_arc = Arc::new(existing_groups_lookup_set);
    info!("Created lookup set with {} entries for NON_MATCH checking.", existing_groups_lookup_arc.len());


    let method_types_for_non_match_backfill = vec![
        MatchMethodType::Email, MatchMethodType::Phone, MatchMethodType::Url,
        MatchMethodType::Address, MatchMethodType::Name, MatchMethodType::Geospatial,
    ];

    for method_type in method_types_for_non_match_backfill {
        info!("Generating and processing NON_MATCH candidates for method: {}", method_type.as_str());
        // This function needs to be robust and accurately reflect how candidates are generated
        // in the main pipeline for each respective matching function.
        let candidate_pairs_for_method = generate_candidates_for_method(&pool, &method_type).await?;
        info!(
            "Found {} candidate pairs for method {} to check for NON_MATCH status.",
            candidate_pairs_for_method.len(), method_type.as_str()
        );

        let mut non_match_tasks = Vec::new();
        for (e1_id_orig, e2_id_orig) in candidate_pairs_for_method {
            let (e1_id, e2_id) = if e1_id_orig.0 < e2_id_orig.0 {
                (e1_id_orig, e2_id_orig)
            } else {
                (e2_id_orig, e1_id_orig)
            };

            let group_key = (e1_id.0.clone(), e2_id.0.clone(), method_type.as_str().to_string());

            if !existing_groups_lookup_arc.contains(&group_key) {
                // This pair was a candidate but is NOT in entity_group for this method.
                // So, it's a NON_MATCH for this (pair, method_type).
                let pool_clone = pool.clone();
                let run_id_clone = pipeline_run_id_for_backfill.clone();
                let mt_clone = method_type.clone();
                let sem_clone = semaphore.clone();

                non_match_tasks.push(tokio::spawn(async move {
                    let _permit = sem_clone
                        .acquire()
                        .await
                        .context("Failed to acquire semaphore permit for NON_MATCH task")?;
                    match get_current_signatures_for_pair(&pool_clone, &e1_id, &e2_id).await {
                        Ok(Some((sig1_data, sig2_data))) => {
                            if let Err(e) = store_in_comparison_cache(
                                &pool_clone, &e1_id, &e2_id,
                                &sig1_data.signature, &sig2_data.signature,
                                &mt_clone, &run_id_clone,
                                "NON_MATCH", None, None,
                            ).await {
                                warn!("Failed to store NON_MATCH in cache for pair ({}, {}), method {}: {}", e1_id.0, e2_id.0, mt_clone.as_str(), e);
                            } else {
                                debug!("Stored NON_MATCH in cache for pair ({}, {}), method {}", e1_id.0, e2_id.0, mt_clone.as_str());
                            }
                        }
                        Ok(None) => warn!("Signatures not found for NON_MATCH pair ({}, {}). Cannot cache.", e1_id.0, e2_id.0),
                        Err(e) => warn!("Error fetching signatures for NON_MATCH pair ({}, {}): {}", e1_id.0, e2_id.0, e),
                    }
                    Ok::<(), anyhow::Error>(())
                }));

                if non_match_tasks.len() >= BATCH_SIZE_CACHE_OPS {
                    info!("Processing batch of {} NON_MATCH tasks for method {}...", non_match_tasks.len(), method_type.as_str());
                    for result in try_join_all(non_match_tasks).await? {
                        if let Err(e) = result { error!("Error in NON_MATCH task: {}", e); }
                    }
                    non_match_tasks = Vec::new();
                }
            }
        }
        if !non_match_tasks.is_empty() {
            info!("Processing final batch of {} NON_MATCH tasks for method {}...", non_match_tasks.len(), method_type.as_str());
            for result in try_join_all(non_match_tasks).await? {
                if let Err(e) = result { error!("Error in final NON_MATCH task: {}", e); }
            }
        }
        info!("Finished NON_MATCH candidates for method: {}", method_type.as_str());
    }

    info!("Comparison_cache backfill utility completed.");
    Ok(())
}