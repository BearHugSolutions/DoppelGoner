// src/bin/backfill_comparison_cache.rs
use anyhow::{Context, Result};
use dedupe_lib::db::{self, PgPool}; // Assuming dedupe_lib is your crate name
use dedupe_lib::models::{EntityId, MatchMethodType}; // Adjust path as needed
use dedupe_lib::pipeline_state_utils::{
    get_current_signatures_for_pair, get_signatures_for_batch, store_batch_in_comparison_cache,
    store_in_comparison_cache, CacheEntryData, BATCH_SIZE_PAIRS,
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
const MAX_CONCURRENT_DB_OPS: usize = 5; // Max parallel DB tasks

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
    let query =
        "SELECT entity_id_1, entity_id_2, method_type, confidence_score FROM public.entity_group"; //
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

/// Generates candidate pairs for a given method type.
/// NOTE: This needs to replicate the actual candidate generation logic.
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
            let email_query = "
                SELECT e.id as entity_id, o.email FROM entity e JOIN organization o ON e.organization_id = o.id WHERE o.email IS NOT NULL AND o.email != ''
                UNION ALL
                SELECT e.id as entity_id, s.email FROM public.entity e JOIN public.entity_feature ef ON e.id = ef.entity_id
                JOIN public.service s ON ef.table_id = s.id AND ef.table_name = 'service' WHERE s.email IS NOT NULL AND s.email != ''"; //
            let rows = conn
                .query(email_query, &[])
                .await
                .context("Email candidate query failed for backfill")?;
            let mut email_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let email_val: String = row.get("email");
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
            let phone_query = "
                SELECT e.id as entity_id, p.number FROM public.entity e
                JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'phone'
                JOIN public.phone p ON ef.table_id = p.id WHERE p.number IS NOT NULL AND p.number != ''"; //
            let rows = conn
                .query(phone_query, &[])
                .await
                .context("Phone candidate query failed for backfill")?;
            let mut phone_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let phone_val: String = row.get("number");
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
            let url_query = r#"
                SELECT e.id AS entity_id, s.url AS service_url FROM public.entity e
                JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'service' JOIN public.service s ON ef.table_id = s.id
                WHERE s.url IS NOT NULL AND s.url != '' AND s.url !~ '^\s*$' AND s.url NOT LIKE 'mailto:%' AND s.url NOT LIKE 'tel:%'
                UNION
                SELECT e.id AS entity_id, o.url AS org_url FROM public.entity e
                JOIN public.organization o ON e.organization_id = o.id WHERE o.url IS NOT NULL AND o.url != '' AND o.url !~ '^\s*$' AND o.url NOT LIKE 'mailto:%' AND o.url NOT LIKE 'tel:%'
            "#; //
            let rows = conn
                .query(url_query, &[])
                .await
                .context("URL candidate query failed for backfill")?;
            let mut domain_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let url_val: String = row.get(1);
                if let Some(normalized_data) =
                    dedupe_lib::matching::url::normalize_url_with_slugs(&url_val)
                {
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
            let address_query = "
                SELECT e.id AS entity_id, a.address_1, a.address_2, a.city, a.state_province, a.postal_code, a.country
                FROM public.entity e
                JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
                JOIN public.location l ON ef.table_id = l.id
                JOIN public.address a ON a.location_id = l.id
                WHERE a.address_1 IS NOT NULL AND a.address_1 != '' AND a.city IS NOT NULL AND a.city != ''"; //
            let rows = conn
                .query(address_query, &[])
                .await
                .context("Address candidate query failed for backfill")?;
            let mut address_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            for row in rows {
                let entity_id_str: String = row.get("entity_id");
                let full_address = dedupe_lib::matching::address::format_full_address(&row)?;
                let normalized_address =
                    dedupe_lib::matching::address::normalize_address(&full_address);
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
            info!("Name candidate generation for backfill is highly simplified and may differ from actual pipeline.");
            let name_query = "SELECT e.id, e.name FROM public.entity e WHERE e.name IS NOT NULL AND e.name != ''"; //
            let rows = conn
                .query(name_query, &[])
                .await
                .context("Name candidate query failed for backfill")?;
            let mut token_map: HashMap<String, Vec<EntityId>> = HashMap::new();
            let stopwords: HashSet<String> = dedupe_lib::matching::name::STOPWORDS
                .iter()
                .map(|&s| s.to_string())
                .collect();

            for row in rows {
                let entity_id_str: String = row.get("id");
                let name_val: String = row.get("name");
                let (normalized_name, _) = dedupe_lib::matching::name::normalize_name(&name_val);

                let entity_tokens: HashSet<String> = normalized_name
                    .split_whitespace()
                    .map(|s| s.to_lowercase())
                    .filter(|t| {
                        !stopwords.contains(t)
                            && t.len() >= dedupe_lib::matching::name::MIN_TOKEN_LENGTH
                    })
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
                            let e1 = entity_list_for_token[i].clone();
                            let e2 = entity_list_for_token[j].clone();
                            if e1.0 < e2.0 {
                                temp_candidate_pairs.insert((e1, e2));
                            } else {
                                temp_candidate_pairs.insert((e2, e1));
                            }
                        }
                    }
                }
            }
            candidates.extend(temp_candidate_pairs.into_iter());
        }
        MatchMethodType::Geospatial => {
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
                ON el1.entity_id < el2.entity_id AND ST_DWithin(el1.geom, el2.geom, $1)"; //
            let rows = conn
                .query(
                    geo_query,
                    &[&dedupe_lib::matching::geospatial::METERS_TO_CHECK],
                )
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

async fn process_and_store_batch(
    pool: PgPool,
    semaphore: Arc<Semaphore>,
    pairs: Vec<(EntityId, EntityId)>,
    method_type: MatchMethodType,
    pipeline_run_id: String,
    outcome: &'static str, // "MATCH" or "NON_MATCH"
    confidence_map: Option<Arc<HashMap<(EntityId, EntityId), Option<f64>>>>, // Only for MATCH
) -> Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }

    let _permit = semaphore
        .acquire()
        .await
        .context("Failed to acquire semaphore permit for batch task")?;

    let mut unique_ids = HashSet::new();
    for (e1, e2) in &pairs {
        unique_ids.insert(e1.clone());
        unique_ids.insert(e2.clone());
    }
    let unique_ids_vec: Vec<EntityId> = unique_ids.into_iter().collect();

    let signatures_map = match get_signatures_for_batch(&pool, &unique_ids_vec).await {
        //
        Ok(map) => map,
        Err(e) => {
            error!("Failed to fetch signatures for batch: {}", e);
            return Err(e);
        }
    };

    let mut cache_entries = Vec::new();
    for (e1, e2) in pairs {
        match (signatures_map.get(&e1), signatures_map.get(&e2)) {
            (Some(sig1_data), Some(sig2_data)) => {
                let confidence_score = if outcome == "MATCH" {
                    confidence_map
                        .as_ref()
                        .and_then(|map| {
                            map.get(&(e1.clone(), e2.clone()))
                                .or_else(|| map.get(&(e2.clone(), e1.clone())))
                        })
                        .and_then(|&score| score)
                } else {
                    None
                };

                cache_entries.push(CacheEntryData {
                    //
                    id1: e1.clone(),
                    id2: e2.clone(),
                    sig1: sig1_data.signature.clone(),
                    sig2: sig2_data.signature.clone(),
                    method_type: method_type.clone(),
                    pipeline_run_id: pipeline_run_id.clone(),
                    comparison_outcome: outcome.to_string(),
                    similarity_score: confidence_score,
                    features_snapshot: None,
                });
            }
            _ => {
                warn!(
                    "Could not find signatures for pair ({}, {}) in fetched batch. Skipping.",
                    e1.0, e2.0
                );
            }
        }
    }

    if !cache_entries.is_empty() {
        if let Err(e) = store_batch_in_comparison_cache(&pool, &cache_entries).await {
            //
            error!(
                "Failed to store batch ({}, {}) in cache: {}",
                outcome,
                method_type.as_str(),
                e
            );
        } else {
            debug!(
                "Stored batch of {} {} entries for {}",
                cache_entries.len(),
                outcome,
                method_type.as_str()
            );
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("Starting comparison_cache backfill utility (Batch Optimized)...");

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

    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;
    info!("Successfully connected to the database.");

    let pipeline_run_id_for_backfill = format!("backfill-cache-{}", Uuid::new_v4());
    info!("Using pipeline_run_id: {}", pipeline_run_id_for_backfill);

    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DB_OPS));

    // --- Fetch ALL groups ONCE ---
    info!("Fetching all entity groups...");
    let all_existing_groups = fetch_all_entity_groups_for_backfill(&pool).await?; //
    info!("Fetched {} entity groups.", all_existing_groups.len());

    // --- 1. Process existing "MATCH" cases (Refactored for Deduplication) ---
    info!("Processing existing entity groups as MATCH cases...");
    let mut match_tasks = Vec::new();
    let mut matches_by_method: HashMap<MatchMethodType, Vec<(EntityId, EntityId, Option<f64>)>> =
        HashMap::new();

    // Group by method_type, normalizing pairs
    for group in all_existing_groups.iter() {
        let (e1, e2) = if group.entity_id_1.0 < group.entity_id_2.0 {
            (group.entity_id_1.clone(), group.entity_id_2.clone())
        } else {
            (group.entity_id_2.clone(), group.entity_id_1.clone())
        };
        matches_by_method
            .entry(group.method_type.clone())
            .or_default()
            .push((e1, e2, group.confidence_score));
    }

    // Process each method type, using HashMap keys for deduplication
    for (method_type, groups) in matches_by_method {
        info!(
            "Processing {} MATCH groups for method {}",
            groups.len(),
            method_type.as_str()
        );

        // Create the confidence map - This inherently deduplicates pairs.
        let confidence_map: HashMap<(EntityId, EntityId), Option<f64>> = groups
            .iter()
            .map(|(e1, e2, conf)| ((e1.clone(), e2.clone()), *conf))
            .collect();

        let confidence_map_arc = Arc::new(confidence_map.clone());
        let mut current_match_batch = Vec::with_capacity(BATCH_SIZE_PAIRS); //

        // Iterate over the *keys* (unique pairs) of the confidence_map
        for (e1_id, e2_id) in confidence_map.keys() {
            current_match_batch.push((e1_id.clone(), e2_id.clone())); // Push clones

            if current_match_batch.len() >= BATCH_SIZE_PAIRS {
                //
                info!(
                    "Spawning MATCH batch task for {} ({} pairs)...",
                    method_type.as_str(),
                    current_match_batch.len()
                );
                match_tasks.push(tokio::spawn(process_and_store_batch(
                    pool.clone(),
                    semaphore.clone(),
                    current_match_batch.drain(..).collect(),
                    method_type.clone(),
                    pipeline_run_id_for_backfill.clone(),
                    "MATCH",
                    Some(confidence_map_arc.clone()),
                )));
            }
        }
        // Spawn any remaining pairs
        if !current_match_batch.is_empty() {
            info!(
                "Spawning final MATCH batch task for {} ({} pairs)...",
                method_type.as_str(),
                current_match_batch.len()
            );
            match_tasks.push(tokio::spawn(process_and_store_batch(
                pool.clone(),
                semaphore.clone(),
                current_match_batch.drain(..).collect(),
                method_type.clone(),
                pipeline_run_id_for_backfill.clone(),
                "MATCH",
                Some(confidence_map_arc.clone()),
            )));
        }
    }

    info!("Awaiting {} MATCH batch tasks...", match_tasks.len());
    for result in try_join_all(match_tasks).await? {
        if let Err(e) = result {
            error!("Error in MATCH batch task: {}", e);
        }
    }
    info!("Finished processing MATCH cases.");

    // --- 2. Process "NON_MATCH" cases (Refactored for Normalization & Deduplication) ---
    // Use the already fetched `all_existing_groups` to build the lookup set
    let mut existing_groups_lookup_set = HashSet::new();
    for group in all_existing_groups.iter() {
        // Use .iter() to borrow
        let (e1_s, e2_s) = if group.entity_id_1.0 < group.entity_id_2.0 {
            (group.entity_id_1.0.clone(), group.entity_id_2.0.clone())
        } else {
            (group.entity_id_2.0.clone(), group.entity_id_1.0.clone())
        };
        existing_groups_lookup_set.insert((e1_s, e2_s, group.method_type.as_str().to_string()));
    }
    let existing_groups_lookup_arc = Arc::new(existing_groups_lookup_set);
    info!(
        "Created lookup set with {} entries for NON_MATCH checking.",
        existing_groups_lookup_arc.len()
    );

    let method_types_for_non_match_backfill = vec![
        MatchMethodType::Email,
        MatchMethodType::Phone,
        MatchMethodType::Url,
        MatchMethodType::Address,
        MatchMethodType::Name,
        MatchMethodType::Geospatial,
    ];

    let mut non_match_tasks = Vec::new();
    for method_type in method_types_for_non_match_backfill {
        info!(
            "Generating NON_MATCH candidates for method: {}",
            method_type.as_str()
        );
        let candidate_pairs_vec = generate_candidates_for_method(&pool, &method_type).await?; //

        // Normalize *before* collecting into HashSet for deduplication
        let candidate_pairs: HashSet<(EntityId, EntityId)> = candidate_pairs_vec
            .into_iter()
            .map(|(e1, e2)| if e1.0 < e2.0 { (e1, e2) } else { (e2, e1) })
            .collect();

        info!(
            "Found {} unique, normalized candidates for {}. Filtering NON_MATCH...",
            candidate_pairs.len(),
            method_type.as_str()
        );

        let mut current_non_match_batch = Vec::with_capacity(BATCH_SIZE_PAIRS); //

        // Iterate over the unique, normalized set
        for (e1_id, e2_id) in candidate_pairs {
            let group_key = (
                e1_id.0.clone(),
                e2_id.0.clone(),
                method_type.as_str().to_string(),
            );

            if !existing_groups_lookup_arc.contains(&group_key) {
                current_non_match_batch.push((e1_id, e2_id)); // Already normalized

                if current_non_match_batch.len() >= BATCH_SIZE_PAIRS {
                    //
                    info!(
                        "Spawning NON_MATCH batch task for {} ({} pairs)...",
                        method_type.as_str(),
                        current_non_match_batch.len()
                    );
                    non_match_tasks.push(tokio::spawn(process_and_store_batch(
                        pool.clone(),
                        semaphore.clone(),
                        current_non_match_batch.drain(..).collect(),
                        method_type.clone(),
                        pipeline_run_id_for_backfill.clone(),
                        "NON_MATCH",
                        None,
                    )));
                }
            }
        }

        // Spawn any remaining pairs for this method_type
        if !current_non_match_batch.is_empty() {
            info!(
                "Spawning final NON_MATCH batch task for {} ({} pairs)...",
                method_type.as_str(),
                current_non_match_batch.len()
            );
            non_match_tasks.push(tokio::spawn(process_and_store_batch(
                pool.clone(),
                semaphore.clone(),
                current_non_match_batch.drain(..).collect(),
                method_type.clone(),
                pipeline_run_id_for_backfill.clone(),
                "NON_MATCH",
                None,
            )));
        }
        info!("Finished candidate filtering for {}.", method_type.as_str());
    }

    info!(
        "Awaiting {} NON_MATCH batch tasks...",
        non_match_tasks.len()
    );
    for result in try_join_all(non_match_tasks).await? {
        if let Err(e) = result {
            error!("Error in NON_MATCH batch task: {}", e);
        }
    }
    info!("Finished processing NON_MATCH cases.");

    info!("Comparison_cache backfill utility completed.");
    Ok(())
}
