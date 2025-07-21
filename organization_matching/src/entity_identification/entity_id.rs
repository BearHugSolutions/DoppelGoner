// src/entity_identification/entity_id.rs
use anyhow::{Context, Result};
use chrono::Utc;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::models::core::{Entity, EntityFeature};
use crate::utils::contributor_filter::ContributorFilterConfig; // NEW: Import contributor filter
use crate::utils::db_connect::PgPool;

/// Main entry point for entity identification phase WITH CONTRIBUTOR FILTERING
/// Extracts entities from organizations and links them to their features
/// Returns the total number of entities identified
pub async fn identify_entities_with_filter(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
    contributor_filter: Option<&ContributorFilterConfig>,
) -> Result<usize> {
    if let Some(filter) = contributor_filter {
        if filter.is_active() {
            info!("Starting entity identification phase with contributor filtering...");
        } else {
            info!("Starting entity identification phase (filter disabled)...");
        }
    } else {
        info!("Starting entity identification phase (no filter)...");
    }

    let entities = extract_entities_with_filter(pool, multi_progress, contributor_filter)
        .await
        .context("Failed to extract entities from organizations with filter")?;

    info!("Identified {} total entities", entities.len());
    Ok(entities.len())
}

/// Original function for backward compatibility
pub async fn identify_entities(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    identify_entities_with_filter(pool, multi_progress, None).await
}

/// Main entry point for entity feature linking phase
/// Links entities to all their related records and updates existing links
/// Returns the total number of features linked
pub async fn link_and_update_entity_features(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    info!("Starting entity feature linking phase...");

    // Create progress tracking for this phase
    let progress_bar = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(3));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.green} [{elapsed_precise}] {bar:25.cyan/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Getting entities...");
        Some(pb)
    } else {
        None
    };

    // First get all entities
    let entities = get_all_entities(pool)
        .await
        .context("Failed to get all entities for feature linking")?;

    if let Some(pb) = &progress_bar {
        pb.inc(1);
        pb.set_message("Linking initial features...");
    }

    // Link features for all entities
    let initial_features =
        link_entity_features(pool, &entities.into_boxed_slice(), multi_progress.clone())
            .await
            .context("Failed to link entity features")?;

    if let Some(pb) = &progress_bar {
        pb.inc(1);
        pb.set_message("Updating new features...");
    }

    // Update any new features that may have been added since last run
    let additional_features = update_entity_features(pool)
        .await
        .context("Failed to update entity features")?;

    let total_features = initial_features + additional_features;

    if let Some(pb) = &progress_bar {
        pb.inc(1);
        pb.finish_with_message(format!("Linked {} total entity features", total_features));
    }

    info!("Total entity features linked: {}", total_features);

    Ok(total_features)
}

/// Get all existing entities from the database
async fn get_all_entities(pool: &PgPool) -> Result<Vec<Entity>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let rows = conn
        .query(
            "SELECT id, organization_id, name, created_at, updated_at, source_system, source_id 
             FROM public.entity ORDER BY created_at",
            &[],
        )
        .await
        .context("Failed to query all entities")?;

    let mut entities = Vec::with_capacity(rows.len());
    for row in rows {
        let entity = Entity {
            id: row.get("id"),
            organization_id: row.get("organization_id"),
            name: row.try_get("name").ok(),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
            source_system: row.try_get("source_system").ok(),
            source_id: row.try_get("source_id").ok(),
        };
        entities.push(entity);
    }

    info!("Retrieved {} existing entities", entities.len());
    Ok(entities)
}

/// NEW: Extracts entities from the organization table WITH CONTRIBUTOR FILTERING
/// Creates an entity record for each organization with its metadata
/// Only creates entities that don't already exist
/// FIXED: Now uses organization.contributor as source_system and organization.contributor_id as source_id
pub async fn extract_entities_with_filter(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
    contributor_filter: Option<&ContributorFilterConfig>,
) -> Result<Vec<Entity>> {
    if let Some(filter) = contributor_filter {
        if filter.is_active() {
            info!("Extracting entities from organizations with contributor filtering...");
        } else {
            info!("Extracting entities from organizations (filter disabled)...");
        }
    } else {
        info!("Extracting entities from organizations...");
    }

    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Create progress bar for this operation
    let extraction_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new_spinner());
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("    {spinner:.green} [{elapsed_precise}] {msg}")
                .unwrap(),
        );
        pb.set_message("Loading existing entities...");
        Some(pb)
    } else {
        None
    };

    // First, get all existing entities to avoid duplicates
    let existing_rows = conn
        .query("SELECT id, organization_id FROM public.entity", &[])
        .await
        .context("Failed to query existing entities")?;

    // Create a set of organization_ids that already have entities
    let mut existing_org_ids = HashSet::new();
    let mut existing_entities = Vec::with_capacity(existing_rows.len());

    for row in &existing_rows {
        let entity_id: String = row.get("id");
        let org_id: String = row.get("organization_id");

        existing_org_ids.insert(org_id.clone());
        existing_entities.push(Entity {
            id: entity_id,
            organization_id: org_id,
            name: None,
            created_at: Utc::now().naive_utc(),
            updated_at: Utc::now().naive_utc(),
            source_system: None,
            source_id: None,
        });
    }

    info!("Found {} existing entities", existing_org_ids.len());

    // Update progress
    if let Some(pb) = &extraction_pb {
        pb.set_message("Loading organizations...");
    }

    // FIXED: Updated query to include contributor and contributor_id columns
    let mut org_query = "SELECT id, name, contributor, contributor_id FROM public.organization".to_string();
    let mut org_params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();

    if let Some(filter) = contributor_filter {
        // FIXED: Use build_simple_sql_filter() instead of build_sql_filter()
        if let Some((where_clause, contributor_params)) = filter.build_simple_sql_filter() {
            org_query.push_str(&format!(" WHERE {}", where_clause));
            for param in contributor_params {
                org_params.push(Box::new(param));
            }
            info!("🔍 Applying contributor filter to organization query");
        }
    }

    // Execute the query with parameters
    let params_slice: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = org_params
        .iter()
        .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    let org_rows = conn
        .query(&org_query, &params_slice)
        .await
        .context("Failed to query organization table with contributor filter")?;

    if let Some(filter) = contributor_filter {
        if filter.is_active() {
            info!("Found {} organizations after contributor filtering", org_rows.len());
        } else {
            info!("Found {} total organizations", org_rows.len());
        }
    } else {
        info!("Found {} total organizations", org_rows.len());
    }

    // Create progress bar for processing organizations
    let processing_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(org_rows.len() as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.blue} [{elapsed_precise}] {bar:25.yellow/red} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Processing organizations...");
        Some(pb)
    } else {
        None
    };

    let now = Utc::now().naive_utc();
    let mut new_entities = Vec::new();

    // Create an entity for each organization that doesn't already have one
    for (i, row) in org_rows.iter().enumerate() {
        let org_id: String = row.get("id");

        // Skip if this organization already has an entity
        if existing_org_ids.contains(&org_id) {
            if let Some(pb) = &processing_pb {
                pb.inc(1);
            }
            continue;
        }

        let name: Option<String> = row.try_get("name").ok();
        
        // FIXED: Use contributor information from organization table
        let contributor: Option<String> = row.try_get("contributor").ok();
        let contributor_id: Option<String> = row.try_get("contributor_id").ok();
        
        // Set source_system to contributor (or fallback if null)
        let source_system = contributor.or_else(|| Some("HSDS Entity Matching Pipeline".into()));
        
        // Set source_id to contributor_id (or fallback if null)
        let source_id = contributor_id.or_else(|| Some(format!("Generated from organization: {}", org_id)));

        let entity = Entity {
            id: Uuid::new_v4().to_string(),
            organization_id: org_id,
            name,
            created_at: now,
            updated_at: now,
            source_system,
            source_id,
        };

        new_entities.push(entity);

        if let Some(pb) = &processing_pb {
            pb.inc(1);
            if (i + 1) % 1000 == 0 {
                pb.set_message(format!(
                    "Processing organizations... ({}/{})",
                    i + 1,
                    org_rows.len()
                ));
            }
        }
    }

    if let Some(pb) = &processing_pb {
        pb.finish_with_message(format!("Processed {} organizations", org_rows.len()));
    }

    if let Some(pb) = &extraction_pb {
        pb.set_message(format!(
            "Found {} new entities to create",
            new_entities.len()
        ));
    }

    info!("Found {} new entities to create", new_entities.len());

    // If there are no new entities, return the existing ones
    if new_entities.is_empty() {
        if let Some(pb) = &extraction_pb {
            pb.finish_with_message("No new entities to insert");
        }
        info!("No new entities to insert");
        return Ok(existing_entities);
    }

    // Create progress bar for batch insertion
    let batch_size = 100;
    let total_batches = (new_entities.len() + batch_size - 1) / batch_size;

    let insertion_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(total_batches as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("    {spinner:.magenta} [{elapsed_precise}] {bar:25.green/red} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Inserting entities...");
        Some(pb)
    } else {
        None
    };

    info!("Starting entity insertion in {} batches", total_batches);
    let mut inserted = 0;

    for (batch_idx, chunk) in new_entities.chunks(batch_size).enumerate() {
        let mut batch_query = String::from(
            "INSERT INTO entity (id, organization_id, name, created_at, updated_at, source_system, source_id) VALUES ",
        );

        for (i, entity) in chunk.iter().enumerate() {
            if i > 0 {
                batch_query.push_str(", ");
            }
            batch_query.push_str(&format!(
                "('{}', '{}', {}, '{}', '{}', {}, {})",
                entity.id,
                entity.organization_id,
                match &entity.name {
                    Some(name) => format!("'{}'", name.replace('\'', "''")),
                    None => "NULL".to_string(),
                },
                entity.created_at,
                entity.updated_at,
                match &entity.source_system {
                    Some(src) => format!("'{}'", src.replace('\'', "''")),
                    None => "NULL".to_string(),
                },
                match &entity.source_id {
                    Some(id) => format!("'{}'", id.replace('\'', "''")),
                    None => "NULL".to_string(),
                }
            ));
        }

        // Execute the batch insert
        let result = conn.execute(&batch_query, &[]).await;
        match result {
            Ok(count) => {
                inserted += count as usize;
            }
            Err(e) => {
                warn!("Error inserting batch of entities: {}", e);
                // Continue with other batches even if one fails
            }
        }

        if let Some(pb) = &insertion_pb {
            pb.inc(1);
            pb.set_message(format!(
                "Inserting entities... (batch {}/{})",
                batch_idx + 1,
                total_batches
            ));
        }
    }

    if let Some(pb) = &insertion_pb {
        pb.finish_with_message(format!("Inserted {} new entities", inserted));
    }

    if let Some(pb) = &extraction_pb {
        pb.finish_with_message(format!(
            "Entity extraction complete: {} new entities",
            inserted
        ));
    }

    info!("Inserted {} new entities into the database", inserted);

    // Combine existing and new entities for return
    let mut all_entities = existing_entities;
    all_entities.extend(new_entities);

    Ok(all_entities)
}

/// Original function for backward compatibility
pub async fn extract_entities(
    pool: &PgPool,
    multi_progress: Option<MultiProgress>,
) -> Result<Vec<Entity>> {
    extract_entities_with_filter(pool, multi_progress, None).await
}

/// Links entities to their features
/// Finds all records related to each entity and creates entity_feature records
/// Only creates features that don't already exist
pub async fn link_entity_features(
    pool: &PgPool,
    entities: &[Entity],
    multi_progress: Option<MultiProgress>,
) -> Result<usize> {
    info!("Linking entities to their features...");

    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Create progress tracking
    let linking_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(7)); // 5 main feature types + setup + finalization
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "    {spinner:.cyan} [{elapsed_precise}] {bar:25.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Setting up feature linking...");
        Some(pb)
    } else {
        None
    };

    // Create a map of organization_id to entity_id for faster lookups
    let mut org_to_entity = HashMap::new();
    for entity in entities {
        org_to_entity.insert(entity.organization_id.clone(), entity.id.clone());
    }

    // First, get all existing entity_feature records to avoid duplicates
    let existing_rows = conn
        .query(
            "SELECT entity_id, table_name, table_id FROM public.entity_feature",
            &[],
        )
        .await
        .context("Failed to query existing entity_features")?;

    // Create a set of (entity_id, table_name, table_id) tuples that already exist
    let mut existing_features = HashSet::new();

    for row in &existing_rows {
        let entity_id: String = row.get("entity_id");
        let table_name: String = row.get("table_name");
        let table_id: String = row.get("table_id");

        existing_features.insert((entity_id, table_name, table_id));
    }

    info!(
        "Found {} existing entity_feature records",
        existing_features.len()
    );

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.set_message("Linking services...");
    }

    let mut new_features = Vec::new();
    let now = Utc::now().naive_utc();

    // 1. Link services
    info!("Linking services to entities...");
    let service_rows = conn
        .query(
            "SELECT id, organization_id FROM public.service WHERE organization_id IS NOT NULL",
            &[],
        )
        .await
        .context("Failed to query services")?;

    for row in &service_rows {
        let service_id: String = row.get("id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "service".to_string();

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                service_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: service_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.set_message("Linking phones...");
    }

    // 2. Link phones with all possible paths
    info!("Linking phones to entities with all relationship paths...");

    // 2.1 Direct organization_id relationship
    let direct_phone_query = "
        SELECT id, organization_id 
        FROM public.phone 
        WHERE organization_id IS NOT NULL";

    let direct_phone_rows = conn
        .query(direct_phone_query, &[])
        .await
        .context("Failed to query phones with direct organization relationship")?;

    for row in &direct_phone_rows {
        let phone_id: String = row.get("id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "phone".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: phone_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    // 2.2 phone -> service -> organization relationship
    let service_phone_query = "
        SELECT p.id as phone_id, s.organization_id
        FROM public.phone p
        JOIN public.service s ON p.service_id = s.id
        WHERE p.service_id IS NOT NULL 
        AND s.organization_id IS NOT NULL
        AND p.organization_id IS NULL"; // Only get phones not directly linked

    let service_phone_rows = conn
        .query(service_phone_query, &[])
        .await
        .context("Failed to query phones linked via service")?;

    for row in &service_phone_rows {
        let phone_id: String = row.get("phone_id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "phone".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: phone_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    // 2.3 phone -> service_at_location -> service -> organization relationship
    let sal_service_phone_query = "
        SELECT p.id as phone_id, s.organization_id
        FROM public.phone p
        JOIN public.service_at_location sal ON p.service_at_location_id = sal.id
        JOIN public.service s ON sal.service_id = s.id
        WHERE p.service_at_location_id IS NOT NULL 
        AND s.organization_id IS NOT NULL
        AND p.organization_id IS NULL
        AND p.service_id IS NULL"; // Only get phones not already linked

    let sal_service_phone_rows = conn
        .query(sal_service_phone_query, &[])
        .await
        .context("Failed to query phones linked via service_at_location->service")?;

    for row in &sal_service_phone_rows {
        let phone_id: String = row.get("phone_id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "phone".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: phone_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    // 2.4 phone -> service_at_location -> location -> organization relationship
    let sal_location_phone_query = "
        SELECT p.id as phone_id, l.organization_id
        FROM public.phone p
        JOIN public.service_at_location sal ON p.service_at_location_id = sal.id
        JOIN public.location l ON sal.location_id = l.id
        WHERE p.service_at_location_id IS NOT NULL 
        AND l.organization_id IS NOT NULL
        AND p.organization_id IS NULL
        AND p.service_id IS NULL"; // Only get phones not already linked

    let sal_location_phone_rows = conn
        .query(sal_location_phone_query, &[])
        .await
        .context("Failed to query phones linked via service_at_location->location")?;

    for row in &sal_location_phone_rows {
        let phone_id: String = row.get("phone_id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "phone".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: phone_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    // 2.5 phone -> contact -> organization relationship
    let contact_phone_query = "
        SELECT p.id as phone_id, c.organization_id
        FROM public.phone p
        JOIN public.contact c ON p.contact_id = c.id
        WHERE p.contact_id IS NOT NULL 
        AND c.organization_id IS NOT NULL
        AND p.organization_id IS NULL
        AND p.service_id IS NULL
        AND p.service_at_location_id IS NULL"; // Only get phones not already linked

    let contact_phone_rows = conn
        .query(contact_phone_query, &[])
        .await
        .context("Failed to query phones linked via contact")?;

    for row in &contact_phone_rows {
        let phone_id: String = row.get("phone_id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "phone".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: phone_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.set_message("Linking locations...");
    }

    // 3. Link locations
    info!("Linking locations to entities...");
    let location_rows = conn
        .query(
            "SELECT id, organization_id FROM public.location WHERE organization_id IS NOT NULL",
            &[],
        )
        .await
        .context("Failed to query locations")?;

    for row in &location_rows {
        let location_id: String = row.get("id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "location".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                location_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: location_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.set_message("Linking contacts...");
    }

    // 4. Link contacts
    info!("Linking contacts to entities...");
    let contact_rows = conn
        .query(
            "SELECT id, organization_id FROM public.contact WHERE organization_id IS NOT NULL",
            &[],
        )
        .await
        .context("Failed to query contacts")?;

    for row in &contact_rows {
        let contact_id: String = row.get("id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "contact".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                contact_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: contact_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.set_message("Linking addresses...");
    }

    // 5. Link addresses via locations
    info!("Linking addresses to entities via locations...");
    let address_rows = conn
        .query(
            "SELECT a.id as address_id, l.organization_id
             FROM public.address a
             JOIN public.location l ON a.location_id = l.id
             WHERE l.organization_id IS NOT NULL",
            &[],
        )
        .await
        .context("Failed to query addresses linked via locations")?;

    for row in &address_rows {
        let address_id: String = row.get("address_id");
        let org_id: String = row.get("organization_id");

        if let Some(entity_id) = org_to_entity.get(&org_id) {
            let table_name = "address".to_string();

            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                address_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: entity_id.clone(),
                table_name,
                table_id: address_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.set_message(format!("Inserting {} new features...", new_features.len()));
    }

    info!("Found {} new features to link", new_features.len());

    // If there are no new features, return 0
    if new_features.is_empty() {
        if let Some(pb) = &linking_pb {
            pb.finish_with_message("No new features to insert");
        }
        info!("No new features to insert");
        return Ok(0);
    }

    // Insert the new features into the database in batches
    let batch_size = 100;
    let total_batches = (new_features.len() + batch_size - 1) / batch_size;

    let batch_pb = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(total_batches as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("      {spinner:.magenta} [{elapsed_precise}] {bar:20.green/red} {pos}/{len} {msg}")
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  ")
        );
        pb.set_message("Inserting feature batches...");
        Some(pb)
    } else {
        None
    };

    info!(
        "Inserting {} new features in {} batches...",
        new_features.len(),
        total_batches
    );

    let mut inserted = 0;

    for (batch_idx, chunk) in new_features.chunks(batch_size).enumerate() {
        let mut batch_query = String::from(
            "INSERT INTO entity_feature (id, entity_id, table_name, table_id, created_at) VALUES ",
        );

        for (i, feature) in chunk.iter().enumerate() {
            if i > 0 {
                batch_query.push_str(", ");
            }
            batch_query.push_str(&format!(
                "('{}', '{}', '{}', '{}', '{}')",
                feature.id,
                feature.entity_id,
                feature.table_name.replace('\'', "''"),
                feature.table_id.replace('\'', "''"),
                feature.created_at
            ));
        }

        // Execute the batch insert
        let result = conn.execute(&batch_query, &[]).await;
        match result {
            Ok(count) => {
                inserted += count as usize;
            }
            Err(e) => {
                warn!("Error inserting batch of features: {}", e);
                // Continue with other batches even if one fails
            }
        }

        if let Some(pb) = &batch_pb {
            pb.inc(1);
        }
    }

    if let Some(pb) = &batch_pb {
        pb.finish_with_message(format!("Inserted {} feature batches", total_batches));
    }

    if let Some(pb) = &linking_pb {
        pb.inc(1);
        pb.finish_with_message(format!(
            "Feature linking complete: {} new features",
            inserted
        ));
    }

    info!(
        "Inserted {} new entity features into the database",
        inserted
    );

    // Return the total number of features (existing + new)
    Ok(existing_features.len() + inserted)
}

/// Updates existing entities with new features that have been added since the
/// last time features were linked. This ensures all entities have complete
/// feature sets even as the database changes over time.
pub async fn update_entity_features(pool: &PgPool) -> Result<usize> {
    info!("Updating entity features for existing entities...");

    let conn = pool.get().await.context("Failed to get DB connection")?;

    info!("Finding and inserting new features for existing entities...");
    let result = conn
        .execute(
            "
            INSERT INTO public.entity_feature (id, entity_id, table_name, table_id, created_at)
            SELECT 
                gen_random_uuid()::text, 
                entity_id, 
                table_name, 
                table_id, 
                NOW() 
            FROM (
                -- Find new services not yet linked to their entities
                SELECT 'service' as table_name, s.id as table_id, e.id as entity_id
                FROM public.service s
                JOIN public.entity e ON s.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'service' AND ef.table_id = s.id
                WHERE ef.id IS NULL AND s.organization_id IS NOT NULL

                UNION ALL

                -- Direct path: organization_id directly on phone
                SELECT 'phone' as table_name, p.id as table_id, e.id as entity_id
                FROM public.phone p
                JOIN public.entity e ON p.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'phone' AND ef.table_id = p.id
                WHERE ef.id IS NULL AND p.organization_id IS NOT NULL

                UNION ALL

                -- Find new locations not yet linked to their entities
                SELECT 'location' as table_name, l.id as table_id, e.id as entity_id
                FROM public.location l
                JOIN public.entity e ON l.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'location' AND ef.table_id = l.id
                WHERE ef.id IS NULL AND l.organization_id IS NOT NULL

                UNION ALL

                -- Find new contacts not yet linked to their entities
                SELECT 'contact' as table_name, c.id as table_id, e.id as entity_id
                FROM public.contact c
                JOIN public.entity e ON c.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'contact' AND ef.table_id = c.id
                WHERE ef.id IS NULL AND c.organization_id IS NOT NULL
                
                UNION ALL

                -- Find new addresses (via location) not yet linked to their entities
                SELECT 'address' as table_name, a.id as table_id, e.id as entity_id
                FROM public.address a
                JOIN public.location l ON a.location_id = l.id
                JOIN public.entity e ON l.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id
                    AND ef.table_name = 'address' AND ef.table_id = a.id
                WHERE ef.id IS NULL AND l.organization_id IS NOT NULL

            ) as new_features
            ",
            &[],
        )
        .await
        .context("Failed to insert new features")?;

    let inserted = result as usize;
    info!(
        "Inserted {} new entity features into the database",
        inserted
    );

    Ok(inserted)
}