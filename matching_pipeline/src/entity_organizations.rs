// src/entity_organization.rs
use anyhow::{Context, Result};
use chrono::Utc;
use futures::{stream, StreamExt};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use crate::db::PgPool;
use crate::models::{Entity, EntityFeature, EntityId, OrganizationId};
use crate::reinforcement::entity::feature_extraction::get_stored_entity_features;

/// Extracts entities from the organization table
/// Creates an entity record for each organization with its metadata
/// Only creates entities that don't already exist
///
/// This involves:
/// 1. Querying the entity table to get existing entities
/// 2. Querying the organization table to get all organizations
/// 3. Creating an entity record for each organization that doesn't already have one
/// 4. Setting entity.organization_id to the original organization.id
/// 5. Populating other entity fields (name, source info, etc.)
/// 6. Batch inserting the new entity records into the entity table
pub async fn extract_entities(pool: &PgPool) -> Result<Vec<Entity>> {
    info!("Extracting entities from organizations...");

    let conn = pool.get().await.context("Failed to get DB connection")?;

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
            id: EntityId(entity_id),
            organization_id: OrganizationId(org_id),
            name: None, // We don't need to populate all fields for existing entities
            created_at: Utc::now().naive_utc(), // Placeholder
            updated_at: Utc::now().naive_utc(), // Placeholder
            source_system: None,
            source_id: None,
        });
    }

    info!("Found {} existing entities", existing_org_ids.len());

    // Query the organization table to get all organizations
    let org_rows = conn
        .query("SELECT id, name FROM public.organization", &[])
        .await
        .context("Failed to query organization table")?;

    info!("Found {} total organizations", org_rows.len());

    let now = Utc::now().naive_utc();
    let mut new_entities = Vec::new();

    // Create an entity for each organization that doesn't already have one
    for (i, row) in org_rows.iter().enumerate() {
        let org_id: String = row.get("id");

        // Skip if this organization already has an entity
        if existing_org_ids.contains(&org_id) {
            continue;
        }

        let name: Option<String> = row.try_get("name").ok();
        let source_system = Some("Snowflake - feature not fully implemented in pipeline".into());
        let source_id = Some("Feature not implemented in pipeline".into());

        let entity = Entity {
            id: EntityId(Uuid::new_v4().to_string()),
            organization_id: OrganizationId(org_id),
            name,
            created_at: now,
            updated_at: now,
            source_system,
            source_id,
        };

        new_entities.push(entity);

        // Log progress at regular intervals
        if (i + 1) % 1000 == 0 || i + 1 == org_rows.len() {
            info!(
                "Processed {}/{} organizations ({:.1}%)",
                i + 1,
                org_rows.len(),
                (i + 1) as f32 / org_rows.len() as f32 * 100.0
            );
        }
    }

    info!("Found {} new entities to create", new_entities.len());

    // If there are no new entities, return the existing ones
    if new_entities.is_empty() {
        info!("No new entities to insert");
        return Ok(existing_entities);
    }

    // Insert the new entities into the database in batches
    let batch_size = 100;
    let total_batches = (new_entities.len() + batch_size - 1) / batch_size;
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
                entity.id.0,
                entity.organization_id.0,
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

        // Log batch progress
        if (batch_idx + 1) % 10 == 0 || batch_idx + 1 == total_batches {
            info!(
                "Processed batch {}/{} ({:.1}%)",
                batch_idx + 1,
                total_batches,
                (batch_idx + 1) as f32 / total_batches as f32 * 100.0
            );
        }
    }

    info!("Inserted {} new entities into the database", inserted);

    // Combine existing and new entities for return
    let mut all_entities = existing_entities;
    all_entities.extend(new_entities);

    Ok(all_entities)
}

/// Links entities to their features
/// Finds all records related to each entity and creates entity_feature records
/// Only creates features that don't already exist
///
/// This involves:
/// 1. Querying existing entity_feature records to avoid duplicates
/// 2. For each entity, find all references to its organization_id in other tables:
///    - services provided by the organization
///    - phones associated with the organization
///    - locations operated by the organization
///    - contacts linked to the organization
/// 3. For each reference found, create an entity_feature record if it doesn't exist with:
///    - entity_id pointing to the entity
///    - table_name indicating the source table (e.g., "service", "phone")
///    - table_id containing the ID of the referenced record
/// 4. Batch insert the new entity_feature records
pub async fn link_entity_features(pool: &PgPool, entities: &[Entity]) -> Result<usize> {
    info!("Linking entities to their features...");

    let conn = pool.get().await.context("Failed to get DB connection")?;

    // Create a map of organization_id to entity_id for faster lookups
    let mut org_to_entity = HashMap::new();
    for entity in entities {
        org_to_entity.insert(entity.organization_id.0.clone(), entity.id.0.clone());
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
                entity_id: EntityId(entity_id.clone()),
                table_name,
                table_id: service_id,
                created_at: now,
            };
            new_features.push(feature);
        }
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                phone_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
                table_name,
                table_id: phone_id,
                created_at: now,
            };
            new_features.push(feature);
        }
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                location_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
                table_name,
                table_id: location_id,
                created_at: now,
            };
            new_features.push(feature);
        }
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

            // Skip if this feature already exists
            if existing_features.contains(&(
                entity_id.clone(),
                table_name.clone(),
                contact_id.clone(),
            )) {
                continue;
            }

            let feature = EntityFeature {
                id: Uuid::new_v4().to_string(),
                entity_id: EntityId(entity_id.clone()),
                table_name,
                table_id: contact_id,
                created_at: now,
            };
            new_features.push(feature);
        }
    }

    info!("Found {} new features to link", new_features.len());

    // If there are no new features, return 0
    if new_features.is_empty() {
        info!("No new features to insert");
        return Ok(0);
    }

    // Insert the new features into the database in batches
    let batch_size = 100;
    let total_batches = (new_features.len() + batch_size - 1) / batch_size;

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
                feature.entity_id.0,
                feature.table_name,
                feature.table_id,
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

        // Log batch progress
        if (batch_idx + 1) % 10 == 0 || batch_idx + 1 == total_batches {
            info!(
                "Processed batch {}/{} ({:.1}%)",
                batch_idx + 1,
                total_batches,
                (batch_idx + 1) as f32 / total_batches as f32 * 100.0
            );
        }
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
///
/// This involves:
/// 1. Finding all features (services, phones, locations, contacts) that reference
///    organizations with existing entities but don't yet have entity_feature links
/// 2. Creating entity_feature records for these new features
/// 3. Efficiently inserting them in a single database operation
pub async fn update_entity_features(pool: &PgPool) -> Result<usize> {
    info!("Updating entity features for existing entities...");

    let conn = pool.get().await.context("Failed to get DB connection")?;

    // The SQL query has been expanded to include all the indirect paths
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

                -- Path: phone -> service -> organization
                SELECT 'phone' as table_name, p.id as table_id, e.id as entity_id
                FROM public.phone p
                JOIN public.service s ON p.service_id = s.id
                JOIN public.entity e ON s.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'phone' AND ef.table_id = p.id
                WHERE ef.id IS NULL 
                  AND p.service_id IS NOT NULL
                  AND s.organization_id IS NOT NULL
                  AND p.organization_id IS NULL

                UNION ALL

                -- Path: phone -> service_at_location -> service -> organization
                SELECT 'phone' as table_name, p.id as table_id, e.id as entity_id
                FROM public.phone p
                JOIN public.service_at_location sal ON p.service_at_location_id = sal.id
                JOIN public.service s ON sal.service_id = s.id
                JOIN public.entity e ON s.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'phone' AND ef.table_id = p.id
                WHERE ef.id IS NULL 
                  AND p.service_at_location_id IS NOT NULL
                  AND s.organization_id IS NOT NULL
                  AND p.organization_id IS NULL
                  AND p.service_id IS NULL

                UNION ALL

                -- Path: phone -> service_at_location -> location -> organization
                SELECT 'phone' as table_name, p.id as table_id, e.id as entity_id
                FROM public.phone p
                JOIN public.service_at_location sal ON p.service_at_location_id = sal.id
                JOIN public.location l ON sal.location_id = l.id
                JOIN public.entity e ON l.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'phone' AND ef.table_id = p.id
                WHERE ef.id IS NULL 
                  AND p.service_at_location_id IS NOT NULL
                  AND l.organization_id IS NOT NULL
                  AND p.organization_id IS NULL
                  AND p.service_id IS NULL

                UNION ALL

                -- Path: phone -> contact -> organization
                SELECT 'phone' as table_name, p.id as table_id, e.id as entity_id
                FROM public.phone p
                JOIN public.contact c ON p.contact_id = c.id
                JOIN public.entity e ON c.organization_id = e.organization_id
                LEFT JOIN public.entity_feature ef ON ef.entity_id = e.id 
                    AND ef.table_name = 'phone' AND ef.table_id = p.id
                WHERE ef.id IS NULL 
                  AND p.contact_id IS NOT NULL
                  AND c.organization_id IS NOT NULL
                  AND p.organization_id IS NULL
                  AND p.service_id IS NULL
                  AND p.service_at_location_id IS NULL

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

/// Extracts and stores context features for all identified entities in PARALLEL.
/// It iterates through all entities in the 'public.entity' table.
/// This version uses `get_stored_entity_features` to avoid recalculating existing, complete features.
pub async fn extract_and_store_all_entity_context_features(pool: &PgPool) -> Result<usize> {
    info!("Starting PARALLEL extraction and storage of context features for all entities...");
    let start_time = std::time::Instant::now();

    // Get a connection to fetch the list of all entity IDs
    let conn_for_list = pool
        .get()
        .await
        .context("Failed to get DB connection for entity list")?;
    let entity_rows = conn_for_list
        .query("SELECT id FROM public.entity", &[]) // EntityId is String, so direct mapping
        .await
        .context("Failed to query entity IDs for feature extraction")?;
    drop(conn_for_list); // Release the connection as soon as the list is fetched

    if entity_rows.is_empty() {
        info!("No entities found to process for context features.");
        return Ok(0);
    }

    let entity_ids: Vec<EntityId> = entity_rows
        .into_iter()
        .map(|row| EntityId(row.get(0))) // Assuming EntityId(String)
        .collect();

    let total_entities_to_process = entity_ids.len();
    info!(
        "Found {} entities to process for context features.",
        total_entities_to_process
    );

    // --- Parallel Processing Logic ---
    // Set desired concurrency. This value should be tuned based on your DB capacity and machine resources.
    // Keeping it at 30 to stay well within the preferred 40 connection limit,
    // assuming other parts of the pipeline might use some connections.
    // This could be made configurable (e.g., from config.rs or an environment variable).
    let desired_concurrency = 30;

    // Atomic counters for tracking progress in a thread-safe manner
    let succeeded_count = Arc::new(AtomicUsize::new(0));
    let processed_for_log_count = Arc::new(AtomicUsize::new(0));

    stream::iter(entity_ids)
        .map(|entity_id| {
            let pool_clone = pool.clone(); // Clone the pool for each spawned task
            let succeeded_clone = Arc::clone(&succeeded_count);
            let processed_log_clone = Arc::clone(&processed_for_log_count);
            // Shadow total_entities_to_process to avoid capturing the outer variable directly in the 'static future
            let total_entities = total_entities_to_process;

            tokio::spawn(async move { // Spawn a new asynchronous task for each entity
                match pool_clone.get().await { // Each task gets its own connection from the pool
                    Ok(conn_guard) => {
                        // Call `get_stored_entity_features` which handles resumability.
                        // It will call the actual feature extraction logic only if necessary.
                        match get_stored_entity_features(&*conn_guard, &entity_id).await {
                            Ok(_features) => {
                                // If features are successfully retrieved or generated.
                                succeeded_clone.fetch_add(1, Ordering::Relaxed);
                                debug!("Features ensured for entity {}", entity_id.0);
                            }
                            Err(e) => {
                                warn!("Failed to ensure features for entity {}: {}", entity_id.0, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to get DB connection for entity {}: {}", entity_id.0, e);
                    }
                }
                // Log progress periodically
                let current_processed = processed_log_clone.fetch_add(1, Ordering::Relaxed) + 1;
                if current_processed % 100 == 0 || current_processed == total_entities { // Log every 100 entities or at the very end
                    info!(
                        "Feature extraction progress: {}/{} entity processing tasks dispatched/completed.",
                        current_processed, total_entities
                    );
                }
            })
        })
        .buffer_unordered(desired_concurrency) // Limit the number of concurrent tasks
        .for_each(|result| async {
            if let Err(e) = result { // Handle potential errors from spawned tasks (e.g., panics)
                warn!("A feature extraction task panicked or failed to join: {:?}", e);
            }
        })
        .await;

    let final_succeeded_count = succeeded_count.load(Ordering::Relaxed);
    let elapsed = start_time.elapsed();
    info!(
        "Parallel context feature extraction and storage complete in {:.2?}. Ensured features for {} out of {} entities.",
        elapsed, final_succeeded_count, total_entities_to_process
    );

    Ok(final_succeeded_count)
}
