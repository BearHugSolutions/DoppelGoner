// src/bin/phone_entity_feature_creation.rs

use anyhow::{Context, Result};
use chrono::Utc;
use dedupe_lib::{
    db::{self, PgPool},
    models::{EntityFeature, EntityId},
};
use futures::future::join_all;
use log::{debug, info, warn};
use std::{
    collections::HashSet,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

// Constants for parallel execution
const BATCH_SIZE: usize = 1000; // How many links to fetch from DB at a time
const MAX_CONCURRENT_DB_TASKS: usize = 10; // Max concurrent tasks that hit the DB for inserts
const MAX_CONCURRENT_ENTITY_PROCESSING_TASKS: usize = 50; // Max concurrent tasks for in-memory processing of links

// Struct to hold intermediate data for linking
#[derive(Debug, Clone)]
struct EntityPhoneLink {
    entity_id: String,
    phone_id: String,
    pathway: String, // Track which pathway this link came from
}

// Struct to hold pathway statistics
#[derive(Debug, Default)]
struct PathwayStats {
    organization_direct: usize,
    service_linked: usize,
    location_linked: usize,
    service_at_location: usize,
}

async fn fetch_existing_phone_features(pool: &PgPool) -> Result<HashSet<(String, String, String)>> {
    info!("Fetching existing phone entity_feature records...");
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for existing phone features")?;
    let rows = conn
        .query(
            "SELECT entity_id, table_name, table_id FROM public.entity_feature WHERE table_name = 'phone'",
            &[],
        )
        .await
        .context("Failed to query existing phone entity_features")?;

    let mut existing_features = HashSet::new();
    for row in rows {
        let entity_id: String = row.get("entity_id");
        let table_name: String = row.get("table_name");
        let table_id: String = row.get("table_id");
        existing_features.insert((entity_id, table_name, table_id));
    }
    info!(
        "✅ Found {} existing 'phone' entity_feature records to exclude.",
        existing_features.len()
    );
    Ok(existing_features)
}

async fn get_pathway_counts(pool: &PgPool) -> Result<PathwayStats> {
    info!("Getting pathway counts for phones...");
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for pathway counts")?;

    let mut stats = PathwayStats::default();

    // Count direct organization phones
    let rows = conn
        .query(
            "SELECT COUNT(*) as count FROM public.phone p 
             JOIN public.entity e ON p.organization_id = e.organization_id 
             WHERE p.organization_id IS NOT NULL",
            &[],
        )
        .await?;
    if let Some(row) = rows.first() {
        stats.organization_direct = row.get::<_, i64>("count") as usize;
    }

    // Count service-linked phones
    let rows = conn
        .query(
            "SELECT COUNT(*) as count FROM public.phone p 
             JOIN public.service s ON p.service_id = s.id 
             JOIN public.entity e ON s.organization_id = e.organization_id 
             WHERE p.service_id IS NOT NULL",
            &[],
        )
        .await?;
    if let Some(row) = rows.first() {
        stats.service_linked = row.get::<_, i64>("count") as usize;
    }

    // Count location-linked phones
    let rows = conn
        .query(
            "SELECT COUNT(*) as count FROM public.phone p 
             JOIN public.location l ON p.location_id = l.id 
             JOIN public.entity e ON l.organization_id = e.organization_id 
             WHERE p.location_id IS NOT NULL",
            &[],
        )
        .await?;
    if let Some(row) = rows.first() {
        stats.location_linked = row.get::<_, i64>("count") as usize;
    }

    // Count service-at-location phones
    let rows = conn
        .query(
            "SELECT COUNT(*) as count FROM public.phone p 
             JOIN public.service_at_location sal ON p.service_at_location_id = sal.id 
             JOIN public.service s ON sal.service_id = s.id 
             JOIN public.entity e ON s.organization_id = e.organization_id 
             WHERE p.service_at_location_id IS NOT NULL",
            &[],
        )
        .await?;
    if let Some(row) = rows.first() {
        stats.service_at_location = row.get::<_, i64>("count") as usize;
    }

    info!("📊 Pathway Statistics:");
    info!("  Direct Organization: {} phones", stats.organization_direct);
    info!("  Service-linked: {} phones", stats.service_linked);
    info!("  Location-linked: {} phones", stats.location_linked);
    info!("  Service-at-location: {} phones", stats.service_at_location);
    info!(
        "  Total: {} phones across all pathways",
        stats.organization_direct + stats.service_linked + stats.location_linked + stats.service_at_location
    );

    Ok(stats)
}

async fn fetch_entity_phone_links_batch(
    pool: &PgPool,
    offset: usize,
    limit: usize,
) -> Result<Vec<EntityPhoneLink>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for entity-phone links")?;

    // Enhanced query that handles all four foreign key pathways using UNION
    let query_str = "
        -- Pathway 1: Direct organization phones
        SELECT DISTINCT 
            e.id AS entity_id, 
            p.id AS phone_id, 
            'organization_direct' AS pathway
        FROM public.phone p
        JOIN public.entity e ON p.organization_id = e.organization_id
        WHERE p.organization_id IS NOT NULL
        
        UNION
        
        -- Pathway 2: Service-linked phones
        SELECT DISTINCT 
            e.id AS entity_id, 
            p.id AS phone_id, 
            'service_linked' AS pathway
        FROM public.phone p
        JOIN public.service s ON p.service_id = s.id
        JOIN public.entity e ON s.organization_id = e.organization_id
        WHERE p.service_id IS NOT NULL
        
        UNION
        
        -- Pathway 3: Location-linked phones
        SELECT DISTINCT 
            e.id AS entity_id, 
            p.id AS phone_id, 
            'location_linked' AS pathway
        FROM public.phone p
        JOIN public.location l ON p.location_id = l.id
        JOIN public.entity e ON l.organization_id = e.organization_id
        WHERE p.location_id IS NOT NULL
        
        UNION
        
        -- Pathway 4: Service-at-Location phones (original behavior)
        SELECT DISTINCT 
            e.id AS entity_id, 
            p.id AS phone_id, 
            'service_at_location' AS pathway
        FROM public.phone p
        JOIN public.service_at_location sal ON p.service_at_location_id = sal.id
        JOIN public.service s ON sal.service_id = s.id
        JOIN public.entity e ON s.organization_id = e.organization_id
        WHERE p.service_at_location_id IS NOT NULL
        
        ORDER BY entity_id, phone_id
        OFFSET $1 LIMIT $2";

    let rows = conn
        .query(query_str, &[&(offset as i64), &(limit as i64)])
        .await
        .context("Failed to query entity-phone links via all pathways")?;

    let mut links = Vec::new();
    let mut pathway_counts = std::collections::HashMap::new();
    
    for row in rows {
        let pathway: String = row.get("pathway");
        *pathway_counts.entry(pathway.clone()).or_insert(0) += 1;
        
        links.push(EntityPhoneLink {
            entity_id: row.get("entity_id"),
            phone_id: row.get("phone_id"),
            pathway,
        });
    }

    if !pathway_counts.is_empty() {
        debug!("Batch pathway breakdown: {:?}", pathway_counts);
    }

    Ok(links)
}

async fn insert_new_features_batch(
    pool: &PgPool,
    features_to_insert: Vec<EntityFeature>,
    semaphore: Arc<Semaphore>,
    inserted_counter: Arc<AtomicUsize>,
) -> Result<()> {
    if features_to_insert.is_empty() {
        return Ok(());
    }

    let _permit = semaphore
        .acquire()
        .await
        .expect("Semaphore acquisition failed for DB insert task");

    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for batch insert")?;

    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for batch insert")?;

    let mut query = String::from(
        "INSERT INTO public.entity_feature (id, entity_id, table_name, table_id, created_at) VALUES ",
    );

    for (i, feature) in features_to_insert.iter().enumerate() {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&format!(
            "('{}', '{}', '{}', '{}', '{}')",
            feature.id.replace('\'', "''"), 
            feature.entity_id.0.replace('\'', "''"),
            feature.table_name.replace('\'', "''"), 
            feature.table_id.replace('\'', "''"),
            feature.created_at 
        ));
    }
    
    // Add ON CONFLICT clause to handle any duplicates gracefully
    query.push_str(" ON CONFLICT (entity_id, table_name, table_id) DO NOTHING");
    
    match transaction.execute(query.as_str(), &[]).await {
        Ok(count) => {
            transaction
                .commit()
                .await
                .context("Failed to commit transaction")?;
            let num_inserted = count as usize;
            inserted_counter.fetch_add(num_inserted, Ordering::Relaxed);
            debug!("✅ Successfully inserted {} phone features.", num_inserted);
        }
        Err(e) => {
            let _ = transaction.rollback().await; 
            warn!("❌ Error inserting batch of phone features: {}. Batch details: {} features.", e, features_to_insert.len());
            if !features_to_insert.is_empty() {
                warn!("First feature in failed batch: entity_id: {}, phone_id: {}", features_to_insert[0].entity_id.0, features_to_insert[0].table_id);
            }
            return Err(e.into());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    info!("🚀 Starting enhanced phone entity feature creation binary (all pathways).");
    let start_time = Instant::now();

    let env_paths = [".env", ".env.local", "../.env", "../../.env"];
    let mut loaded_env = false;
    for path_str in env_paths.iter() {
        let path = Path::new(path_str);
        if path.exists() {
            if let Err(e) = db::load_env_from_file(path_str) {
                warn!("Failed to load environment from {}: {}", path_str, e);
            } else {
                info!("✅ Loaded environment variables from {}", path_str);
                loaded_env = true;
                break;
            }
        } else {
            debug!("Env file not found at: {}", path_str);
        }
    }
    if !loaded_env {
        info!("No .env file found in checked paths, using system environment variables.");
    }

    let pool = db::connect()
        .await
        .context("Failed to connect to database")?;
    info!("✅ Successfully connected to the database.");

    // Get pathway statistics first
    let _pathway_stats = get_pathway_counts(&pool)
        .await
        .context("Failed to get pathway statistics")?;

    let existing_features = Arc::new(
        fetch_existing_phone_features(&pool)
            .await
            .context("Failed to fetch existing phone features")?,
    );

    let total_processed_links = Arc::new(AtomicUsize::new(0));
    let new_features_identified_count = Arc::new(AtomicUsize::new(0));
    let total_inserted_count = Arc::new(AtomicUsize::new(0));

    let db_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DB_TASKS));
    let processing_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_ENTITY_PROCESSING_TASKS));

    let mut offset = 0;
    let mut total_pathway_counts = std::collections::HashMap::new();

    loop {
        info!(
            "🔄 Fetching entity-phone links batch: offset {}, limit {}",
            offset, BATCH_SIZE
        );
        let links_batch = fetch_entity_phone_links_batch(&pool, offset, BATCH_SIZE)
            .await
            .context(format!(
                "Failed to fetch phone links batch (offset: {})",
                offset
            ))?;

        if links_batch.is_empty() {
            info!("✅ No more entity-phone links to process.");
            break;
        }
        
        let current_batch_size = links_batch.len();
        
        // Count pathways in this batch
        for link in &links_batch {
            *total_pathway_counts.entry(link.pathway.clone()).or_insert(0) += 1;
        }
        
        info!(
            "📝 Processing {} phone links from offset {} across all pathways.",
            current_batch_size, offset
        );

        let mut tasks = Vec::new();
        for link_chunk in links_batch.chunks(MAX_CONCURRENT_ENTITY_PROCESSING_TASKS / 5 + 1) {
            let permit = processing_semaphore.clone().acquire_owned().await.unwrap();
            let chunk_to_process = link_chunk.to_vec(); 
            let existing_features_clone = existing_features.clone();
            let new_features_identified_count_clone = new_features_identified_count.clone();
            let total_processed_links_clone = total_processed_links.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = permit; 
                let mut features_for_this_processing_chunk = Vec::new();
                let now = Utc::now().naive_utc();
                for link in chunk_to_process {
                    total_processed_links_clone.fetch_add(1, Ordering::Relaxed);
                    let feature_key = (
                        link.entity_id.clone(),
                        "phone".to_string(),
                        link.phone_id.clone(),
                    );

                    if !existing_features_clone.contains(&feature_key) {
                        features_for_this_processing_chunk.push(EntityFeature {
                            id: Uuid::new_v4().to_string(),
                            entity_id: EntityId(link.entity_id),
                            table_name: "phone".to_string(),
                            table_id: link.phone_id,
                            created_at: now,
                        });
                        new_features_identified_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
                features_for_this_processing_chunk
            }));
        }
        
        let mut all_new_features_for_main_batch = Vec::new();
        for task_result in join_all(tasks).await {
            match task_result {
                Ok(chunk_features) => all_new_features_for_main_batch.extend(chunk_features),
                Err(e) => warn!("⚠️ Entity phone link processing task panicked: {}", e),
            }
        }

        // Deduplicate within this batch
        let mut unique_features_to_insert_for_main_batch = Vec::new();
        let mut seen_in_this_main_batch = HashSet::new();
        for feature in all_new_features_for_main_batch {
            let key = (feature.entity_id.0.clone(), feature.table_name.clone(), feature.table_id.clone());
            if seen_in_this_main_batch.insert(key) {
                unique_features_to_insert_for_main_batch.push(feature);
            }
        }

        if !unique_features_to_insert_for_main_batch.is_empty() {
            info!(
                "💾 Found {} unique new phone features to insert from this batch (total links: {}).",
                unique_features_to_insert_for_main_batch.len(),
                current_batch_size
            );
            
            let mut insert_tasks = Vec::new();
            for insert_chunk in unique_features_to_insert_for_main_batch.chunks(BATCH_SIZE / 5) { 
                if insert_chunk.is_empty() { continue; }
                let features_to_insert_clone = insert_chunk.to_vec();
                let pool_clone = pool.clone();
                let db_semaphore_clone = db_semaphore.clone();
                let total_inserted_count_clone = total_inserted_count.clone();
                
                insert_tasks.push(tokio::spawn(async move {
                    if let Err(e) = insert_new_features_batch(
                        &pool_clone,
                        features_to_insert_clone,
                        db_semaphore_clone,
                        total_inserted_count_clone,
                    )
                    .await
                    {
                        warn!("❌ Insert task failed: {}", e);
                    }
                }));
            }
            join_all(insert_tasks).await; 
        } else {
            info!("ℹ️ No new unique phone features to insert from this batch (total links: {}).", current_batch_size);
        }
        
        offset += current_batch_size;
        if current_batch_size < BATCH_SIZE {
            info!("🏁 Processed last batch of phone links.");
            break; 
        }
    }

    let elapsed = start_time.elapsed();
    info!("🎉 Enhanced phone entity feature creation completed in {:.2?}.", elapsed);
    info!("📊 FINAL STATISTICS:");
    info!("  Total entity-phone links processed: {}", total_processed_links.load(Ordering::Relaxed));
    info!("  Total new phone features identified: {}", new_features_identified_count.load(Ordering::Relaxed));
    info!("  Total new phone features inserted: {}", total_inserted_count.load(Ordering::Relaxed));
    
    info!("📈 PATHWAY BREAKDOWN:");
    for (pathway, count) in total_pathway_counts {
        info!("  {}: {} links", pathway, count);
    }

    Ok(())
}