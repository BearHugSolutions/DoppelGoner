// src/candidate_generation/candidate_generation.rs

use crate::{models::{matching::ServiceInfo, stats_models::ServiceCluster}, utils::db_connect::PgPool};
use anyhow::{Context, Result};
use log::debug;
use std::collections::{HashMap, HashSet};

pub async fn get_entity_clusters(pool: &PgPool) -> Result<HashMap<Option<String>, Vec<String>>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let query = "
        SELECT DISTINCT entity_id_1, entity_id_2, group_cluster_id
        FROM public.entity_group
        WHERE group_cluster_id IS NOT NULL
    ";

    debug!("Querying entity_group table...");
    let rows = conn.query(query, &[]).await.context("Failed to query entity_group")?;
    debug!("Found {} rows in entity_group.", rows.len());

    let mut clusters: HashMap<Option<String>, Vec<String>> = HashMap::new();

    for row in rows {
        let entity_id_1: String = row.get("entity_id_1");
        let entity_id_2: String = row.get("entity_id_2");
        let cluster_id: Option<String> = row.get("group_cluster_id");

        let cluster_key = cluster_id.clone();
        let cluster_entities = clusters.entry(cluster_key).or_insert_with(Vec::new);

        cluster_entities.push(entity_id_1);
        cluster_entities.push(entity_id_2);
    }

    for entities in clusters.values_mut() {
        entities.sort_by(|a, b| a.cmp(&b));
        entities.dedup_by(|a, b| a == b);
    }

    debug!("Organized {} entities into {} clusters",
          clusters.values().map(|v| v.len()).sum::<usize>(),
          clusters.len());

    Ok(clusters)
}

pub async fn add_unmatched_entities(
    pool: &PgPool,
    clusters: &mut HashMap<Option<String>, Vec<String>>
) -> Result<usize> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let clustered_entities: HashSet<String> = clusters
        .values()
        .flatten()
        .cloned()
        .collect();

    let query = "SELECT id FROM public.entity";
    debug!("Querying all entities...");
    let rows = conn.query(query, &[]).await.context("Failed to query all entities")?;
    debug!("Found {} total entities.", rows.len());

    let mut unmatched_count = 0;

    for row in rows {
        let entity_id = row.get::<_, String>("id");

        if !clustered_entities.contains(&entity_id) {
            let pseudo_cluster_key = Some(format!("individual-{}", entity_id));
            clusters.insert(pseudo_cluster_key, vec![entity_id]);
            unmatched_count += 1;
        }
    }
    Ok(unmatched_count)
}

/// Load services for each cluster with optional service contributor filtering
/// NEW: Updated to accept and apply service contributor filter
pub async fn load_service_clusters(
    pool: &PgPool,
    entity_clusters: HashMap<Option<String>, Vec<String>>,
    service_contributor_filter: Option<&crate::utils::service_contributor_filter::ServiceContributorFilterConfig>,
) -> Result<Vec<ServiceCluster>> {
    if entity_clusters.is_empty() {
        return Ok(Vec::new());
    }

    let conn = pool.get().await.context("Failed to get DB connection")?;

    let mut all_entity_ids = Vec::new();
    let mut entity_to_cluster: HashMap<String, Option<String>> = HashMap::new();

    for (cluster_id, entities) in &entity_clusters {
        for entity_id in entities {
            all_entity_ids.push(entity_id.clone());
            entity_to_cluster.insert(entity_id.clone(), cluster_id.clone());
        }
    }

    if all_entity_ids.is_empty() {
        return Ok(Vec::new());
    }

    debug!("Loading services for {} entities across {} clusters",
          all_entity_ids.len(), entity_clusters.len());

    // NEW: Build query with optional service contributor filtering
    let mut query = "
        SELECT s.id, s.name, s.email, s.url, s.embedding_v2, ef.entity_id
        FROM public.service s
        JOIN public.entity_feature ef ON ef.table_id = s.id AND ef.table_name = 'service'
        WHERE ef.entity_id = ANY($1)".to_string();
    
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
    params.push(Box::new(all_entity_ids));
    let mut param_offset = 1;

    // NEW: Add service contributor filtering if enabled
    if let Some(filter) = service_contributor_filter {
        if let Some((where_clause, contributor_params)) = filter.build_service_sql_filter_with_offset(param_offset) {
            query.push_str(&format!(" AND {}", where_clause));
            for param in contributor_params {
                params.push(Box::new(param));
            }
            debug!("Applied service contributor filter to query");
        }
    }

    query.push_str(" ORDER BY ef.entity_id, s.id");

    let rows = conn.query(&query, &params.iter().map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect::<Vec<_>>()).await
        .context("Failed to query services for all clusters")?;

    debug!("Retrieved {} service rows from database", rows.len());

    let mut cluster_services: HashMap<Option<String>, Vec<ServiceInfo>> = HashMap::new();

    for row in rows {
        let service_id = row.get::<_, String>("id");
        let entity_id = row.get::<_, String>("entity_id");
        let name: Option<String> = row.get("name");
        let email: Option<String> = row.get("email");
        let url: Option<String> = row.get("url");

        let embedding: Option<Vec<f32>> = row.get::<_, Option<pgvector::Vector>>("embedding_v2")
            .map(|v| v.to_vec());

        let service_info = ServiceInfo {
            service_id,
            entity_id: entity_id.clone(),
            name,
            email,
            url,
            embedding,
        };

        if let Some(cluster_id) = entity_to_cluster.get(&entity_id) {
            cluster_services
                .entry(cluster_id.clone())
                .or_insert_with(Vec::new)
                .push(service_info);
        }
    }

    let mut service_clusters = Vec::new();
    for (cluster_id, entities) in entity_clusters {
        if let Some(services) = cluster_services.remove(&cluster_id) {
            if !services.is_empty() {
                service_clusters.push(ServiceCluster {
                    cluster_id,
                    entities,
                    services,
                });
            }
        }
    }

    debug!("Created {} service clusters with services", service_clusters.len());
    
    // Log filtering results if filtering was applied
    if let Some(filter) = service_contributor_filter {
        if filter.enabled {
            debug!("Service contributor filtering was applied - {} clusters created from filtered services", service_clusters.len());
        }
    }
    
    Ok(service_clusters)
}