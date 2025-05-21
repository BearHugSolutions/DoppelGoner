// src/results/results.rs

use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use log::info;
use postgres_types::ToSql;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::db::PgPool;
// Ensure MatchMethodType is correctly imported if it's in models.rs
use crate::models::{EntityGroup, GroupCluster, MatchMethodType}; // Assuming these are needed

#[derive(Clone, Debug, Default)]
pub struct PairMlResult {
    pub features: Option<Vec<f64>>,
    pub predicted_method: Option<MatchMethodType>,
    pub prediction_confidence: Option<f64>,
}

/// Statistics for each matching method
#[derive(Debug, Clone)]
pub struct MatchMethodStats {
    pub method_type: MatchMethodType,
    /// Number of new pairwise links (entity_group records) created.
    pub groups_created: usize, // "groups" now means pairwise links
    /// Number of unique entities that participated in at least one new pairwise link.
    pub entities_matched: usize,
    /// Average of the RL-derived confidence_score from new pairwise entity_group records.
    pub avg_confidence: f64,
    /// This will always be 2.0 as entity_group records are now pairwise.
    pub avg_group_size: f64,
}

/// Cluster statistics
#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub total_clusters: usize,
    /// Average number of unique entities per consolidated cluster.
    pub avg_entities_per_cluster: f64,
    /// Average number of pairwise links (entity_group records) that constitute a consolidated cluster.
    pub avg_groups_per_cluster: f64, // "groups" here means pairwise links
    /// Size of the largest cluster in terms of unique entities.
    pub largest_cluster_size: usize, // In terms of entities
    /// Clusters formed by just one pairwise link.
    pub single_group_clusters: usize, // "group" means a single pairwise link
    /// Clusters formed by two or more pairwise links.
    pub multi_group_clusters: usize,
}

/// Service match statistics
#[derive(Debug, Clone)]
pub struct ServiceMatchStats {
    pub total_matches: usize,
    pub avg_similarity: f64,
    pub high_similarity_matches: usize,
    pub medium_similarity_matches: usize,
    pub low_similarity_matches: usize,
    pub clusters_with_matches: usize,
}

/// Complete pipeline run statistics
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub run_id: String,
    pub run_timestamp: NaiveDateTime,
    pub description: Option<String>,

    pub total_entities: usize,
    /// Total number of pairwise entity_group records in the system.
    pub total_groups: usize, // "groups" means pairwise links
    pub total_clusters: usize,
    pub total_service_matches: usize,
    pub total_service_clusters: usize,
    pub total_service_visualization_edges: usize,
    pub total_visualization_edges: usize, // New field for tracking visualization edges

    pub entity_processing_time: f64,
    pub context_feature_extraction_time: f64,
    pub service_context_feature_extraction_time: f64,
    pub matching_time: f64,
    pub clustering_time: f64,
    pub visualization_edge_calculation_time: f64, // New timing field
    pub service_matching_time: f64,
    pub total_processing_time: f64,
    pub service_clustering_time: f64,
    pub service_visualization_edge_calculation_time: f64,

    pub method_stats: Vec<MatchMethodStats>,
    pub cluster_stats: Option<ClusterStats>,
    pub service_stats: Option<ServiceMatchStats>,
}

#[derive(Debug, Clone)] // Ensure Clone is derived if you clone stats later
pub enum AnyMatchResult {
    Email(EmailMatchResult),
    Phone(PhoneMatchResult),
    Url(UrlMatchResult),
    Address(AddressMatchResult),
    Name(NameMatchResult),
    Geospatial(GeospatialMatchResult), // Using the specific type from your main.rs imports
}

impl AnyMatchResult {
    pub fn groups_created(&self) -> usize {
        match self {
            AnyMatchResult::Email(r) => r.groups_created,
            AnyMatchResult::Phone(r) => r.groups_created,
            AnyMatchResult::Url(r) => r.groups_created,
            AnyMatchResult::Address(r) => r.groups_created,
            AnyMatchResult::Name(r) => r.groups_created,
            AnyMatchResult::Geospatial(r) => r.groups_created,
        }
    }

    pub fn stats(&self) -> &MatchMethodStats {
        // Return a reference to avoid cloning MatchMethodStats prematurely
        match self {
            AnyMatchResult::Email(r) => &r.stats,
            AnyMatchResult::Phone(r) => &r.stats,
            AnyMatchResult::Url(r) => &r.stats,
            AnyMatchResult::Address(r) => &r.stats,
            AnyMatchResult::Name(r) => &r.stats,
            AnyMatchResult::Geospatial(r) => &r.stats,
        }
    }
}

// Result structs for individual matching processes
// Their `stats` field of type MatchMethodStats will now reflect the new meaning of its fields.
// `groups_created` will mean pairs created.
#[derive(Debug, Clone)]
pub struct ServiceMatchResult {
    pub groups_created: usize,
    pub stats: MatchMethodStats, // Primary stats (typically first method or combined)
    pub method_stats: Vec<MatchMethodStats>, // Stats for all methods used
}
#[derive(Debug, Clone)]
pub struct EmailMatchResult {
    pub groups_created: usize, // Number of new pairwise entity_group links
    pub stats: MatchMethodStats,
}
#[derive(Debug, Clone)]
pub struct PhoneMatchResult {
    pub groups_created: usize,
    pub stats: MatchMethodStats,
}
#[derive(Debug, Clone)]
pub struct UrlMatchResult {
    pub groups_created: usize,
    pub stats: MatchMethodStats,
}
#[derive(Debug, Clone)]
pub struct AddressMatchResult {
    pub groups_created: usize,
    pub stats: MatchMethodStats,
}
#[derive(Debug, Clone)]
pub struct GeospatialMatchResult {
    pub groups_created: usize,
    pub stats: MatchMethodStats,
}
#[derive(Debug, Clone)]
pub struct NameMatchResult {
    pub groups_created: usize,
    pub stats: MatchMethodStats,
}

/// Collect matching method statistics from the database
pub async fn collect_method_stats(
    pool: &PgPool,
    run_id_filter: Option<&str>,
) -> Result<Vec<MatchMethodStats>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for method stats")?;

    // The refactoring plan mentions `group_method` table, but it's not in the DDL.
    // Assuming `method_type` and `confidence_score` are directly on `entity_group`.
    // The DDL shows `entity_group` has `method_type` and `confidence_score`.
    // If `group_method` is indeed used and links to `entity_group` by `entity_group_id`,
    // the query would need to join it. For now, querying `entity_group` directly.
    // Added a placeholder for run_id_filter if stats are per run.

    // Base query
    let mut query = String::from(
        "
        WITH PairwiseGroupEntities AS (
            SELECT 
                eg.id AS entity_group_id,
                eg.method_type,
                eg.confidence_score,
                eg.entity_id_1 AS entity_id
            FROM public.entity_group eg
            WHERE eg.confidence_score IS NOT NULL -- Consider only groups with confidence
            -- AND eg.pipeline_run_id = $1 -- If filtering by run_id stored on entity_group
            UNION ALL
            SELECT 
                eg.id AS entity_group_id,
                eg.method_type,
                eg.confidence_score,
                eg.entity_id_2 AS entity_id
            FROM public.entity_group eg
            WHERE eg.confidence_score IS NOT NULL
            -- AND eg.pipeline_run_id = $1 -- If filtering by run_id
        )
        SELECT 
            COALESCE(pg.method_type, 'UNKNOWN') as method_type,
            COUNT(DISTINCT pg.entity_group_id)::bigint as groups_created,
            COUNT(DISTINCT pg.entity_id)::bigint as entities_matched,
            AVG(COALESCE(pg.confidence_score, 0))::double precision as avg_confidence,
            2.0::double precision as avg_group_size -- Always 2 for pairwise
        FROM 
            PairwiseGroupEntities pg
    ",
    );

    let params: Vec<&(dyn ToSql + Sync)> = Vec::new();

    // Example: If stats are tied to a specific pipeline_run_id that might be on entity_group
    // (This depends on whether entity_group tracks its creation run_id)
    // For now, this filter is conceptual. If `entity_group` doesn't have `pipeline_run_id`,
    // this filtering needs to happen based on `group_method` if that table links methods to runs.
    // if let Some(run_id) = run_id_filter {
    //     query.push_str(" WHERE eg.pipeline_run_id = $1"); // This assumes entity_group has pipeline_run_id
    //     params.push(run_id);
    // }
    // query.push_str(" GROUP BY pg.method_type");

    // If `group_method` table IS used as originally planned by the refactoring doc:
    // Assume group_method has (id, entity_group_id, method_type, confidence_score, pipeline_run_id etc.)
    // And entity_group has (id, entity_id_1, entity_id_2)
    // The query would be closer to:
    /*
    let query_with_group_method = "
        WITH RunGroupMethods AS (
            SELECT entity_group_id, method_type, confidence_score
            FROM public.group_method -- Assuming this table exists
            WHERE pipeline_run_id = $1 -- Filter by run
        ),
        PairwiseGroupEntities AS (
            SELECT
                eg.id AS entity_group_id,
                rgm.method_type,
                rgm.confidence_score,
                eg.entity_id_1 AS entity_id
            FROM public.entity_group eg
            JOIN RunGroupMethods rgm ON eg.id = rgm.entity_group_id
            UNION ALL
            SELECT
                eg.id AS entity_group_id,
                rgm.method_type,
                rgm.confidence_score,
                eg.entity_id_2 AS entity_id
            FROM public.entity_group eg
            JOIN RunGroupMethods rgm ON eg.id = rgm.entity_group_id
        )
        SELECT
            COALESCE(pge.method_type, 'UNKNOWN') as method_type,
            COUNT(DISTINCT pge.entity_group_id)::bigint as groups_created,
            COUNT(DISTINCT pge.entity_id)::bigint as entities_matched,
            AVG(COALESCE(pge.confidence_score, 0))::double precision as avg_confidence,
            2.0::double precision as avg_group_size
        FROM PairwiseGroupEntities pge
        GROUP BY pge.method_type
    ";
    // if run_id_filter.is_some() {
    //    params.push(run_id_filter.unwrap());
    //    rows = conn.query(query_with_group_method, &params).await.context("Failed to query method stats with group_method")?;
    // } else {
    //    // Fallback or error if run_id is required for group_method based stats
    //    return Err(anyhow::anyhow!("run_id_filter is required for group_method based stats"));
    // }
    */

    // Using the simpler query based on entity_group directly for now
    // This query aggregates over ALL entity_groups. If stats per run are needed,
    // and entity_group doesn't have pipeline_run_id, this needs rethinking based on how
    // methods/runs are tracked. The current `main.rs` collects method_stats *during* the run.
    // This `collect_method_stats` seems to be for a post-run report from DB state.

    query.push_str(" GROUP BY pg.method_type");

    let rows = conn
        .query(&query, &params[..])
        .await
        .context("Failed to query method statistics")?;

    let mut stats = Vec::with_capacity(rows.len());
    for row in rows {
        let method_type_str: String = row.get(0);
        stats.push(MatchMethodStats {
            method_type: MatchMethodType::from_str(&method_type_str),
            groups_created: row.get::<_, i64>(1) as usize,
            entities_matched: row.get::<_, i64>(2) as usize,
            avg_confidence: row.get(3),
            avg_group_size: row.get(4), // Should be 2.0
        });
    }
    Ok(stats)
}

/// Collect cluster statistics from the database
pub async fn collect_cluster_stats(pool: &PgPool) -> Result<Option<ClusterStats>> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for cluster stats")?;

    // The group_cluster table (as per DDL and models.rs) should now have accurate
    // entity_count (unique entities in the cluster) and
    // group_count (number of pairwise links in the cluster).
    // These are populated by the refactored consolidate_clusters.rs.

    let count_query = "SELECT COUNT(*)::bigint FROM public.group_cluster";
    let count_row = conn
        .query_one(count_query, &[])
        .await
        .context("Failed to count clusters")?;
    if count_row.get::<_, i64>(0) == 0 {
        return Ok(None);
    }

    let query = "
        SELECT 
            COUNT(id)::bigint as total_clusters,
            COALESCE(AVG(entity_count), 0.0)::double precision as avg_entities_per_cluster,
            COALESCE(AVG(group_count), 0.0)::double precision as avg_groups_per_cluster, -- group_count is now number of pairs
            COALESCE(MAX(entity_count), 0)::bigint as largest_cluster_size, -- in terms of entities
            SUM(CASE WHEN group_count = 1 THEN 1 ELSE 0 END)::bigint as single_group_clusters, -- cluster with one pair
            SUM(CASE WHEN group_count > 1 THEN 1 ELSE 0 END)::bigint as multi_group_clusters
        FROM 
            public.group_cluster
    ";
    // Note: The DDL for group_cluster has entity_count and group_count.
    // Ensure these are correctly populated by consolidate_clusters.rs.
    // entity_count = total unique entities in the cluster.
    // group_count = total pairwise entity_group records in the cluster.

    let row = conn
        .query_one(query, &[])
        .await
        .context("Failed to query cluster statistics")?;

    Ok(Some(ClusterStats {
        total_clusters: row.get::<_, i64>(0) as usize,
        avg_entities_per_cluster: row.get(1),
        avg_groups_per_cluster: row.get(2),
        largest_cluster_size: row.get::<_, i64>(3) as usize,
        single_group_clusters: row.get::<_, i64>(4) as usize,
        multi_group_clusters: row.get::<_, i64>(5) as usize,
    }))
}

/// Collect service match statistics from the database
pub async fn collect_service_stats(pool: &PgPool) -> Result<Option<ServiceMatchStats>> {
    let conn = pool.get().await.context("Failed to get DB connection")?;

    let count_query = "SELECT COUNT(*)::bigint FROM public.service_match";
    let count_row = conn
        .query_one(count_query, &[])
        .await
        .context("Failed to count service matches")?;
    if count_row.get::<_, i64>(0) == 0 {
        return Ok(None);
    }

    let query = "
        SELECT 
            COUNT(*)::bigint as total_matches,
            COALESCE(AVG(similarity_score), 0.0)::double precision as avg_similarity,
            SUM(CASE WHEN similarity_score > 0.8 THEN 1 ELSE 0 END)::bigint as high_similarity,
            SUM(CASE WHEN similarity_score BETWEEN 0.6 AND 0.8 THEN 1 ELSE 0 END)::bigint as medium_similarity,
            SUM(CASE WHEN similarity_score < 0.6 THEN 1 ELSE 0 END)::bigint as low_similarity,
            COUNT(DISTINCT group_cluster_id)::bigint as clusters_with_matches
        FROM 
            public.service_match
    ";

    let row = conn
        .query_one(query, &[])
        .await
        .context("Failed to query service match statistics")?;

    Ok(Some(ServiceMatchStats {
        total_matches: row.get::<_, i64>(0) as usize,
        avg_similarity: row.get(1),
        high_similarity_matches: row.get::<_, i64>(2) as usize,
        medium_similarity_matches: row.get::<_, i64>(3) as usize,
        low_similarity_matches: row.get::<_, i64>(4) as usize,
        clusters_with_matches: row.get::<_, i64>(5) as usize,
    }))
}

async fn store_pipeline_stats(pool: &PgPool, stats: &PipelineStats) -> Result<()> {
    let mut conn = pool
        .get()
        .await
        .context("Failed to get DB connection for storing stats")?;
    let transaction = conn
        .transaction()
        .await
        .context("Failed to start transaction for storing stats")?;

    // Main pipeline run record
    // Add new fields for visualization edges
    let insert_run = "
        INSERT INTO clustering_metadata.pipeline_run (
            id, run_timestamp, description, 
            total_entities, total_groups, total_clusters, total_service_matches, total_visualization_edges,
            entity_processing_time, context_feature_extraction_time, matching_time, 
            clustering_time, visualization_edge_calculation_time, service_matching_time, total_processing_time
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
    ";
    transaction
        .execute(
            insert_run,
            &[
                &stats.run_id,
                &stats.run_timestamp,
                &stats.description,
                &(stats.total_entities as i64),
                &(stats.total_groups as i64),
                &(stats.total_clusters as i64),
                &(stats.total_service_matches as i64),
                &(stats.total_visualization_edges as i64), // New parameter
                &stats.entity_processing_time,
                &stats.context_feature_extraction_time,
                &stats.matching_time,
                &stats.clustering_time,
                &stats.visualization_edge_calculation_time, // New parameter
                &stats.service_matching_time,
                &stats.total_processing_time,
            ],
        )
        .await
        .context("Failed to insert pipeline run")?;

    for method_stat in &stats.method_stats {
        let id = Uuid::new_v4().to_string();
        transaction.execute(
            "INSERT INTO clustering_metadata.matching_method_stats 
             (id, pipeline_run_id, method_type, groups_created, entities_matched, avg_confidence, avg_group_size)
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
            &[
                &id, &stats.run_id, &method_stat.method_type.as_str(),
                &(method_stat.groups_created as i64), &(method_stat.entities_matched as i64),
                &method_stat.avg_confidence, &method_stat.avg_group_size, // Will be 2.0
            ],
        ).await.context("Failed to insert method stats")?;
    }

    if let Some(cs) = &stats.cluster_stats {
        let id = Uuid::new_v4().to_string();
        transaction.execute(
            "INSERT INTO clustering_metadata.cluster_stats
             (id, pipeline_run_id, total_clusters, avg_entities_per_cluster, avg_groups_per_cluster, 
             largest_cluster_size, single_group_clusters, multi_group_clusters)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &id, &stats.run_id, &(cs.total_clusters as i64), &cs.avg_entities_per_cluster,
                &cs.avg_groups_per_cluster, &(cs.largest_cluster_size as i64),
                &(cs.single_group_clusters as i64), &(cs.multi_group_clusters as i64),
            ],
        ).await.context("Failed to insert cluster stats")?;
    }

    if let Some(ss) = &stats.service_stats {
        let id = Uuid::new_v4().to_string();
        transaction
            .execute(
                "INSERT INTO clustering_metadata.service_match_stats
             (id, pipeline_run_id, total_matches, avg_similarity, high_similarity_matches,
             medium_similarity_matches, low_similarity_matches, clusters_with_matches)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &id,
                    &stats.run_id,
                    &(ss.total_matches as i64),
                    &ss.avg_similarity,
                    &(ss.high_similarity_matches as i64),
                    &(ss.medium_similarity_matches as i64),
                    &(ss.low_similarity_matches as i64),
                    &(ss.clusters_with_matches as i64),
                ],
            )
            .await
            .context("Failed to insert service match stats")?;
    }

    transaction
        .commit()
        .await
        .context("Failed to commit stats transaction")?;
    info!(
        "Pipeline report stored in clustering_metadata with run_id: {}",
        stats.run_id
    );
    Ok(())
}

fn print_report(stats: &PipelineStats) {
    println!("\n========== HSDS ENTITY GROUPING PIPELINE REPORT ==========");
    println!("Run ID: {}", stats.run_id);
    println!("Timestamp: {}", stats.run_timestamp);
    if let Some(desc) = &stats.description {
        println!("Description: {}", desc);
    }

    println!("\n--- GENERAL STATISTICS ---");
    println!("Total entities processed: {}", stats.total_entities);
    println!(
        "Total pairwise links (entity_group records): {}",
        stats.total_groups
    );
    println!(
        "Total consolidated clusters formed: {}",
        stats.total_clusters
    );
    println!(
        "Total visualization edges calculated: {}",
        stats.total_visualization_edges
    );
    println!(
        "Total service matches identified: {}",
        stats.total_service_matches
    );

    println!("\n--- TIMING INFORMATION ---");
    println!(
        "Entity processing time: {:.2} seconds",
        stats.entity_processing_time
    );
    println!(
        "Context Feature Extraction time: {:.2} seconds",
        stats.context_feature_extraction_time
    );
    println!(
        "Entity matching time (pairwise links): {:.2} seconds",
        stats.matching_time
    );
    println!(
        "Cluster consolidation time: {:.2} seconds",
        stats.clustering_time
    );
    println!(
        "Visualization edge calculation time: {:.2} seconds", // New line
        stats.visualization_edge_calculation_time
    );
    println!(
        "Service matching time: {:.2} seconds",
        stats.service_matching_time
    );
    println!(
        "Total processing time: {:.2} seconds",
        stats.total_processing_time
    );

    println!("\n--- MATCHING METHOD STATISTICS (Pairwise Links) ---");
    for method in &stats.method_stats {
        println!("\nMethod: {}", method.method_type.as_str());
        println!("  Pairwise Links created: {}", method.groups_created);
        println!(
            "  Unique Entities in new Links: {}",
            method.entities_matched
        );
        println!(
            "  Average Confidence of Links: {:.2}",
            method.avg_confidence
        );
        println!(
            "  Average Link Size (always 2.0): {:.2}",
            method.avg_group_size
        );
    }

    if let Some(cluster_stats) = &stats.cluster_stats {
        println!("\n--- CLUSTERING STATISTICS ---");
        println!(
            "Total consolidated clusters: {}",
            cluster_stats.total_clusters
        );
        println!(
            "  Avg entities per consolidated cluster: {:.2}",
            cluster_stats.avg_entities_per_cluster
        );
        println!(
            "  Avg pairwise links per consolidated cluster: {:.2}",
            cluster_stats.avg_groups_per_cluster
        ); // Clarified
        println!(
            "  Largest consolidated cluster size (entities): {}",
            cluster_stats.largest_cluster_size
        );
        println!(
            "  Consolidated clusters from a single pairwise link: {}",
            cluster_stats.single_group_clusters
        );
        println!(
            "  Consolidated clusters from multiple pairwise links: {}",
            cluster_stats.multi_group_clusters
        );
    }

    if let Some(service_stats) = &stats.service_stats {
        println!("\n--- SERVICE MATCHING STATISTICS ---");
        // This section remains largely the same
        println!("Total service matches: {}", service_stats.total_matches);
        println!(
            "Average similarity score: {:.2}",
            service_stats.avg_similarity
        );
        // ... other service stats
    }
    println!("\n==========================================================\n");
}

pub async fn generate_report(
    pool: &PgPool,
    mut run_stats: PipelineStats, // Make mutable to update fields before storing
    phase_times: &HashMap<String, Duration>,
    description: Option<String>,
) -> Result<()> {
    // The `run_stats` passed in from main.rs already contains totals from the run.
    // Here we fetch aggregate stats from the DB *for the entire DB state* or a *specific run_id*
    // which might be different if `collect_*_stats` are not filtered by the current run_id.
    // For this refactor, let's assume `collect_method_stats` can be filtered if needed,
    // or `main.rs` provides the definitive `method_stats` for the current run.

    // If method_stats are already populated by main.rs, we might not need to call collect_method_stats here again,
    // unless this report is meant to be a global DB state report.
    // The original `main.rs` extended `stats.method_stats` during the run and also added service_match_stats.
    // Let's assume `run_stats` from `main.rs` already has the correct `method_stats` for *this specific run*.
    // We still need to collect overall cluster and service stats from the DB.

    let (cluster_stats_res, service_stats_res) =
        tokio::join!(collect_cluster_stats(pool), collect_service_stats(pool));

    // Update the run_stats with freshly collected cluster and service stats
    run_stats.cluster_stats = cluster_stats_res?;
    run_stats.service_stats = service_stats_res?;

    // Update timing from phase_times (already done in main.rs before calling, but ensure consistency)
    run_stats.entity_processing_time = phase_times
        .get("entity_identification")
        .map_or(0.0, |d| d.as_secs_f64());
    run_stats.context_feature_extraction_time = phase_times
        .get("context_feature_extraction")
        .map_or(0.0, |d| d.as_secs_f64());
    run_stats.matching_time = phase_times
        .get("entity_matching")
        .map_or(0.0, |d| d.as_secs_f64());
    run_stats.clustering_time = phase_times
        .get("cluster_consolidation")
        .map_or(0.0, |d| d.as_secs_f64());
    run_stats.service_matching_time = phase_times
        .get("service_matching")
        .map_or(0.0, |d| d.as_secs_f64());
    run_stats.total_processing_time = run_stats.entity_processing_time
        + run_stats.context_feature_extraction_time
        + run_stats.matching_time
        + run_stats.clustering_time
        + run_stats.service_matching_time;

    run_stats.description = description.or(run_stats.description); // Prioritize passed-in description

    // If run_stats didn't have a run_id from main (it should), generate one.
    if run_stats.run_id.is_empty() {
        run_stats.run_id = Uuid::new_v4().to_string();
    }
    // Ensure timestamp is set
    run_stats.run_timestamp = Utc::now().naive_utc();

    // Store statistics in database
    store_pipeline_stats(pool, &run_stats).await?;

    // Print report
    print_report(&run_stats);

    info!(
        "Pipeline report generated and stored with run_id: {}",
        run_stats.run_id
    );

    Ok(())
}
