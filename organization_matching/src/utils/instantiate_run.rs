use crate::{models::stats_models::PipelineStats, utils::db_connect::PgPool};
use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use log::info;

pub async fn create_initial_pipeline_run(
    pool: &PgPool,
    run_id: &str,
    run_timestamp: NaiveDateTime,
    description: Option<&str>,
) -> Result<PipelineStats> {
    let conn = pool
        .get()
        .await
        .context("Failed to get DB connection for create_initial_pipeline_run")?;

    const INSERT_SQL: &str = "
        INSERT INTO clustering_metadata.pipeline_run (
            id, run_timestamp, description,
            total_entities, total_groups, total_clusters, total_service_matches, total_visualization_edges,
            entity_processing_time, context_feature_extraction_time, matching_time,
            clustering_time, visualization_edge_calculation_time, service_matching_time, total_processing_time
        )
        VALUES ($1, $2, $3, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    ";

    conn.execute(INSERT_SQL, &[&run_id, &run_timestamp, &description])
        .await
        .context("Failed to insert initial pipeline_run record")?;

    info!("Created initial pipeline_run record with ID: {}", run_id);

    let stats = PipelineStats {
        run_id: run_id.to_string(),
        run_timestamp: run_timestamp.clone(),
        description: description.map(|s| s.to_string()),
        total_entities: 0,
        total_entity_features: 0,
        total_groups: 0,
        total_clusters: 0,
        total_service_matches: 0,
        total_visualization_edges: 0,
        total_service_clusters: 0,
        total_service_visualization_edges: 0,
        entity_processing_time: 0.0,
        context_feature_extraction_time: 0.0,
        service_context_feature_extraction_time: 0.0,
        matching_time: 0.0,
        clustering_time: 0.0,
        visualization_edge_calculation_time: 0.0,
        service_matching_time: 0.0,
        total_processing_time: 0.0,
        service_clustering_time: 0.0,
        service_visualization_edge_calculation_time: 0.0,
        method_stats: Vec::new(),
        cluster_stats: None,
        service_stats: None,
    };

    Ok(stats)
}
