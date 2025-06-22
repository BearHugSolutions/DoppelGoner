// src/bin/backfill_features.rs

use anyhow::{Context, Result};
use dedupe_lib::{
    rl::feature_extraction::extract_context_for_pair,
    utils::{
        db_connect::{connect, PgPool},
        env::load_env,
    },
};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use serde_json::json;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Represents a decision record that needs its features backfilled.
#[derive(Debug, Clone)]
struct DecisionToBackfill {
    decision_id: String,
    entity_group_id: String,
}

/// Fetches decision records from the database that have empty snapshotted features.
async fn fetch_decisions_to_backfill(pool: &PgPool) -> Result<Vec<DecisionToBackfill>> {
    info!("Fetching match decision records with empty features...");
    let client = pool.get().await.context("Failed to get DB connection")?;

    let rows = client
        .query(
            r#"
            SELECT id, entity_group_id
            FROM clustering_metadata.match_decision_details
            WHERE snapshotted_features = '{}'::jsonb
            ORDER BY created_at ASC
            "#,
            &[],
        )
        .await
        .context("Failed to query for decision details to backfill")?;

    let decisions: Vec<DecisionToBackfill> = rows
        .into_iter()
        .map(|row| DecisionToBackfill {
            decision_id: row.get("id"),
            entity_group_id: row.get("entity_group_id"),
        })
        .collect();

    info!("Found {} decision records to backfill.", decisions.len());
    Ok(decisions)
}

/// For a given entity_group_id, fetches the two entity IDs involved.
async fn fetch_entity_ids_for_group(
    pool: &PgPool,
    entity_group_id: &str,
) -> Result<(String, String)> {
    let client = pool.get().await.context("Failed to get DB connection")?;

    let row = client
        .query_one(
            "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE id = $1",
            &[&entity_group_id],
        )
        .await
        .context(format!(
            "Failed to fetch entity IDs for group {}",
            entity_group_id
        ))?;

    let entity1_id: String = row.get("entity_id_1");
    let entity2_id: String = row.get("entity_id_2");

    Ok((entity1_id, entity2_id))
}

/// Updates a specific decision record with the newly calculated features.
async fn update_decision_features(
    pool: &PgPool,
    decision_id: &str,
    features: Vec<f64>,
) -> Result<()> {
    let client = pool.get().await.context("Failed to get DB connection")?;
    let features_json = json!(features);

    let rows_affected = client
        .execute(
            "UPDATE clustering_metadata.match_decision_details SET snapshotted_features = $1 WHERE id = $2",
            &[&features_json, &decision_id],
        )
        .await
        .context(format!("Failed to update features for decision {}", decision_id))?;

    if rows_affected == 1 {
        debug!("Successfully updated features for decision {}", decision_id);
    } else {
        warn!(
            "Expected 1 row to be affected for decision {}, but {} were. Possible issue.",
            decision_id, rows_affected
        );
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    load_env();

    let args: Vec<String> = env::args().collect();
    let dry_run = args.contains(&"--dry-run".to_string());

    if dry_run {
        info!("--- DRY RUN MODE ---");
        info!("No changes will be written to the database.");
    }

    let pool = connect().await.context("Failed to create database pool")?;

    let decisions = fetch_decisions_to_backfill(&pool).await?;
    let total_items = decisions.len();

    if total_items == 0 {
        info!("No decision records need backfilling. Exiting.");
        return Ok(());
    }

    let pb = ProgressBar::new(total_items as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("#>-"));
    pb.set_message("Backfilling features...");

    let num_cpus = num_cpus::get();
    let backfilled_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    stream::iter(decisions)
        .map(|decision| {
            let pool = pool.clone();
            let pb = pb.clone();
            let backfilled_count = backfilled_count.clone();
            let error_count = error_count.clone();

            tokio::spawn(async move {
                let process_result = async {
                    // 1. Fetch entity IDs for the group
                    let (entity1_id, entity2_id) =
                        fetch_entity_ids_for_group(&pool, &decision.entity_group_id).await?;

                    // 2. Extract the feature vector
                    let features =
                        extract_context_for_pair(&pool, &entity1_id, &entity2_id).await?;

                    debug!(
                        "Successfully extracted {} features for decision {}",
                        features.len(),
                        decision.decision_id
                    );

                    // 3. Update the database record (if not a dry run)
                    if !dry_run {
                        update_decision_features(&pool, &decision.decision_id, features).await?;
                    }

                    Ok::<(), anyhow::Error>(())
                }
                .await;

                if let Err(e) = process_result {
                    warn!("Failed to process decision {}: {}", decision.decision_id, e);
                    error_count.fetch_add(1, Ordering::SeqCst);
                } else {
                    backfilled_count.fetch_add(1, Ordering::SeqCst);
                }
                pb.inc(1);
            })
        })
        .buffer_unordered(num_cpus)
        .collect::<Vec<_>>()
        .await;

    pb.finish_with_message("Backfill process complete.");

    let final_backfilled = backfilled_count.load(Ordering::SeqCst);
    let final_errors = error_count.load(Ordering::SeqCst);

    info!("\n--- BACKFILL SUMMARY ---");
    info!("Successfully processed (backfilled): {}", final_backfilled);
    info!("Failed to process: {}", final_errors);
    if dry_run {
        info!("NOTE: This was a dry run. No data was actually changed.");
    }
    info!("----------------------");

    Ok(())
}
