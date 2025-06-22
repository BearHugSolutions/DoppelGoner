// src/bin/url_distance_tuner.rs
//
// This binary provides an interactive tool to assist in tuning the
// MAX_LOCATION_DISTANCE_METERS value within the `url.rs` matching strategy.
//
// It connects to the PostgreSQL database, samples entity pairs that share
// the same domain, calculates their geospatial distance and URL slug matches,
// and prompts the user to confirm or deny matches.
// Based on user feedback, it suggests an optimal geospatial distance threshold.

use anyhow::{Context, Result};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::Arc;

use dedupe_lib::utils::db_connect::{self, PgPool};
use dedupe_lib::matching::url::{normalize_url_with_slugs, is_ignored_domain, DomainType, categorize_domain, get_max_entities_threshold};
use dedupe_lib::models::core::Entity; // Import Entity model
use dedupe_lib::models::stats_models::MatchMethodType; // Import MatchMethodType

// Constants for sampling and thresholds
const MIN_DISTANCE_TIER: f64 = 0.0; // Meters
const MAX_DISTANCE_TIER: f64 = 5000.0; // Meters (e.g., 5km)
const TIER_INCREMENT: f64 = 500.0; // Meters per tier
const SAMPLES_PER_TIER: usize = 5; // Number of pairs to sample for review per tier
const MAX_CONCURRENT_DB_FETCHES: usize = 15; // Limit concurrent DB ops for fetching embeddings/data

/// Represents a pair of entities with their URL and location information,
/// and the calculated distance/slug match for user review.
#[derive(Debug, Clone)]
struct ReviewedPair {
    entity_id_1: String,
    entity_id_2: String,
    url1: String,
    url2: String,
    entity_name1: String,
    entity_name2: String,
    org_description1: String,
    org_description2: String,
    domain: String,
    geospatial_distance_meters: Option<f64>,
    matching_slugs_count: usize,
    is_match: bool, // True if user confirmed it's a match
}

/// Helper struct to hold entity URL and location information
#[derive(Clone)]
struct EntityUrlLocationInfo {
    entity_id: String,
    organization_id: String,
    url: String,
    entity_name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    normalized_domain: String,
    path_slugs: Vec<String>,
}

/// Main entry point for the binary
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Load environment variables and initialize logging
    dedupe_lib::utils::env::load_env();
    env_logger::init();

    info!("Starting URL Distance Tuner...");

    // 2. Connect to the database
    let pool = db_connect::connect()
        .await
        .context("Failed to connect to PostgreSQL")?;
    info!("Successfully connected to the database.");

    // 3. Fetch all entities with URLs, names, organization descriptions, and locations
    info!("Loading entities with URLs, names, descriptions, and locations...");
    let all_entity_url_location_info = fetch_entities_for_tuner(&pool).await?;
    info!(
        "Found {} entities with URLs and associated location/organization data.",
        all_entity_url_location_info.len()
    );

    if all_entity_url_location_info.len() < 2 {
        eprintln!("Not enough entities (need at least 2) with URL and location data to perform matching. Exiting.");
        return Ok(());
    }

    // 4. Group entities by normalized domain
    info!("Grouping entities by normalized domain...");
    let domain_to_entities: HashMap<String, Vec<EntityUrlLocationInfo>> =
        all_entity_url_location_info
            .into_iter()
            .filter_map(|info| {
                // Ensure URLs are valid and not ignored
                if let Some(normalized_data) = normalize_url_with_slugs(&info.url) {
                    if !is_ignored_domain(&normalized_data.domain) {
                        Some((
                            normalized_data.domain.clone(),
                            EntityUrlLocationInfo {
                                normalized_domain: normalized_data.domain,
                                path_slugs: normalized_data.path_slugs,
                                ..info
                            },
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .fold(HashMap::new(), |mut map, (domain, entity_info)| {
                map.entry(domain).or_default().push(entity_info);
                map
            });

    info!(
        "Found {} unique domains with at least one entity.",
        domain_to_entities.len()
    );

    // 5. Generate candidate pairs from domains for sampling
    info!("Generating candidate pairs for similarity calculation...");
    let mut candidate_pairs_with_details: Vec<(EntityUrlLocationInfo, EntityUrlLocationInfo)> = Vec::new();
    for (domain, entities_in_domain) in domain_to_entities {
        if entities_in_domain.len() < 2 {
            continue; // Need at least two entities to form a pair
        }

        // Apply business logic filtering similar to url.rs
        let domain_type = categorize_domain(&domain);
        let threshold = get_max_entities_threshold(&domain_type);
        if entities_in_domain.len() > threshold {
            debug!(
                "Tuner: Skipping domain '{}' ({:?}) due to too many entities: {} (threshold: {})",
                domain,
                domain_type,
                entities_in_domain.len(),
                threshold
            );
            continue;
        }

        for i in 0..entities_in_domain.len() {
            for j in (i + 1)..entities_in_domain.len() {
                let e1 = entities_in_domain[i].clone();
                let e2 = entities_in_domain[j].clone();
                // Only consider pairs that have at least one location coordinate to calculate distance
                if e1.latitude.is_some() || e2.latitude.is_some() {
                    candidate_pairs_with_details.push((e1, e2));
                }
            }
        }
    }
    info!(
        "Generated {} candidate pairs with location data.",
        candidate_pairs_with_details.len()
    );

    if candidate_pairs_with_details.is_empty() {
        eprintln!("No candidate pairs with location data generated. Exiting.");
        return Ok(());
    }

    // 6. Calculate distances/slug counts for candidate pairs and bin them
    info!("Calculating distances and slug counts, then binning pairs...");
    let mut distance_bins: HashMap<String, Vec<ReviewedPair>> = HashMap::new(); // Key: "distance_X.X_slugs_Y"
    let total_candidates_to_score = candidate_pairs_with_details.len();

    let scoring_pb = ProgressBar::new(total_candidates_to_score as u64);
    scoring_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    scoring_pb.set_message("Calculating scores and binning...");

    for (i, (e1_info, e2_info)) in candidate_pairs_with_details.into_iter().enumerate() {
        scoring_pb.inc(1);
        if i % 1000 == 0 {
            scoring_pb.set_message(format!(
                "Calculating scores and binning... ({}/{})",
                i, total_candidates_to_score
            ));
        }

        let distance = if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) = (
            e1_info.latitude,
            e1_info.longitude,
            e2_info.latitude,
            e2_info.longitude,
        ) {
            Some(calculate_distance(lat1, lon1, lat2, lon2))
        } else {
            None
        };

        let matching_slugs = count_matching_slugs(&e1_info.path_slugs, &e2_info.path_slugs);

        let reviewed_pair = ReviewedPair {
            entity_id_1: e1_info.entity_id.clone(),
            entity_id_2: e2_info.entity_id.clone(),
            url1: e1_info.url.clone(),
            url2: e2_info.url.clone(),
            entity_name1: e1_info.entity_name.unwrap_or_else(|| "[No Name]".to_string()),
            entity_name2: e2_info.entity_name.unwrap_or_else(|| "[No Name]".to_string()),
            org_description1: get_organization_description(&pool, &e1_info.organization_id).await?,
            org_description2: get_organization_description(&pool, &e2_info.organization_id).await?,
            domain: e1_info.normalized_domain.clone(),
            geospatial_distance_meters: distance,
            matching_slugs_count: matching_slugs,
            is_match: false, // Default, will be set by user
        };

        // Binning by distance and slug count
        let dist_tier = if let Some(dist) = distance {
            ((dist / TIER_INCREMENT).floor() * TIER_INCREMENT).max(MIN_DISTANCE_TIER)
        } else {
            -1.0 // Special bin for no location data
        };

        let bin_key = format!("dist_{:.0}_slugs_{}", dist_tier, matching_slugs);
        distance_bins.entry(bin_key).or_default().push(reviewed_pair);
    }
    scoring_pb.finish_with_message("Distance calculation and binning complete.");

    // 7. Sample from bins and conduct interactive review
    let mut sampled_pairs_for_review: Vec<ReviewedPair> = Vec::new();
    let mut rng = thread_rng();

    info!(
        "Sampling pairs for interactive review ({} samples per tier)...",
        SAMPLES_PER_TIER
    );
    for tier in (MIN_DISTANCE_TIER as u32)..=((MAX_DISTANCE_TIER / TIER_INCREMENT) as u32) {
        let dist_f = (tier as f64) * TIER_INCREMENT;
        for slugs in 0..=3 { // Sample for 0, 1, 2, 3+ matching slugs
            let bin_key = format!("dist_{:.0}_slugs_{}", dist_f, slugs);
            if let Some(pairs) = distance_bins.get_mut(&bin_key) {
                pairs.shuffle(&mut rng);
                sampled_pairs_for_review.extend(pairs.iter().take(SAMPLES_PER_TIER).cloned());
            }
        }
    }
    // Also sample from the "no location" bin if it exists
    if let Some(pairs) = distance_bins.get_mut("dist_-1_slugs_0") { // Default slug 0 for no location
        pairs.shuffle(&mut rng);
        sampled_pairs_for_review.extend(pairs.iter().take(SAMPLES_PER_TIER).cloned());
    }
    sampled_pairs_for_review.shuffle(&mut rng); // Shuffle for random presentation order

    info!(
        "Total unique pairs to review: {}",
        sampled_pairs_for_review.len()
    );

    let mut reviewed_pairs: Vec<ReviewedPair> = Vec::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    let review_pb = ProgressBar::new(sampled_pairs_for_review.len() as u64);
    review_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.yellow} [{elapsed_precise}] {bar:40.magenta/cyan} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    review_pb.set_message("Starting interactive review...");

    for (i, mut pair) in sampled_pairs_for_review.clone().into_iter().enumerate() {
        review_pb.inc(1);
        review_pb.set_message(format!(
            "Reviewing pair {}/{}",
            i + 1,
            reviewed_pairs.len() + 1
        )); // reviewed_pairs.len() + 1 accounts for current pair

        println!("\n--- Pair {}/{} ---", i + 1, sampled_pairs_for_review.len());
        println!("Domain: {}", pair.domain);
        println!("Geospatial Distance: {} meters", pair.geospatial_distance_meters.map_or("N/A".to_string(), |d| format!("{:.2}", d)));
        println!("Matching Slugs: {}", pair.matching_slugs_count);

        println!("\nEntity 1 (ID: {})", pair.entity_id_1);
        println!("  Name: {}", pair.entity_name1);
        println!("  URL: {}", pair.url1);
        println!("  Org Description: {}", pair.org_description1.trim().lines().next().unwrap_or("")); // Show first line
        if pair.org_description1.trim().lines().count() > 1 {
            println!("    (Description truncated...)");
        }

        println!("\nEntity 2 (ID: {})", pair.entity_id_2);
        println!("  Name: {}", pair.entity_name2);
        println!("  URL: {}", pair.url2);
        println!("  Org Description: {}", pair.org_description2.trim().lines().next().unwrap_or("")); // Show first line
        if pair.org_description2.trim().lines().count() > 1 {
            println!("    (Description truncated...)");
        }

        let mut input = String::new();
        loop {
            print!("Do these URLs/organizations represent the same thing? (y/n/q to quit): ");
            stdout.flush()?;
            input.clear();
            stdin.read_line(&mut input)?;
            let choice = input.trim().to_lowercase();

            if choice == "y" {
                pair.is_match = true;
                reviewed_pairs.push(pair);
                break;
            } else if choice == "n" {
                pair.is_match = false;
                reviewed_pairs.push(pair);
                break;
            } else if choice == "q" {
                info!("User quit review. Analyzing collected data.");
                review_pb.finish_with_message("Review incomplete (user quit)");
                // Break out of both loops
                return analyze_and_suggest_thresholds(reviewed_pairs);
            } else {
                println!("Invalid input. Please enter 'y', 'n', or 'q'.");
            }
        }
    }
    review_pb.finish_with_message("Interactive review complete!");

    // 8. Analyze collected data and suggest thresholds
    analyze_and_suggest_thresholds(reviewed_pairs)
}

/// Fetches entities with their URLs, names, organization descriptions, and the
/// latitude/longitude of their most recently created location.
async fn fetch_entities_for_tuner(pool: &PgPool) -> Result<Vec<EntityUrlLocationInfo>> {
    let conn = pool.get().await.context("DB conn for tuner entities")?;
    let query = r#"
        WITH EntityServiceURLs AS (
            SELECT
                e.id AS entity_id,
                e.organization_id,
                s.url AS service_url,
                e.name AS entity_name
            FROM public.entity e
            JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'service'
            JOIN public.service s ON ef.table_id = s.id
            WHERE s.url IS NOT NULL AND s.url != '' AND s.url !~ '^\s*$' AND s.url NOT LIKE 'mailto:%' AND s.url NOT LIKE 'tel:%'
        ), EntityOrgURLs AS (
            SELECT
                e.id AS entity_id,
                e.organization_id,
                o.url AS org_url,
                e.name AS entity_name
            FROM public.entity e
            JOIN public.organization o ON e.organization_id = o.id
            WHERE o.url IS NOT NULL AND o.url != '' AND o.url !~ '^\s*$' AND o.url NOT LIKE 'mailto:%' AND o.url NOT LIKE 'tel:%'
        ), CombinedURLs AS (
            SELECT entity_id, organization_id, service_url AS url, entity_name FROM EntityServiceURLs
            UNION ALL -- Use UNION ALL to keep all combinations, DISTINCT applied later
            SELECT entity_id, organization_id, org_url AS url, entity_name FROM EntityOrgURLs
        ), EntityLocations AS (
            SELECT
                ef.entity_id,
                l.latitude,
                l.longitude,
                ROW_NUMBER() OVER(PARTITION BY ef.entity_id ORDER BY l.created DESC NULLS LAST) as rn
            FROM public.entity_feature ef
            JOIN public.location l ON ef.table_id = l.id
            WHERE ef.table_name = 'location' AND l.latitude IS NOT NULL AND l.longitude IS NOT NULL
        ), OrgDescriptions AS (
            SELECT
                id AS organization_id,
                description
            FROM public.organization
        )
        SELECT DISTINCT ON (cu.entity_id, cu.url) -- Ensure unique (entity_id, url) pairs
            cu.entity_id,
            cu.organization_id,
            cu.url,
            cu.entity_name,
            el.latitude,
            el.longitude,
            od.description AS organization_description
        FROM CombinedURLs cu
        LEFT JOIN EntityLocations el ON cu.entity_id = el.entity_id AND el.rn = 1
        LEFT JOIN OrgDescriptions od ON cu.organization_id = od.organization_id
        WHERE EXISTS (
            SELECT 1 FROM public.entity_feature ef_loc
            JOIN public.location loc ON ef_loc.table_id = loc.id
            WHERE ef_loc.entity_id = cu.entity_id AND ef_loc.table_name = 'location'
            AND loc.latitude IS NOT NULL AND loc.longitude IS NOT NULL
        ) -- Only include entities that have at least one location with coordinates
    "#;

    let rows = conn.query(query, &[]).await.context("Failed to query entities for tuner")?;
    let mut entities_info = Vec::new();
    for r in rows {
        let entity_id_str: String = r.get("entity_id");
        let organization_id_str: String = r.get("organization_id");
        let url_str: String = r.get("url");
        let entity_name_opt: Option<String> = r.get("entity_name");
        let latitude_opt: Option<f64> = r.get("latitude");
        let longitude_opt: Option<f64> = r.get("longitude");
        let org_description_opt: Option<String> = r.get("organization_description");

        // Normalize URL and check if it's ignored
        if let Some(normalized_data) = normalize_url_with_slugs(&url_str) {
            if !is_ignored_domain(&normalized_data.domain) {
                entities_info.push(EntityUrlLocationInfo {
                    entity_id: entity_id_str,
                    organization_id: organization_id_str,
                    url: url_str,
                    entity_name: entity_name_opt,
                    latitude: latitude_opt,
                    longitude: longitude_opt,
                    normalized_domain: normalized_data.domain,
                    path_slugs: normalized_data.path_slugs,
                });
            }
        }
    }
    Ok(entities_info)
}

/// Fetches the description for a given organization ID.
async fn get_organization_description(pool: &PgPool, org_id: &str) -> Result<String> {
    let conn = pool.get().await.context("DB conn for org description")?;
    let query = "SELECT description FROM public.organization WHERE id = $1";
    let row = conn.query_opt(query, &[&org_id]).await
        .context(format!("Failed to query description for organization ID: {}", org_id))?;

    Ok(row.map(|r| r.get("description")).unwrap_or_else(|| "[No Description]".to_string()))
}


/// Calculates the distance between two geographical points using the Haversine formula.
fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in meters
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let (delta_phi, delta_lambda) = ((lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (delta_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

/// Counts the number of matching slugs from the beginning of the path.
fn count_matching_slugs(slugs1: &[String], slugs2: &[String]) -> usize {
    slugs1
        .iter()
        .zip(slugs2.iter())
        .take_while(|(s1, s2)| s1 == s2)
        .count()
}

/// Analyzes reviewed pairs and suggests a MAX_LOCATION_DISTANCE_METERS.
fn analyze_and_suggest_thresholds(reviewed_pairs: Vec<ReviewedPair>) -> Result<()> {
    if reviewed_pairs.is_empty() {
        println!("\nNo pairs were reviewed. Cannot suggest thresholds.");
        return Ok(());
    }

    let mut distances_for_matches: Vec<f64> = Vec::new();
    let mut distances_for_non_matches: Vec<f64> = Vec::new();

    let mut matches_count = 0;
    let mut non_matches_count = 0;
    let mut no_distance_data_count = 0;

    for pair in reviewed_pairs {
        if let Some(distance) = pair.geospatial_distance_meters {
            if pair.is_match {
                distances_for_matches.push(distance);
                matches_count += 1;
            } else {
                distances_for_non_matches.push(distance);
                non_matches_count += 1;
            }
        } else {
            no_distance_data_count += 1;
        }
    }

    println!("\n--- Analysis of Reviewed Pairs ---");
    println!("Total Reviewed Pairs: {}", matches_count + non_matches_count + no_distance_data_count);
    println!("Confirmed Matches (with distance): {}", matches_count);
    println!("Confirmed Non-Matches (with distance): {}", non_matches_count);
    println!("Pairs without geospatial data: {}", no_distance_data_count);

    println!("\n--- Geospatial Distance Analysis (for Matched URLs) ---");
    if !distances_for_matches.is_empty() {
        distances_for_matches.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!("  Matches (min/avg/max): {:.2}m / {:.2}m / {:.2}m",
            distances_for_matches[0],
            distances_for_matches.iter().sum::<f64>() / distances_for_matches.len() as f64,
            distances_for_matches[distances_for_matches.len() - 1]
        );
        let p95_match = percentile(&distances_for_matches, 0.95);
        println!("  95th Percentile of Matches: {:.2}m", p95_match);
    } else {
        println!("  No matches with distance data reviewed.");
    }

    println!("\n--- Geospatial Distance Analysis (for Non-Matched URLs) ---");
    if !distances_for_non_matches.is_empty() {
        distances_for_non_matches.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!("  Non-Matches (min/avg/max): {:.2}m / {:.2}m / {:.2}m",
            distances_for_non_matches[0],
            distances_for_non_matches.iter().sum::<f64>() / distances_for_non_matches.len() as f64,
            distances_for_non_matches[distances_for_non_matches.len() - 1]
        );
        let p5_non_match = percentile(&distances_for_non_matches, 0.05);
        println!("  5th Percentile of Non-Matches: {:.2}m", p5_non_match);
    } else {
        println!("  No non-matches with distance data reviewed.");
    }

    println!("\n--- Suggested MAX_LOCATION_DISTANCE_METERS ---");

    let suggested_threshold = if !distances_for_matches.is_empty() {
        let p95_matches = percentile(&distances_for_matches, 0.95);
        // Suggest a value that covers most matches but starts to exclude non-matches
        // Consider the upper bound of matches and lower bound of non-matches
        if !distances_for_non_matches.is_empty() {
            let p5_non_matches = percentile(&distances_for_non_matches, 0.05);
            // Suggest something between 95th percentile of matches and 5th percentile of non-matches,
            // or just slightly above the 95th percentile of matches.
            (p95_matches * 1.1).min(p5_non_matches.max(p95_matches + 100.0)) // Add a buffer
        } else {
            p95_matches * 1.2 // If no non-matches, be a bit more generous
        }
    } else if !distances_for_non_matches.is_empty() {
        // If no matches, but non-matches, suggest a very low value to be strict
        distances_for_non_matches[0].max(500.0) // At least 500m default
    } else {
        2000.0 // Default if no data
    };

    println!("Suggested MAX_LOCATION_DISTANCE_METERS: {:.0} meters", suggested_threshold);
    println!("\nConsider adjusting this value in `src/matching/url.rs` for confident matches while limiting bad ones, especially for cases where organizations share a domain but are geographically disparate.");
    println!("You may want to run this tool multiple times to get a broader sample.");

    Ok(())
}

// Helper to calculate percentiles for a sorted list of scores
fn percentile(sorted_scores: &[f64], p: f64) -> f64 {
    if sorted_scores.is_empty() {
        return 0.0;
    }
    let n = sorted_scores.len() as f64;
    let rank = p * (n - 1.0);
    let lower_index = rank.floor() as usize;
    let upper_index = rank.ceil() as usize;

    if lower_index == upper_index {
        sorted_scores[lower_index]
    } else {
        let weight = rank - lower_index as f64;
        sorted_scores[lower_index] * (1.0 - weight) + sorted_scores[upper_index] * weight
    }
}
