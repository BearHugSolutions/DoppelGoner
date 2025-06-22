// src/bin/name_threshold_tuner.rs
//
// This binary provides an interactive tool to assist in tuning the
// MIN_FUZZY_SIMILARITY_THRESHOLD and MIN_SEMANTIC_SIMILARITY_THRESHOLD
// values within the `name.rs` matching strategy.
//
// It connects to the PostgreSQL database, samples entity pairs across various
// similarity score tiers, and prompts the user to confirm or deny matches.
// Based on user feedback, it suggests optimal threshold values.

use anyhow::{Context, Result};
use futures::future::join_all;
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::{HashMap, HashSet};
use std::io::{self, Write};
use std::sync::Arc;
use dedupe_lib::matching::name::{STOPWORDS};
use dedupe_lib::utils::db_connect::{self};
use dedupe_lib::utils::candle::cosine_similarity_candle;
use strsim::jaro_winkler;

// Constants for sampling and thresholds
const MIN_SIMILARITY_TIER: f32 = 0.60;
const MAX_SIMILARITY_TIER: f32 = 0.95; // Go a bit higher than current thresholds to find boundary
const TIER_INCREMENT: f32 = 0.03;
const SAMPLES_PER_TIER: usize = 10; // Number of pairs to sample for review per tier
const MAX_CONCURRENT_DB_FETCHES: usize = 15; // Limit concurrent DB ops for fetching embeddings/data

// Represents a pair that has been reviewed by the user
#[derive(Debug, Clone)] // Derive Clone to allow .cloned() later
struct ReviewedPair {
    entity_id_1: String,
    entity_id_2: String,
    fuzzy_score: f32,
    semantic_score: f32,
    is_match: bool, // True if user confirmed it's a match
}

// Main entry point for the binary
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Load environment variables and initialize logging
    dedupe_lib::utils::env::load_env();
    env_logger::init();

    info!("Starting Name Threshold Tuner...");

    // 2. Connect to the database
    let pool = db_connect::connect()
        .await
        .context("Failed to connect to PostgreSQL")?;
    info!("Successfully connected to the database.");

    // 3. Fetch all entities with names and organization embeddings
    let initial_conn = pool.get().await.context("Failed to get DB connection")?;
    info!("Loading all entities with names...");
    let all_entities_with_names_vec =
        dedupe_lib::matching::name::get_all_entities_with_names_and_locations(&*initial_conn).await?;
    info!(
        "Found {} entities with non-empty names.",
        all_entities_with_names_vec.len()
    );

    if all_entities_with_names_vec.len() < 2 {
        eprintln!("Not enough entities (need at least 2) to perform name matching. Exiting.");
        return Ok(());
    }

    info!("Loading organization embeddings...");
    let org_embeddings_map =
        dedupe_lib::matching::name::get_organization_embeddings(&*initial_conn, &all_entities_with_names_vec).await?;
    drop(initial_conn); // Release connection after initial data fetch

    info!("Preparing entity data (normalizing names, tokenizing)...");
    let stopwords: HashSet<String> = STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let (entity_data_vec, token_to_entities_idx_map, _token_stats) =
        dedupe_lib::matching::name::prepare_entity_data_and_index(&all_entities_with_names_vec, &stopwords).await;
    let entity_data_arc = Arc::new(entity_data_vec);

    info!("Generating candidate pairs for similarity calculation...");
    let candidate_pairs_indices =
        dedupe_lib::matching::name::generate_candidate_pairs_indices_limited(&entity_data_arc, &token_to_entities_idx_map)
            .await?;
    info!("Generated {} candidate pairs.", candidate_pairs_indices.len());

    if candidate_pairs_indices.is_empty() {
        eprintln!("No candidate pairs generated. Exiting.");
        return Ok(());
    }

    // 4. Calculate fuzzy and semantic scores for candidate pairs and bin them
    info!("Calculating similarity scores and binning pairs...");
    let mut similarity_bins: HashMap<String, Vec<(usize, usize, f32, f32)>> = HashMap::new(); // Key: "fuzzy_X.XX" or "semantic_X.XX"
    let total_candidates_to_score = candidate_pairs_indices.len();

    let scoring_pb = ProgressBar::new(total_candidates_to_score as u64);
    scoring_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    scoring_pb.set_message("Calculating scores and binning...");


    // To efficiently calculate scores, we'll iterate through candidate_pairs_indices
    // and store the scores.
    for (i, &(idx1, idx2)) in candidate_pairs_indices.iter().enumerate() {
        scoring_pb.inc(1);
        if i % 5000 == 0 {
            scoring_pb.set_message(format!("Calculating scores and binning... ({}/{})", i, total_candidates_to_score));
        }

        let entity1_data = &entity_data_arc[idx1];
        let entity2_data = &entity_data_arc[idx2];

        // Only calculate scores if both entities have normalized names.
        // This is a safety check, as `generate_candidate_pairs_indices_limited` should
        // already filter out empty normalized names.
        if entity1_data.normalized_name.is_empty() || entity2_data.normalized_name.is_empty() {
            continue;
        }

        let fuzzy_score = jaro_winkler(&entity1_data.normalized_name, &entity2_data.normalized_name) as f32;

        let embedding1_opt = org_embeddings_map
            .get(&entity1_data.entity.id)
            .and_then(|o| o.as_ref());
        let embedding2_opt = org_embeddings_map
            .get(&entity2_data.entity.id)
            .and_then(|o| o.as_ref());

        let semantic_score = match (embedding1_opt, embedding2_opt) {
            (Some(e1), Some(e2)) => {
                // Ensure dimensions match before calculating similarity
                if e1.len() == e2.len() {
                    cosine_similarity_candle(e1, e2).unwrap_or(0.0) as f32
                } else {
                    warn!("Dimension mismatch for embeddings between {} and {}. Skipping semantic score.", entity1_data.entity.id, entity2_data.entity.id);
                    0.0
                }
            },
            _ => 0.0,
        };

        // Binning for fuzzy scores
        for tier in (MIN_SIMILARITY_TIER * 100.0) as u32..=(MAX_SIMILARITY_TIER * 100.0) as u32 {
            let tier_f = tier as f32 / 100.0;
            if fuzzy_score >= tier_f && fuzzy_score < tier_f + TIER_INCREMENT {
                let bin_key = format!("fuzzy_{:.2}", tier_f);
                similarity_bins.entry(bin_key).or_default().push((idx1, idx2, fuzzy_score, semantic_score));
                break; // Found the bin for fuzzy score, move on
            }
        }

        // Binning for semantic scores
        for tier in (MIN_SIMILARITY_TIER * 100.0) as u32..=(MAX_SIMILARITY_TIER * 100.0) as u32 {
            let tier_f = tier as f32 / 100.0;
            if semantic_score >= tier_f && semantic_score < tier_f + TIER_INCREMENT {
                let bin_key = format!("semantic_{:.2}", tier_f);
                similarity_bins.entry(bin_key).or_default().push((idx1, idx2, fuzzy_score, semantic_score));
                break; // Found the bin for semantic score, move on
            }
        }
    }
    scoring_pb.finish_with_message("Similarity score calculation and binning complete.");

    // 5. Sample from bins and conduct interactive review
    let mut sampled_pairs_with_scores: Vec<(usize, usize, f32, f32)> = Vec::new(); // Temporarily store pairs with scores
    let mut rng = thread_rng();

    info!("Sampling pairs for interactive review ({} samples per tier)...", SAMPLES_PER_TIER);
    for tier in (MIN_SIMILARITY_TIER * 100.0) as u32..=(MAX_SIMILARITY_TIER * 100.0) as u32 {
        let tier_f = tier as f32 / 100.0;
        let fuzzy_key = format!("fuzzy_{:.2}", tier_f);
        let semantic_key = format!("semantic_{:.2}", tier_f);

        if let Some(pairs) = similarity_bins.get_mut(&fuzzy_key) {
            pairs.shuffle(&mut rng);
            sampled_pairs_with_scores.extend(pairs.iter().take(SAMPLES_PER_TIER).cloned());
        }
        if let Some(pairs) = similarity_bins.get_mut(&semantic_key) {
            pairs.shuffle(&mut rng);
            sampled_pairs_with_scores.extend(pairs.iter().take(SAMPLES_PER_TIER).cloned());
        }
    }
    
    // Use a HashSet of (usize, usize) to store unique *entity index pairs*
    let mut unique_sampled_indices = HashSet::<(usize, usize)>::new();
    let mut final_sampled_pairs_for_review: Vec<(usize, usize, f32, f32)> = Vec::new();

    for p in sampled_pairs_with_scores {
        // Normalize tuple for hashset based on entity indices only
        let normalized_idx_pair = if p.0 < p.1 { (p.0, p.1) } else { (p.1, p.0) };
        if unique_sampled_indices.insert(normalized_idx_pair) {
            // If successfully inserted (i.e., it was unique), add the full tuple to final list
            final_sampled_pairs_for_review.push(p);
        }
    }
    final_sampled_pairs_for_review.shuffle(&mut rng); // Shuffle for random presentation order

    info!("Total unique pairs to review: {}", final_sampled_pairs_for_review.len());

    let mut reviewed_pairs: Vec<ReviewedPair> = Vec::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();

    let review_pb = ProgressBar::new(final_sampled_pairs_for_review.len() as u64);
    review_pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.yellow} [{elapsed_precise}] {bar:40.magenta/cyan} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    review_pb.set_message("Starting interactive review...");

    for (i, (idx1, idx2, fuzzy_score, semantic_score)) in final_sampled_pairs_for_review.iter().enumerate() {
        review_pb.inc(1);
        review_pb.set_message(format!("Reviewing pair {}/{}", i + 1, final_sampled_pairs_for_review.len()));

        let entity1 = &entity_data_arc[*idx1].entity;
        let entity2 = &entity_data_arc[*idx2].entity;

        println!("\n--- Pair {}/{} ---", i + 1, final_sampled_pairs_for_review.len());
        println!("Entity 1 ID: {}", entity1.id);
        println!("  Name 1: {}", entity1.name.as_deref().unwrap_or("[No Name]"));
        println!("Entity 2 ID: {}", entity2.id);
        println!("  Name 2: {}", entity2.name.as_deref().unwrap_or("[No Name]"));
        println!("Fuzzy Similarity: {:.4}", fuzzy_score);
        println!("Semantic Similarity: {:.4}", semantic_score);

        let mut input = String::new();
        loop {
            print!("Are these names representing the same thing? (y/n/q to quit): ");
            stdout.flush()?;
            input.clear();
            stdin.read_line(&mut input)?;
            let choice = input.trim().to_lowercase();

            if choice == "y" {
                reviewed_pairs.push(ReviewedPair {
                    entity_id_1: entity1.id.clone(),
                    entity_id_2: entity2.id.clone(),
                    fuzzy_score: *fuzzy_score,
                    semantic_score: *semantic_score,
                    is_match: true,
                });
                break;
            } else if choice == "n" {
                reviewed_pairs.push(ReviewedPair {
                    entity_id_1: entity1.id.clone(),
                    entity_id_2: entity2.id.clone(),
                    fuzzy_score: *fuzzy_score,
                    semantic_score: *semantic_score,
                    is_match: false,
                });
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

    // 6. Analyze collected data and suggest thresholds
    analyze_and_suggest_thresholds(reviewed_pairs)
}

// Function to analyze reviewed pairs and suggest thresholds
fn analyze_and_suggest_thresholds(reviewed_pairs: Vec<ReviewedPair>) -> Result<()> {
    if reviewed_pairs.is_empty() {
        println!("\nNo pairs were reviewed. Cannot suggest thresholds.");
        return Ok(());
    }

    let mut fuzzy_matches: Vec<f32> = Vec::new();
    let mut fuzzy_non_matches: Vec<f32> = Vec::new();
    let mut semantic_matches: Vec<f32> = Vec::new();
    let mut semantic_non_matches: Vec<f32> = Vec::new();

    for pair in reviewed_pairs {
        if pair.is_match {
            fuzzy_matches.push(pair.fuzzy_score);
            semantic_matches.push(pair.semantic_score);
        } else {
            fuzzy_non_matches.push(pair.fuzzy_score);
            semantic_non_matches.push(pair.semantic_score);
        }
    }

    println!("\n--- Analysis of Reviewed Pairs ---");
    println!("Total Reviewed Pairs: {}", fuzzy_matches.len() + fuzzy_non_matches.len());
    println!("Confirmed Matches: {}", fuzzy_matches.len());
    println!("Confirmed Non-Matches: {}", fuzzy_non_matches.len());

    println!("\n--- Fuzzy Similarity Analysis ---");
    if !fuzzy_matches.is_empty() {
        fuzzy_matches.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!("  Matches (min/avg/max): {:.4} / {:.4} / {:.4}",
            fuzzy_matches[0],
            fuzzy_matches.iter().sum::<f32>() / fuzzy_matches.len() as f32,
            fuzzy_matches[fuzzy_matches.len() - 1]
        );
        let p5_match = percentile(&fuzzy_matches, 0.05);
        let p25_match = percentile(&fuzzy_matches, 0.25);
        let p75_match = percentile(&fuzzy_matches, 0.75);
        let p95_match = percentile(&fuzzy_matches, 0.95);
        println!("  Matches (P5/P25/P75/P95): {:.4} / {:.4} / {:.4} / {:.4}", p5_match, p25_match, p75_match, p95_match);
    } else {
        println!("  No fuzzy matches reviewed.");
    }

    if !fuzzy_non_matches.is_empty() {
        fuzzy_non_matches.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!("  Non-Matches (min/avg/max): {:.4} / {:.4} / {:.4}",
            fuzzy_non_matches[0],
            fuzzy_non_matches.iter().sum::<f32>() / fuzzy_non_matches.len() as f32,
            fuzzy_non_matches[fuzzy_non_matches.len() - 1]
        );
        let p5_non_match = percentile(&fuzzy_non_matches, 0.05);
        let p25_non_match = percentile(&fuzzy_non_matches, 0.25);
        let p75_non_match = percentile(&fuzzy_non_matches, 0.75);
        let p95_non_match = percentile(&fuzzy_non_matches, 0.95);
        println!("  Non-Matches (P5/P25/P75/P95): {:.4} / {:.4} / {:.4} / {:.4}", p5_non_match, p25_non_match, p75_non_match, p95_non_match);
    } else {
        println!("  No fuzzy non-matches reviewed.");
    }

    println!("\n--- Semantic Similarity Analysis ---");
    if !semantic_matches.is_empty() {
        semantic_matches.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!("  Matches (min/avg/max): {:.4} / {:.4} / {:.4}",
            semantic_matches[0],
            semantic_matches.iter().sum::<f32>() / semantic_matches.len() as f32,
            semantic_matches[semantic_matches.len() - 1]
        );
        let p5_match = percentile(&semantic_matches, 0.05);
        let p25_match = percentile(&semantic_matches, 0.25);
        let p75_match = percentile(&semantic_matches, 0.75);
        let p95_match = percentile(&semantic_matches, 0.95);
        println!("  Matches (P5/P25/P75/P95): {:.4} / {:.4} / {:.4} / {:.4}", p5_match, p25_match, p75_match, p95_match);
    } else {
        println!("  No semantic matches reviewed.");
    }

    if !semantic_non_matches.is_empty() {
        semantic_non_matches.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        println!("  Non-Matches (min/avg/max): {:.4} / {:.4} / {:.4}",
            semantic_non_matches[0],
            semantic_non_matches.iter().sum::<f32>() / semantic_non_matches.len() as f32,
            semantic_non_matches[semantic_non_matches.len() - 1]
        );
        let p5_non_match = percentile(&semantic_non_matches, 0.05);
        let p25_non_match = percentile(&semantic_non_matches, 0.25);
        let p75_non_match = percentile(&semantic_non_matches, 0.75);
        let p95_non_match = percentile(&semantic_non_matches, 0.95);
        println!("  Non-Matches (P5/P25/P75/P95): {:.4} / {:.4} / {:.4} / {:.4}", p5_non_match, p25_non_match, p75_non_match, p95_non_match);
    } else {
        println!("  No semantic non-matches reviewed.");
    }

    println!("\n--- Suggested Thresholds (Based on 95th Percentile of Non-Matches + Safety Margin) ---");

    let suggested_fuzzy_threshold = if !fuzzy_non_matches.is_empty() {
        let p95_non_match = percentile(&fuzzy_non_matches, 0.95);
        (p95_non_match + 0.01).min(0.99) // Add a small margin and cap at 0.99
    } else if !fuzzy_matches.is_empty() {
        // If no non-matches, suggest just below the lowest match fuzzy score
        fuzzy_matches[0].max(0.6).min(0.95) // Ensure it's not too low/high
    } else {
        0.90 // Default if no data
    };

    let suggested_semantic_threshold = if !semantic_non_matches.is_empty() {
        let p95_non_match = percentile(&semantic_non_matches, 0.95);
        (p95_non_match + 0.01).min(0.99)
    } else if !semantic_matches.is_empty() {
        semantic_matches[0].max(0.6).min(0.95)
    } else {
        0.90 // Default if no data
    };

    println!("Suggested MIN_FUZZY_SIMILARITY_THRESHOLD: {:.2}", suggested_fuzzy_threshold);
    println!("Suggested MIN_SEMANTIC_SIMILARITY_THRESHOLD: {:.2}", suggested_semantic_threshold);
    println!("\nConsider adjusting these values in `src/matching/name.rs` for confident matches while limiting bad ones.");
    println!("You may want to run this tool multiple times to get a broader sample.");

    Ok(())
}

// Helper to calculate percentiles for a sorted list of scores
fn percentile(sorted_scores: &[f32], p: f32) -> f32 {
    if sorted_scores.is_empty() {
        return 0.0;
    }
    let n = sorted_scores.len() as f32;
    let rank = p * (n - 1.0);
    let lower_index = rank.floor() as usize;
    let upper_index = rank.ceil() as usize;

    if lower_index == upper_index {
        sorted_scores[lower_index]
    } else {
        let weight = rank - lower_index as f32;
        sorted_scores[lower_index] * (1.0 - weight) + sorted_scores[upper_index] * weight
    }
}
