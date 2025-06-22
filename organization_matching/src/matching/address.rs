// src/matching/address.rs - Progress Callback Integration
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::matching::db::{
    batch_insert_match_decision_details, batch_upsert_entity_groups, EntityGroupBatchData,
    MatchDecisionDetailBatchData,
};
use crate::models::matching::{AddressMatchDetail, AddressMatchValues, AnyMatchResult, MatchResult, MatchValues};
use crate::models::stats_models::{MatchMethodStats, MatchMethodType};
use crate::rl::orchestrator::RLOrchestrator;
use crate::rl::SharedFeatureCache;
use crate::utils::db_connect::PgPool;
use crate::utils::progress_bars::logging::MatchingLogger; // Import the new logger
use crate::utils::pipeline_state::{
    batch_check_comparison_cache, batch_check_entity_completion_status, batch_get_current_signatures_for_pairs, batch_mark_entity_completion, batch_store_in_comparison_cache, ComparisonCacheEntry, EntityCompletionCheck, EntitySignature as SignatureData
};
use crate::utils::constants::MAX_DISTANCE_FOR_SAME_CITY_METERS; // Import the new constant

// NEW IMPORTS: Progress callback functionality
use crate::utils::progress_bars::progress_callback::ProgressCallback;
use crate::{update_progress, update_detailed_progress};

/// Internal struct to hold data for a pair that needs to be processed,
/// including all information required for DB upsert and decision detail insert.
struct PairToProcessAddress {
    entity_id_1: String, // Ordered
    entity_id_2: String, // Ordered
    match_values: MatchValues,
    pre_rl_confidence_score: f64,
    final_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    original_signature_1: Option<SignatureData>,
    original_signature_2: Option<SignatureData>,
    comparison_cache_hit: bool, // Track if this pair was a cache hit
}

// Add a struct to hold address data with location info
struct AddressInfo {
    entity_id: String,
    full_address: String,
    normalized_address: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<RLOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<AnyMatchResult> {
    let logger = MatchingLogger::new(MatchMethodType::Address); // Initialize logger
    logger.log_start(pipeline_run_id, reinforcement_orchestrator_option.is_some());

    // PROGRESS UPDATE: Starting phase
    update_progress!(progress_callback, "Starting", "Initializing address matching");

    let conn_init = pool
        .get()
        .await
        .context("Address: Failed to get DB connection for initial reads")?;

    logger.log_phase("Loading existing pairs", Some("querying entity_group table"));
    update_progress!(progress_callback, "Loading existing pairs", "querying entity_group table");
    
    let existing_entity_groups: HashSet<(String, String)> =
        fetch_existing_pairs(&*conn_init, MatchMethodType::Address).await?;
    logger.log_existing_pairs(existing_entity_groups.len());
    
    // PROGRESS UPDATE: Existing pairs loaded
    update_progress!(progress_callback, "Loading existing pairs", 
        format!("Found {} existing pairs", existing_entity_groups.len()));

    // Phase 1 Changes: Entity Filtering
    logger.log_phase("Loading address data", Some("querying entities with addresses (excluding completed)"));
    update_progress!(progress_callback, "Loading address data", "querying entities with addresses");
    
    // Step 1: Get all potential entity IDs with addresses
    let potential_entities_query = "
        SELECT DISTINCT e.id
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
        JOIN public.location l ON ef.table_id = l.id
        JOIN public.address a ON a.location_id = l.id
        WHERE a.address_1 IS NOT NULL AND a.address_1 != ''
        AND a.city IS NOT NULL AND a.city != ''";
    let potential_entity_rows = conn_init
        .query(potential_entities_query, &[])
        .await
        .context("Address: Failed to query potential entities with addresses")?;
    let all_potential_entity_ids: Vec<String> = potential_entity_rows
        .iter()
        .map(|row| row.get::<_, String>("id"))
        .collect();
    logger.log_debug(&format!("Address: Found {} entities with addresses", all_potential_entity_ids.len()));

    // PROGRESS UPDATE: Potential entities found
    update_progress!(progress_callback, "Loading address data", 
        format!("Found {} entities with addresses", all_potential_entity_ids.len()));

    // Step 2: Check completion status
    update_progress!(progress_callback, "Loading address data", "checking completion status");
    let completion_status: HashMap<String, EntityCompletionCheck> = batch_check_entity_completion_status(
        pool,
        &all_potential_entity_ids,
        &MatchMethodType::Address
    ).await?;

    let incomplete_entity_ids: Vec<String> = completion_status
    .iter()
    .filter_map(|(entity_id, status)| {
        if !status.is_complete {
            Some(entity_id.clone())
        } else {
            None
        }
    })
    .collect();

    let completed_count = all_potential_entity_ids.len() - incomplete_entity_ids.len();
    let total_potential_entities = all_potential_entity_ids.len(); // Change A: Add total_potential_entities
    logger.log_debug(&format!(
        "Address: {} total entities, {} already complete, {} to process",
        all_potential_entity_ids.len(),
        completed_count,
        incomplete_entity_ids.len()
    ));

    // PROGRESS UPDATE: Completion status results
    update_progress!(progress_callback, "Loading address data", 
        format!("{} to process ({} already complete)", incomplete_entity_ids.len(), completed_count));

    // Step 4: Load detailed data only for incomplete entities
    let phone_rows = if incomplete_entity_ids.is_empty() {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No incomplete entities to process");
        
        // Change B: Early Return #1 (no incomplete entities)
        return Ok(AnyMatchResult::Address(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Address,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: completed_count,
                entities_total: total_potential_entities,
            },
        }));
    } else {
        update_progress!(progress_callback, "Loading address data", "querying address records");
        
        let address_query = "
            SELECT e.id AS entity_id, l.id as location_id,
                   a.address_1, a.address_2, a.city, a.state_province, a.postal_code, a.country,
                   l.latitude, l.longitude
            FROM public.entity e
            JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'location'
            JOIN public.location l ON ef.table_id = l.id
            JOIN public.address a ON a.location_id = l.id
            WHERE e.id = ANY($1)
            AND a.address_1 IS NOT NULL AND a.address_1 != ''
            AND a.city IS NOT NULL AND a.city != ''";
        conn_init
            .query(address_query, &[&incomplete_entity_ids])
            .await
            .context("Address: Failed to query addresses for incomplete entities")?
    };

    logger.log_data_loaded(phone_rows.len(), "address");
    // PROGRESS UPDATE: Data loaded
    update_progress!(progress_callback, "Loading address data", 
        format!("Loaded {} address records", phone_rows.len()));
    
    drop(conn_init); // Release connection

    logger.log_phase("Processing and normalizing data", None);
    update_progress!(progress_callback, "Processing addresses", "normalizing address data");

    // Create progress bar for address processing
    let processing_pb = ProgressBar::new(phone_rows.len() as u64);
    processing_pb.set_style(
        ProgressStyle::default_bar()
            .template("  📍 [{elapsed_precise}] {bar:30.yellow/red} {pos}/{len} Processing addresses...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );

    let mut address_map: HashMap<String, Vec<AddressInfo>> = HashMap::new(); // Changed to Vec<AddressInfo>
    let mut duplicate_entries = 0; // Data quality metric
    let mut invalid_formats = 0; // Data quality metric

    for (i, row) in phone_rows.iter().enumerate() {
        // PROGRESS UPDATE: Update every 1000 items
        if i % 1000 == 0 || i == phone_rows.len() - 1 {
            processing_pb.inc(1);
            update_detailed_progress!(progress_callback, "Processing addresses", i + 1, phone_rows.len(),
                format!("{} unique addresses", address_map.len()));
        }
        logger.log_progress_update(i + 1, phone_rows.len(), None);

        let entity_id: String = row.get("entity_id");
        let full_address = match format_full_address(row) {
            Ok(addr) => addr,
            Err(e) => {
                logger.log_warning(&format!("Failed to format address for entity {}: {}", entity_id, e));
                invalid_formats += 1;
                continue;
            }
        };
        let normalized_address_str = normalize_address(&full_address);
        let latitude: Option<f64> = row.try_get("latitude").ok();
        let longitude: Option<f64> = row.try_get("longitude").ok();

        if !normalized_address_str.is_empty() {
            // Check if an AddressInfo with this entity_id already exists for this normalized address
            let entry = address_map.entry(normalized_address_str.clone()).or_default();
            if !entry.iter().any(|info| info.entity_id == entity_id) {
                entry.push(AddressInfo {
                    entity_id,
                    full_address,
                    normalized_address: normalized_address_str,
                    latitude,
                    longitude,
                });
            } else {
                duplicate_entries += 1; // Increment for duplicate entity-address combinations
            }
        } else {
            invalid_formats += 1; // Increment for entries that result in empty normalized addresses
        }
    }
    processing_pb.finish_with_message(format!(
        "Processed {} addresses into {} unique normalized addresses (groups)",
        phone_rows.len(),
        address_map.len()
    ));

    let groups_with_multiple = address_map.values().filter(|entities| entities.len() >= 2).count();
    logger.log_data_quality_issue("duplicate entity-address combinations", duplicate_entries);
    logger.log_data_quality_issue("invalid address formats", invalid_formats);
    logger.log_processing_complete(phone_rows.len(), address_map.len(), groups_with_multiple);

    // PROGRESS UPDATE: Processing complete
    update_progress!(progress_callback, "Processing addresses", 
        format!("Processed {} addresses into {} groups", phone_rows.len(), address_map.len()));

    logger.log_phase("Comparing address pairs", Some("generating and filtering potential matches"));
    update_progress!(progress_callback, "Generating pairs", "creating candidate address pairs");

    let mut all_pairs_to_process: Vec<PairToProcessAddress> = Vec::new();
    let mut processed_pairs_this_run: HashSet<(String, String)> = HashSet::new(); // Local cache for this run's processed pairs
    let mut pairs_for_signature_fetch: Vec<(String, String)> = Vec::new();

    // Calculate total pairs for progress tracking
    let total_conceptual_pairs: usize = address_map
        .values()
        .map(|entities| {
            let n = entities.len();
            if n >= 2 {
                n * (n - 1) / 2
            } else {
                0
            }
        })
        .sum();

    logger.log_pair_generation(total_conceptual_pairs, groups_with_multiple);
    // PROGRESS UPDATE: Pair generation started
    update_progress!(progress_callback, "Generating pairs", 
        format!("{} potential pairs to evaluate", total_conceptual_pairs));

    let pairs_pb = if total_conceptual_pairs > 0 {
        let pb = ProgressBar::new(total_conceptual_pairs as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  📍 [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} Generating pairs...",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        Some(pb)
    } else {
        None
    };

    let mut current_pair_count_for_pb = 0;
    let mut existing_skipped = 0; // Track existing skipped pairs
    let mut geospatial_filtered = 0; // Track geospatial filtered pairs

    for (normalized_shared_address, current_entity_list) in address_map {
        if current_entity_list.len() < 2 {
            continue;
        }
        
        for i in 0..current_entity_list.len() {
            for j in (i + 1)..current_entity_list.len() {
                let e1_info = &current_entity_list[i];
                let e2_info = &current_entity_list[j];

                if e1_info.entity_id == e2_info.entity_id {
                    continue;
                }

                current_pair_count_for_pb += 1;
                if let Some(pb) = &pairs_pb {
                    pb.inc(1);
                    if current_pair_count_for_pb % 100 == 0 {
                        pb.set_message(format!(
                            "Generating pairs... ({} generated)",
                            all_pairs_to_process.len()
                        ));
                        
                        // PROGRESS UPDATE: Detailed pair generation progress
                        if current_pair_count_for_pb % 500 == 0 {
                            update_detailed_progress!(progress_callback, "Generating pairs", 
                                current_pair_count_for_pb, total_conceptual_pairs,
                                format!("{} candidates created", all_pairs_to_process.len()));
                        }
                    }
                }

                let (e1_id, e1_orig_addr, e1_lat, e1_lon, e2_id, e2_orig_addr, e2_lat, e2_lon) =
                    if e1_info.entity_id < e2_info.entity_id {
                        (
                            &e1_info.entity_id,
                            &e1_info.full_address,
                            e1_info.latitude,
                            e1_info.longitude,
                            &e2_info.entity_id,
                            &e2_info.full_address,
                            e2_info.latitude,
                            e2_info.longitude,
                        )
                    } else {
                        (
                            &e2_info.entity_id,
                            &e2_info.full_address,
                            e2_info.latitude,
                            e2_info.longitude,
                            &e1_info.entity_id,
                            &e1_info.full_address,
                            e1_info.latitude,
                            e1_info.longitude,
                        )
                    };
                let current_pair_ordered = (e1_id.clone(), e2_id.clone());

                if existing_entity_groups.contains(&current_pair_ordered)
                    || processed_pairs_this_run.contains(&current_pair_ordered)
                {
                    existing_skipped += 1;
                    logger.log_debug(&format!("Pair ({}, {}) already in entity_group or processed this run. Skipping.", e1_id, e2_id));
                    continue;
                }

                // --- Geospatial Filtering ---
                if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) =
                    (e1_lat, e1_lon, e2_lat, e2_lon)
                {
                    let distance = calculate_distance(lat1, lon1, lat2, lon2);
                    if distance > MAX_DISTANCE_FOR_SAME_CITY_METERS {
                        geospatial_filtered += 1;
                        logger.log_debug(&format!("Pair ({}, {}) skipped due to large geospatial distance ({}m > {}m).", e1_id, e2_id, distance, MAX_DISTANCE_FOR_SAME_CITY_METERS));
                        processed_pairs_this_run.insert(current_pair_ordered); // Mark as processed to prevent re-evaluation
                        continue;
                    }
                }

                // Add to list for signature fetch and cache check
                pairs_for_signature_fetch.push(current_pair_ordered.clone());

                let mut pre_rl_confidence_score = 0.95;
                let unit1 = extract_unit(e1_orig_addr);
                let unit2 = extract_unit(e2_orig_addr);
                if !unit1.is_empty() && !unit2.is_empty() && unit1 != unit2 {
                    pre_rl_confidence_score *= 0.85;
                }

                let match_values_obj = MatchValues::Address(AddressMatchDetail {
                    values: AddressMatchValues {
                        normalized_address1: normalize_address(e1_orig_addr).to_string(),
                        normalized_address2: normalize_address(e2_orig_addr).to_string(),
                        matched_normalized_address: normalized_shared_address.clone(),
                    },
                    original1: e1_orig_addr.to_string(),
                    original2: e2_orig_addr.to_string(),
                    normalized_shared: normalized_shared_address.clone(),
                    pre_rl_confidence: Some(pre_rl_confidence_score as f32),
                });

                all_pairs_to_process.push(PairToProcessAddress {
                    entity_id_1: e1_id.clone(),
                    entity_id_2: e2_id.clone(),
                    match_values: match_values_obj,
                    pre_rl_confidence_score,
                    final_confidence_score: pre_rl_confidence_score, // Initial, will be tuned
                    features_for_snapshot: None,
                    original_signature_1: None,  // Will be filled later
                    original_signature_2: None,  // Will be filled later
                    comparison_cache_hit: false, // Will be set later
                });

                processed_pairs_this_run.insert(current_pair_ordered);
            }
        }
    }
    if let Some(pb) = &pairs_pb {
        pb.finish_with_message(format!(
            "Generated {} candidate pairs after geospatial filter",
            all_pairs_to_process.len()
        ));
    }

    logger.log_filtering_results(total_conceptual_pairs, all_pairs_to_process.len(), existing_skipped, geospatial_filtered);
    if geospatial_filtered > 0 {
        logger.log_geospatial_filtering(total_conceptual_pairs, geospatial_filtered, MAX_DISTANCE_FOR_SAME_CITY_METERS);
    }

    // PROGRESS UPDATE: Pair generation complete
    update_progress!(progress_callback, "Generating pairs", 
        format!("{} candidate pairs after filtering", all_pairs_to_process.len()));

    if all_pairs_to_process.is_empty() {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No pairs to process");
        
        // Change C: Early Return #2 (no pairs to process)
        return Ok(AnyMatchResult::Address(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Address,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: completed_count,
                entities_total: total_potential_entities,
            },
        }));
    }

    // --- Batch processing starts here ---
    logger.log_cache_and_signatures(pairs_for_signature_fetch.len());
    update_progress!(progress_callback, "Cache checking", "fetching entity signatures");
    
    let signatures_map =
        batch_get_current_signatures_for_pairs(pool, &pairs_for_signature_fetch).await?;

    let mut cache_check_inputs = Vec::new();
    for pair_data in all_pairs_to_process.iter_mut() {
        let pair_key = (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone());
        if let Some((sig1_data, sig2_data)) = signatures_map.get(&pair_key) {
            pair_data.original_signature_1 = Some(sig1_data.clone());
            pair_data.original_signature_2 = Some(sig2_data.clone());
            cache_check_inputs.push((
                pair_data.entity_id_1.clone(),
                pair_data.entity_id_2.clone(),
                sig1_data.signature.clone(),
                sig2_data.signature.clone(),
                MatchMethodType::Address,
            ));
        }
    }

    update_progress!(progress_callback, "Cache checking", "checking comparison cache");
    let cache_results_map = batch_check_comparison_cache(pool, &cache_check_inputs).await?;

    let mut pairs_to_process_after_cache: Vec<PairToProcessAddress> = Vec::new();
    let mut cache_hits_count = 0;

    for mut pair_data in all_pairs_to_process {
        let pair_key = (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone());
        if let Some(_cached_eval) = cache_results_map.get(&pair_key) {
            // Assuming cached_eval means 'already processed/matched', skip re-evaluation
            cache_hits_count += 1;
            pair_data.comparison_cache_hit = true;
            // No need to add to pairs_to_process_after_cache if already handled
        } else {
            pairs_to_process_after_cache.push(pair_data);
        }
    }

    logger.log_cache_results(cache_hits_count, pairs_to_process_after_cache.len());
    // PROGRESS UPDATE: Cache results
    update_progress!(progress_callback, "Cache checking", 
        format!("{} cache hits, {} pairs to evaluate", cache_hits_count, pairs_to_process_after_cache.len()));

    logger.log_phase("Evaluating pairs with RL confidence tuning", None);
    update_progress!(progress_callback, "Evaluating pairs", "running confidence analysis");
    
    let eval_pb = ProgressBar::new(pairs_to_process_after_cache.len() as u64);
    eval_pb.set_style(
        ProgressStyle::default_bar()
            .template("  📍 [{elapsed_precise}] {bar:30.cyan/blue} {pos}/{len} Evaluating pairs...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );

    let mut final_groups_created_count = 0;
    let mut final_entities_in_new_pairs: HashSet<String> = HashSet::new();
    let mut final_confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0; // These are errors *during* processing/DB ops, not skipped pairs
    let mut feature_extraction_attempted = 0;
    let mut feature_extraction_successful = 0;
    let mut feature_extraction_failed = 0;
    let total_pairs = pairs_to_process_after_cache.len();

    const BATCH_DB_OPS_SIZE: usize = 1000; // Define a batch size for DB ops
    logger.log_batch_processing_start(pairs_to_process_after_cache.len(), BATCH_DB_OPS_SIZE);
    // PROGRESS UPDATE: Batch processing start
    update_progress!(progress_callback, "Batch processing", 
        format!("Processing {} pairs in batches of {}", pairs_to_process_after_cache.len(), BATCH_DB_OPS_SIZE));

    let total_batches = (pairs_to_process_after_cache.len() + BATCH_DB_OPS_SIZE - 1) / BATCH_DB_OPS_SIZE;
    for (batch_idx, chunk) in pairs_to_process_after_cache.chunks_mut(BATCH_DB_OPS_SIZE).enumerate() {
        logger.log_batch_progress(batch_idx + 1, total_batches, chunk.len());
        // PROGRESS UPDATE: Batch progress
        update_detailed_progress!(progress_callback, "Batch processing", 
            batch_idx + 1, total_batches, format!("{} pairs in this batch", chunk.len()));
        
        let chunk_len = chunk.len();
        let mut batch_entity_group_data = Vec::new();
        let mut temp_decision_detail_data_parts: Vec<(
            (String, String), // Pair key (entity_id_1, entity_id_2)
            serde_json::Value,
            String,      // MatchMethodType as String
            f64,         // pre_rl_confidence_at_decision
            f64,         // tuned_confidence_at_decision
            Option<i32>, // confidence_tuner_version_at_decision
        )> = Vec::new();
        let mut batch_cache_store_data = Vec::new();
        let mut batch_decision_detail_data = Vec::new(); // Moved here to be collected per chunk

        for (pair_idx, pair_data) in chunk.iter_mut().enumerate() {
            let mut final_confidence_score = pair_data.pre_rl_confidence_score;
            let mut features_json_for_cache: Option<serde_json::Value> = None;

            if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                feature_extraction_attempted += 1;
                match if let Some(cache_service_arc) = feature_cache.as_ref() {
                    let mut cache_service_guard = cache_service_arc.lock().await;
                    cache_service_guard
                        .get_pair_features(pool, &pair_data.entity_id_1, &pair_data.entity_id_2)
                        .await
                } else {
                    RLOrchestrator::extract_pair_context_features(
                        pool,
                        &pair_data.entity_id_1,
                        &pair_data.entity_id_2,
                    )
                    .await
                } {
                    Ok(features) => {
                        if !features.is_empty() {
                            feature_extraction_successful += 1;
                            pair_data.features_for_snapshot = Some(features.clone()); // Now this assignment is valid
                            features_json_for_cache = serde_json::to_value(features.clone()).ok();
                            match ro_arc.lock().await.get_tuned_confidence(
                                &MatchMethodType::Address,
                                pair_data.pre_rl_confidence_score,
                                features.as_ref(), // Use the local features variable
                            ) {
                                Ok(tuned_score) => final_confidence_score = tuned_score,
                                Err(e) => logger.log_warning(&format!(
                                    "RL tuning failed for ({}, {}): {}",
                                    pair_data.entity_id_1, pair_data.entity_id_2, e
                                )),
                            }
                        } else {
                            logger.log_warning(&format!(
                                "Extracted features vector is empty for pair ({}, {})",
                                pair_data.entity_id_1, pair_data.entity_id_2
                            ));
                        }
                    }
                    Err(e) => {
                        feature_extraction_failed += 1;
                        logger.log_warning(&format!(
                            "Feature extraction failed for ({}, {}): {}",
                            pair_data.entity_id_1, pair_data.entity_id_2, e
                        ));
                    }
                }
            }

            // Update final_confidence_score on the pair_data struct before batching
            pair_data.final_confidence_score = final_confidence_score;

            // Prepare data for batch upsert
            let new_entity_group_id_str = Uuid::new_v4().to_string(); // Generate ID upfront
            batch_entity_group_data.push(EntityGroupBatchData {
                proposed_id: new_entity_group_id_str.clone(),
                entity_id_1: pair_data.entity_id_1.clone(),
                entity_id_2: pair_data.entity_id_2.clone(),
                confidence_score: pair_data.final_confidence_score,
                pre_rl_confidence_score: pair_data.pre_rl_confidence_score,
                method_type: MatchMethodType::Address,
                match_values: pair_data.match_values.clone(),
            });

            // Prepare parts for batch decision detail data, to be completed after upsert_entity_groups
            if let Some(ro_arc) = reinforcement_orchestrator_option.as_ref() {
                if let Some(features_vec) = &pair_data.features_for_snapshot {
                    let version = ro_arc.lock().await.confidence_tuner.version;
                    let snapshot_features_json =
                        serde_json::to_value(features_vec).unwrap_or(serde_json::Value::Null);
                    temp_decision_detail_data_parts.push((
                        (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone()),
                        snapshot_features_json,
                        MatchMethodType::Address.as_str().to_string(),
                        pair_data.pre_rl_confidence_score,
                        pair_data.final_confidence_score,
                        Some(version as i32),
                    ));
                }
            }

            // Prepare data for batch cache store (MATCH outcome)
            if let (Some(sig1_data), Some(sig2_data)) = (
                &pair_data.original_signature_1,
                &pair_data.original_signature_2,
            ) {
                batch_cache_store_data.push(ComparisonCacheEntry {
                    entity_id_1: pair_data.entity_id_1.clone(),
                    entity_id_2: pair_data.entity_id_2.clone(),
                    signature_1: sig1_data.signature.clone(),
                    signature_2: sig2_data.signature.clone(),
                    method_type: MatchMethodType::Address,
                    pipeline_run_id: pipeline_run_id.to_string(),
                    comparison_result: "MATCH".to_string(),
                    similarity_score: Some(pair_data.final_confidence_score),
                    features_snapshot: features_json_for_cache.clone(),
                });
            }

            // Update progress bar within batch processing
            eval_pb.set_position((batch_idx * BATCH_DB_OPS_SIZE + pair_idx + 1) as u64);
            
            // PROGRESS UPDATE: Detailed evaluation progress
            if pair_idx % 100 == 0 || pair_idx == chunk_len - 1 {
                update_detailed_progress!(progress_callback, "Evaluating pairs", 
                    batch_idx * BATCH_DB_OPS_SIZE + pair_idx + 1, total_pairs,
                    format!("RL features: {}/{}", feature_extraction_successful, feature_extraction_attempted));
            }
        } // End of chunk.iter_mut()

        // Execute batch DB operations
        let upsert_results = batch_upsert_entity_groups(pool, batch_entity_group_data).await;

        match upsert_results {
            Ok(results_map) => {
                // Now, iterate over the collected temporary data to create decision details
                for (
                    pair_key,
                    snapshot_features_json,
                    method_type_str,
                    pre_rl_conf,
                    tuned_conf,
                    tuner_version,
                ) in temp_decision_detail_data_parts
                {
                    if let Some((group_id, was_newly_inserted)) = results_map.get(&pair_key) {
                        if *was_newly_inserted {
                            final_groups_created_count += 1;
                            final_entities_in_new_pairs.insert(pair_key.0.clone());
                            final_entities_in_new_pairs.insert(pair_key.1.clone());
                            final_confidence_scores_for_stats.push(tuned_conf);

                            batch_decision_detail_data.push(MatchDecisionDetailBatchData {
                                entity_group_id: group_id.clone(),
                                pipeline_run_id: pipeline_run_id.to_string(),
                                snapshotted_features: snapshot_features_json,
                                method_type_at_decision: method_type_str,
                                pre_rl_confidence_at_decision: pre_rl_conf,
                                tuned_confidence_at_decision: tuned_conf,
                                confidence_tuner_version_at_decision: tuner_version,
                            });
                        }
                    }
                }
            }
            Err(e) => {
                logger.log_warning(&format!("Batch upsert failed: {}", e));
                individual_operation_errors += chunk_len;
            }
        }

        if !batch_decision_detail_data.is_empty() {
            if let Err(e) =
                batch_insert_match_decision_details(pool, batch_decision_detail_data).await
            {
                logger.log_warning(&format!("Batch insert decision details failed: {}", e));
            }
        }

        if !batch_cache_store_data.is_empty() {
            if let Err(e) = batch_store_in_comparison_cache(pool, batch_cache_store_data).await {
                logger.log_warning(&format!("Batch store comparison cache failed: {}", e));
            }
        }
    }

    eval_pb.finish_with_message(format!(
        "📍 Address matching complete: {} new groups created",
        final_groups_created_count
    ));

    let avg_confidence = if !final_confidence_scores_for_stats.is_empty() {
        final_confidence_scores_for_stats.iter().sum::<f64>()
            / final_confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };
    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Address,
        groups_created: final_groups_created_count,
        entities_matched: final_entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_groups_created_count > 0 {
            2.0
        } else {
            0.0
        },
        entities_skipped_complete: completed_count, // Change D: Add new fields
        entities_total: total_potential_entities,   // Change D: Add new fields
    };
    logger.log_completion(
        final_groups_created_count,
        final_entities_in_new_pairs.len(),
        avg_confidence,
        pairs_to_process_after_cache.len()
    );
    logger.log_performance_summary(
        cache_hits_count,
        individual_operation_errors,
        Some((feature_extraction_attempted, feature_extraction_successful, feature_extraction_failed))
    );

    // Phase 2: Mark entities as complete
    logger.log_phase("Tracking completion status", Some("marking entities with no remaining comparisons"));
    update_progress!(progress_callback, "Completion tracking", "marking completed entities");
    
    // Track entities that had all their possible comparisons processed
    let mut entity_comparison_counts: HashMap<String, i32> = HashMap::new();
    let mut entities_with_comparisons: HashSet<String> = HashSet::new();

    // Count comparisons per entity from the pairs we processed
    for pair_data in &pairs_to_process_after_cache {
        *entity_comparison_counts.entry(pair_data.entity_id_1.clone()).or_insert(0) += 1;
        *entity_comparison_counts.entry(pair_data.entity_id_2.clone()).or_insert(0) += 1;
        entities_with_comparisons.insert(pair_data.entity_id_1.clone());
        entities_with_comparisons.insert(pair_data.entity_id_2.clone());
    }

    // Prepare batch completion data
    let mut completion_batch: Vec<(String, MatchMethodType, String, String, i32)> = Vec::new();
    for entity_id in entities_with_comparisons {
        if let Some(status) = completion_status.get(&entity_id) {
            if let Some(current_sig) = &status.current_signature {
                let comparison_count = entity_comparison_counts.get(&entity_id).copied().unwrap_or(0);
                completion_batch.push((
                    entity_id.clone(),
                    MatchMethodType::Address,
                    pipeline_run_id.to_string(),
                    current_sig.clone(),
                    comparison_count,
                ));
            }
        }
    }

    // Batch mark completions
    if !completion_batch.is_empty() {
        if let Err(e) = batch_mark_entity_completion(pool, &completion_batch).await {
            logger.log_warning(&format!(
                "Failed to batch mark {} entities as complete: {}",
                completion_batch.len(), e
            ));
        } else {
            logger.log_debug(&format!(
                "Marked {} entities as complete for Address matching",
                completion_batch.len()
            ));
        }
    }

    // FINAL PROGRESS UPDATE: Completion
    update_progress!(progress_callback, "Completed", 
        format!("{} address groups created, {} entities matched", 
                final_groups_created_count, final_entities_in_new_pairs.len()));

    Ok(AnyMatchResult::Address(MatchResult {
        groups_created: method_stats.groups_created,
        stats: method_stats,
    }))
}

// Re-use `calculate_distance` from `url.rs` by making it `pub` in a shared `utils` module or by copying.
// For now, let's copy it for simplicity within this module, assuming it's not made public yet.
fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371000.0; // Earth radius in meters
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let (delta_phi, delta_lambda) = ((lat2 - lat1).to_radians(), (lon2 - lon1).to_radians());
    let a = (delta_phi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (delta_lambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

pub fn normalize_address(address: &str) -> String {
    let lower = address.to_lowercase();
    let mut normalized = lower
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace() || *c == '#')
        .collect::<String>();
    normalized = normalized
        .replace(" st ", " street ")
        .replace(" str ", " street ")
        .replace(" rd ", " road ")
        .replace(" ave ", " avenue ")
        .replace(" av ", " avenue ")
        .replace(" blvd ", " boulevard ")
        .replace(" blv ", " boulevard ")
        .replace(" dr ", " drive ")
        .replace(" ln ", " lane ")
        .replace(" ct ", " court ")
        .replace(" pl ", " place ")
        .replace(" sq ", " square ")
        .replace(" pkwy ", " parkway ")
        .replace(" cir ", " circle ");
    let patterns_to_remove = [
        "apt ",
        "apartment ",
        "suite ",
        "ste ",
        "unit ",
        // "#", // Be careful with removing '#' as it might be part of the primary address
        "bldg ",
        "building ",
        "fl ",
        "floor ",
        "dept ",
        "department ",
        "room ",
        "rm ",
        "po box ",
        "p o box ",
        "p.o. box ",
    ];
    // Handle '#' more carefully to remove unit numbers like "# 123" or "#123"
    if let Some(idx) = normalized.find('#') {
        // Check if it's likely a unit designator, e.g., followed by a digit or space then digit
        let after_hash = &normalized[idx + 1..];
        if after_hash
            .trim_start()
            .chars()
            .next()
            .map_or(false, |c| c.is_ascii_digit())
        {
            let (before, after_pattern) = normalized.split_at(idx);
            let mut rest = after_pattern
                .trim_start_matches('#')
                .trim_start()
                .to_string();
            // Remove the unit number part
            if let Some(space_idx) = rest.find(|c: char| c.is_whitespace() || c == ',') {
                rest = rest[space_idx..].to_string(); // Keep what's after the unit number
            } else {
                rest.clear(); // Unit number was at the end
            }
            normalized = format!("{}{}", before.trim_end(), rest.trim_start());
        }
    }
    for pattern_base in patterns_to_remove {
        // Ensure we are matching whole words or words followed by numbers
        // This is a simplified approach; regex would be more robust for word boundaries.
        let pattern_with_space = format!("{} ", pattern_base);
        while let Some(idx) = normalized.find(&pattern_with_space) {
            let (before, after_pattern_full) = normalized.split_at(idx);
            let mut rest_of_string = after_pattern_full
                .strip_prefix(&pattern_with_space) // Use strip_prefix
                .unwrap_or(after_pattern_full) // Should not happen if find worked
                .to_string();

            // Remove the unit identifier (e.g., number or letter)
            if let Some(end_of_unit_idx) =
                rest_of_string.find(|c: char| c.is_whitespace() || c == ',')
            {
                rest_of_string = rest_of_string[end_of_unit_idx..].to_string();
            } else {
                rest_of_string.clear(); // Unit identifier was at the end
            }
            normalized = format!("{}{}", before.trim_end(), rest_of_string.trim_start());
            normalized = normalized.trim().to_string();
        }
    }
    normalized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

fn extract_unit(address: &str) -> String {
    let lower = address.to_lowercase();
    let unit_patterns = [
        "apt",
        "apartment",
        "suite",
        "ste",
        "unit",
        "#",
        "bldg",
        "building",
        "fl",
        "floor",
        "room",
        "rm",
    ];
    for pattern in unit_patterns {
        if let Some(idx) = lower.find(pattern) {
            let after_pattern_start_idx = idx + pattern.len();
            let after_pattern_content = lower[after_pattern_start_idx..].trim_start();

            if after_pattern_content.is_empty() {
                // Pattern was at the end with no unit value
                continue;
            }

            // Find where the unit value ends (next space, comma, or end of string)
            let unit_value_end_idx = after_pattern_content
                .find(|c: char| c.is_whitespace() || c == ',')
                .unwrap_or(after_pattern_content.len());

            let unit_value = after_pattern_content[..unit_value_end_idx].trim();

            if !unit_value.is_empty() {
                // For '#', ensure it's followed by a digit or is a digit itself if pattern is just '#'
                if pattern == "#"
                    && !unit_value
                        .chars()
                        .all(|c| c.is_ascii_digit() || c.is_alphabetic())
                {
                    // Allow alphanumeric unit for #
                    continue;
                }
                return format!("{} {}", pattern, unit_value).trim().to_string();
            }
        }
    }
    String::new()
}

pub fn format_full_address(row: &tokio_postgres::Row) -> Result<String> {
    let address_1: String = row.try_get("address_1").context("Missing address_1")?;
    let address_2: Option<String> = row
        .try_get("address_2")
        .ok()
        .flatten()
        .filter(|s: &String| !s.trim().is_empty());
    let city: String = row.try_get("city").context("Missing city")?;
    let state_province: String = row
        .try_get("state_province")
        .context("Missing state_province")?;
    let postal_code: String = row.try_get("postal_code").context("Missing postal_code")?;
    let country: String = row.try_get("country").context("Missing country")?;
    Ok(format!(
        "{}{}, {}, {} {}, {}",
        address_1.trim(),
        address_2.map_or("".to_string(), |a| format!(", {}", a.trim())),
        city.trim(),
        state_province.trim(),
        postal_code.trim(),
        country.trim()
    ))
}

async fn fetch_existing_pairs(
    conn: &impl tokio_postgres::GenericClient,
    method_type: MatchMethodType,
) -> Result<HashSet<(String, String)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&method_type.as_str()])
        .await
        .with_context(|| format!("Failed to query existing {:?}-matched pairs", method_type))?;
    let mut existing_pairs = HashSet::new();
    for row in rows {
        let id1_str: String = row.get("entity_id_1");
        let id2_str: String = row.get("entity_id_2");
        if id1_str < id2_str {
            existing_pairs.insert((id1_str, id2_str));
        } else {
            existing_pairs.insert((id2_str, id1_str));
        }
    }
    Ok(existing_pairs)
}