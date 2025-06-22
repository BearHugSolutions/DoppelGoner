// src/matching/phone.rs - Progress Callback Integration
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
use crate::models::matching::{AnyMatchResult, MatchResult, MatchValues, PhoneMatchDetail, PhoneMatchValues};
use crate::models::stats_models::{MatchMethodStats, MatchMethodType};
use crate::rl::orchestrator::RLOrchestrator;
use crate::rl::SharedFeatureCache;
use crate::utils::db_connect::PgPool;
use crate::utils::progress_bars::logging::MatchingLogger;
use crate::utils::pipeline_state::{
    batch_check_comparison_cache, batch_check_entity_completion_status, batch_get_current_signatures_for_pairs, batch_mark_entity_completion, batch_store_in_comparison_cache, ComparisonCacheEntry, EntityCompletionCheck, EntitySignature as SignatureData
};
use crate::utils::constants::MAX_DISTANCE_FOR_SAME_CITY_METERS;

// NEW IMPORTS: Progress callback functionality
use crate::utils::progress_bars::progress_callback::ProgressCallback;
use crate::{update_progress, update_detailed_progress};

/// Internal struct to hold data for a pair that needs to be processed,
/// including all information required for DB upsert and decision detail insert.
struct PairToProcessPhone {
    entity_id_1: String, // Ordered
    entity_id_2: String, // Ordered
    match_values: MatchValues,
    pre_rl_confidence_score: f64,
    final_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    original_signature_1: Option<SignatureData>,
    original_signature_2: Option<SignatureData>,
    comparison_cache_hit: bool,
}

// Add a struct to hold phone data with location info
struct PhoneInfo {
    entity_id: String,
    number: String,
    extension: Option<String>,
    normalized_phone: String,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

// UPDATED FUNCTION SIGNATURE: Added ProgressCallback parameter
#[allow(clippy::too_many_arguments)]
pub async fn find_matches(
    pool: &PgPool,
    reinforcement_orchestrator_option: Option<Arc<Mutex<RLOrchestrator>>>,
    pipeline_run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    progress_callback: Option<ProgressCallback>, // NEW PARAMETER
) -> Result<AnyMatchResult> {
    let logger = MatchingLogger::new(MatchMethodType::Phone);
    logger.log_start(pipeline_run_id, reinforcement_orchestrator_option.is_some());

    // PROGRESS UPDATE: Starting phase
    update_progress!(progress_callback, "Starting", "Initializing phone matching");

    let initial_conn = pool
        .get()
        .await
        .context("Phone: Failed to get DB connection for initial queries")?;

    logger.log_phase("Loading existing pairs", Some("querying entity_group table"));
    update_progress!(progress_callback, "Loading existing pairs", "querying entity_group table");
    
    debug!("Phone: Fetching existing phone-matched pairs from entity_group...");
    let existing_entity_groups = fetch_existing_entity_groups(&*initial_conn).await?;
    logger.log_existing_pairs(existing_entity_groups.len());

    // PROGRESS UPDATE: Existing pairs loaded
    update_progress!(progress_callback, "Loading existing pairs", 
        format!("Found {} existing pairs", existing_entity_groups.len()));

    // Phase 1 Changes: Entity Filtering
    logger.log_phase("Loading phone data", Some("querying entities with phone numbers (excluding completed)"));
    update_progress!(progress_callback, "Loading phone data", "querying entities with phone numbers");
    
    // Step 1: Get all potential entity IDs with phones
    let potential_entities_query = "
        SELECT DISTINCT e.id
        FROM public.entity e
        JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'phone'
        JOIN public.phone p ON ef.table_id = p.id
        WHERE p.number IS NOT NULL AND p.number != ''";
    let potential_entity_rows = initial_conn
        .query(potential_entities_query, &[])
        .await
        .context("Phone: Failed to query potential entities with phones")?;
    let all_potential_entity_ids: Vec<String> = potential_entity_rows
        .iter()
        .map(|row| row.get::<_, String>("id"))
        .collect();
    logger.log_debug(&format!("Phone: Found {} entities with phones", all_potential_entity_ids.len()));

    // PROGRESS UPDATE: Potential entities found
    update_progress!(progress_callback, "Loading phone data", 
        format!("Found {} entities with phones", all_potential_entity_ids.len()));

    // Step 2: Check completion status
    update_progress!(progress_callback, "Loading phone data", "checking completion status");
    let completion_status: HashMap<String, EntityCompletionCheck> = batch_check_entity_completion_status(
        pool,
        &all_potential_entity_ids,
        &MatchMethodType::Phone
    ).await?;

    // Step 3: Filter out completed entities
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
        "Phone: {} total entities, {} already complete, {} to process",
        all_potential_entity_ids.len(),
        completed_count,
        incomplete_entity_ids.len()
    ));

    // PROGRESS UPDATE: Completion status results
    update_progress!(progress_callback, "Loading phone data", 
        format!("{} to process ({} already complete)", incomplete_entity_ids.len(), completed_count));

    // Step 4: Load detailed data only for incomplete entities
    let phone_rows = if incomplete_entity_ids.is_empty() {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No incomplete entities to process");
        
        // Change B: Early Return #1 (no incomplete entities)
        return Ok(AnyMatchResult::Phone(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Phone,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: completed_count,
                entities_total: total_potential_entities,
            },
        }));
    } else {
        update_progress!(progress_callback, "Loading phone data", "querying phone records");
        
        let phone_query = "
            SELECT e.id as entity_id, p.number, p.extension,
                   l.latitude, l.longitude
            FROM public.entity e
            JOIN public.entity_feature ef ON e.id = ef.entity_id AND ef.table_name = 'phone'
            JOIN public.phone p ON ef.table_id = p.id
            LEFT JOIN public.entity_feature ef_loc ON e.id = ef_loc.entity_id AND ef_loc.table_name = 'location'
            LEFT JOIN public.location l ON ef_loc.table_id = l.id
            WHERE e.id = ANY($1) AND p.number IS NOT NULL AND p.number != ''";
        initial_conn
            .query(phone_query, &[&incomplete_entity_ids])
            .await
            .context("Phone: Failed to query phones for incomplete entities")?
    };

    logger.log_data_loaded(phone_rows.len(), "phone");
    // PROGRESS UPDATE: Data loaded
    update_progress!(progress_callback, "Loading phone data", 
        format!("Loaded {} phone records", phone_rows.len()));
    
    drop(initial_conn); // Release connection

    logger.log_phase("Processing and normalizing phone numbers", None);
    update_progress!(progress_callback, "Processing phones", "normalizing phone numbers");

    // Create progress bar for phone processing
    let processing_pb = ProgressBar::new(phone_rows.len() as u64);
    processing_pb.set_style(
        ProgressStyle::default_bar()
            .template("  📞 [{elapsed_precise}] {bar:30.yellow/red} {pos}/{len} Processing phones...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );

    let mut phone_map: HashMap<String, Vec<PhoneInfo>> = HashMap::new();
    let mut duplicate_entries = 0;
    let mut invalid_formats = 0;

    // ENHANCED PROCESSING LOOP: Add detailed progress updates
    for (i, row) in phone_rows.iter().enumerate() {
        // PROGRESS UPDATE: Update every 500 items (smaller batches for phone processing)
        if i % 500 == 0 || i == phone_rows.len() - 1 {
            processing_pb.set_position(i as u64 + 1);
            update_detailed_progress!(progress_callback, "Processing phones", i + 1, phone_rows.len(),
                format!("{} unique phones found", phone_map.len()));
        }
        logger.log_progress_update(i + 1, phone_rows.len(), None);

        let entity_id: String = row.get("entity_id");
        let number: String = row.get("number");
        let extension: Option<String> = row
            .try_get("extension")
            .ok()
            .flatten()
            .filter(|s: &String| !s.is_empty());
        let latitude: Option<f64> = row.try_get("latitude").ok();
        let longitude: Option<f64> = row.try_get("longitude").ok();
        let normalized_phone = normalize_phone(&number);

        if normalized_phone.is_empty() {
            invalid_formats += 1;
            continue;
        }

        // Check if a PhoneInfo with this entity_id already exists for this normalized phone
        let entry = phone_map.entry(normalized_phone.clone()).or_default();
        if !entry.iter().any(|info| info.entity_id == entity_id) {
            entry.push(PhoneInfo {
                entity_id,
                number,
                extension,
                normalized_phone,
                latitude,
                longitude,
            });
        } else {
            duplicate_entries += 1;
        }
    }
    
    let groups_with_multiple = phone_map.values().filter(|entities| entities.len() >= 2).count();
    
    processing_pb.finish_with_message(format!(
        "📞 Processed {} phones into {} unique normalized numbers (groups)",
        phone_rows.len(),
        phone_map.len()
    ));

    logger.log_data_quality_issue("duplicate entity-phone combinations", duplicate_entries);
    logger.log_data_quality_issue("invalid phone formats", invalid_formats);
    logger.log_processing_complete(phone_rows.len(), phone_map.len(), groups_with_multiple);

    // PROGRESS UPDATE: Processing complete
    update_progress!(progress_callback, "Processing phones", 
        format!("Processed {} phones into {} groups", phone_rows.len(), phone_map.len()));

    logger.log_phase("Comparing phone pairs", Some("generating and filtering potential matches"));
    update_progress!(progress_callback, "Generating pairs", "creating candidate phone pairs");

    let mut all_pairs_to_process: Vec<PairToProcessPhone> = Vec::new();
    let mut processed_pairs_this_run: HashSet<(String, String)> = HashSet::new();
    let mut pairs_for_signature_fetch: Vec<(String, String)> = Vec::new();

    // Calculate total pairs for progress tracking
    let total_conceptual_pairs: usize = phone_map
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
                    "  📞 [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Generating pairs...");
        Some(pb)
    } else {
        None
    };

    let mut current_pair_count_for_pb = 0;
    let mut existing_skipped = 0;
    let mut geospatial_filtered = 0;

    for (normalized_shared_phone, current_entity_list) in phone_map {
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
                    if current_pair_count_for_pb % 100 == 0 || current_pair_count_for_pb == total_conceptual_pairs {
                        pb.set_position(current_pair_count_for_pb as u64);
                        pb.set_message(format!(
                            "Generated {} pairs...",
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

                let (e1_id, e1_orig_phone, e1_orig_ext, e1_lat, e1_lon, e2_id, e2_orig_phone, e2_orig_ext, e2_lat, e2_lon) =
                    if e1_info.entity_id < e2_info.entity_id {
                        (
                            &e1_info.entity_id,
                            &e1_info.number,
                            &e1_info.extension,
                            e1_info.latitude,
                            e1_info.longitude,
                            &e2_info.entity_id,
                            &e2_info.number,
                            &e2_info.extension,
                            e2_info.latitude,
                            e2_info.longitude,
                        )
                    } else {
                        (
                            &e2_info.entity_id,
                            &e2_info.number,
                            &e2_info.extension,
                            e2_info.latitude,
                            e2_info.longitude,
                            &e1_info.entity_id,
                            &e1_info.number,
                            &e1_info.extension,
                            e1_info.latitude,
                            e1_info.longitude,
                        )
                    };

                let current_pair_ordered = (e1_id.clone(), e2_id.clone());

                if existing_entity_groups.contains(&current_pair_ordered)
                    || processed_pairs_this_run.contains(&current_pair_ordered)
                {
                    existing_skipped += 1;
                    debug!("Phone: Pair ({}, {}) already in entity_group or processed this run. Skipping.", e1_id, e2_id);
                    continue;
                }

                // --- Geospatial Filtering ---
                if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) =
                    (e1_lat, e1_lon, e2_lat, e2_lon)
                {
                    let distance = calculate_distance(lat1, lon1, lat2, lon2);
                    if distance > MAX_DISTANCE_FOR_SAME_CITY_METERS {
                        geospatial_filtered += 1;
                        debug!("Phone: Pair ({}, {}) skipped due to large geospatial distance ({}m > {}m).", e1_id, e2_id, distance, MAX_DISTANCE_FOR_SAME_CITY_METERS);
                        processed_pairs_this_run.insert(current_pair_ordered);
                        continue;
                    }
                }

                pairs_for_signature_fetch.push(current_pair_ordered.clone());

                let base_confidence = if e1_orig_ext == e2_orig_ext {
                    0.95
                } else {
                    0.85
                };
                let pre_rl_confidence = base_confidence;

                let match_values_obj = MatchValues::Phone(PhoneMatchDetail {
                    values: PhoneMatchValues {
                        normalized_phone1: normalize_phone(e1_orig_phone),
                        normalized_phone2: normalize_phone(e2_orig_phone),
                        matched_normalized_phone: normalized_shared_phone.clone(),
                        extension1: e1_orig_ext.clone(),
                        extension2: e2_orig_ext.clone(),
                    },
                    original1: e1_orig_phone.clone(),
                    original2: e2_orig_phone.clone(),
                    normalized_shared: normalized_shared_phone.clone(),
                    original_ext1: e1_orig_ext.clone(),
                    original_ext2: e2_orig_ext.clone(),
                });

                all_pairs_to_process.push(PairToProcessPhone {
                    entity_id_1: e1_id.clone(),
                    entity_id_2: e2_id.clone(),
                    match_values: match_values_obj,
                    pre_rl_confidence_score: pre_rl_confidence,
                    final_confidence_score: pre_rl_confidence, // Initial, will be tuned
                    features_for_snapshot: None,
                    original_signature_1: None,
                    original_signature_2: None,
                    comparison_cache_hit: false,
                });
                processed_pairs_this_run.insert(current_pair_ordered);
            }
        }
    }
    
    if let Some(pb) = &pairs_pb {
        pb.finish_with_message(format!(
            "📞 Generated {} candidate pairs after filtering",
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
        return Ok(AnyMatchResult::Phone(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Phone,
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
                MatchMethodType::Phone,
            ));
        }
    }

    update_progress!(progress_callback, "Cache checking", "checking comparison cache");
    let cache_results_map = batch_check_comparison_cache(pool, &cache_check_inputs).await?;

    let mut pairs_to_process_after_cache: Vec<PairToProcessPhone> = Vec::new();
    let mut cache_hits_count = 0;

    for mut pair_data in all_pairs_to_process {
        let pair_key = (pair_data.entity_id_1.clone(), pair_data.entity_id_2.clone());
        if let Some(_cached_eval) = cache_results_map.get(&pair_key) {
            cache_hits_count += 1;
            pair_data.comparison_cache_hit = true;
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

    // Add evaluation progress bar
    let eval_pb = ProgressBar::new(pairs_to_process_after_cache.len() as u64);
    eval_pb.set_style(
        ProgressStyle::default_bar()
            .template("  📞 [{elapsed_precise}] {bar:30.cyan/blue} {pos}/{len} Evaluating pairs...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );
    eval_pb.set_message(format!(
        "Evaluating {} pairs...",
        pairs_to_process_after_cache.len()
    ));

    const BATCH_DB_OPS_SIZE: usize = 1000;
    logger.log_batch_processing_start(pairs_to_process_after_cache.len(), BATCH_DB_OPS_SIZE);
    // PROGRESS UPDATE: Batch processing start
    update_progress!(progress_callback, "Batch processing", 
        format!("Processing {} pairs in batches of {}", pairs_to_process_after_cache.len(), BATCH_DB_OPS_SIZE));

    let mut final_groups_created_count = 0;
    let mut final_entities_in_new_pairs: HashSet<String> = HashSet::new();
    let mut final_confidence_scores_for_stats: Vec<f64> = Vec::new();
    let mut individual_operation_errors = 0;
    let mut feature_extraction_attempted = 0;
    let mut feature_extraction_successful = 0;
    let mut feature_extraction_failed = 0;
    let mut processed_pairs = 0;
    let total_pairs = pairs_to_process_after_cache.len();

    let total_batches = (pairs_to_process_after_cache.len() + BATCH_DB_OPS_SIZE - 1) / BATCH_DB_OPS_SIZE;
    for (batch_idx, chunk) in pairs_to_process_after_cache.chunks_mut(BATCH_DB_OPS_SIZE).enumerate() { // Changed to chunks_mut
        logger.log_batch_progress(batch_idx + 1, total_batches, chunk.len());
        // PROGRESS UPDATE: Batch progress
        update_detailed_progress!(progress_callback, "Batch processing", 
            batch_idx + 1, total_batches, format!("{} pairs in this batch", chunk.len()));
        
        let mut batch_entity_group_data = Vec::new();
        let mut batch_decision_detail_data = Vec::new();
        let mut batch_cache_store_data = Vec::new();
        let mut chunk_len = chunk.len();

        for (pair_idx, pair_data) in chunk.iter_mut().enumerate() { // Iterate mutably here
            let mut final_confidence_score = pair_data.pre_rl_confidence_score;
            let mut features_vec_for_rl: Option<Vec<f64>> = None;
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
                    Ok(features_vec) => {
                        if !features_vec.is_empty() {
                            feature_extraction_successful += 1;
                            features_vec_for_rl = Some(features_vec.clone());
                            features_json_for_cache =
                                serde_json::to_value(features_vec.clone()).ok();
                            let orchestrator_guard = ro_arc.lock().await;
                            match orchestrator_guard.get_tuned_confidence(
                                &MatchMethodType::Phone,
                                pair_data.pre_rl_confidence_score,
                                features_vec_for_rl.as_ref().unwrap(),
                            ) {
                                Ok(tuned_score) => final_confidence_score = tuned_score,
                                Err(e) => logger.log_warning(&format!(
                                    "RL tuning failed for ({}, {}): {}. Using pre-RL score.",
                                    pair_data.entity_id_1, pair_data.entity_id_2, e
                                )),
                            }
                        } else {
                            logger.log_warning(&format!(
                                "Empty features vector for ({}, {}). Using pre-RL score.",
                                pair_data.entity_id_1, pair_data.entity_id_2
                            ));
                        }
                    }
                    Err(e) => {
                        feature_extraction_failed += 1;
                        logger.log_warning(&format!(
                            "Feature extraction failed for ({}, {}): {}. Using pre-RL score.",
                            pair_data.entity_id_1, pair_data.entity_id_2, e
                        ));
                    }
                }
            }
            // Update final_confidence_score on the pair_data struct before batching
            pair_data.final_confidence_score = final_confidence_score;
            pair_data.features_for_snapshot = features_vec_for_rl; // Store the features for later use

            // Prepare data for batch upsert
            let new_entity_group_id_str = Uuid::new_v4().to_string();
            batch_entity_group_data.push(EntityGroupBatchData {
                proposed_id: new_entity_group_id_str.clone(),
                entity_id_1: pair_data.entity_id_1.clone(),
                entity_id_2: pair_data.entity_id_2.clone(),
                confidence_score: pair_data.final_confidence_score, // Use the potentially tuned score
                pre_rl_confidence_score: pair_data.pre_rl_confidence_score,
                method_type: MatchMethodType::Phone,
                match_values: pair_data.match_values.clone(),
            });

            // Prepare data for batch cache store
            if let (Some(sig1_data), Some(sig2_data)) = (
                &pair_data.original_signature_1,
                &pair_data.original_signature_2,
            ) {
                batch_cache_store_data.push(ComparisonCacheEntry {
                    entity_id_1: pair_data.entity_id_1.clone(),
                    entity_id_2: pair_data.entity_id_2.clone(),
                    signature_1: sig1_data.signature.clone(),
                    signature_2: sig2_data.signature.clone(),
                    method_type: MatchMethodType::Phone,
                    pipeline_run_id: pipeline_run_id.to_string(),
                    comparison_result: "MATCH".to_string(),
                    similarity_score: Some(pair_data.final_confidence_score),
                    features_snapshot: features_json_for_cache.clone(),
                });
            }

            // Update progress bar within the batch loop
            processed_pairs += 1;
            if pair_idx % 50 == 0 || pair_idx == chunk_len - 1 {
                eval_pb.set_position(processed_pairs as u64);
                // PROGRESS UPDATE: Detailed evaluation progress
                update_detailed_progress!(progress_callback, "Evaluating pairs", 
                    processed_pairs, total_pairs,
                    format!("RL features: {}/{}", feature_extraction_successful, feature_extraction_attempted));
            }
        } // End of chunk iteration

        // Execute batch DB operations
        let upsert_results = batch_upsert_entity_groups(pool, batch_entity_group_data).await;

        match upsert_results {
            Ok(results_map) => {
                for data in &mut *chunk {
                    let pair_key = (data.entity_id_1.clone(), data.entity_id_2.clone());
                    if let Some((group_id, was_newly_inserted)) = results_map.get(&pair_key) {
                        if *was_newly_inserted {
                            final_groups_created_count += 1;
                            final_entities_in_new_pairs.insert(data.entity_id_1.clone());
                            final_entities_in_new_pairs.insert(data.entity_id_2.clone());
                            final_confidence_scores_for_stats.push(data.final_confidence_score);

                            if let (Some(ro_arc), Some(features_vec)) = (
                                reinforcement_orchestrator_option.as_ref(),
                                &data.features_for_snapshot,
                            ) {
                                let version = ro_arc.lock().await.confidence_tuner.version;
                                let snapshot_features_json = serde_json::to_value(features_vec)
                                    .unwrap_or(serde_json::Value::Null);
                                batch_decision_detail_data.push(MatchDecisionDetailBatchData {
                                    entity_group_id: group_id.clone(),
                                    pipeline_run_id: pipeline_run_id.to_string(),
                                    snapshotted_features: snapshot_features_json,
                                    method_type_at_decision: MatchMethodType::Phone
                                        .as_str()
                                        .to_string(),
                                    pre_rl_confidence_at_decision: data.pre_rl_confidence_score,
                                    tuned_confidence_at_decision: data.final_confidence_score,
                                    confidence_tuner_version_at_decision: Some(version as i32),
                                });
                            }
                        }
                    }
                }
            }
            Err(e) => {
                logger.log_warning(&format!("Batch upsert failed: {}", e));
                individual_operation_errors += chunk.len();
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
        "📞 Phone matching complete: {} new groups created",
        final_groups_created_count
    ));

    let avg_confidence: f64 = if !final_confidence_scores_for_stats.is_empty() {
        final_confidence_scores_for_stats.iter().sum::<f64>()
            / final_confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Phone,
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

    // Phase 2: Mark entities as complete for Phone matching
    logger.log_phase("Tracking completion status", Some("marking entities with no remaining phone comparisons"));
    update_progress!(progress_callback, "Completion tracking", "marking completed entities");
    
    let mut entity_comparison_counts: HashMap<String, i32> = HashMap::new();
    let mut entities_with_comparisons: HashSet<String> = HashSet::new();
    for pair_data in &pairs_to_process_after_cache {
        *entity_comparison_counts.entry(pair_data.entity_id_1.clone()).or_insert(0) += 1;
        *entity_comparison_counts.entry(pair_data.entity_id_2.clone()).or_insert(0) += 1;
        entities_with_comparisons.insert(pair_data.entity_id_1.clone());
        entities_with_comparisons.insert(pair_data.entity_id_2.clone());
    }

    let mut completion_batch: Vec<(String, MatchMethodType, String, String, i32)> = Vec::new();
    for entity_id in entities_with_comparisons {
        if let Some(status) = completion_status.get(&entity_id) {
            if let Some(current_sig) = &status.current_signature {
                let comparison_count = entity_comparison_counts.get(&entity_id).copied().unwrap_or(0);
                completion_batch.push((
                    entity_id.clone(),
                    MatchMethodType::Phone,
                    pipeline_run_id.to_string(),
                    current_sig.clone(),
                    comparison_count,
                ));
            }
        }
    }

    if !completion_batch.is_empty() {
        if let Err(e) = batch_mark_entity_completion(pool, &completion_batch).await {
            logger.log_warning(&format!(
                "Failed to batch mark {} entities as complete: {}",
                completion_batch.len(), e
            ));
        } else {
            logger.log_debug(&format!(
                "Marked {} entities as complete for Phone matching",
                completion_batch.len()
            ));
        }
    }

    // FINAL PROGRESS UPDATE: Completion
    update_progress!(progress_callback, "Completed", 
        format!("{} phone groups created, {} entities matched", 
                final_groups_created_count, final_entities_in_new_pairs.len()));

    Ok(AnyMatchResult::Phone(MatchResult {
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

async fn fetch_existing_entity_groups(
    conn: &impl tokio_postgres::GenericClient,
) -> Result<HashSet<(String, String)>> {
    let query = "SELECT entity_id_1, entity_id_2 FROM public.entity_group WHERE method_type = $1";
    let rows = conn
        .query(query, &[&MatchMethodType::Phone.as_str()])
        .await
        .context("Phone: Failed to query existing entity_groups")?;

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

pub fn normalize_phone(phone: &str) -> String {
    let digits_only: String = phone.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits_only.len() == 11 && digits_only.starts_with('1') {
        return digits_only[1..].to_string();
    }
    if (7..=15).contains(&digits_only.len()) {
        return digits_only;
    }
    debug!(
        "Phone number '{}' normalized to '{}', considered invalid for matching.",
        phone, digits_only
    );
    String::new()
}