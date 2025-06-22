// src/matching/email.rs - COMPLETE EXAMPLE with Progress Callback Integration
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
use crate::models::matching::{AnyMatchResult, EmailMatchDetail, EmailMatchValues, MatchResult, MatchValues};
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
struct PairToProcessEmail {
    entity_id_1: String, // Ordered
    entity_id_2: String, // Ordered
    match_values: MatchValues,
    pre_rl_confidence_score: f64,
    final_confidence_score: f64,
    features_for_snapshot: Option<Vec<f64>>,
    original_signature_1: Option<SignatureData>,
    original_signature_2: Option<SignatureData>,
    normalized_shared_email: String,
    comparison_cache_hit: bool,
}

// Add a struct to hold email data with location info
struct EmailInfo {
    entity_id: String,
    original_email: String,
    normalized_email: String,
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
    let logger = MatchingLogger::new(MatchMethodType::Email);
    logger.log_start(pipeline_run_id, reinforcement_orchestrator_option.is_some());
    
    // PROGRESS UPDATE: Starting phase
    update_progress!(progress_callback, "Starting", "Initializing email matching");

    let initial_read_conn = pool
        .get()
        .await
        .context("Email: Failed to get DB connection for initial reads")?;

    // ENHANCED PHASE LOGGING: Combine logger with progress callback
    logger.log_phase("Loading existing pairs", Some("querying entity_group table"));
    update_progress!(progress_callback, "Loading existing pairs", "querying entity_group table");
    
    let existing_entity_groups: HashSet<(String, String)> =
        fetch_existing_pairs(&*initial_read_conn, MatchMethodType::Email).await?;
    logger.log_existing_pairs(existing_entity_groups.len());
    
    // PROGRESS UPDATE: Include results in progress
    update_progress!(progress_callback, "Loading existing pairs", 
        format!("Found {} existing pairs", existing_entity_groups.len()));

    // Phase 1 Changes: Entity Filtering
    logger.log_phase("Loading email data", Some("querying entities with email addresses (excluding completed)"));
    update_progress!(progress_callback, "Loading email data", "checking entity completion status");
    
    // Step 1: Get all potential entity IDs with emails
    let potential_entities_query = "
        WITH EntityEmails AS (
            SELECT DISTINCT e.id as entity_id
            FROM entity e
            JOIN organization o ON e.organization_id = o.id
            WHERE o.email IS NOT NULL AND o.email != ''
            UNION
            SELECT DISTINCT e.id as entity_id
            FROM public.entity e
            JOIN public.entity_feature ef_svc ON e.id = ef_svc.entity_id AND ef_svc.table_name = 'service'
            JOIN public.service s ON ef_svc.table_id = s.id
            WHERE s.email IS NOT NULL AND s.email != ''
        )
        SELECT entity_id FROM EntityEmails";
    let potential_entity_rows = initial_read_conn
        .query(potential_entities_query, &[])
        .await
        .context("Email: Failed to query potential entities with emails")?;
    let all_potential_entity_ids: Vec<String> = potential_entity_rows
        .iter()
        .map(|row| row.get::<_, String>("entity_id"))
        .collect();
    logger.log_debug(&format!("Email: Found {} entities with emails", all_potential_entity_ids.len()));
    
    // PROGRESS UPDATE: Found potential entities
    update_progress!(progress_callback, "Loading email data", 
        format!("Found {} entities with emails", all_potential_entity_ids.len()));

    // Step 2: Check completion status
    update_progress!(progress_callback, "Loading email data", "checking completion status");
    let completion_status: HashMap<String, EntityCompletionCheck> = batch_check_entity_completion_status(
        pool,
        &all_potential_entity_ids,
        &MatchMethodType::Email
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
    let total_potential_entities = all_potential_entity_ids.len();
    logger.log_debug(&format!(
        "Email: {} total entities, {} already complete, {} to process",
        all_potential_entity_ids.len(),
        completed_count,
        incomplete_entity_ids.len()
    ));
    
    // PROGRESS UPDATE: Completion status results
    update_progress!(progress_callback, "Loading email data", 
        format!("{} to process ({} already complete)", incomplete_entity_ids.len(), completed_count));

    // Step 4: Load detailed data only for incomplete entities
    let email_rows = if incomplete_entity_ids.is_empty() {
        // PROGRESS UPDATE: Early return case
        update_progress!(progress_callback, "Completed", "No incomplete entities to process");
        
        // Early Return: No incomplete entities
        return Ok(AnyMatchResult::Email(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Email,
                groups_created: 0,
                entities_matched: 0,
                avg_confidence: 0.0,
                avg_group_size: 0.0,
                entities_skipped_complete: completed_count,
                entities_total: total_potential_entities,
            },
        }));
    } else {
        update_progress!(progress_callback, "Loading email data", "querying email records");
        
        let email_query = "
            SELECT 'organization' as source, e.id as entity_id, o.email, e.name as entity_name,
                   l.latitude, l.longitude
            FROM entity e
            JOIN organization o ON e.organization_id = o.id
            LEFT JOIN public.entity_feature ef_loc ON e.id = ef_loc.entity_id AND ef_loc.table_name = 'location'
            LEFT JOIN public.location l ON ef_loc.table_id = l.id
            WHERE e.id = ANY($1) AND o.email IS NOT NULL AND o.email != ''
            UNION ALL
            SELECT 'service' as source, e.id as entity_id, s.email, e.name as entity_name,
                   l.latitude, l.longitude
            FROM public.entity e
            JOIN public.entity_feature ef_svc ON e.id = ef_svc.entity_id AND ef_svc.table_name = 'service'
            JOIN public.service s ON ef_svc.table_id = s.id
            LEFT JOIN public.entity_feature ef_loc ON e.id = ef_loc.entity_id AND ef_loc.table_name = 'location'
            LEFT JOIN public.location l ON ef_loc.table_id = l.id
            WHERE e.id = ANY($1) AND s.email IS NOT NULL AND s.email != ''";
        initial_read_conn
            .query(email_query, &[&incomplete_entity_ids])
            .await
            .context("Email: Failed to query emails for incomplete entities")?
    };

    logger.log_data_loaded(email_rows.len(), "email");
    // PROGRESS UPDATE: Data loaded
    update_progress!(progress_callback, "Loading email data", 
        format!("Loaded {} email records", email_rows.len()));
    
    drop(initial_read_conn); // Release connection

    logger.log_phase("Processing and normalizing emails", None);
    update_progress!(progress_callback, "Processing emails", "normalizing email addresses");

    // Create progress bar for email processing with simpler styling
    let processing_pb = ProgressBar::new(email_rows.len() as u64);
    processing_pb.set_style(
        ProgressStyle::default_bar()
            .template("  📧 [{elapsed_precise}] {bar:30.yellow/red} {pos}/{len} Processing emails...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );

    let mut email_map: HashMap<String, Vec<EmailInfo>> = HashMap::new();
    let mut duplicate_entries = 0;
    let mut invalid_formats = 0;

    // ENHANCED PROCESSING LOOP: Add detailed progress updates
    for (i, row) in email_rows.iter().enumerate() {
        // PROGRESS UPDATE: Update every 1000 items or on completion
        if i % 1000 == 0 || i == email_rows.len() - 1 {
            processing_pb.set_position(i as u64 + 1);
            update_detailed_progress!(progress_callback, "Processing emails", i + 1, email_rows.len(),
                format!("{} unique emails found", email_map.len()));
        }
        logger.log_progress_update(i + 1, email_rows.len(), None);

        let entity_id: String = row.get("entity_id");
        let email: String = row.get("email");
        let latitude: Option<f64> = row.try_get("latitude").ok();
        let longitude: Option<f64> = row.try_get("longitude").ok();
        let normalized = normalize_email(&email);
        
        if normalized.is_empty() {
            invalid_formats += 1;
            continue;
        }

        // Check if an EmailInfo with this entity_id already exists for this normalized email
        let entry = email_map.entry(normalized.clone()).or_default();
        if !entry.iter().any(|info| info.entity_id == entity_id) {
            entry.push(EmailInfo {
                entity_id,
                original_email: email,
                normalized_email: normalized,
                latitude,
                longitude,
            });
        } else {
            duplicate_entries += 1;
        }
    }

    let email_frequency = calculate_email_frequency(&email_map);
    let groups_with_multiple = email_map.values().filter(|entities| entities.len() >= 2).count();
    
    processing_pb.finish_with_message(format!(
        "📧 Processed {} emails → {} groups ({} with multiple entities)",
        email_rows.len(),
        email_map.len(),
        groups_with_multiple
    ));

    logger.log_data_quality_issue("duplicate entity-email combinations", duplicate_entries);
    logger.log_data_quality_issue("invalid email formats", invalid_formats);
    logger.log_processing_complete(email_rows.len(), email_map.len(), groups_with_multiple);
    
    // PROGRESS UPDATE: Processing complete
    update_progress!(progress_callback, "Processing emails", 
        format!("Processed {} emails into {} groups", email_rows.len(), email_map.len()));

    logger.log_phase("Comparing email pairs", Some("generating and filtering potential matches"));
    update_progress!(progress_callback, "Generating pairs", "creating candidate email pairs");

    let mut all_pairs_to_process: Vec<PairToProcessEmail> = Vec::new();
    let mut processed_pairs_this_run: HashSet<(String, String)> = HashSet::new();
    let mut pairs_for_signature_fetch: Vec<(String, String)> = Vec::new();

    // Calculate total pairs for progress tracking
    let total_conceptual_pairs: usize = email_map
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
                    "  📧 [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} {msg}",
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

    for (normalized_shared_email, current_entity_list) in email_map {
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
                        update_detailed_progress!(progress_callback, "Generating pairs", 
                            current_pair_count_for_pb, total_conceptual_pairs,
                            format!("{} candidates created", all_pairs_to_process.len()));
                    }
                }

                let (e1_id, e1_orig_email, e1_lat, e1_lon, e2_id, e2_orig_email, e2_lat, e2_lon) =
                    if e1_info.entity_id < e2_info.entity_id {
                        (
                            &e1_info.entity_id,
                            &e1_info.original_email,
                            e1_info.latitude,
                            e1_info.longitude,
                            &e2_info.entity_id,
                            &e2_info.original_email,
                            e2_info.latitude,
                            e2_info.longitude,
                        )
                    } else {
                        (
                            &e2_info.entity_id,
                            &e2_info.original_email,
                            e2_info.latitude,
                            e2_info.longitude,
                            &e1_info.entity_id,
                            &e1_info.original_email,
                            e1_info.latitude,
                            e1_info.longitude,
                        )
                    };
                let current_pair_ordered = (e1_id.clone(), e2_id.clone());

                if existing_entity_groups.contains(&current_pair_ordered)
                    || processed_pairs_this_run.contains(&current_pair_ordered)
                {
                    existing_skipped += 1;
                    debug!("Email: Pair ({}, {}) already in entity_group or processed this run. Skipping.", e1_id, e2_id);
                    continue;
                }

                // --- Geospatial Filtering ---
                if let (Some(lat1), Some(lon1), Some(lat2), Some(lon2)) =
                    (e1_lat, e1_lon, e2_lat, e2_lon)
                {
                    let distance = calculate_distance(lat1, lon1, lat2, lon2);
                    if distance > MAX_DISTANCE_FOR_SAME_CITY_METERS {
                        geospatial_filtered += 1;
                        debug!("Email: Pair ({}, {}) skipped due to large geospatial distance ({}m > {}m).", e1_id, e2_id, distance, MAX_DISTANCE_FOR_SAME_CITY_METERS);
                        processed_pairs_this_run.insert(current_pair_ordered);
                        continue;
                    }
                }

                pairs_for_signature_fetch.push(current_pair_ordered.clone());

                let mut pre_rl_confidence_score = 1.0; // Default high for email
                if is_generic_organizational_email(&normalized_shared_email) {
                    pre_rl_confidence_score *= 0.9;
                }
                let email_count = email_frequency
                    .get(&normalized_shared_email)
                    .cloned()
                    .unwrap_or(1);
                if email_count > 10 {
                    pre_rl_confidence_score *= 0.85;
                } else if email_count > 5 {
                    pre_rl_confidence_score *= 0.92;
                }

                let match_values_obj = MatchValues::Email(EmailMatchDetail {
                    values: EmailMatchValues {
                        normalized_email1: normalize_email(e1_orig_email),
                        normalized_email2: normalize_email(e2_orig_email),
                        matched_normalized_email: normalized_shared_email.clone(),
                    },
                    original1: e1_orig_email.clone(),
                    original2: e2_orig_email.clone(),
                    normalized_shared: normalized_shared_email.clone(),
                });

                all_pairs_to_process.push(PairToProcessEmail {
                    entity_id_1: e1_id.clone(),
                    entity_id_2: e2_id.clone(),
                    match_values: match_values_obj,
                    pre_rl_confidence_score,
                    final_confidence_score: pre_rl_confidence_score, // Initial, will be tuned
                    features_for_snapshot: None,
                    original_signature_1: None,
                    original_signature_2: None,
                    normalized_shared_email: normalized_shared_email.clone(),
                    comparison_cache_hit: false,
                });
                processed_pairs_this_run.insert(current_pair_ordered);
            }
        }
    }
    
    if let Some(pb) = &pairs_pb {
        pb.finish_with_message(format!(
            "📧 Generated {} candidate pairs after filtering",
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
        
        // Early Return: No pairs to process
        return Ok(AnyMatchResult::Email(MatchResult {
            groups_created: 0,
            stats: MatchMethodStats {
                method_type: MatchMethodType::Email,
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
    
    // Create progress bar for the evaluation phase
    let eval_pb = ProgressBar::new(all_pairs_to_process.len() as u64);
    eval_pb.set_style(
        ProgressStyle::default_bar()
            .template("  📧 [{elapsed_precise}] {bar:30.cyan/blue} {pos}/{len} Evaluating pairs...")
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ")
    );
    
    let signatures_map =
        batch_get_current_signatures_for_pairs(pool, &pairs_for_signature_fetch).await?;

    let mut cache_check_inputs = Vec::new();
    // Iterate mutably over `all_pairs_to_process` to update `original_signature_1/2`
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
                MatchMethodType::Email,
            ));
        }
    }

    update_progress!(progress_callback, "Cache checking", "checking comparison cache");
    let cache_results_map = batch_check_comparison_cache(pool, &cache_check_inputs).await?;

    let mut pairs_to_process_after_cache: Vec<PairToProcessEmail> = Vec::new();
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
    update_progress!(progress_callback, "Evaluating pairs", "running confidence analysis and batch processing");
    
    eval_pb.set_length(pairs_to_process_after_cache.len() as u64);

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
    let total_pairs = pairs_to_process_after_cache.len();  // Store the total length before iteration

    // Use .chunks_mut() to get mutable slices, allowing modification of PairToProcessEmail
    let total_batches = (pairs_to_process_after_cache.len() + BATCH_DB_OPS_SIZE - 1) / BATCH_DB_OPS_SIZE;
    for (batch_idx, chunk) in pairs_to_process_after_cache.chunks_mut(BATCH_DB_OPS_SIZE).enumerate() {
        logger.log_batch_progress(batch_idx + 1, total_batches, chunk.len());
        // PROGRESS UPDATE: Batch progress
        update_detailed_progress!(progress_callback, "Batch processing", 
            batch_idx + 1, total_batches, format!("{} pairs in this batch", chunk.len()));
        
        let chunk_len = chunk.len(); // Store length here before iteration consumes `chunk`
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
        let mut batch_decision_detail_data = Vec::new();

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
                                &MatchMethodType::Email,
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
            let new_entity_group_id_str = Uuid::new_v4().to_string();
            batch_entity_group_data.push(EntityGroupBatchData {
                proposed_id: new_entity_group_id_str.clone(),
                entity_id_1: pair_data.entity_id_1.clone(),
                entity_id_2: pair_data.entity_id_2.clone(),
                confidence_score: pair_data.final_confidence_score,
                pre_rl_confidence_score: pair_data.pre_rl_confidence_score,
                method_type: MatchMethodType::Email,
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
                        MatchMethodType::Email.as_str().to_string(),
                        pair_data.pre_rl_confidence_score,
                        pair_data.final_confidence_score,
                        Some(version as i32),
                    ));
                }
            }

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
                    method_type: MatchMethodType::Email,
                    pipeline_run_id: pipeline_run_id.to_string(),
                    comparison_result: "MATCH".to_string(),
                    similarity_score: Some(pair_data.final_confidence_score),
                    features_snapshot: features_json_for_cache.clone(),
                });
            }

            // Update progress bar every 10 pairs or on last pair in batch
            processed_pairs += 1;
            if pair_idx % 10 == 0 || pair_idx == chunk_len - 1 {
                eval_pb.set_position(processed_pairs as u64);
                // PROGRESS UPDATE: Detailed evaluation progress
                if pair_idx % 50 == 0 || pair_idx == chunk_len - 1 {
                    update_detailed_progress!(progress_callback, "Evaluating pairs", 
                        processed_pairs, total_pairs,
                        format!("RL features: {}/{}", feature_extraction_successful, feature_extraction_attempted));
                }
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
        "📧 Email matching complete: {} new groups created",
        final_groups_created_count
    ));

    let avg_confidence: f64 = if !final_confidence_scores_for_stats.is_empty() {
        final_confidence_scores_for_stats.iter().sum::<f64>()
            / final_confidence_scores_for_stats.len() as f64
    } else {
        0.0
    };

    let method_stats = MatchMethodStats {
        method_type: MatchMethodType::Email,
        groups_created: final_groups_created_count,
        entities_matched: final_entities_in_new_pairs.len(),
        avg_confidence,
        avg_group_size: if final_groups_created_count > 0 {
            2.0
        } else {
            0.0
        },
        entities_skipped_complete: completed_count,
        entities_total: total_potential_entities,
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

    // Phase 2: Mark entities as complete for Email matching
    logger.log_phase("Tracking completion status", Some("marking entities with no remaining email comparisons"));
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
                    MatchMethodType::Email,
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
                "Marked {} entities as complete for Email matching",
                completion_batch.len()
            ));
        }
    }

    // FINAL PROGRESS UPDATE: Completion
    update_progress!(progress_callback, "Completed", 
        format!("{} email groups created, {} entities matched", 
                final_groups_created_count, final_entities_in_new_pairs.len()));

    Ok(AnyMatchResult::Email(MatchResult {
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

fn calculate_email_frequency(
    email_map: &HashMap<String, Vec<EmailInfo>>,
) -> HashMap<String, usize> {
    email_map
        .iter()
        .map(|(k, v)| (k.clone(), v.len()))
        .collect()
}

pub fn normalize_email(email: &str) -> String {
    let email_trimmed = email.trim().to_lowercase();
    if !email_trimmed.contains('@') {
        return email_trimmed; // Or handle as invalid
    }
    let parts: Vec<&str> = email_trimmed.splitn(2, '@').collect();
    if parts.len() != 2 {
        return email_trimmed; // Or handle as invalid
    }
    let (local_part_full, domain_part) = (parts[0], parts[1]);

    // Remove part after '+'
    let local_part_no_plus = local_part_full.split('+').next().unwrap_or("").to_string();

    // Normalize domain (e.g., googlemail.com -> gmail.com)
    let final_domain_part = match domain_part {
        "googlemail.com" => "gmail.com",
        _ => domain_part,
    };

    // Remove dots from local part for Gmail addresses
    let final_local_part = if final_domain_part == "gmail.com" {
        local_part_no_plus.replace('.', "")
    } else {
        local_part_no_plus
    };

    if final_local_part.is_empty() {
        String::new() // Invalid or empty local part after normalization
    } else {
        format!("{}@{}", final_local_part, final_domain_part)
    }
}

fn is_generic_organizational_email(email: &str) -> bool {
    [
        "info@", "contact@", "office@", "admin@", "support@", "sales@", "hello@", "help@",
    ]
    .iter()
    .any(|prefix| email.starts_with(prefix))
}