// src/matching/manager.rs - Enhanced coordination with progress callback architecture
use crate::matching::address;
use crate::matching::email;
use crate::matching::name;
use crate::matching::phone;
use crate::matching::url;
use crate::models::matching::AnyMatchResult;
use crate::models::stats_models::{MatchMethodStats, MatchMethodType};
use crate::rl::feature_cache::SharedFeatureCache;
use crate::rl::orchestrator::RLOrchestrator;
use crate::utils::db_connect::{PgPool, get_pool_status};
use crate::utils::get_memory_usage;
use crate::utils::progress_bars::logging::{
    log_pipeline_start, log_pipeline_phase, log_pipeline_method_starting, 
    log_pipeline_method_completed, log_pipeline_method_failed, log_pipeline_progress_update, 
    log_pipeline_completion, log_feature_cache_stats
};
use crate::utils::progress_bars::progress_callback::{
    ProgressCallback, ProgressCallbackManager, TaskHealthMonitor, 
    MatchingTaskStatus, TaskStatus  // Import the types from progress_callback
};
use anyhow::Context;
use anyhow::Result;
use futures::future::join_all;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{info, warn, error, debug};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use std::collections::HashMap;

const MAX_CONCURRENT_MATCHING_TASKS: usize = 10;
const TASK_TIMEOUT_SECONDS: u64 = 1800; // 30 minutes per task
const HEALTH_CHECK_INTERVAL_SECONDS: u64 = 30;
const PROGRESS_UPDATE_INTERVAL_SECONDS: u64 = 5;

pub async fn run_entity_matching_pipeline(
    pool: &PgPool,
    rl_orchestrator: Arc<Mutex<RLOrchestrator>>,
    run_id: String,
    entity_feature_cache: SharedFeatureCache,
    multi_progress: Option<MultiProgress>,
) -> Result<(usize, Vec<MatchMethodStats>)> {
    let start_time_matching = Instant::now();
    
    // Initialize pipeline logging
    log_pipeline_start(&run_id, 5, MAX_CONCURRENT_MATCHING_TASKS, true);
    
    // Define matching methods in execution order
    let matching_methods = vec![
        MatchMethodType::Email,
        MatchMethodType::Phone,
        MatchMethodType::Url,
        MatchMethodType::Address,
        MatchMethodType::Name,
    ];

    // Create shared status tracker with enhanced tracking
    let status_tracker = Arc::new(Mutex::new(HashMap::<MatchMethodType, MatchingTaskStatus>::new()));
    
    // Create progress callback manager
    let callback_manager = Arc::new(ProgressCallbackManager::new(status_tracker.clone()));
    
    // Create health monitor
    let health_monitor = Arc::new(TaskHealthMonitor::new(
        status_tracker.clone(),
        Duration::from_secs(TASK_TIMEOUT_SECONDS),
    ));

    // Create main progress tracking for the matching phase
    let (matching_pb, status_pb) = if let Some(mp) = &multi_progress {
        let pb = mp.add(ProgressBar::new(matching_methods.len() as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "  {spinner:.blue} [{elapsed_precise}] {bar:30.green/blue} {pos}/{len} {msg}",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message("Preparing matching tasks...");
        
        let status = mp.add(ProgressBar::new_spinner());
        status.set_style(
            ProgressStyle::default_spinner()
                .template("    {spinner:.cyan} {msg}")
                .unwrap(),
        );
        status.set_message("Initializing matching methods...");
        
        (Some(pb), Some(status))
    } else {
        (None, None)
    };

    // Initialize status for each method with enhanced tracking
    log_pipeline_phase("Initialization", Some("setting up task tracking with progress callbacks"));
    {
        let mut tracker = status_tracker.lock().await;
        for method in &matching_methods {
            let mut status = MatchingTaskStatus::default();
            status.method_type = method.clone();
            status.processing_phase = "Queued".to_string();
            status.detailed_progress = "Waiting to start...".to_string();
            tracker.insert(method.clone(), status);
        }
    }
    
    info!("📋 Matching methods queued: {:?}", matching_methods);
    info!("🔧 Enhanced configuration:");
    info!("   • {} max simultaneous tasks", MAX_CONCURRENT_MATCHING_TASKS);
    info!("   • {} second timeout per task", TASK_TIMEOUT_SECONDS);
    info!("   • Progress callbacks enabled for real-time updates");
    info!("   • Resource monitoring enabled");

    let mut tasks: Vec<JoinHandle<Result<AnyMatchResult, anyhow::Error>>> = Vec::new();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_MATCHING_TASKS));

    // Start health monitoring task
    let health_monitor_task = {
        let monitor = Arc::clone(&health_monitor);
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            monitor.start_monitoring(pool_clone).await;
        })
    };

    // Enhanced status updater with resource monitoring
    let status_updater = if let Some(status_bar) = status_pb.clone() {
        let tracker_clone = Arc::clone(&status_tracker);
        let matching_pb_clone = matching_pb.clone();
        let pool_clone = pool.clone();
        
        Some(tokio::spawn(async move {
            let mut last_log_time = Instant::now();
            let mut last_logged_state = String::new();
            
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                
                // Get current resource stats
                let memory_mb = get_memory_usage().await;
                let (db_total, db_available, _) = get_pool_status(&pool_clone);
                let db_used = db_total - db_available;
                
                let tracker = tracker_clone.lock().await;
                let mut status_parts = Vec::new();
                let mut summary_parts = Vec::new();
                
                let mut running_count = 0;
                let mut completed_count = 0;
                let mut failed_count = 0;
                let mut timed_out_count = 0;
                let mut total_groups = 0;
                
                for (method, status) in tracker.iter() {
                    let elapsed = if let Some(end) = status.end_time {
                        format!("{:.1}s", (end - status.start_time).as_secs_f32())
                    } else {
                        format!("{:.1}s", status.start_time.elapsed().as_secs_f32())
                    };
                    
                    // Check for stale updates (might indicate hanging)
                    let time_since_update = status.last_update_time.elapsed().as_secs();
                    let staleness_indicator = if time_since_update > 60 && status.status == TaskStatus::Running {
                        "⚠️"
                    } else {
                        ""
                    };
                    
                    match status.status {
                        TaskStatus::Running => running_count += 1,
                        TaskStatus::Complete => completed_count += 1,
                        TaskStatus::Failed => failed_count += 1,
                        TaskStatus::TimedOut => timed_out_count += 1,
                        _ => {}
                    }
                    
                    total_groups += status.groups_created;
                    
                    // Enhanced status display with detailed progress
                    let phase_display = if status.detailed_progress.is_empty() {
                        status.processing_phase.clone()
                    } else {
                        format!("{}: {}", status.processing_phase, status.detailed_progress)
                    };
                    
                    status_parts.push(format!(
                        "{} {:?}: {} ({}){}",
                        status.status.emoji(),
                        method,
                        phase_display,
                        elapsed,
                        staleness_indicator
                    ));
                    
                    // Enhanced summary with completion stats
                    if status.entities_total > 0 && status.entities_skipped_complete > 0 {
                        summary_parts.push(format!(
                            "{:?}: {}g/{}c/{}s/{}t",
                            method,
                            status.groups_created,
                            status.cache_hits,
                            status.entities_skipped_complete,
                            status.entities_total,
                        ));
                    } else if status.groups_created > 0 || status.cache_hits > 0 {
                        summary_parts.push(format!(
                            "{:?}: {}g/{}c",
                            method,
                            status.groups_created,
                            status.cache_hits
                        ));
                    }
                }
                
                // Update the main progress bar position
                if let Some(ref pb) = matching_pb_clone {
                    pb.set_position(completed_count as u64);
                }
                
                // Create comprehensive display message
                let resource_info = format!("Mem: {}MB, DB: {}/{}", memory_mb, db_used, db_total);
                let status_summary = if timed_out_count > 0 {
                    format!("✓{} ▶️{} ❌{} ⏰{}", completed_count, running_count, failed_count, timed_out_count)
                } else {
                    format!("✓{} ▶️{} ❌{}", completed_count, running_count, failed_count)
                };
                
                let display_msg = if summary_parts.is_empty() {
                    format!("{} | {} | {}", status_parts.join(" | "), status_summary, resource_info)
                } else {
                    format!("{} | {} | {} | [{}]", 
                        status_parts.join(" | "), 
                        status_summary, 
                        resource_info,
                        summary_parts.join(", ")
                    )
                };
                
                status_bar.set_message(display_msg);
                
                // Enhanced periodic logging with resource monitoring
                if last_log_time.elapsed().as_secs() >= 30 {
                    let current_state = format!("{}/{}/{}/{}", completed_count, running_count, failed_count, timed_out_count);
                    if current_state != last_logged_state {
                        log_pipeline_progress_update(completed_count, tracker.len(), running_count, total_groups);
                        
                        // Log resource warnings
                        if memory_mb > 8000 { // > 8GB
                            warn!("⚠️ High memory usage: {} MB", memory_mb);
                        }
                        if db_used as f64 / db_total as f64 > 0.8 { // > 80% of DB connections used
                            warn!("⚠️ High database connection usage: {}/{}", db_used, db_total);
                        }
                        
                        if running_count > 0 {
                            let running_methods: Vec<String> = tracker.values()
                                .filter(|s| s.status == TaskStatus::Running)
                                .map(|s| {
                                    let phase = if s.detailed_progress.is_empty() {
                                        s.processing_phase.clone()
                                    } else {
                                        format!("{}: {}", s.processing_phase, s.detailed_progress)
                                    };
                                    format!("{:?} ({})", s.method_type, phase)
                                })
                                .collect();
                            info!("🔄 Currently running: {}", running_methods.join(", "));
                        }
                        
                        last_logged_state = current_state;
                    }
                    last_log_time = Instant::now();
                }
                
                // Exit when all tasks are done
                if completed_count + failed_count + timed_out_count == tracker.len() {
                    break;
                }
            }
        }))
    } else {
        None
    };

    log_pipeline_phase("Task Creation", Some("spawning concurrent matching tasks with timeout protection"));

    // Enhanced task creation with timeout and progress callbacks
    for method_type in matching_methods.iter() {
        let task = create_matching_task_with_timeout(
            method_type.clone(),
            pool.clone(),
            rl_orchestrator.clone(),
            run_id.clone(),
            entity_feature_cache.clone(),
            semaphore.clone(),
            status_tracker.clone(),
            callback_manager.clone(),
        );
        tasks.push(task);
    }

    if let Some(pb) = &matching_pb {
        pb.set_message("All matching tasks launched with timeout protection...");
    }

    info!("🚀 All {} matching tasks launched with enhanced monitoring", tasks.len());
    info!("⏳ Waiting for all matching tasks to complete ({}s timeout per task)...", TASK_TIMEOUT_SECONDS);

    log_pipeline_phase("Execution", Some("processing matches across all methods with real-time monitoring"));

    let join_handle_results = join_all(tasks).await;
    let mut total_groups = 0;
    let mut method_stats_vec = Vec::new();
    let mut successful_tasks = 0;
    let mut failed_tasks = 0;
    let mut timed_out_tasks = 0;

    // Stop monitoring tasks
    health_monitor.stop_monitoring().await;
    if let Some(updater) = status_updater {
        updater.abort();
    }
    health_monitor_task.abort();

    log_pipeline_phase("Results Processing", Some("aggregating results from all matching methods"));

    for (i, join_result) in join_handle_results.into_iter().enumerate() {
        match join_result {
            Ok(task_outcome_result) => match task_outcome_result {
                Ok(any_match_result) => {
                    total_groups += any_match_result.groups_created();
                    method_stats_vec.push(any_match_result.stats().clone());
                    successful_tasks += 1;
                }
                Err(e) => {
                    let error_msg = format!("{:?}", e);
                    if error_msg.contains("timed out") || error_msg.contains("timeout") {
                        timed_out_tasks += 1;
                        error!("⏰ Matching task {} timed out: {:?}", i + 1, e);
                    } else {
                        failed_tasks += 1;
                        error!("❌ Matching task {} returned an error: {:?}", i + 1, e);
                    }
                }
            },
            Err(e) => {
                failed_tasks += 1;
                error!("💥 Matching task {} panicked or failed to join: {:?}", i + 1, e);
            }
        }
    }

    if let Some(pb) = &matching_pb {
        pb.set_message("Processing final results...");
        pb.set_position(matching_methods.len() as u64);
    }

    log_pipeline_phase("Summary Generation", Some("compiling comprehensive pipeline results"));

    // Enhanced summary generation with timeout reporting
    let pipeline_duration = start_time_matching.elapsed();
    
    {
        let tracker = status_tracker.lock().await;
        info!("📋 ===== ENHANCED METHOD SUMMARY =====");
        
        for method in &matching_methods {
            if let Some(status) = tracker.get(method) {
                let duration = if let Some(end) = status.end_time {
                    (end - status.start_time).as_secs_f32()
                } else {
                    status.start_time.elapsed().as_secs_f32()
                };
                
                let status_details = match status.status {
                    TaskStatus::Complete => {
                        format!(
                            "✅ SUCCESS: {} groups, {} entities, {:.3} avg confidence, {:.2}s",
                            status.groups_created, status.entities_matched, status.avg_confidence, duration
                        )
                    },
                    TaskStatus::Failed => {
                        format!(
                            "❌ FAILED: {} (after {:.2}s)",
                            status.error.as_ref().unwrap_or(&"Unknown error".to_string()), duration
                        )
                    },
                    TaskStatus::TimedOut => {
                        format!(
                            "⏰ TIMED OUT: Exceeded {}s limit (last phase: {})",
                            TASK_TIMEOUT_SECONDS, status.processing_phase
                        )
                    },
                    _ => {
                        format!("⚠️  INCOMPLETE: {} (after {:.2}s)", status.processing_phase, duration)
                    }
                };
                
                info!("  {:?}: {}", method, status_details);
                
                if status.cache_hits > 0 {
                    info!("    💾 Cache performance: {} hits", status.cache_hits);
                }
                
                if status.entities_total > 0 && status.entities_skipped_complete > 0 {
                    let completion_rate = (status.entities_skipped_complete as f64 / status.entities_total as f64) * 100.0;
                    info!("    📊 Completion: {:.1}% entities completed ({}/{})",
                            completion_rate, status.entities_skipped_complete, status.entities_total);
                }
                
                // Resource usage summary
                if status.resource_stats.memory_mb > 0 {
                    info!("    🔧 Peak resources: {} MB memory, {}/{} DB connections",
                        status.resource_stats.memory_mb,
                        status.resource_stats.db_connections_used,
                        status.resource_stats.db_connections_total
                    );
                }
            }
        }
        info!("=====================================");
    }

    // Report and clear entity feature cache stats
    {
        let mut cache_guard = entity_feature_cache.lock().await;
        let (hits, misses, ind_hits, ind_misses) = cache_guard.get_stats();
        let pair_hit_rate = if hits + misses > 0 {
            (hits as f64 / (hits + misses) as f64) * 100.0
        } else {
            0.0
        };
        
        log_feature_cache_stats(hits, misses, ind_hits, ind_misses);
        cache_guard.clear();
        
        let cache_stats = if hits + misses > 0 {
            Some((hits, misses, pair_hit_rate))
        } else {
            None
        };
        
        log_pipeline_completion(&run_id, pipeline_duration, total_groups, &method_stats_vec, cache_stats);
    }

    // Finalize progress bars with enhanced completion messages
    if let Some(pb) = &matching_pb {
        let completion_msg = if timed_out_tasks > 0 {
            format!("Pipeline complete: {} groups created ({} timeouts)", total_groups, timed_out_tasks)
        } else {
            format!("Pipeline complete: {} groups created", total_groups)
        };
        pb.finish_with_message(completion_msg);
    }

    if let Some(pb) = &status_pb {
        pb.finish_with_message("All matching methods completed".to_string());
    }

    // Enhanced final performance and outcome summary
    if failed_tasks > 0 || timed_out_tasks > 0 {
        warn!("⚠️  Pipeline completed with {} failed tasks and {} timed out tasks out of {}", 
              failed_tasks, timed_out_tasks, matching_methods.len());
        
        if timed_out_tasks > 0 {
            warn!("💡 Consider increasing TASK_TIMEOUT_SECONDS or investigating task performance");
        }
    } else {
        info!("🎯 Pipeline completed successfully - all {} methods executed without errors", matching_methods.len());
    }
    
    info!(
        "🏁 Enhanced entity matching pipeline completed: {} total groups created in {:.2?}",
        total_groups, pipeline_duration
    );
    
    Ok((total_groups, method_stats_vec))
}

fn create_matching_task_with_timeout(
    method_type: MatchMethodType,
    pool: PgPool,
    orchestrator: Arc<Mutex<RLOrchestrator>>,
    run_id: String,
    feature_cache: SharedFeatureCache,
    semaphore: Arc<Semaphore>,
    status_tracker: Arc<Mutex<HashMap<MatchMethodType, MatchingTaskStatus>>>,
    callback_manager: Arc<ProgressCallbackManager>,
) -> JoinHandle<Result<AnyMatchResult, anyhow::Error>> {
    tokio::spawn(async move {
        // Update status to waiting for slot
        {
            let mut tracker = status_tracker.lock().await;
            if let Some(status) = tracker.get_mut(&method_type) {
                status.status = TaskStatus::WaitingForSlot;
                status.processing_phase = "Waiting for processing slot".to_string();
                status.last_update_time = Instant::now();
            }
        }
        
        log_pipeline_method_starting(method_type.clone(), false);
        
        let permit = semaphore
            .acquire_owned()
            .await
            .context("Failed to acquire semaphore permit")?;
        let _permit_guard = permit;

        log_pipeline_method_starting(method_type.clone(), true);

        // Update status to running
        let task_start_time = {
            let mut tracker = status_tracker.lock().await;
            if let Some(status) = tracker.get_mut(&method_type) {
                status.status = TaskStatus::Running;
                status.processing_phase = "Starting".to_string();
                status.detailed_progress = "Initializing method...".to_string();
                status.start_time = Instant::now();
                status.last_update_time = Instant::now();
            }
            Instant::now()
        };

        // Create progress callback for this specific method
        let progress_callback = callback_manager.create_callback(method_type.clone(), pool.clone());

        // Wrap the matching function call with timeout
        let timeout_duration = Duration::from_secs(TASK_TIMEOUT_SECONDS);
        let result = tokio::time::timeout(
            timeout_duration,
            call_matching_method(
                method_type.clone(),
                &pool,
                // Some(orchestrator),
                None,
                &run_id,
                Some(feature_cache),
                Some(progress_callback),
            )
        ).await;

        // Handle timeout and regular completion
        let task_duration = task_start_time.elapsed();
        let final_result = match result {
            Ok(method_result) => {
                let mut tracker = status_tracker.lock().await;
                if let Some(status) = tracker.get_mut(&method_type) {
                    match &method_result {
                        Ok(any_result) => {
                            status.groups_created = any_result.groups_created();
                            status.entities_matched = any_result.stats().entities_matched;
                            status.avg_confidence = any_result.stats().avg_confidence;
                            status.entities_skipped_complete = any_result.stats().entities_skipped_complete;
                            status.entities_total = any_result.stats().entities_total;
                            status.status = TaskStatus::Complete;
                            status.processing_phase = "Completed".to_string();
                            status.detailed_progress = format!("{} groups created", any_result.groups_created());
                            status.end_time = Some(Instant::now());
                            status.last_update_time = Instant::now();
                            
                            log_pipeline_method_completed(
                                method_type.clone(),
                                any_result.groups_created(),
                                any_result.stats().entities_matched,
                                task_duration,
                                any_result.stats().avg_confidence
                            );
                        }
                        Err(e) => {
                            status.status = TaskStatus::Failed;
                            status.processing_phase = "Error".to_string();
                            status.detailed_progress = "Failed during execution".to_string();
                            status.error = Some(format!("{:?}", e));
                            status.end_time = Some(Instant::now());
                            status.last_update_time = Instant::now();
                            
                            log_pipeline_method_failed(method_type.clone(), task_duration, &format!("{:?}", e));
                        }
                    }
                }
                method_result
            }
            Err(_timeout_error) => {
                let mut tracker = status_tracker.lock().await;
                if let Some(status) = tracker.get_mut(&method_type) {
                    status.status = TaskStatus::TimedOut;
                    status.processing_phase = "Timed out".to_string();
                    status.detailed_progress = format!("Exceeded {} second limit", TASK_TIMEOUT_SECONDS);
                    status.error = Some(format!("Task timed out after {} seconds", TASK_TIMEOUT_SECONDS));
                    status.end_time = Some(Instant::now());
                    status.last_update_time = Instant::now();
                }
                
                error!("⏰ {} matching timed out after {:.2?}", method_type.as_str(), task_duration);
                Err(anyhow::anyhow!("Task timed out after {} seconds", TASK_TIMEOUT_SECONDS))
            }
        };

        final_result
    })
}

async fn call_matching_method(
    method_type: MatchMethodType,
    pool: &PgPool,
    orchestrator: Option<Arc<Mutex<RLOrchestrator>>>,
    run_id: &str,
    feature_cache: Option<SharedFeatureCache>,
    progress_callback: Option<ProgressCallback>,
) -> Result<AnyMatchResult, anyhow::Error> {
    match method_type {
        MatchMethodType::Email => {
            email::find_matches(
                pool,
                // orchestrator,
                None,
                run_id,
                feature_cache,
                progress_callback,
            ).await
        },
        MatchMethodType::Phone => {
            phone::find_matches(
                pool,
                // orchestrator,
                None,
                run_id,
                feature_cache,
                progress_callback,
            ).await
        },
        MatchMethodType::Url => {
            url::find_matches(
                pool,
                // orchestrator,
                None,
                run_id,
                feature_cache,
                progress_callback,
            ).await
        },
        MatchMethodType::Address => {
            address::find_matches(
                pool,
                // orchestrator,
                None,
                run_id,
                feature_cache,
                progress_callback,
            ).await
        },
        MatchMethodType::Name => {
            name::find_matches(
                pool,
                // orchestrator,
                None,
                run_id,
                feature_cache,
                progress_callback,
            ).await
        },
        MatchMethodType::Custom(_) => {
            Err(anyhow::anyhow!("Custom method types not supported in this pipeline"))
        }
    }
}