// src/utils/logging.rs - Comprehensive logging helpers for matching methods
use log::{info, warn, error, debug};
use std::time::Instant;
use crate::models::stats_models::MatchMethodType;

#[derive(Clone)]
pub struct MatchingLogger {
    method_name: &'static str,
    method_emoji: &'static str,
    start_time: Instant,
}

impl MatchingLogger {
    pub fn new(method_type: MatchMethodType) -> Self {
        let (method_name, method_emoji) = match method_type {
            MatchMethodType::Email => ("EMAIL", "📧"),
            MatchMethodType::Phone => ("PHONE", "📞"), 
            MatchMethodType::Url => ("URL", "🌐"),
            MatchMethodType::Address => ("ADDRESS", "📍"),
            MatchMethodType::Name => ("NAME", "👤"),
            MatchMethodType::Custom(ref name) => {
                // For custom types, we'll use a generic approach
                // This is a bit of a hack since we need static lifetime
                return Self {
                    method_name: "CUSTOM",
                    method_emoji: "⚙️",
                    start_time: Instant::now(),
                };
            }
        };
        
        Self {
            method_name,
            method_emoji,
            start_time: Instant::now(),
        }
    }
    
    pub fn log_start(&self, pipeline_run_id: &str, has_rl: bool) {
        info!(
            "[{}] {} 🚀 Starting {} matching (run ID: {}){}",
            self.method_name,
            self.method_emoji,
            self.method_name.to_lowercase(),
            pipeline_run_id,
            if has_rl { " with RL confidence tuning" } else { " (no RL tuning)" }
        );
        info!(
            "[{}] {} ⚙️  Configuration: incremental checks enabled, geospatial filtering enabled",
            self.method_name, self.method_emoji
        );
    }
    
    pub fn log_phase(&self, phase: &str, details: Option<&str>) {
        let elapsed = self.start_time.elapsed();
        let msg = if let Some(details) = details {
            format!(
                "[{}] {} 🔄 Phase: {} - {} [+{:.1}s]",
                self.method_name, self.method_emoji, phase, details, elapsed.as_secs_f32()
            )
        } else {
            format!(
                "[{}] {} 🔄 Phase: {} [+{:.1}s]",
                self.method_name, self.method_emoji, phase, elapsed.as_secs_f32()
            )
        };
        info!("{}", msg);
    }
    
    pub fn log_data_loaded(&self, count: usize, data_type: &str) {
        info!(
            "[{}] {} 📊 Found {} {} records from database",
            self.method_name, self.method_emoji, count, data_type
        );
    }
    
    pub fn log_existing_pairs(&self, count: usize) {
        if count > 0 {
            info!(
                "[{}] {} ⏭️  Found {} existing {}-matched pairs (will skip to avoid duplicates)",
                self.method_name, self.method_emoji, count, self.method_name.to_lowercase()
            );
        } else {
            info!(
                "[{}] {} ✨ No existing {}-matched pairs found - clean slate",
                self.method_name, self.method_emoji, self.method_name.to_lowercase()
            );
        }
    }
    
    pub fn log_processing_complete(&self, raw_count: usize, unique_groups: usize, groups_with_multiple: usize) {
        info!(
            "[{}] {} ✅ Processing complete: {} raw records → {} unique normalized groups → {} groups with 2+ entities",
            self.method_name, self.method_emoji, raw_count, unique_groups, groups_with_multiple
        );
    }
    
    pub fn log_pair_generation(&self, total_pairs: usize, groups_count: usize) {
        info!(
            "[{}] {} 📈 Potential pairs to evaluate: {} (from {} groups with multiple entities)",
            self.method_name, self.method_emoji, total_pairs, groups_count
        );
    }
    
    pub fn log_filtering_results(&self, original_pairs: usize, remaining_pairs: usize, 
                                existing_skipped: usize, geospatial_filtered: usize) {
        let total_filtered = existing_skipped + geospatial_filtered;
        let percent_kept = if original_pairs > 0 {
            (remaining_pairs as f64 / original_pairs as f64) * 100.0
        } else {
            0.0
        };
        
        info!(
            "[{}] {} 🎯 Pair filtering: {} total → {} remaining ({:.1}% kept)",
            self.method_name, self.method_emoji, original_pairs, remaining_pairs, percent_kept
        );
        
        if total_filtered > 0 {
            info!(
                "[{}] {} 🚫 Filtered out: {} existing pairs, {} geospatially distant pairs",
                self.method_name, self.method_emoji, existing_skipped, geospatial_filtered
            );
        }
    }
    
    pub fn log_cache_and_signatures(&self, pairs_count: usize) {
        info!(
            "[{}] {} 🔍 Fetching entity signatures and checking comparison cache for {} pairs...",
            self.method_name, self.method_emoji, pairs_count
        );
    }
    
    pub fn log_cache_results(&self, cache_hits: usize, cache_misses: usize) {
        let total = cache_hits + cache_misses;
        if total > 0 {
            let hit_rate = (cache_hits as f64 / total as f64) * 100.0;
            info!(
                "[{}] {} 💾 Cache results: {} hits, {} misses ({:.1}% hit rate)",
                self.method_name, self.method_emoji, cache_hits, cache_misses, hit_rate
            );
        }
    }
    
    pub fn log_batch_processing_start(&self, total_pairs: usize, batch_size: usize) {
        let batch_count = (total_pairs + batch_size - 1) / batch_size;
        info!(
            "[{}] {} ⚙️  Processing {} pairs in {} batches (batch size: {})",
            self.method_name, self.method_emoji, total_pairs, batch_count, batch_size
        );
    }
    
    pub fn log_batch_progress(&self, batch_num: usize, total_batches: usize, pairs_in_batch: usize) {
        if batch_num % 5 == 0 || batch_num == 1 || batch_num == total_batches {
            info!(
                "[{}] {} 📦 Processing batch {}/{} ({} pairs)",
                self.method_name, self.method_emoji, batch_num, total_batches, pairs_in_batch
            );
        }
    }
    
    pub fn log_completion(&self, groups_created: usize, entities_matched: usize, 
                         avg_confidence: f64, total_pairs_processed: usize) {
        let duration = self.start_time.elapsed();
        info!(
            "[{}] {} 🎉 COMPLETED: {} new entity groups created in {:.2?}",
            self.method_name, self.method_emoji, groups_created, duration
        );
        info!(
            "[{}] {} 📊 Results: {} entities matched, avg confidence: {:.3}, {} total pairs processed",
            self.method_name, self.method_emoji, entities_matched, avg_confidence, total_pairs_processed
        );
    }
    
    pub fn log_performance_summary(&self, cache_hits: usize, errors: usize, feature_extraction_stats: Option<(usize, usize, usize)>) {
        if cache_hits > 0 {
            info!(
                "[{}] {} ⚡ Performance: {} cache hits accelerated processing",
                self.method_name, self.method_emoji, cache_hits
            );
        }
        
        if let Some((attempted, successful, failed)) = feature_extraction_stats {
            if attempted > 0 {
                let success_rate = (successful as f64 / attempted as f64) * 100.0;
                info!(
                    "[{}] {} 🧠 Feature extraction: {}/{} successful ({:.1}% success rate)",
                    self.method_name, self.method_emoji, successful, attempted, success_rate
                );
            }
        }
        
        if errors > 0 {
            warn!(
                "[{}] {} ⚠️  {} processing errors encountered during matching",
                self.method_name, self.method_emoji, errors
            );
        }
    }
    
    pub fn log_warning(&self, message: &str) {
        warn!(
            "[{}] {} ⚠️  {}",
            self.method_name, self.method_emoji, message
        );
    }
    
    pub fn log_error(&self, message: &str) {
        error!(
            "[{}] {} ❌ {}",
            self.method_name, self.method_emoji, message
        );
    }
    
    pub fn log_debug(&self, message: &str) {
        debug!(
            "[{}] {} {}",
            self.method_name, self.method_emoji, message
        );
    }
    
    pub fn log_progress_update(&self, current: usize, total: usize, additional_info: Option<&str>) {
        // Only log every 5000 items to avoid spam, but always log milestone percentages
        let should_log = current % 5000 == 0 
            || current == total 
            || (total >= 100 && current % (total / 10) == 0); // Every 10%
            
        if should_log && current > 0 {
            let percent = (current as f64 / total as f64) * 100.0;
            let msg = if let Some(info) = additional_info {
                format!("Progress: {}/{} ({:.1}%) - {}", current, total, percent, info)
            } else {
                format!("Progress: {}/{} ({:.1}%)", current, total, percent)
            };
            info!(
                "[{}] {} 📊 {}",
                self.method_name, self.method_emoji, msg
            );
        }
    }
    
    pub fn log_data_quality_issue(&self, issue_type: &str, count: usize) {
        if count > 0 {
            warn!(
                "[{}] {} ⚠️  Data quality: {} instances of {}",
                self.method_name, self.method_emoji, count, issue_type
            );
        }
    }
    
    pub fn log_feature_extraction_batch(&self, batch_num: usize, attempted: usize, successful: usize) {
        if batch_num % 10 == 0 { // Log every 10th batch
            let success_rate = if attempted > 0 {
                (successful as f64 / attempted as f64) * 100.0
            } else {
                0.0
            };
            info!(
                "[{}] {} 🧠 Feature extraction batch {}: {}/{} successful ({:.1}%)",
                self.method_name, self.method_emoji, batch_num, successful, attempted, success_rate
            );
        }
    }
    
    pub fn log_rl_tuning_results(&self, pairs_tuned: usize, avg_confidence_before: f64, avg_confidence_after: f64) {
        if pairs_tuned > 0 {
            let confidence_change = avg_confidence_after - avg_confidence_before;
            let change_direction = if confidence_change > 0.01 { 
                "↗️ improved" 
            } else if confidence_change < -0.01 { 
                "↘️ decreased" 
            } else { 
                "→ stable" 
            };
            
            info!(
                "[{}] {} 🧠 RL tuning: {} pairs processed, confidence {:.3} → {:.3} ({})",
                self.method_name, self.method_emoji, pairs_tuned, avg_confidence_before, avg_confidence_after, change_direction
            );
        }
    }
    
    pub fn log_geospatial_filtering(&self, total_pairs: usize, filtered_pairs: usize, max_distance_km: f64) {
        if filtered_pairs > 0 {
            let percent_filtered = (filtered_pairs as f64 / total_pairs as f64) * 100.0;
            info!(
                "[{}] {} 🌍 Geospatial filter: removed {} pairs ({:.1}%) exceeding {:.1}km distance",
                self.method_name, self.method_emoji, filtered_pairs, percent_filtered, max_distance_km / 1000.0
            );
        }
    }
    
    pub fn log_domain_stats(&self, domain_count: usize, avg_entities_per_domain: f64, largest_domain_size: usize) {
        info!(
            "[{}] {} 🏷️  Domain analysis: {} unique domains, avg {:.1} entities/domain, largest: {} entities",
            self.method_name, self.method_emoji, domain_count, avg_entities_per_domain, largest_domain_size
        );
    }
    
    pub fn get_elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }
    
    pub fn get_method_name(&self) -> &'static str {
        self.method_name
    }
}

// Pipeline-level logging functions
pub fn log_pipeline_start(run_id: &str, method_count: usize, concurrency_limit: usize, has_feature_cache: bool) {
    info!("🚀 ===== ENTITY MATCHING PIPELINE STARTING =====");
    info!("📅 Pipeline Run ID: {}", run_id);
    info!("⚙️  Configuration:");
    info!("   • {} matching methods enabled", method_count);
    info!("   • Concurrency limit: {} simultaneous tasks", concurrency_limit);
    info!("   • Feature caching: {}", if has_feature_cache { "enabled" } else { "disabled" });
    info!("   • Geospatial filtering: enabled");
    info!("   • Incremental processing: enabled");
    info!("🎯 Methods: Email 📧, Phone 📞, URL 🌐, Address 📍, Name 👤");
    info!("================================================");
}

pub fn log_pipeline_phase(phase: &str, details: Option<&str>) {
    let msg = if let Some(details) = details {
        format!("🔄 Pipeline Phase: {} - {}", phase, details)
    } else {
        format!("🔄 Pipeline Phase: {}", phase)
    };
    info!("{}", msg);
}

pub fn log_pipeline_method_starting(method_type: MatchMethodType, slot_acquired: bool) {
    let (name, emoji) = match method_type {
        MatchMethodType::Email => ("EMAIL", "📧"),
        MatchMethodType::Phone => ("PHONE", "📞"),
        MatchMethodType::Url => ("URL", "🌐"),
        MatchMethodType::Address => ("ADDRESS", "📍"),
        MatchMethodType::Name => ("NAME", "👤"),
        MatchMethodType::Custom(_) => ("CUSTOM", "⚙️"),
    };
    
    if slot_acquired {
        info!("🚦 [{}] {} Processing slot acquired, starting {} matching", name, emoji, name.to_lowercase());
    } else {
        info!("⏳ [{}] {} Waiting for processing slot...", name, emoji);
    }
}

pub fn log_pipeline_method_completed(method_type: MatchMethodType, groups_created: usize, 
                                   entities_matched: usize, duration: std::time::Duration, 
                                   avg_confidence: f64) {
    let (name, emoji) = match method_type {
        MatchMethodType::Email => ("EMAIL", "📧"),
        MatchMethodType::Phone => ("PHONE", "📞"),
        MatchMethodType::Url => ("URL", "🌐"),
        MatchMethodType::Address => ("ADDRESS", "📍"),
        MatchMethodType::Name => ("NAME", "👤"),
        MatchMethodType::Custom(_) => ("CUSTOM", "⚙️"),
    };
    
    info!(
        "✅ [{}] {} {} matching completed in {:.2?}: {} groups, {} entities (avg confidence: {:.3})",
        name, emoji, name.to_lowercase(), duration, groups_created, entities_matched, avg_confidence
    );
}

pub fn log_pipeline_method_failed(method_type: MatchMethodType, duration: std::time::Duration, error: &str) {
    let (name, emoji) = match method_type {
        MatchMethodType::Email => ("EMAIL", "📧"),
        MatchMethodType::Phone => ("PHONE", "📞"),
        MatchMethodType::Url => ("URL", "🌐"),
        MatchMethodType::Address => ("ADDRESS", "📍"),
        MatchMethodType::Name => ("NAME", "👤"),
        MatchMethodType::Custom(_) => ("CUSTOM", "⚙️"),
    };
    
    error!(
        "❌ [{}] {} {} matching failed after {:.2?}: {}",
        name, emoji, name.to_lowercase(), duration, error
    );
}

pub fn log_pipeline_progress_update(completed: usize, total: usize, running: usize, total_groups_so_far: usize) {
    info!(
        "📊 Pipeline Progress: {}/{} methods completed, {} running, {} total groups created so far",
        completed, total, running, total_groups_so_far
    );
}

pub fn log_pipeline_completion(run_id: &str, duration: std::time::Duration, total_groups: usize, 
                              method_stats: &[crate::models::stats_models::MatchMethodStats],
                              cache_stats: Option<(usize, usize, f64)>) {
    info!("🎉 ===== ENTITY MATCHING PIPELINE COMPLETED =====");
    info!("📅 Pipeline Run ID: {}", run_id);
    info!("⏱️  Total Duration: {:.2?}", duration);
    info!("🎯 Total Groups Created: {}", total_groups);
    info!("");
    info!("📈 Method Breakdown:");
    
    let mut total_entities = 0;
    for stats in method_stats {
        let (name, emoji) = match stats.method_type {
            MatchMethodType::Email => ("EMAIL", "📧"),
            MatchMethodType::Phone => ("PHONE", "📞"),
            MatchMethodType::Url => ("URL", "🌐"),
            MatchMethodType::Address => ("ADDRESS", "📍"),
            MatchMethodType::Name => ("NAME", "👤"),
            MatchMethodType::Custom(_) => ("CUSTOM", "⚙️"),
        };
        
        total_entities += stats.entities_matched;
        
        info!(
            "  {} {}: {} groups, {} entities (avg confidence: {:.3}, avg group size: {:.1})",
            emoji, name, stats.groups_created, stats.entities_matched, stats.avg_confidence, stats.avg_group_size
        );
    }
    
    info!("");
    info!("📊 Summary:");
    info!("  • Total entities involved in matches: {}", total_entities);
    info!("  • Average groups per method: {:.1}", total_groups as f64 / method_stats.len() as f64);
    
    if let Some((hits, misses, hit_rate)) = cache_stats {
        info!("  • Feature cache performance: {} hits, {} misses ({:.1}% hit rate)", hits, misses, hit_rate);
    }
    
    info!("===============================================");
}

pub fn log_feature_cache_stats(hits: usize, misses: usize, individual_hits: usize, individual_misses: usize) {
    let pair_total = hits + misses;
    let individual_total = individual_hits + individual_misses;
    
    if pair_total > 0 || individual_total > 0 {
        info!("💾 ===== FEATURE CACHE PERFORMANCE =====");
        
        if pair_total > 0 {
            let pair_hit_rate = (hits as f64 / pair_total as f64) * 100.0;
            info!("📊 Pair Features: {} hits, {} misses ({:.1}% hit rate)", hits, misses, pair_hit_rate);
        }
        
        if individual_total > 0 {
            let individual_hit_rate = (individual_hits as f64 / individual_total as f64) * 100.0;
            info!("📊 Individual Features: {} hits, {} misses ({:.1}% hit rate)", individual_hits, individual_misses, individual_hit_rate);
        }
        
        info!("=====================================");
    }
}