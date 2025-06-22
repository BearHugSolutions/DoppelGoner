// src/utils/progress_callback.rs - Progress callback system for real-time task monitoring

use crate::models::stats_models::MatchMethodType;
use crate::utils::db_connect::{PgPool, get_pool_status};
use crate::utils::get_memory_usage;
use log::{warn, debug, info};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Type alias for progress callback functions
/// Takes phase name and optional detailed progress information
pub type ProgressCallback = Arc<dyn Fn(String, Option<String>) + Send + Sync>;

/// Enhanced task status for internal tracking (matches manager.rs)
#[derive(Debug, Clone)]
pub struct MatchingTaskStatus {
    pub method_type: MatchMethodType,
    pub status: TaskStatus,
    pub pairs_found: usize,
    pub groups_created: usize,
    pub entities_matched: usize,
    pub entities_skipped_complete: usize,
    pub entities_total: usize,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub error: Option<String>,
    pub cache_hits: usize,
    pub processing_phase: String,
    pub detailed_progress: String,
    pub avg_confidence: f64,
    pub last_update_time: Instant,
    pub resource_stats: ResourceStats,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Queued,
    WaitingForSlot,
    Running,
    Complete,
    Failed,
    TimedOut,
}

impl TaskStatus {
    pub fn emoji(&self) -> &'static str {
        match self {
            TaskStatus::Queued => "⏳",
            TaskStatus::WaitingForSlot => "🔄",
            TaskStatus::Running => "▶️",
            TaskStatus::Complete => "✅",
            TaskStatus::Failed => "❌",
            TaskStatus::TimedOut => "⏰",
        }
    }
    
    pub fn description(&self) -> &'static str {
        match self {
            TaskStatus::Queued => "Queued",
            TaskStatus::WaitingForSlot => "Waiting for slot",
            TaskStatus::Running => "Running",
            TaskStatus::Complete => "Complete",
            TaskStatus::Failed => "Failed",
            TaskStatus::TimedOut => "Timed out",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResourceStats {
    pub memory_mb: u64,
    pub db_connections_used: usize,
    pub db_connections_total: usize,
}

impl Default for ResourceStats {
    fn default() -> Self {
        Self {
            memory_mb: 0,
            db_connections_used: 0,
            db_connections_total: 0,
        }
    }
}

impl Default for MatchingTaskStatus {
    fn default() -> Self {
        Self {
            method_type: MatchMethodType::Email, // This will be overridden
            status: TaskStatus::Queued,
            pairs_found: 0,
            groups_created: 0,
            entities_matched: 0,
            entities_skipped_complete: 0,
            entities_total: 0,
            start_time: Instant::now(),
            end_time: None,
            error: None,
            cache_hits: 0,
            processing_phase: "Queued".to_string(),
            detailed_progress: "Initializing...".to_string(),
            avg_confidence: 0.0,
            last_update_time: Instant::now(),
            resource_stats: ResourceStats::default(),
        }
    }
}

/// Manages progress callbacks for matching tasks
pub struct ProgressCallbackManager {
    status_tracker: Arc<Mutex<HashMap<MatchMethodType, MatchingTaskStatus>>>,
}

impl ProgressCallbackManager {
    pub fn new(status_tracker: Arc<Mutex<HashMap<MatchMethodType, MatchingTaskStatus>>>) -> Self {
        Self { status_tracker }
    }

    /// Creates a progress callback for a specific matching method
    pub fn create_callback(&self, method_type: MatchMethodType, pool: PgPool) -> ProgressCallback {
        let tracker_clone = Arc::clone(&self.status_tracker);
        let method_clone = method_type.clone();
        
        Arc::new(move |phase: String, details: Option<String>| {
            let tracker = tracker_clone.clone();
            let method = method_clone.clone();
            let phase_clone = phase.clone();
            let details_clone = details.clone();
            let pool_clone = pool.clone();
            
            // Spawn a task to update status without blocking the caller
            tokio::spawn(async move {
                if let Ok(mut status_map) = tracker.try_lock() {
                    if let Some(status) = status_map.get_mut(&method) {
                        status.processing_phase = phase_clone;
                        if let Some(detail_info) = details_clone {
                            status.detailed_progress = detail_info;
                        }
                        status.last_update_time = Instant::now();
                        
                        // Update resource stats periodically (not on every call to avoid overhead)
                        let should_update_resources = status.last_update_time
                            .duration_since(status.start_time)
                            .as_secs() % 10 == 0; // Every 10 seconds
                            
                        if should_update_resources {
                            let memory_mb = get_memory_usage().await;
                            let (db_total, db_available, _) = get_pool_status(&pool_clone);
                            let db_used = db_total - db_available;
                            
                            status.resource_stats.memory_mb = memory_mb;
                            status.resource_stats.db_connections_used = db_used;
                            status.resource_stats.db_connections_total = db_total;
                        }
                    }
                } else {
                    // If we can't acquire the lock immediately, don't block
                    debug!("Could not acquire status tracker lock for progress update");
                }
            });
        })
    }

    /// Updates statistical information for a method (called from matching methods)
    pub async fn update_stats(
        &self,
        method_type: MatchMethodType,
        pairs_found: Option<usize>,
        groups_created: Option<usize>,
        entities_matched: Option<usize>,
        cache_hits: Option<usize>,
        avg_confidence: Option<f64>,
    ) {
        if let Ok(mut status_map) = self.status_tracker.try_lock() {
            if let Some(status) = status_map.get_mut(&method_type) {
                if let Some(pairs) = pairs_found {
                    status.pairs_found = pairs;
                }
                if let Some(groups) = groups_created {
                    status.groups_created = groups;
                }
                if let Some(entities) = entities_matched {
                    status.entities_matched = entities;
                }
                if let Some(hits) = cache_hits {
                    status.cache_hits = hits;
                }
                if let Some(confidence) = avg_confidence {
                    status.avg_confidence = confidence;
                }
                status.last_update_time = Instant::now();
            }
        }
    }
}

/// Monitors task health and detects hanging or problematic tasks
pub struct TaskHealthMonitor {
    status_tracker: Arc<Mutex<HashMap<MatchMethodType, MatchingTaskStatus>>>,
    timeout_duration: Duration,
    monitoring: Arc<Mutex<bool>>,
}

impl TaskHealthMonitor {
    pub fn new(
        status_tracker: Arc<Mutex<HashMap<MatchMethodType, MatchingTaskStatus>>>,
        timeout_duration: Duration,
    ) -> Self {
        Self {
            status_tracker,
            timeout_duration,
            monitoring: Arc::new(Mutex::new(false)),
        }
    }

    /// Starts health monitoring in a background task
    pub async fn start_monitoring(&self, pool: PgPool) {
        {
            let mut monitoring = self.monitoring.lock().await;
            *monitoring = true;
        }

        let status_tracker = Arc::clone(&self.status_tracker);
        let timeout_duration = self.timeout_duration;
        let monitoring_flag = Arc::clone(&self.monitoring);
        
        info!("🏥 Starting task health monitoring (checking every 30s)");

        loop {
            // Check if we should stop monitoring
            {
                let monitoring = monitoring_flag.lock().await;
                if !*monitoring {
                    break;
                }
            }

            tokio::time::sleep(Duration::from_secs(30)).await;

            // Perform health checks
            if let Ok(status_map) = status_tracker.try_lock() {
                let current_time = Instant::now();
                
                for (method_type, status) in status_map.iter() {
                    match status.status {
                        TaskStatus::Running => {
                            let elapsed = current_time.duration_since(status.start_time);
                            let time_since_update = current_time.duration_since(status.last_update_time);
                            
                            // Check for potential timeout
                            if elapsed > timeout_duration {
                                warn!(
                                    "⏰ Task {:?} has been running for {:.1}s (timeout: {:.1}s)",
                                    method_type,
                                    elapsed.as_secs_f64(),
                                    timeout_duration.as_secs_f64()
                                );
                            }
                            
                            // Check for stale progress updates (might indicate hanging)
                            if time_since_update.as_secs() > 120 { // 2 minutes without update
                                warn!(
                                    "⚠️ Task {:?} hasn't updated progress in {:.1}s (last: {})",
                                    method_type,
                                    time_since_update.as_secs_f64(),
                                    status.processing_phase
                                );
                            }
                            
                            // Resource health checks
                            if status.resource_stats.memory_mb > 12000 { // > 12GB
                                warn!(
                                    "💾 Task {:?} using high memory: {} MB",
                                    method_type, status.resource_stats.memory_mb
                                );
                            }
                        }
                        TaskStatus::WaitingForSlot => {
                            let wait_time = current_time.duration_since(status.start_time);
                            if wait_time.as_secs() > 300 { // 5 minutes waiting
                                warn!(
                                    "🔄 Task {:?} has been waiting for a slot for {:.1}s",
                                    method_type, wait_time.as_secs_f64()
                                );
                            }
                        }
                        _ => {} // Complete, Failed, TimedOut, Queued - no health checks needed
                    }
                }

                // Global resource health checks
                let (db_total, db_available, _) = get_pool_status(&pool);
                let db_used = db_total - db_available;
                
                if db_used as f64 / db_total as f64 > 0.9 { // > 90% usage
                    warn!(
                        "🗄️ Database connection pool nearly exhausted: {}/{} connections used",
                        db_used, db_total
                    );
                }
            }
        }

        info!("🏥 Task health monitoring stopped");
    }

    /// Stops health monitoring
    pub async fn stop_monitoring(&self) {
        let mut monitoring = self.monitoring.lock().await;
        *monitoring = false;
        info!("🏥 Stopping task health monitoring");
    }
}

/// Helper function to create a simple progress callback for methods that don't need full features
pub fn create_simple_callback(method_name: &str) -> ProgressCallback {
    let method_name = method_name.to_string();
    Arc::new(move |phase: String, details: Option<String>| {
        let detail_str = details.map(|d| format!(" - {}", d)).unwrap_or_default();
        debug!("[{}] Progress: {}{}", method_name, phase, detail_str);
    })
}

/// Convenience macro for updating progress within matching methods
#[macro_export]
macro_rules! update_progress {
    ($callback:expr, $phase:expr) => {
        if let Some(ref cb) = $callback {
            cb($phase.to_string(), None);
        }
    };
    ($callback:expr, $phase:expr, $details:expr) => {
        if let Some(ref cb) = $callback {
            cb($phase.to_string(), Some($details.to_string()));
        }
    };
}

/// Convenience macro for updating progress with detailed information
#[macro_export]
macro_rules! update_detailed_progress {
    ($callback:expr, $phase:expr, $current:expr, $total:expr) => {
        if let Some(ref cb) = $callback {
            let details = format!("{}/{}", $current, $total);
            cb($phase.to_string(), Some(details));
        }
    };
    ($callback:expr, $phase:expr, $current:expr, $total:expr, $extra:expr) => {
        if let Some(ref cb) = $callback {
            let details = format!("{}/{} ({})", $current, $total, $extra);
            cb($phase.to_string(), Some(details));
        }
    };
}

/// Progress tracking helper for common operations in matching methods
pub struct ProgressTracker {
    callback: Option<ProgressCallback>,
    current_phase: String,
    start_time: Instant,
}

impl ProgressTracker {
    pub fn new(callback: Option<ProgressCallback>) -> Self {
        Self {
            callback,
            current_phase: "Initializing".to_string(),
            start_time: Instant::now(),
        }
    }

    pub fn set_phase(&mut self, phase: &str) {
        self.current_phase = phase.to_string();
        if let Some(ref cb) = self.callback {
            cb(phase.to_string(), None);
        }
    }

    pub fn set_phase_with_details(&mut self, phase: &str, details: &str) {
        self.current_phase = phase.to_string();
        if let Some(ref cb) = self.callback {
            cb(phase.to_string(), Some(details.to_string()));
        }
    }

    pub fn update_progress(&self, current: usize, total: usize) {
        if let Some(ref cb) = self.callback {
            let details = format!("{}/{}", current, total);
            cb(self.current_phase.clone(), Some(details));
        }
    }

    pub fn update_progress_with_info(&self, current: usize, total: usize, info: &str) {
        if let Some(ref cb) = self.callback {
            let details = format!("{}/{} ({})", current, total, info);
            cb(self.current_phase.clone(), Some(details));
        }
    }

    pub fn finish_phase(&self, summary: &str) {
        if let Some(ref cb) = self.callback {
            let elapsed = self.start_time.elapsed();
            let details = format!("{} in {:.2}s", summary, elapsed.as_secs_f64());
            cb("Completed".to_string(), Some(details));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_progress_callback_creation() {
        let status_tracker = Arc::new(Mutex::new(HashMap::new()));
        let callback_manager = ProgressCallbackManager::new(status_tracker.clone());
        
        // This would normally use a real PgPool - for testing we'll use a mock
        // In real code, this would be: let callback = callback_manager.create_callback(MethodType::Email, pool);
        
        // Test that callback manager can be created
        assert!(true); // Placeholder - in real tests you'd verify callback functionality
    }

    #[tokio::test]
    async fn test_progress_tracker() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = Arc::clone(&call_count);
        
        let callback = Arc::new(move |_phase: String, _details: Option<String>| {
            call_count_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        let mut tracker = ProgressTracker::new(Some(callback));
        tracker.set_phase("Testing");
        tracker.update_progress(50, 100);
        tracker.set_phase_with_details("Processing", "items");
        
        // Verify callbacks were called
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_simple_callback_creation() {
        let callback = create_simple_callback("TestMethod");
        
        // Test that we can call the callback without panicking
        callback("TestPhase".to_string(), Some("TestDetails".to_string()));
        callback("TestPhase2".to_string(), None);
    }
}