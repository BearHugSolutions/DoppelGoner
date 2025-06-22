pub mod candle;
pub mod db_connect;
pub mod env;
pub mod instantiate_run;
pub mod pipeline_state;
pub mod progress_bars;
pub mod constants;

pub async fn get_memory_usage() -> u64 {
    use sysinfo::System;
    let mut sys = System::new_all();
    sys.refresh_memory();
    sys.used_memory() / (1024 * 1024) // Convert to MB
}
