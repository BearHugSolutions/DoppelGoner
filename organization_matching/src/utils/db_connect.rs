// src/utils/db_connect.rs

use anyhow::{Context, Result};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use log::info;
use std::time::Duration;
use tokio_postgres::{Config, NoTls};

/// Builds the PostgreSQL connection configuration from environment variables.
/// This function sets up host, port, database name, user, password,
/// application name, and connection timeout.
fn build_pg_config() -> Config {
    let mut config = Config::new();
    let host = std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port_str = std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
    let port = port_str.parse::<u16>().unwrap_or(5432);
    let dbname = std::env::var("POSTGRES_DB").unwrap_or_else(|_| "dataplatform".to_string());
    let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("POSTGRES_PASSWORD").unwrap_or_default();

    info!(
        "DB Config: Host={}, Port={}, DB={}, User={}",
        host, port, dbname, user
    );
    config
        .host(&host)
        .port(port)
        .dbname(&dbname)
        .user(&user)
        .password(&password);
    config.application_name("deduplication_pipeline");
    config.connect_timeout(Duration::from_secs(10));
    config
}

/// Type alias for the PostgreSQL connection pool.
/// This uses `bb8` for connection pooling with `tokio_postgres`.
pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;

/// Establishes and initializes the PostgreSQL database connection pool.
///
/// Configures the pool with:
/// - `max_size`: Maximum number of connections in the pool (90).
/// - `min_idle`: Minimum number of idle connections to maintain (2).
/// - `idle_timeout`: How long an idle connection can live before being closed (180 seconds).
/// - `connection_timeout`: How long to wait to establish a new connection (40 seconds).
///
/// It also performs a test query (`SELECT 1`) to ensure the pool is working.
pub async fn connect() -> Result<PgPool> {
    let config = build_pg_config();
    info!("Connecting to PostgreSQL database...");
    let manager = PostgresConnectionManager::new(config, NoTls);

    // Define pool configuration values to be logged
    let pool_max_size = 90;
    let pool_min_idle = Some(2);
    let pool_idle_timeout = Some(Duration::from_secs(180));
    let pool_connection_timeout = Duration::from_secs(40);

    let pool = Pool::builder()
        .max_size(pool_max_size) // Max number of connections in the pool
        .min_idle(pool_min_idle) // Min number of idle connections to maintain
        .idle_timeout(pool_idle_timeout) // How long an idle connection can live
        .connection_timeout(pool_connection_timeout) // How long to wait for a new connection
        .build(manager)
        .await
        .context("Failed to build database connection pool")?;

    // Perform a test query to ensure the pool is functional
    let conn = pool
        .get()
        .await
        .context("Failed to get test connection from pool")?;
    conn.query_one("SELECT 1", &[])
        .await
        .context("Test query 'SELECT 1' failed")?;
    info!(
        "Database connection pool initialized successfully with configured max_size: {}, configured idle_timeout: {:?}.",
        pool_max_size, // Use the captured configured value
        pool_idle_timeout, // Use the captured configured value
    );
    Ok(pool.clone())
}

/// Returns the current status of the database connection pool.
///
/// # Arguments
/// * `pool` - A reference to the `PgPool`.
///
/// # Returns
/// A tuple containing:
/// * `total_connections`: The total number of connections currently in the pool (size).
/// * `available_connections`: The number of idle connections ready for use.
/// * `in_use_connections`: The number of connections currently being used.
pub fn get_pool_status(pool: &PgPool) -> (usize, usize, usize) {
    let status = pool.state(); // Changed from .status() to .state()
    let total_connections = status.connections as usize;
    let idle_connections = status.idle_connections as usize;
    let in_use_connections = total_connections - idle_connections; // Calculate in_use_connections from total and idle
    (total_connections, idle_connections, in_use_connections)
}
