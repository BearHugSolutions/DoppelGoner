// src/bin/report_model_weights.rs

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

// Re-defining necessary structs from `confidence_tuner.rs` and `feature_extraction.rs`
// to make this binary self-contained. In a real-world scenario, these would be
// imported from your library's modules.
mod organization_matching {
    pub mod utils {
        // Mocking the db_connect and env modules for compilation.
        // Replace with your actual project structure.
        pub mod db_connect {
            use anyhow::{Context, Result};
            use bb8::Pool;
            use bb8_postgres::PostgresConnectionManager;
            use log::info;
            use std::time::Duration;
            use tokio_postgres::{Config, NoTls};

            fn build_pg_config() -> Config {
                let mut config = Config::new();
                let host =
                    std::env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
                let port_str =
                    std::env::var("POSTGRES_PORT").unwrap_or_else(|_| "5432".to_string());
                let port = port_str.parse::<u16>().unwrap_or(5432);
                let dbname =
                    std::env::var("POSTGRES_DB").unwrap_or_else(|_| "dataplatform".to_string());
                let user =
                    std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
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
                config.application_name("model_weights_reporter"); // Set a specific app name
                config.connect_timeout(Duration::from_secs(10));
                config
            }

            pub type PgPool = Pool<PostgresConnectionManager<NoTls>>;
            pub async fn connect() -> Result<PgPool> {
                let config = build_pg_config();
                info!("Connecting to PostgreSQL database...");
                let manager = PostgresConnectionManager::new(config, NoTls);

                let pool = Pool::builder()
                    .max_size(10) // A smaller pool is fine for a reporting tool
                    .min_idle(Some(1))
                    .idle_timeout(Some(Duration::from_secs(180)))
                    .connection_timeout(Duration::from_secs(40))
                    .build(manager)
                    .await
                    .context("Failed to build database connection pool")?;

                // Test the connection
                let conn = pool
                    .get()
                    .await
                    .context("Failed to get test connection from pool")?;
                conn.query_one("SELECT 1", &[])
                    .await
                    .context("Test query 'SELECT 1' failed")?;
                info!("Database connection pool initialized successfully.");
                Ok(pool.clone())
            }
        }
        pub mod env {
            use anyhow::{Context, Result};
            use log::{debug, info, warn};
            use std::path::Path;

            pub fn load_env() {
                let env_paths = [".env", ".env.local", "../.env"];
                let mut loaded_env = false;
                for path in env_paths.iter() {
                    if Path::new(path).exists() {
                        if let Err(e) = load_env_from_file(path) {
                            warn!("Failed to load environment from {}: {}", path, e);
                        } else {
                            info!("Loaded environment variables from {}", path);
                            loaded_env = true;
                            break;
                        }
                    }
                }
                if !loaded_env {
                    info!("No .env file found, using environment variables from system");
                }
            }

            fn load_env_from_file(file_path: &str) -> Result<()> {
                use std::fs::File;
                use std::io::{BufRead, BufReader};

                info!(
                    "Attempting to load environment variables from: {}",
                    file_path
                );
                match File::open(file_path) {
                    Ok(file) => {
                        let reader = BufReader::new(file);
                        for line in reader.lines() {
                            let line = line.context("Failed to read line from env file")?;
                            if line.starts_with('#') || line.trim().is_empty() {
                                continue;
                            }
                            if let Some(idx) = line.find('=') {
                                let key = line[..idx].trim();
                                let value = line[idx + 1..].trim().trim_matches('"');
                                if std::env::var(key).is_err() {
                                    std::env::set_var(key, value);
                                    debug!(
                                        "Set env var from file: {} = {}",
                                        key,
                                        if key == "POSTGRES_PASSWORD" {
                                            "[hidden]"
                                        } else {
                                            value
                                        }
                                    );
                                }
                            }
                        }
                        info!("Successfully processed env file: {}", file_path);
                    }
                    Err(e) => {
                        warn!(
                            "Could not open env file '{}': {}. Proceeding with system environment variables.",
                            file_path, e
                        );
                    }
                }
                Ok(())
            }
        }
    }
}

// Structs from `confidence_tuner.rs`
#[derive(Serialize, Deserialize, Debug, Clone)]
struct OnlineLogisticRegression {
    weights: Vec<f64>,
    #[allow(dead_code)]
    learning_rate: f64,
    trials: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ConfidenceArm {
    target_confidence: f64,
    model: OnlineLogisticRegression,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConfidenceTuner {
    method_arms: HashMap<String, Vec<ConfidenceArm>>,
    #[allow(dead_code)]
    epsilon: f64,
    pub version: u32,
}

impl ConfidenceTuner {
    // Simplified loader function for the reporting tool.
    pub async fn load_from_db(
        pool: &organization_matching::utils::db_connect::PgPool,
    ) -> Result<Self> {
        let conn = pool.get().await.context("Failed to get DB connection")?;
        let id_prefix = "confidence_tuner";
        let binary_model_type = format!("{}_binary", id_prefix);

        let binary_row_opt = conn
            .query_opt(
                "SELECT parameters FROM clustering_metadata.ml_models WHERE model_type = $1 ORDER BY version DESC LIMIT 1",
                &[&binary_model_type],
            )
            .await?;

        if let Some(binary_row) = binary_row_opt {
            let model_json: JsonValue = binary_row.get(0);
            let loaded_tuner: ConfidenceTuner =
                serde_json::from_value(model_json).context("Failed to deserialize model JSON")?;
            println!(
                "Successfully loaded Contextual Bandit model version {}.",
                loaded_tuner.version
            );
            Ok(loaded_tuner)
        } else {
            anyhow::bail!("No ConfidenceTuner model found in the database.")
        }
    }
}

// Struct from `feature_extraction.rs`
struct FeatureMetadata {
    name: String,
    description: String,
}

/// Defines the names and descriptions of the 31 features in the model's context vector.
/// This is based on your `feature_extraction.rs` file.
fn get_feature_metadata() -> Vec<FeatureMetadata> {
    vec![
        // --- Individual Entity Features (Indices 0-11) ---
        FeatureMetadata {
            name: "name_complexity".to_string(),
            description: "Complexity of the organization name.".to_string(),
        },
        FeatureMetadata {
            name: "data_completeness".to_string(),
            description: "Completeness of core organization data fields.".to_string(),
        },
        FeatureMetadata {
            name: "has_email".to_string(),
            description: "Boolean indicating if an email exists.".to_string(),
        },
        FeatureMetadata {
            name: "has_phone".to_string(),
            description: "Boolean indicating if a phone number exists.".to_string(),
        },
        FeatureMetadata {
            name: "has_url".to_string(),
            description: "Boolean indicating if a URL exists.".to_string(),
        },
        FeatureMetadata {
            name: "has_address".to_string(),
            description: "Boolean indicating if an address exists.".to_string(),
        },
        FeatureMetadata {
            name: "has_location".to_string(),
            description: "Boolean indicating if location coordinates exist.".to_string(),
        },
        FeatureMetadata {
            name: "organization_size".to_string(),
            description: "Estimated size of the organization based on features.".to_string(),
        },
        FeatureMetadata {
            name: "service_count".to_string(),
            description: "Normalized count of services offered.".to_string(),
        },
        FeatureMetadata {
            name: "embedding_centroid_distance".to_string(),
            description: "Distance from organization embedding to global centroid.".to_string(),
        },
        FeatureMetadata {
            name: "service_semantic_coherence".to_string(),
            description: "Average semantic similarity between an org's services.".to_string(),
        },
        FeatureMetadata {
            name: "embedding_quality".to_string(),
            description: "Quality score of the organization's embedding.".to_string(),
        },
        // --- Pairwise Features (Indices 12-18 in this list) ---
        FeatureMetadata {
            name: "name_similarity".to_string(),
            description: "Similarity between two entity names.".to_string(),
        },
        FeatureMetadata {
            name: "embedding_similarity".to_string(),
            description: "Cosine similarity between two entity embeddings.".to_string(),
        },
        FeatureMetadata {
            name: "max_service_similarity".to_string(),
            description: "Max semantic similarity between services of two entities.".to_string(),
        },
        FeatureMetadata {
            name: "geographic_distance".to_string(),
            description: "Normalized geographic proximity between entities.".to_string(),
        },
        FeatureMetadata {
            name: "shared_domain".to_string(),
            description: "Boolean if entities share the same website domain.".to_string(),
        },
        FeatureMetadata {
            name: "shared_phone".to_string(),
            description: "Boolean if entities share the same phone number.".to_string(),
        },
        FeatureMetadata {
            name: "service_geo_semantic_score".to_string(),
            description: "Hybrid score combining service similarity and geo-proximity.".to_string(),
        },
    ]
}

/// Constructs the full list of 31 feature names as they appear in the model's weight vector.
fn get_full_feature_list() -> Vec<String> {
    let metadata = get_feature_metadata();
    let mut full_list = Vec::with_capacity(31);

    // Add entity1's 12 features
    for i in 0..12 {
        full_list.push(format!("e1_{}", metadata[i].name));
    }
    // Add entity2's 12 features
    for i in 0..12 {
        full_list.push(format!("e2_{}", metadata[i].name));
    }
    // Add the 7 pairwise features
    for i in 12..19 {
        full_list.push(metadata[i].name.clone());
    }
    full_list
}

fn print_arm_report(arm: &ConfidenceArm, feature_names: &[String]) {
    println!("    --------------------------------------------------");
    println!(
        "    Arm - Target Confidence: {:.3} (Trials: {})",
        arm.target_confidence, arm.model.trials
    );
    println!("    --------------------------------------------------");

    if arm.model.trials == 0 {
        println!("      No trials for this arm yet. Weights are all 0.");
        return;
    }

    let mut weighted_features: Vec<(String, f64)> = arm
        .model
        .weights
        .iter()
        .zip(feature_names.iter().cloned())
        .map(|(weight, name)| (name, *weight))
        .collect();

    // Sort by the absolute value of the weight to find the most influential features
    weighted_features.sort_by(|a, b| b.1.abs().partial_cmp(&a.1.abs()).unwrap());

    println!("      Most Influential Features (Sorted by Absolute Weight):");
    println!("      ------------------------------------------------------");
    println!("      | {:<35} | {:>10} |", "Feature Name", "Weight");
    println!("      |---------------------------------------|------------|");

    for (name, weight) in weighted_features.iter().take(10) {
        println!("      | {:<35} | {:>10.4} |", name, weight);
    }

    // Find and print the bias term
    if let Some(bias_weight) = arm.model.weights.last() {
        println!("      |---------------------------------------|------------|");
        println!("      | {:<35} | {:>10.4} |", "(Bias Term)", bias_weight);
    }
    println!("      ------------------------------------------------------\n");
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables (e.g., DATABASE_URL)
    organization_matching::utils::env::load_env();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!("--- Contextual Bandit Model Weight Report ---");

    // Establish database connection
    let pool = organization_matching::utils::db_connect::connect()
        .await
        .context("Database connection failed")?;

    // Load the latest model from the database
    let tuner = ConfidenceTuner::load_from_db(&pool).await?;
    let feature_names = get_full_feature_list();

    println!("\nGenerating report for Model Version: {}\n", tuner.version);

    // Iterate through each method and its arms to print the report
    let mut methods: Vec<_> = tuner.method_arms.keys().cloned().collect();
    methods.sort(); // Sort methods for consistent output order

    for method_name in methods {
        if let Some(arms) = tuner.method_arms.get(&method_name) {
            println!("==================================================");
            println!("  Method: {}", method_name.to_uppercase());
            println!("==================================================");

            let mut sorted_arms = arms.clone();
            // Sort arms by number of trials to see most used ones first
            sorted_arms.sort_by_key(|b| std::cmp::Reverse(b.model.trials));

            if sorted_arms.is_empty() {
                println!("  No arms configured for this method.\n");
                continue;
            }

            for arm in sorted_arms {
                print_arm_report(&arm, &feature_names);
            }
        }
    }

    println!("--- End of Report ---");
    Ok(())
}
