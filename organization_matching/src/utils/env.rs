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
