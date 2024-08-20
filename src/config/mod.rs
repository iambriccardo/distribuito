use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};
use tokio::io;

use crate::io::file::create_file;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub database_name: String,
    pub database_path: String,
}

impl Config {
    pub async fn from_file<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        // We create all the necessary dirs and the config file if not existing.
        create_dir_all(&path).await?;
        create_file("config.json", &path).await?;

        // We load the config as string and parse it into the object.
        let config_path = path.as_ref().join("config.json");
        let config_data = read_to_string(&config_path).await?;
        let config: Config = serde_json::from_str(&config_data)?;

        Ok(config)
    }
}
