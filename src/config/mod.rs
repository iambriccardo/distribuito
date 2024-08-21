use std::path::Path;

use serde::Deserialize;
use tokio::fs::{create_dir_all, read_to_string};
use tokio::io;

use crate::io::file::create_file;

#[derive(Debug, Deserialize)]
#[serde(rename_all(deserialize = "lowercase"))]
pub enum InstanceRole {
    Master,
    Slave,
}

impl<'a> From<&'a InstanceRole> for &'a str {
    fn from(value: &'a InstanceRole) -> Self {
        match value {
            InstanceRole::Master => "master",
            InstanceRole::Slave => "slave",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct Instance {
    pub ip_port: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub instance_role: InstanceRole,
    pub database_ip_port: String,
    pub database_name: String,
    pub database_path: String,
    pub instances: Vec<Instance>,
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
