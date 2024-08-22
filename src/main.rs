use std::env;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

use axum::{routing::post, Router};
use log::info;

use crate::config::{Config, InstanceRole};
use crate::transport::api::{create_table, insert, query, DatabaseState};
use crate::transport::shard::Shards;

mod config;
mod io;
mod table;
mod transport;

fn config_path() -> tokio::io::Result<PathBuf> {
    // Check if the user has specified a custom config path via the environment variable
    if let Ok(custom_path) = env::var("CONFIG_PATH") {
        return Ok(PathBuf::from(custom_path));
    }

    // If not, default to the user's home directory with .distribuito
    let home_path = home::home_dir()
        .ok_or_else(|| Error::new(ErrorKind::NotFound, "Impossible to get home directory"))?;

    Ok(home_path.join(".distribuito"))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_path = config_path().unwrap();
    let config = Config::from_file(config_path).await.unwrap();

    info!(
        "Starting the database '{}' with role {} on {}",
        config.database_name,
        <&InstanceRole as Into<&str>>::into(&config.instance_role),
        config.database_ip_port
    );

    let shards = if matches!(config.instance_role, InstanceRole::Master) {
        Some(Shards::new(&config))
    } else {
        None
    };

    let ip_port = config.database_ip_port.clone();

    let app_state = DatabaseState {
        config: Arc::new(config),
        shards: Arc::new(shards),
    };

    let app = Router::new()
        .route("/create_table", post(create_table))
        .route("/insert", post(insert))
        .route("/query", post(query))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(ip_port).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
