use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;

use axum::{Router, routing::post};
use log::info;

use crate::config::Config;
use crate::transport::api::{create_table, DatabaseState, insert, query};

mod config;
mod io;
mod table;
mod transport;

fn config_path() -> tokio::io::Result<PathBuf> {
    let home_path = home::home_dir()
        .ok_or_else(|| Error::new(ErrorKind::NotFound, "Impossible to get home directory"))?;

    Ok(home_path.join(".distribuito"))
}

#[tokio::main]
async fn main() {
    std_logger::Config::logfmt().init();

    let config_path = config_path().unwrap();
    let config = Config::from_file(config_path).await.unwrap();

    // TODO: add shards and other data for communication here.
    let app_state = DatabaseState {
        config: Arc::new(config),
    };

    info!("Starting the database...");

    let app = Router::new()
        .route("/create_table", post(create_table))
        .route("/insert", post(insert))
        .route("/query", post(query))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
