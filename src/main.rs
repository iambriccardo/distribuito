use std::collections::HashMap;
use std::sync::Arc;

use axum::{Router, routing::post};
use log::info;
use tokio::sync::Mutex;

use crate::transport::api::{AppState, create_table, insert, query};

mod dio;
mod table;
mod transport;

#[tokio::main]
async fn main() {
    std_logger::Config::logfmt().init();

    let app_state = AppState {
        open_tables: Arc::new(Mutex::new(HashMap::new())),
    };

    info!("Starting distribuito");

    let app = Router::new()
        .route("/create_table", post(create_table))
        .route("/insert", post(insert))
        .route("/query", post(query))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
