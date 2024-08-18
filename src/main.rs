use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::sync::Arc;

use axum::{Router, routing::post};
use tokio::sync::Mutex;

use crate::transport::api::{AppState, create_table, query};

mod dio;
mod table;
mod transport;

#[tokio::main]
async fn main() {
    let app_state = AppState {
        open_tables: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/create_table", post(create_table))
        .route("/query", post(query))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
