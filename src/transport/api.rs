use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::table::core::{Column as TableColumn, Table, TableDefinition};
use crate::table::types::ColumnType as TableColumnType;

#[derive(Deserialize)]
pub struct CreateTableRequest {
    name: String,
    columns: Vec<Column>,
}

#[derive(Deserialize)]
pub struct Column {
    name: String,
    ty: ColumnType,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Integer,
    Float,
    String,
}

impl From<ColumnType> for TableColumnType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Integer => TableColumnType::Integer,
            ColumnType::Float => TableColumnType::Float,
            ColumnType::String => TableColumnType::String,
        }
    }
}

#[derive(Deserialize)]
pub struct QueryRequest {
    select: Vec<String>,
    from: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    results: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Clone)]
pub struct AppState {
    pub open_tables: Arc<Mutex<HashMap<String, Table>>>,
}

pub async fn create_table(
    State(state): State<AppState>,
    Json(request): Json<CreateTableRequest>,
) -> Json<String> {
    let mut tables = state.open_tables.lock().await;

    let columns = request
        .columns
        .into_iter()
        .map(|c| TableColumn::new(c.name, c.ty.into()))
        .collect();

    let table = TableDefinition::new(request.name, columns).load().await;
    match table {
        Ok(table) => {
            tables.insert(table.name().to_string(), table);
            Json("Table created successfully".to_string())
        }
        Err(error) => Json(format!("Could not create table: {}", error)),
    }
}

pub async fn query_table(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    let tables = state.open_tables.lock().await;
    // TODO: implement querying.

    Json(QueryResponse { results: vec![] })
}
