use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::table::column::{Column as TableColumn, ColumnType as TableColumnType};
use crate::table::table::{Table, TableDefinition};

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
pub struct InsertRequest {
    insert: Vec<String>,
    into: String,
    values: Vec<Vec<serde_json::Value>>,
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
    State(_): State<AppState>,
    Json(request): Json<CreateTableRequest>,
) -> Json<String> {
    let columns = request
        .columns
        .into_iter()
        .map(|c| TableColumn::new(c.name, c.ty.into()))
        .collect();

    match TableDefinition::create(request.name, columns).await {
        Ok(_) => Json("Table created successfully".to_string()),
        Err(error) => Json(format!("Unable to create table: {}", error)),
    }
}

pub async fn insert(State(_): State<AppState>, Json(request): Json<InsertRequest>) -> Json<String> {
    let Ok(table_definition) = TableDefinition::open(request.into).await else {
        return Json("Could not open table".to_string());
    };

    let Ok(mut table) = table_definition.load().await else {
        return Json("Could not load table".to_string());
    };

    if let Err(error) = table.insert(request.insert, request.values).await {
        return Json(format!("Could not write into the table: {}", error));
    };

    Json("Data inserted successfully".to_string())
}

pub async fn query(
    State(_): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    let Ok(table_definition) = TableDefinition::open(request.from).await else {
        return Json(QueryResponse { results: vec![] });
    };

    let Ok(mut table) = table_definition.load().await else {
        return Json(QueryResponse { results: vec![] });
    };

    match table.query(request.select).await {
        Ok(rows) => {
            println!("ROWS {:?}", rows);
            Json(QueryResponse { results: vec![] })
        }
        Err(error) => {
            info!("Error while querying table {}: {}", table.name(), error);
            Json(QueryResponse { results: vec![] })
        }
    }
}
