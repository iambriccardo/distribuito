use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use tokio::sync::Mutex;

use crate::table::column::{Column as TableColumn, ColumnType as TableColumnType, ColumnValue};
use crate::table::cursor::Row;
use crate::table::table::{Table, TableDefinition};

#[derive(Deserialize)]
pub struct CreateTableRequest {
    name: String,
    columns: Vec<Column>,
}

#[derive(Deserialize, Serialize)]
pub struct Column {
    name: String,
    ty: ColumnType,
}

#[derive(Deserialize, Serialize)]
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

impl From<TableColumnType> for ColumnType {
    fn from(value: TableColumnType) -> Self {
        match value {
            TableColumnType::Integer => ColumnType::Integer,
            TableColumnType::Float => ColumnType::Float,
            TableColumnType::String => ColumnType::String,
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
#[serde(untagged)]
pub enum QueryResponse {
    Empty {
        errors: Vec<String>,
    },
    WithData {
        columns: Vec<Column>,
        data: Vec<Vec<serde_json::Value>>,
    },
}

impl QueryResponse {
    pub fn empty() -> Self {
        Self::Empty { errors: vec![] }
    }
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
        Ok(_) => {
            info!("Table created successfully");
            Json("Table created successfully".to_string())
        }
        Err(error) => {
            info!("Unable to create table: {}", error);
            Json(format!("Unable to create table: {}", error))
        }
    }
}

pub async fn insert(State(_): State<AppState>, Json(request): Json<InsertRequest>) -> Json<String> {
    let Ok(table_definition) = TableDefinition::open(request.into).await else {
        info!("Could not open table");
        return Json("Could not open table".to_string());
    };

    let Ok(mut table) = table_definition.load().await else {
        info!("Could not load table");
        return Json("Could not load table".to_string());
    };

    if let Err(error) = table.insert(request.insert, request.values).await {
        info!("Could not write into the table: {}", error);
        return Json(format!("Could not write into the table: {}", error));
    };

    info!("Data inserted successfully");
    Json("Data inserted successfully".to_string())
}

pub async fn query(
    State(_): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    let Ok(table_definition) = TableDefinition::open(request.from).await else {
        return Json(QueryResponse::empty());
    };

    let Ok(mut table) = table_definition.load().await else {
        return Json(QueryResponse::empty());
    };

    match table.query(request.select).await {
        Ok(rows) => Json(rows_to_response(rows)),
        Err(error) => {
            info!("Error while querying table {}: {}", table.name(), error);
            Json(QueryResponse::empty())
        }
    }
}

fn rows_to_response(rows: Vec<Row<ColumnValue>>) -> QueryResponse {
    if rows.is_empty() {
        return QueryResponse::empty();
    }

    let columns = rows[0]
        .columns()
        .iter()
        .map(|c| Column {
            name: c.name.clone(),
            ty: c.ty.into(),
        })
        .collect();

    QueryResponse::WithData {
        columns,
        data: serialize_data(rows),
    }
}

fn serialize_data(rows: Vec<Row<ColumnValue>>) -> Vec<Vec<serde_json::Value>> {
    let mut serialized_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.into_values();
        let mut serialized_values = Vec::with_capacity(values.len());
        for value in values {
            let serialized_value = match value {
                ColumnValue::Integer(value) => serde_json::Value::Number(Number::from(value)),
                ColumnValue::Float(value) => {
                    serde_json::Value::Number(Number::from_f64(value).unwrap())
                }
                ColumnValue::String(value) => serde_json::Value::String(value),
                ColumnValue::Null => serde_json::Value::Null,
            };

            serialized_values.push(serialized_value);
        }

        serialized_rows.push(serialized_values);
    }

    serialized_rows
}
