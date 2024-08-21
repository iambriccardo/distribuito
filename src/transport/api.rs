use std::ops::Deref;
use std::sync::Arc;

use axum::extract::State;
use axum::Json;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Number;

use crate::config::Config;
use crate::table::column::{Column as TableColumn, ColumnType as TableColumnType, ColumnValue};
use crate::table::cursor::{AggregatedRow, Row};
use crate::table::table::{QueryResult, TableDefinition};
use crate::transport::shard::Shards;
use crate::transport::shard_op::create_table::CreateTable;
use crate::transport::shard_op::insert::Insert;

#[derive(Deserialize, Serialize)]
pub struct CreateTableRequest {
    name: String,
    columns: Vec<Column>,
}

#[derive(Deserialize, Serialize)]
pub struct Column {
    name: String,
    ty: ColumnType,
}

impl From<TableColumn> for Column {
    fn from(value: TableColumn) -> Self {
        Self {
            name: value.name,
            ty: value.ty.into(),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Null,
}

impl From<ColumnType> for TableColumnType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Integer => TableColumnType::Integer,
            ColumnType::Float => TableColumnType::Float,
            ColumnType::String => TableColumnType::String,
            ColumnType::Null => TableColumnType::Null,
        }
    }
}

impl From<TableColumnType> for ColumnType {
    fn from(value: TableColumnType) -> Self {
        match value {
            TableColumnType::Integer => ColumnType::Integer,
            TableColumnType::Float => ColumnType::Float,
            TableColumnType::String => ColumnType::String,
            TableColumnType::Null => panic!("Invalid column type"),
        }
    }
}

impl<'a> From<&'a ColumnValue> for ColumnType {
    fn from(value: &'a ColumnValue) -> Self {
        match value {
            ColumnValue::Integer(_) => ColumnType::Integer,
            ColumnValue::Float(_) => ColumnType::Float,
            ColumnValue::String(_) => ColumnType::String,
            ColumnValue::Null => ColumnType::Null,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct InsertRequest {
    insert: Vec<String>,
    into: String,
    values: Vec<Vec<serde_json::Value>>,
}

impl InsertRequest {
    /// Splits the insert request into multiple insert requests that contain a subset of the values
    /// each.
    pub fn split(&mut self, n: usize) -> Vec<InsertRequest> {
        // Calculate the size of each chunk
        let chunk_size = (self.values.len() + n - 1) / n;

        // Create an iterator over the values split into chunks
        let chunks = self.values.chunks(chunk_size);

        // Map each chunk into a new InsertRequest
        chunks
            .map(|chunk| InsertRequest {
                insert: self.insert.clone(),
                into: self.into.clone(),
                values: chunk.to_vec(),
            })
            .collect()
    }
}

#[derive(Deserialize)]
pub struct QueryRequest {
    select: Vec<String>,
    from: String,
    #[serde(default)]
    group_by: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct Aggregate {
    value: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    components: Option<Vec<serde_json::Value>>,
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
    WithAggregatedData {
        columns: Vec<Column>,
        aggregate_columns: Vec<Column>,
        data: Vec<Vec<serde_json::Value>>,
        aggregates: Vec<Vec<Aggregate>>,
    },
}

impl QueryResponse {
    pub fn empty() -> Self {
        Self::Empty { errors: vec![] }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseState {
    pub config: Arc<Config>,
    pub shards: Arc<Option<Shards>>,
}

pub async fn create_table(
    State(state): State<DatabaseState>,
    Json(request): Json<CreateTableRequest>,
) -> Json<String> {
    // We broadcast table creation to all shards.
    if let Some(shards) = state.shards.deref() {
        let create_table = CreateTable::new(&request);
        let _ = shards.broadcast(create_table).await;
    }

    let columns = request
        .columns
        .into_iter()
        .map(|c| TableColumn::new(c.name, c.ty.into()))
        .collect();

    match TableDefinition::create(state.config.clone(), request.name, columns).await {
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

pub async fn insert(
    State(state): State<DatabaseState>,
    Json(mut request): Json<InsertRequest>,
) -> Json<String> {
    // We unicast in a round-robin fashion the insertions to the shards.
    if let Some(shards) = state.shards.deref() {
        let mut requests = request.split(shards.number_of_shards() + 1);
        println!("REQUESTS {:?}", requests.len());
        request = requests.remove(0);

        for request in requests {
            let insert = Insert::new(&request);
            let _ = shards.rr_unicast(insert).await;
        }
    }

    let Ok(table_definition) = TableDefinition::open(state.config.clone(), request.into).await
    else {
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
    State(state): State<DatabaseState>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    let Ok(table_definition) = TableDefinition::open(state.config.clone(), request.from).await
    else {
        return Json(QueryResponse::empty());
    };

    let Ok(mut table) = table_definition.load().await else {
        return Json(QueryResponse::empty());
    };

    match table.query(request.select, request.group_by).await {
        Ok(query_result) => Json(serialize_query_result(query_result)),
        Err(error) => {
            info!("Error while querying table {}: {}", table.name(), error);
            Json(QueryResponse::empty())
        }
    }
}

fn serialize_query_result(query_result: QueryResult) -> QueryResponse {
    if query_result.is_empty() {
        return QueryResponse::empty();
    }

    match query_result {
        QueryResult::Rows(rows) => serialize_rows(rows),
        QueryResult::AggregatedRows(aggregated_rows) => serialize_aggregated_rows(aggregated_rows),
    }
}

fn serialize_rows(rows: Vec<Row<ColumnValue>>) -> QueryResponse {
    let columns = rows[0].columns().into_iter().map(|c| c.into()).collect();

    QueryResponse::WithData {
        columns,
        data: serialize_rows_data(rows),
    }
}

fn serialize_aggregated_rows(aggregated_rows: Vec<AggregatedRow<ColumnValue>>) -> QueryResponse {
    let first_row = &aggregated_rows[0];
    let columns = first_row.columns().into_iter().map(|c| c.into()).collect();
    let aggregate_columns = first_row
        .aggregate_columns()
        .into_iter()
        .map(|(a, c)| Column {
            name: a.into(),
            ty: c.into(),
        })
        .collect();

    let (data, aggregates) = serialize_aggregated_rows_data(aggregated_rows);
    QueryResponse::WithAggregatedData {
        columns,
        aggregate_columns,
        data,
        aggregates,
    }
}

fn serialize_rows_data(rows: Vec<Row<ColumnValue>>) -> Vec<Vec<serde_json::Value>> {
    let mut serialized_data = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.into_values();

        let mut serialized_values = Vec::with_capacity(values.len());
        for value in values {
            serialized_values.push(serialize_column_value(value));
        }

        serialized_data.push(serialized_values);
    }

    serialized_data
}

fn serialize_aggregated_rows_data(
    aggregated_rows: Vec<AggregatedRow<ColumnValue>>,
) -> (Vec<Vec<serde_json::Value>>, Vec<Vec<Aggregate>>) {
    let mut serialized_data = Vec::with_capacity(aggregated_rows.len());
    let mut serialized_aggregates = Vec::with_capacity(aggregated_rows.len());

    for aggregated_row in aggregated_rows {
        let (values, aggregate_values) = aggregated_row.into_values();

        let mut serialized_values = Vec::with_capacity(values.len());
        for value in values {
            serialized_values.push(serialize_column_value(value));
        }
        serialized_data.push(serialized_values);

        let mut serialized_aggregate_values = Vec::with_capacity(aggregate_values.len());
        for (aggregate_value, aggregate_components) in aggregate_values {
            let serialized_aggregate = Aggregate {
                value: serialize_column_value(aggregate_value),
                components: aggregate_components
                    .map(|a| a.into_iter().map(|v| serialize_column_value(v)).collect()),
            };
            serialized_aggregate_values.push(serialized_aggregate);
        }
        serialized_aggregates.push(serialized_aggregate_values);
    }

    (serialized_data, serialized_aggregates)
}

fn serialize_column_value(column_value: ColumnValue) -> serde_json::Value {
    match column_value {
        ColumnValue::Integer(value) => serde_json::Value::Number(Number::from(value)),
        ColumnValue::Float(value) => serde_json::Value::Number(Number::from_f64(value).unwrap()),
        ColumnValue::String(value) => serde_json::Value::String(value),
        ColumnValue::Null => serde_json::Value::Null,
    }
}
