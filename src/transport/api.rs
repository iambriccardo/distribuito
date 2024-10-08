use axum::extract::State;
use axum::Json;
use log::info;
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::io::{Error, ErrorKind};
use std::ops::Deref;
use std::sync::Arc;

use crate::config::Config;
use crate::table::aggregate::Aggregate;
use crate::table::column::{
    try_parse_queried_column, AggregateColumn, Column as TableColumn,
    ColumnType as TableColumnType, ColumnValue,
};
use crate::table::cursor::{AggregatedRow, Row};
use crate::table::table::{QueryResult, TableDefinition};
use crate::transport::shard::Shards;
use crate::transport::shard_op::create_table::CreateTable;
use crate::transport::shard_op::insert::Insert;
use crate::transport::shard_op::query::Query;
use futures::future::{join, join_all, BoxFuture, FutureExt};
use tokio::io;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CreateTableRequest {
    name: String,
    columns: Vec<Column>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Column {
    name: String,
    ty: ColumnType,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_ty: Option<ColumnType>,
}

impl From<TableColumn> for Column {
    fn from(value: TableColumn) -> Self {
        Self {
            name: value.name,
            ty: value.ty.into(),
            source_ty: None,
        }
    }
}

impl From<Column> for TableColumn {
    fn from(value: Column) -> Self {
        TableColumn::new(value.name.clone(), value.ty.into())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryRequest {
    select: Vec<String>,
    from: String,
    #[serde(default)]
    group_by: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregateData {
    value: serde_json::Value,
    components: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum QueryResponse {
    Empty {
        errors: Vec<String>,
    },
    WithAggregatedData {
        columns: Vec<Column>,
        aggregate_columns: Vec<Column>,
        data: Vec<Vec<serde_json::Value>>,
        aggregates: Vec<Vec<AggregateData>>,
    },
    WithData {
        columns: Vec<Column>,
        data: Vec<Vec<serde_json::Value>>,
    },
}

impl QueryResponse {
    pub fn to_query_result(self) -> QueryResult {
        match self {
            QueryResponse::Empty { .. } => {
                info!("An empty query response was received and was converted to empty rows");
                QueryResult::Rows(vec![])
            }
            QueryResponse::WithData { columns, data } => {
                Self::build_row_query_result(columns, data)
            }
            QueryResponse::WithAggregatedData {
                columns,
                aggregate_columns,
                data,
                aggregates,
            } => Self::build_aggregated_row_query_result(
                columns,
                aggregate_columns,
                data,
                aggregates,
            ),
        }
    }

    fn build_row_query_result(
        columns: Vec<Column>,
        data: Vec<Vec<serde_json::Value>>,
    ) -> QueryResult {
        let mut rows = vec![];
        for data_row in data {
            let Some(row) = Row::from_components(
                // TODO: figure out if we need propagation of index_id and timestamp.
                0,
                0,
                columns
                    .iter()
                    .zip(data_row.into_iter())
                    .map(|(c, v)| Self::build_column_and_column_value(c, v)),
            ) else {
                info!("Row skipped during conversion");
                continue;
            };
            rows.push(row);
        }

        QueryResult::Rows(rows)
    }

    fn build_aggregated_row_query_result(
        columns: Vec<Column>,
        aggregate_columns: Vec<Column>,
        data: Vec<Vec<serde_json::Value>>,
        aggregates: Vec<Vec<AggregateData>>,
    ) -> QueryResult {
        let mut aggregated_rows = vec![];

        for (data_row, aggregates_row) in data.into_iter().zip(aggregates.into_iter()) {
            let values = columns
                .iter()
                .zip(data_row.into_iter())
                .map(|(c, v)| Self::build_column_and_column_value(c, v));

            let aggregates = aggregate_columns
                .iter()
                .zip(aggregates_row.into_iter())
                .map(|(c, v)| Self::build_aggregated_row_component(c, v));

            let aggregated_row = AggregatedRow::new(values, aggregates);
            aggregated_rows.push(aggregated_row);
        }

        QueryResult::AggregatedRows(aggregated_rows)
    }

    fn build_column_and_column_value(
        column: &Column,
        value: serde_json::Value,
    ) -> (TableColumn, ColumnValue) {
        let table_column = column.clone().into();
        match (&column.ty, value) {
            (ColumnType::Integer, serde_json::Value::Number(number)) => {
                if number.is_i64() {
                    return (table_column, ColumnValue::Integer(number.as_i64().unwrap()));
                }
            }
            (ColumnType::Float, serde_json::Value::Number(number)) => {
                if number.is_f64() {
                    return (table_column, ColumnValue::Float(number.as_f64().unwrap()));
                }
            }
            (ColumnType::String, serde_json::Value::String(string)) => {
                return (table_column, ColumnValue::String(string));
            }
            (ColumnType::Null, serde_json::Value::Null) => {
                return (table_column, ColumnValue::Null);
            }
            _ => {}
        }

        (table_column, ColumnValue::Null)
    }

    fn build_aggregated_row_component(
        column: &Column,
        aggregate_data: AggregateData,
    ) -> (AggregateColumn, ColumnValue, Vec<ColumnValue>) {
        let (Some(aggregate), column_name) =
            try_parse_queried_column(&column.name).expect("Error while parsing column")
        else {
            return (
                AggregateColumn(Aggregate::Count, column.clone().into()),
                ColumnValue::Null,
                vec![],
            );
        };

        // Since we don't have access to the original column on which the aggregate was run, we type
        // it to null.
        let original_column = Column {
            name: column_name.to_string(),
            ty: column
                .source_ty
                .as_ref()
                .expect("An aggregate column must have a source type")
                .clone(),
            source_ty: None,
        };
        let (main_column, column_value) =
            Self::build_column_and_column_value(&original_column, aggregate_data.value);
        let aggregate_column = AggregateColumn(aggregate, main_column);

        let aggregate_components = aggregate_data
            .components
            .into_iter()
            .map(|v| Self::build_column_and_column_value(column, v).1)
            .collect();

        (aggregate_column, column_value, aggregate_components)
    }

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
    // Create a future for the shard broadcast operation
    let shard_broadcast_future = async {
        if let Some(shards) = state.shards.deref() {
            let create_table = CreateTable::new(&request);
            shards.broadcast(create_table).await.map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Error while creating table in the shards: {}", e),
                )
            })?;
        }

        Ok(())
    }
    .boxed();

    // Create a future for the local table creation operation
    let request = request.clone();
    let local_create_future = async {
        let columns = request.columns.into_iter().map(|c| c.into()).collect();
        TableDefinition::create(state.config.clone(), request.name, columns)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("Error while creating table in the shards: {}", e),
                )
            })?;

        Ok(())
    }
    .boxed();

    let (shard_result, local_result): (io::Result<()>, io::Result<()>) =
        join(shard_broadcast_future, local_create_future).await;
    match (shard_result, local_result) {
        (Ok(_), Ok(_)) => {
            info!("Table created successfully");
            Json("Table created successfully".to_string())
        }
        (Err(e), _) => {
            info!("Error in shard table creation: {}", e);
            Json(format!("Error in shard table creation: {}", e))
        }
        (_, Err(e)) => {
            info!("Error in local table creation: {}", e);
            Json(format!("Error in local table creation: {}", e))
        }
    }
}

pub async fn insert(
    State(state): State<DatabaseState>,
    Json(mut request): Json<InsertRequest>,
) -> Json<String> {
    let mut requests = vec![];
    if let Some(shards) = state.shards.deref() {
        requests = request.split(shards.number_of_shards() + 1);
        request = requests.remove(0);
    }

    // Create futures for each shard insertion operation
    let shard_insert_futures = requests
        .into_iter()
        .map(|request| {
            let shards = state.shards.clone();
            async move {
                if let Some(shards) = shards.deref() {
                    let insert = Insert::new(&request);
                    shards.rr_unicast(insert).await.map_err(|error| {
                        Error::new(
                            ErrorKind::InvalidData,
                            format!("Error while inserting data in the shards: {}", error),
                        )
                    })?;
                }

                Ok(())
            }
            .boxed()
        })
        .collect::<Vec<BoxFuture<io::Result<()>>>>();

    // Create a future for all shard insertions
    let shard_insert_future = async {
        let results = join_all(shard_insert_futures).await;
        if let Some(error) = results.into_iter().find(|r| r.is_err()) {
            error?;
        }

        Ok(())
    }
    .boxed();

    // Create a future for the table insertion operation
    let request = request.clone();
    let table_insert_future = async {
        let table_definition = TableDefinition::open(state.config.clone(), request.into).await?;
        let mut table = table_definition.load().await?;
        table.insert(request.insert, request.values).await?;
        Ok(())
    }
    .boxed();

    // Join the shard insertion and table insertion futures
    let (shard_result, table_result): (io::Result<()>, io::Result<()>) =
        join(shard_insert_future, table_insert_future).await;

    match (shard_result, table_result) {
        (Ok(_), Ok(_)) => {
            info!("Data inserted successfully");
            Json("Data inserted successfully".to_string())
        }
        (Err(e), _) => {
            info!("Error in shard insertion: {}", e);
            Json(format!("Error in shard insertion: {}", e))
        }
        (_, Err(e)) => {
            info!("Error in table insertion: {}", e);
            Json(format!("Error in table insertion: {}", e))
        }
    }
}

pub async fn query(
    State(state): State<DatabaseState>,
    Json(request): Json<QueryRequest>,
) -> Json<QueryResponse> {
    // Create a future for the broadcast operation
    let broadcast_future = async {
        let mut shard_query_results = vec![];
        if let Some(shards) = state.shards.deref() {
            let query = Query::new(&request);
            match shards.broadcast(query).await {
                Ok(query_responses) => {
                    for query_response in query_responses {
                        shard_query_results.push(query_response.to_query_result());
                    }
                }
                Err(error) => {
                    info!("Error while querying data from the shards: {}", error);
                }
            }
        }

        shard_query_results
    }
    .boxed();

    // Create a future for the table query operation
    let request = request.clone();
    let table_query_future = async {
        let table_definition = TableDefinition::open(state.config.clone(), request.from).await;
        match table_definition {
            Ok(table_def) => match table_def.load().await {
                Ok(mut table) => table.query(request.select, request.group_by).await,
                Err(_) => {
                    info!("Could not load table");
                    Err(Error::new(ErrorKind::InvalidData, "Could not load table"))
                }
            },
            Err(_) => {
                info!("Could not open table");
                Err(Error::new(ErrorKind::InvalidData, "Could not open table"))
            }
        }
    }
    .boxed();

    let (shard_query_results, table_query_result) =
        join(broadcast_future, table_query_future).await;
    match table_query_result {
        Ok(mut query_result) => {
            for shard_query_result in shard_query_results {
                match query_result.merge(shard_query_result) {
                    Ok(merged_result) => query_result = merged_result,
                    Err(_) => {
                        info!("Merging of query results failed");
                        return Json(QueryResponse::empty());
                    }
                }
            }
            Json(serialize_query_result(query_result))
        }
        Err(error) => {
            info!("Error while querying table: {}", error);
            Json(QueryResponse::empty())
        }
    }
}

fn serialize_query_result(query_result: QueryResult) -> QueryResponse {
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
        .map(|(a, c)| {
            // We add the type of the column which was used to build the aggregate.
            let source_ty = Some(a.1.ty.into());
            Column {
                name: a.into(),
                ty: c.into(),
                source_ty,
            }
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
            serialized_values.push(value.into());
        }

        serialized_data.push(serialized_values);
    }

    serialized_data
}

fn serialize_aggregated_rows_data(
    aggregated_rows: Vec<AggregatedRow<ColumnValue>>,
) -> (Vec<Vec<serde_json::Value>>, Vec<Vec<AggregateData>>) {
    let mut serialized_data = Vec::with_capacity(aggregated_rows.len());
    let mut serialized_aggregates = Vec::with_capacity(aggregated_rows.len());

    for aggregated_row in aggregated_rows {
        let (values, aggregate_values) = aggregated_row.into_values();

        let mut serialized_values = Vec::with_capacity(values.len());
        for value in values {
            serialized_values.push(value.into());
        }
        serialized_data.push(serialized_values);

        let mut serialized_aggregate_values = Vec::with_capacity(aggregate_values.len());
        for (aggregate_value, aggregate_components) in aggregate_values {
            let serialized_aggregate = AggregateData {
                value: aggregate_value.into(),
                components: aggregate_components.into_iter().map(|a| a.into()).collect(),
            };
            serialized_aggregate_values.push(serialized_aggregate);
        }
        serialized_aggregates.push(serialized_aggregate_values);
    }

    (serialized_data, serialized_aggregates)
}

impl From<ColumnValue> for serde_json::Value {
    fn from(value: ColumnValue) -> Self {
        match value {
            ColumnValue::Integer(value) => serde_json::Value::Number(Number::from(value)),
            ColumnValue::Float(value) => {
                serde_json::Value::Number(Number::from_f64(value).unwrap())
            }
            ColumnValue::String(value) => serde_json::Value::String(value),
            ColumnValue::Null => serde_json::Value::Null,
        }
    }
}
