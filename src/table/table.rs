use std::collections::hash_map::Entry;
use axum::extract::Query;
use log::info;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::u64;
use tokio::fs::{create_dir_all, File};
use tokio::io;

use crate::config::Config;
use crate::io::file::{create_and_open_file, create_file, seek_or, write, write_end};
use crate::table::aggregate::{GroupKey, GroupValue};
use crate::table::column::{
    get_columns, parse_and_validate_columns, parse_and_validate_queried_columns, AggregateColumn,
    Column, ColumnType, ColumnValue,
};
use crate::table::cursor::{AggregatedRow, ColumnCursor, IndexCursor, Row};
use crate::transport::api::QueryResponse;

fn add_extension(file_name: &str) -> String {
    format!("{}.dsto", file_name)
}

fn build_table_path(config: &Config, table_name: &str) -> PathBuf {
    let mut path_buf = PathBuf::new();
    path_buf.push(config.database_path.clone());
    path_buf.push(config.database_name.clone());
    path_buf.push(table_name);

    path_buf
}

#[derive(Debug)]
pub struct TableDefinition {
    config: Arc<Config>,
    name: String,
    columns: Vec<Column>,
}

impl TableDefinition {
    pub async fn create(
        config: Arc<Config>,
        name: String,
        columns: Vec<Column>,
    ) -> io::Result<Self> {
        let table_path = build_table_path(&config, &name);

        create_dir_all(&table_path).await?;

        create_file(&add_extension(".index"), &table_path).await?;
        create_file(&add_extension(".stats"), &table_path).await?;

        for column in columns.iter() {
            let column_file_name: String = column.into();
            create_file(&add_extension(&column_file_name), &table_path).await?;
        }

        info!("Created table {name} with {} columns", columns.len());

        Ok(Self {
            config: config.clone(),
            name,
            columns,
        })
    }

    pub async fn open(config: Arc<Config>, name: String) -> io::Result<Self> {
        let table_path = build_table_path(&config, &name);

        info!("Opened table {name}");

        Ok(Self {
            config: config.clone(),
            name,
            columns: get_columns(&table_path).await?,
        })
    }

    pub async fn load(self) -> io::Result<Table> {
        let table_path = build_table_path(&self.config, &self.name);
        create_dir_all(&table_path).await?;

        let index_file = create_and_open_file(&add_extension(".index"), &table_path).await?;
        let stats_file = create_and_open_file(&add_extension(".stats"), &table_path).await?;

        info!("Loaded table {} in memory", self.name);

        let stats = TableStats::from_file(stats_file).await?;
        info!(
            "Table stats for {}: rows {}, next index: {}",
            self.name, stats.row_count, stats.next_index
        );

        Ok(Table {
            definition: self,
            stats,
            index: TableIndex::new(index_file),
        })
    }
}

/// Struct representing the stats of the table.
///
/// The structure of the stats file is as follows:
/// - 8 bytes for storing the row count
/// - 8 bytes for storing the next index value
#[derive(Debug)]
pub struct TableStats {
    file: File,
    row_count: u64,
    next_index: u64,
}

impl TableStats {
    pub async fn from_file(mut file: File) -> io::Result<Self> {
        // We try to read the row count or default it to 0.
        let mut row_count = [0u8; ColumnType::Integer.size()];
        seek_or(&mut file, 0, &mut row_count, &u64::to_le_bytes(0)).await?;

        // We try to read the next index or default it to 0.
        let mut next_index = [0u8; ColumnType::Integer.size()];
        seek_or(
            &mut file,
            ColumnType::Integer.size() as u64,
            &mut next_index,
            &u64::to_le_bytes(0),
        )
            .await?;

        Ok(TableStats {
            file,
            row_count: u64::from_le_bytes(row_count),
            next_index: u64::from_le_bytes(next_index),
        })
    }

    pub async fn increment(&mut self) -> io::Result<()> {
        self.row_count += 1;
        self.next_index += 1;

        write(&mut self.file, 0, &u64::to_le_bytes(self.row_count)).await?;
        write(
            &mut self.file,
            ColumnType::Integer.size() as u64,
            &u64::to_le_bytes(self.row_count),
        )
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct TableIndex {
    file: File,
}

impl TableIndex {
    pub fn new(file: File) -> Self {
        Self { file }
    }

    pub async fn append(&mut self, timestamp: u64, stats: &TableStats) -> io::Result<()> {
        write_end(&mut self.file, &u64::to_le_bytes(stats.next_index)).await?;
        write_end(&mut self.file, &u64::to_le_bytes(timestamp)).await?;

        Ok(())
    }
}

pub struct Table {
    definition: TableDefinition,
    stats: TableStats,
    index: TableIndex,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.definition.name
    }

    pub async fn insert(
        &mut self,
        columns: Vec<String>,
        values: Vec<Vec<serde_json::Value>>,
    ) -> io::Result<()> {
        let columns = parse_and_validate_columns(&self.definition.columns, &columns)?;
        let mut column_files = self.open_column_files(&columns).await?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // For each value we insert into the file.
        for value in values {
            if value.len() != columns.len() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "The values supplied do not match the number of columns",
                ));
            }

            // We add an entry in the index for each set of columns.
            self.index.append(timestamp, &self.stats).await?;

            for ((inner_value, column), column_file) in value
                .into_iter()
                .zip(columns.iter())
                .zip(column_files.iter_mut())
            {
                self.insert_value(timestamp, column, column_file, inner_value)
                    .await?;
            }

            // Once insertion has been done, we update the table stats and persist them.
            self.stats.increment().await?;
        }

        Ok(())
    }

    pub async fn query(
        &mut self,
        columns: Vec<String>,
        group_by_columns: Option<Vec<String>>,
    ) -> io::Result<QueryResult> {
        // TODO: implement proper column deduplication via hash sets.
        let (columns, aggregate_columns) =
            parse_and_validate_queried_columns(&self.definition.columns, &columns)?;
        let group_by_columns = parse_and_validate_columns(
            &self.definition.columns,
            &group_by_columns.unwrap_or(vec![]),
        )?;
        // TODO: add group by validation to make sure that the selected and grouped columns are the same.
        let column_files = self.open_column_files(&columns).await?;

        // We query the rows and early return in case no aggregates are supplied.
        let rows = self.query_values(&columns, column_files).await?;
        if aggregate_columns.is_empty() {
            return Ok(QueryResult::Rows(rows));
        }

        // If aggregates are supplied, we will perform grouping in memory.
        let aggregated_rows = self.aggregate_rows(rows, aggregate_columns, group_by_columns)?;

        Ok(QueryResult::AggregatedRows(aggregated_rows))
    }

    async fn query_values(
        &mut self,
        columns: &Vec<Column>,
        column_files: Vec<File>,
    ) -> io::Result<Vec<Row<ColumnValue>>> {
        let mut index_cursor = IndexCursor::new(self.index.file.try_clone().await?);
        let mut column_cursors: Vec<ColumnCursor> = columns
            .into_iter()
            .zip(column_files.into_iter())
            .map(|(c, f)| ColumnCursor::new(c.clone(), f))
            .collect();

        let mut rows = vec![];
        while let Ok(index_row_component) = index_cursor.read::<ColumnValue>().await {
            let mut row_components: Vec<(Column, ColumnValue)> =
                Vec::with_capacity(column_cursors.len());

            for (column_index, column_cursor) in column_cursors.iter_mut().enumerate() {
                // By default, we assume that the column we are reading is null.
                row_components.push((column_cursor.column.clone(), ColumnValue::Null));

                // We loop and try to seek through the next column.
                loop {
                    let column_row_component = column_cursor.read::<ColumnValue>().await?;
                    let same_row = column_row_component.same_row(&index_row_component);
                    let Some(column_value) = column_row_component.value else {
                        break;
                    };

                    // - If the values have the same index (aka belong to the same row), we
                    // advance the cursor and return the read value.
                    // - If the column has a higher index than the index, we just skip the iteration
                    // and let the index continue.
                    // - Otherwise, we just advance the cursor and try to get the next element with
                    // the same index.
                    if same_row {
                        row_components[column_index] = (column_cursor.column.clone(), column_value);
                        column_cursor.advance();
                        break;
                    } else if column_row_component.index_id > index_row_component.index_id {
                        break;
                    } else {
                        column_cursor.advance();
                    }
                }
            }

            // We build the row from all the row components.
            let row = Row::from_components(
                index_row_component.index_id,
                index_row_component.timestamp,
                row_components,
            );
            if let Some(row) = row {
                rows.push(row);
            }

            // We move onto the next index.
            index_cursor.advance();
        }

        Ok(rows)
    }

    fn aggregate_rows(
        &mut self,
        rows: Vec<Row<ColumnValue>>,
        aggregate_columns: Vec<AggregateColumn>,
        group_by_columns: Vec<Column>,
    ) -> io::Result<Vec<AggregatedRow<ColumnValue>>> {
        let mut groups = HashMap::new();
        for row in rows {
            // TODO: for now we group by each individual column, but we will add.
            let group_key = row.group(&group_by_columns);
            let group_value = groups
                .entry(group_key)
                .or_insert_with(|| GroupValue::<ColumnValue>::new(aggregate_columns.clone()));
            group_value.add(row);
        }

        let mut aggregated_rows = vec![];
        for (group_key, group_value) in groups {
            // TODO: return columns ordered in the order in which they were supplied.
            aggregated_rows.push(AggregatedRow::from_group(group_key, group_value));
        }

        Ok(aggregated_rows)
    }

    async fn insert_value(
        &mut self,
        timestamp: u64,
        column: &Column,
        column_file: &mut File,
        value: serde_json::Value,
    ) -> io::Result<()> {
        // We write the data into the specific column.
        match value {
            Value::Number(number) => {
                if !(matches!(column.ty, ColumnType::Integer)
                    || matches!(column.ty, ColumnType::Float))
                {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "Column {} has type {} but you supplied a number",
                            column.name,
                            <&ColumnType as Into<&str>>::into(&column.ty)
                        ),
                    ));
                };

                if number.is_i64() {
                    self.write_value(
                        column_file,
                        timestamp,
                        &i64::to_le_bytes(number.as_i64().unwrap()),
                    )
                        .await?;
                } else if number.is_f64() {
                    self.write_value(
                        column_file,
                        timestamp,
                        &f64::to_le_bytes(number.as_f64().unwrap()),
                    )
                        .await?;
                } else {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "The number is not supported",
                    ));
                }
            }
            Value::String(string) => {
                if !matches!(column.ty, ColumnType::String) {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "Column {} has type {} but you supplied a string",
                            column.name,
                            <&ColumnType as Into<&str>>::into(&column.ty)
                        ),
                    ));
                }

                // We build a string with bytes set to 0 when the string is smaller.
                let mut bytes = [0u8; ColumnType::String.size()];
                for (index, byte) in string
                    .as_bytes()
                    .iter()
                    .take(ColumnType::String.size())
                    .enumerate()
                {
                    bytes[index] = *byte;
                }

                self.write_value(column_file, timestamp, &bytes).await?;
            }
            _ => return Err(Error::new(ErrorKind::Unsupported, "Unsupported value type")),
        }

        Ok(())
    }

    async fn write_value(
        &self,
        column_file: &mut File,
        timestamp: u64,
        data: &[u8],
    ) -> io::Result<()> {
        write_end(column_file, &u64::to_le_bytes(self.stats.next_index)).await?;
        write_end(column_file, &u64::to_le_bytes(timestamp)).await?;
        write_end(column_file, data).await?;

        Ok(())
    }

    async fn open_column_files(&self, columns: &Vec<Column>) -> io::Result<Vec<File>> {
        // We open all columns files since we want to append to each of them.
        let table_path = build_table_path(&self.definition.config, &self.definition.name);

        let mut column_files = vec![];
        for column in columns {
            let column_file_name: String = column.into();
            let column_file =
                create_and_open_file(&add_extension(&column_file_name), &table_path).await?;

            column_files.push(column_file);
        }

        Ok(column_files)
    }
}

#[derive(Debug)]
pub enum QueryResult {
    Rows(Vec<Row<ColumnValue>>),
    AggregatedRows(Vec<AggregatedRow<ColumnValue>>),
}

impl QueryResult {
    pub fn merge(self, other: QueryResult) -> io::Result<QueryResult> {
        match (self, other) {
            (QueryResult::Rows(left), QueryResult::Rows(right)) => Ok(QueryResult::Rows(Self::merge_rows(left, right))),
            (QueryResult::AggregatedRows(left), QueryResult::AggregatedRows(right)) => Ok(QueryResult::AggregatedRows(Self::merge_aggregated_rows(left, right))),
            (_, _) => Err(Error::new(ErrorKind::InvalidData, "Merging rows of different type is not possible"))
        }
    }

    fn merge_rows(mut left: Vec<Row<ColumnValue>>, mut right: Vec<Row<ColumnValue>>) -> Vec<Row<ColumnValue>> {
        left.append(&mut right);
        left
    }

    fn merge_aggregated_rows(left: Vec<AggregatedRow<ColumnValue>>, right: Vec<AggregatedRow<ColumnValue>>) -> Vec<AggregatedRow<ColumnValue>> {
        let mut groups: HashMap<GroupKey<ColumnValue>, GroupValue<ColumnValue>> = HashMap::new();

        // TODO: reduce duplication.
        for left_row in left {
            let (group_key, group_value) = left_row.to_group();
            match groups.entry(group_key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().merge(group_value);
                }
                Entry::Vacant(entry) => {
                    entry.insert(group_value);
                }
            }
        }

        for right_row in right {
            let (group_key, group_value) = right_row.to_group();
            match groups.entry(group_key) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().merge(group_value);
                }
                Entry::Vacant(entry) => {
                    entry.insert(group_value);
                }
            }
        }

        let mut aggregated_rows = vec![];
        for (group_key, group_value) in groups {
            // TODO: return columns ordered in the order in which they were supplied.
            aggregated_rows.push(AggregatedRow::from_group(group_key, group_value));
        }

        aggregated_rows
    }

    pub fn is_empty(&self) -> bool {
        match self {
            QueryResult::Rows(rows) => rows.is_empty(),
            QueryResult::AggregatedRows(aggregated_rows) => aggregated_rows.is_empty(),
        }
    }
}
