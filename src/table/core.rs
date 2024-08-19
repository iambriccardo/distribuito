use std::cmp::min;
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::u64;
use log::info;
use serde_json::Value;
use tokio::fs::{create_dir_all, File, read_dir};
use tokio::io;

use crate::dio::file::{create_and_open_file, create_file, seek_or, write, write_end};
use crate::table::types::ColumnType;

fn add_extension(file_name: &str) -> String {
    format!("{}.dsto", file_name)
}

fn build_table_path(table_name: &str) -> io::Result<PathBuf> {
    let home_path = home::home_dir()
        .ok_or_else(|| Error::new(ErrorKind::NotFound, "Impossible to get home directory"))?;

    // TODO: we might want to make the path configurable via CLI.
    Ok(home_path.join(format!(".distribuito/{}", table_name)))
}

async fn get_columns<P: AsRef<Path>>(path: P) -> io::Result<Vec<Column>> {
    let mut columns = vec![];

    let mut dir = read_dir(path.as_ref()).await?;
    while let Some(entry) = dir.next_entry().await? {
        if let Ok(file_type) = entry.file_type().await {
            if file_type.is_file() {
                if let Ok(file_name) = entry.file_name().into_string() {
                    if let Some((column_name, column_type)) = parse_column_file_name(&file_name) {
                        columns.push(Column::new(column_name, column_type));
                    }
                }
            }
        }
    }

    Ok(columns)
}

fn parse_column_file_name(file_name: &str) -> Option<(String, ColumnType)> {
    let parts: Vec<&str> = file_name.split('.').collect();
    if parts.len() != 3 {
        return None;
    }

    let column_name = parts[0];
    let column_type = parts[1];
    let extension = parts[2];

    // Check that the extension is correct
    if extension != "dsto" {
        return None;
    }

    // Check if column_type is not empty
    if column_type.is_empty() {
        return None;
    }

    // Check if column_name is not empty and contains only alphanumeric characters and underscores
    if column_name.is_empty() || !column_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return None;
    }

    Some((column_name.to_string(), column_type.into()))
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    ty: ColumnType,
}

impl Column {
    pub fn new(name: String, ty: ColumnType) -> Self {
        Self { name, ty }
    }
}

impl<'a> From<&'a Column> for String {
    fn from(value: &'a Column) -> Self {
        format!(
            "{}.{}",
            value.name,
            <&ColumnType as Into<&str>>::into(&value.ty)
        )
    }
}

#[derive(Debug)]
pub struct TableDefinition {
    name: String,
    columns: Vec<Column>,
}

impl TableDefinition {
    pub async fn create(name: String, columns: Vec<Column>) -> io::Result<Self> {
        let table_path = build_table_path(&name)?;

        create_dir_all(&table_path).await?;

        create_file(&add_extension(".index"), &table_path).await?;
        create_file(&add_extension(".stats"), &table_path).await?;

        for column in columns.iter() {
            let column_file_name: String = column.into();
            create_file(&add_extension(&column_file_name), &table_path).await?;
        }
        
        info!("Created table {name} with {} columns", columns.len());

        Ok(Self { name, columns })
    }

    pub async fn open(name: String) -> io::Result<Self> {
        let table_path = build_table_path(&name)?;

        info!("Opened table {name}");

        Ok(Self {
            name,
            columns: get_columns(&table_path).await?,
        })
    }

    pub async fn load(self) -> io::Result<Table> {
        let table_path = build_table_path(&self.name)?;
        create_dir_all(&table_path).await?;

        let index_file = create_and_open_file(".index", &table_path).await?;
        let stats_file = create_and_open_file(".stats", &table_path).await?;
        
        info!("Loaded table {} in memory", self.name);
        
        let stats = TableStats::from_file(stats_file).await?;
        info!("Table stats for {}: rows {}, next index: {}", self.name, stats.row_count, stats.next_index);

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
        let Some(columns) = self.validate_columns(columns) else {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "One or more columns do not exist on table",
            ));
        };

        // We open all columns files since we want to append to each of them.
        let table_path = build_table_path(&self.definition.name)?;
        let mut column_files = HashMap::new();
        for column in columns.iter() {
            let column_file_name: String = column.into();
            let column_file =
                create_and_open_file(&add_extension(&column_file_name), &table_path).await?;

            column_files.insert(column.name.clone(), column_file);
        }

        // For each value we insert into the file.
        for value in values {
            if value.len() != columns.len() {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "The values supplied do not match the number of columns",
                ));
            }

            for (inner_value, column) in value.into_iter().zip(columns.iter()) {
                if let Some(column_file) = column_files.get_mut(&column.name) {
                    self.insert_value(column, column_file, inner_value).await?;
                };
            }
        }

        Ok(())
    }

    async fn insert_value(
        &mut self,
        column: &Column,
        column_file: &mut File,
        value: serde_json::Value,
    ) -> io::Result<()> {
        let timestamp = SystemTime::now().elapsed().unwrap().as_secs();

        // We write the data into the index.
        self.index.append(timestamp, &self.stats).await?;

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

                let bytes = string.as_bytes();
                self.write_value(
                    column_file,
                    timestamp,
                    &bytes[..min(bytes.len(), ColumnType::String.size())],
                )
                .await?;
            }
            _ => return Err(Error::new(ErrorKind::Unsupported, "Unsupported value type")),
        }

        // Once insertion has been done, we update the table stats and persist them.
        self.stats.increment().await?;

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

    fn validate_columns(&self, columns: Vec<String>) -> Option<Vec<Column>> {
        let mut found_columns = Vec::with_capacity(columns.len());
        for column in columns {
            let Some(found_column) = self.definition.columns.iter().find(|&c| c.name == column)
            else {
                return None;
            };
            found_columns.push(found_column.clone());
        }

        Some(found_columns)
    }
}
