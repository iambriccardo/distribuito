use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::u64;

use serde_json::Value;
use tokio::fs::{create_dir_all, File, read_dir};
use tokio::io;

use crate::dio::file::{create_and_open_file, create_file, seek_or};
use crate::table::types::ColumnType;

#[derive(Debug)]
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
        let table_path = Self::build_path(&name)?;

        create_dir_all(&table_path).await?;

        create_file(".index", &table_path).await?;
        create_file(".stats", &table_path).await?;

        for column in columns.iter() {
            let file_name: String = column.into();
            create_file(&file_name, &table_path).await?;
        }

        Ok(Self { name, columns })
    }

    pub async fn open(name: String) -> io::Result<Self> {
        let table_path = Self::build_path(&name)?;

        Ok(Self {
            name,
            columns: Self::get_columns(&table_path).await?,
        })
    }

    pub async fn load(self) -> io::Result<Table> {
        let table_path = Self::build_path(&self.name)?;
        create_dir_all(&table_path).await?;

        let index_file = create_and_open_file(".index", &table_path).await?;
        let stats_file = create_and_open_file(".stats", &table_path).await?;

        Ok(Table {
            definition: self,
            stats: TableStats::from_file(stats_file).await?,
            index_file,
        })
    }

    fn build_path(table_name: &str) -> io::Result<PathBuf> {
        let home_dir = home::home_dir()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Impossible to get home directory."))?;

        let folder_path = format!(".distribuito/{}", table_name);

        Ok(home_dir.join(folder_path))
    }

    async fn get_columns<P: AsRef<Path>>(path: P) -> io::Result<Vec<Column>> {
        let mut columns = vec![];

        let mut dir = read_dir(path.as_ref()).await?;
        while let Some(entry) = dir.next_entry().await? {
            if let Ok(file_type) = entry.file_type().await {
                if file_type.is_file() {
                    if let Ok(file_name) = entry.file_name().into_string() {
                        if let Some((column_name, column_type)) = Self::parse_file_name(&file_name)
                        {
                            columns.push(Column::new(column_name, column_type));
                        }
                    }
                }
            }
        }

        Ok(columns)
    }

    fn parse_file_name(file_name: &str) -> Option<(String, ColumnType)> {
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
}

/// Struct representing the stats of the table.
///
/// The structure of the stats file is as follows:
/// - 8 bytes for storing the row count
/// - 8 bytes for storing the next index value
#[derive(Debug)]
pub struct TableStats {
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
        seek_or(&mut file, 0, &mut next_index, &u64::to_le_bytes(0)).await?;

        Ok(TableStats {
            row_count: u64::from_le_bytes(row_count),
            next_index: u64::from_le_bytes(next_index),
        })
    }
}

#[derive(Debug)]
pub struct Table {
    definition: TableDefinition,
    stats: TableStats,
    index_file: File,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.definition.name
    }

    pub async fn insert(
        &self,
        columns: Vec<String>,
        values: Vec<Vec<serde_json::Value>>,
    ) -> io::Result<()> {
        if !self.validate_columns(&columns) {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "The column does not exist",
            ));
        }

        for value in values {
            for inner_value in value {
                self.insert_value(inner_value).await?;
            }
        }

        Ok(())
    }

    async fn insert_value(&self, value: serde_json::Value) -> io::Result<()> {
        // TODO: insert index entry.
        match value {
            Value::Number(number) => {
                if number.is_i64() {
                    self.write_value(&i64::to_le_bytes(number.as_i64().unwrap()))
                        .await?;
                } else if number.is_f64() {
                    self.write_value(&f64::to_le_bytes(number.as_f64().unwrap()))
                        .await?;
                } else {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "The number is not supported",
                    ));
                }
            }
            Value::String(string) => {
                self.write_value(&string.as_bytes()[..ColumnType::String.size()])
                    .await?;
            }
            _ => return Err(Error::new(ErrorKind::Unsupported, "Cannot write value")),
        }

        Ok(())
    }

    async fn write_value(&self, data: &[u8]) -> io::Result<()> {
        // TODO: before data insert index and timestamp.
        Ok(())
    }

    fn validate_columns(&self, columns: &Vec<String>) -> bool {
        for column in columns {
            for inner_column in self.definition.columns.iter() {
                if *column == *inner_column.name {
                    return true;
                }
            }
        }

        return false;
    }
}
