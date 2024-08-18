use std::io::{Error, ErrorKind};
use std::path::PathBuf;

use tokio::fs::{create_dir_all, File};
use tokio::io;

use crate::dio::file::create_and_open_file;
use crate::table::types::ColumnType;

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

pub struct TableDefinition {
    name: String,
    columns: Vec<Column>,
}

impl TableDefinition {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self { name, columns }
    }

    pub async fn load(self) -> io::Result<Table> {
        let table_path = Self::build_path(&self.name)?;
        create_dir_all(&table_path).await?;

        let index_file = create_and_open_file(".index", &table_path).await?;
        let metadata_file = create_and_open_file(".metadata", &table_path).await?;

        let mut column_files = Vec::with_capacity(self.columns.len());
        for column in self.columns.iter() {
            let file_name: String = column.into();
            let column_file = create_and_open_file(&file_name, &table_path).await?;
            column_files.push(column_file);
        }

        Ok(Table {
            definition: self,
            stats: TableStats::from_file(metadata_file).await?,
            index_file,
            column_files,
        })
    }

    fn build_path(table_name: &str) -> io::Result<PathBuf> {
        let home_dir = home::home_dir()
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Impossible to get home directory."))?;

        let folder_path = format!(".distribuito/{}", table_name);

        Ok(home_dir.join(folder_path))
    }
}

pub struct TableStats {
    row_count: u64,
}

impl TableStats {
    pub async fn from_file(file: File) -> io::Result<Self> {
        Ok(TableStats { row_count: 0 })
    }
}

pub struct Table {
    definition: TableDefinition,
    stats: TableStats,
    index_file: File,
    column_files: Vec<File>,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.definition.name
    }
}
