use std::f64;
use std::path::Path;
use std::str;

use tokio::fs::read_dir;
use tokio::io;

use crate::table::FromDisk;

const INTEGER_VALUE_SIZE: usize = std::mem::size_of::<i64>();
const FLOAT_VALUE_SIZE: usize = std::mem::size_of::<f64>();
// For now, we can store strings up to 256 bytes.
const STRING_VALUE_SIZE: usize = 256;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum ColumnType {
    Integer,
    Float,
    String,
}

impl ColumnType {
    pub const fn size(&self) -> usize {
        match self {
            ColumnType::Integer => INTEGER_VALUE_SIZE,
            ColumnType::Float => FLOAT_VALUE_SIZE,
            ColumnType::String => STRING_VALUE_SIZE,
        }
    }
}

impl<'a> From<&'a ColumnType> for &'a str {
    fn from(value: &'a ColumnType) -> Self {
        match value {
            ColumnType::Integer => "integer",
            ColumnType::Float => "float",
            ColumnType::String => "string",
        }
    }
}

impl<'a> From<&'a str> for ColumnType {
    fn from(value: &'a str) -> Self {
        match value {
            "integer" => ColumnType::Integer,
            "float" => ColumnType::Float,
            "string" => ColumnType::String,
            _ => ColumnType::Integer,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ColumnValue {
    Integer(i64),
    Float(f64),
    String(String),
    Null,
}

fn to_array(vec: Vec<u8>, array: &mut [u8], length: usize) {
    for (index, value) in vec.into_iter().take(length).enumerate() {
        array[index] = value;
    }
}

fn until_null_char(array: &[u8]) -> &[u8] {
    if let Some(index) = array.iter().position(|&b| b == 0) {
        &array[..index]
    } else {
        array
    }
}

impl FromDisk for ColumnValue {
    fn from(column_type: ColumnType, data: Vec<u8>) -> ColumnValue {
        match column_type {
            ColumnType::Integer => {
                let mut new_data = [0u8; ColumnType::Integer.size()];
                to_array(data, &mut new_data, ColumnType::Integer.size());

                ColumnValue::Integer(i64::from_le_bytes(new_data))
            }
            ColumnType::Float => {
                let mut new_data = [0u8; ColumnType::Float.size()];
                to_array(data, &mut new_data, ColumnType::Float.size());

                ColumnValue::Float(f64::from_le_bytes(new_data))
            }
            ColumnType::String => {
                let mut new_data = [0u8; ColumnType::String.size()];
                to_array(data, &mut new_data, ColumnType::String.size());

                ColumnValue::String(
                    str::from_utf8(until_null_char(&new_data))
                        .unwrap()
                        .to_string(),
                )
            }
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

impl Column {
    pub fn new(name: String, ty: ColumnType) -> Self {
        Self { name, ty }
    }

    pub fn size(&self) -> usize {
        self.ty.size()
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

pub async fn get_columns<P: AsRef<Path>>(path: P) -> io::Result<Vec<Column>> {
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

pub fn parse_column_file_name(file_name: &str) -> Option<(String, ColumnType)> {
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

/// The size of the index and timestamp columns which are both of type [`ColumnType::Integer`].
pub fn index_and_timestamp_size() -> usize {
    ColumnType::Integer.size() + ColumnType::Integer.size()
}
