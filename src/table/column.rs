use std::cmp::Ordering;
use std::f64;
use std::hash::{Hash, Hasher};
use std::io::{Error, ErrorKind};
use std::ops::{Add, AddAssign, Div, Mul};
use std::path::Path;
use std::str;

use tokio::fs::read_dir;
use tokio::io;

use crate::table::aggregate::Aggregate;
use crate::table::FromDisk;

const INTEGER_VALUE_SIZE: usize = std::mem::size_of::<i64>();
const FLOAT_VALUE_SIZE: usize = std::mem::size_of::<f64>();
// For now, we can store strings up to 256 bytes.
const STRING_VALUE_SIZE: usize = 256;
const NULL_VALUE_SIZE: usize = 0;

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum ColumnType {
    Integer,
    Float,
    String,
    Null,
}

impl ColumnType {
    pub const fn size(&self) -> usize {
        match self {
            ColumnType::Integer => INTEGER_VALUE_SIZE,
            ColumnType::Float => FLOAT_VALUE_SIZE,
            ColumnType::String => STRING_VALUE_SIZE,
            ColumnType::Null => NULL_VALUE_SIZE,
        }
    }
}

impl<'a> From<&'a ColumnType> for &'a str {
    fn from(value: &'a ColumnType) -> Self {
        match value {
            ColumnType::Integer => "integer",
            ColumnType::Float => "float",
            ColumnType::String => "string",
            ColumnType::Null => "null",
        }
    }
}

impl<'a> From<&'a str> for ColumnType {
    fn from(value: &'a str) -> Self {
        match value {
            "integer" => ColumnType::Integer,
            "float" => ColumnType::Float,
            "string" => ColumnType::String,
            _ => panic!("Invalid column type"),
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

impl ColumnValue {
    pub fn default_integer() -> ColumnValue {
        ColumnValue::Integer(0)
    }

    pub fn default_float() -> ColumnValue {
        ColumnValue::Float(0.0)
    }

    pub fn default_string() -> ColumnValue {
        ColumnValue::String("".to_string())
    }
}

impl From<ColumnType> for ColumnValue {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Integer => ColumnValue::default_integer(),
            ColumnType::Float => ColumnValue::default_float(),
            ColumnType::String => ColumnValue::default_string(),
            ColumnType::Null => ColumnValue::Null,
        }
    }
}

impl PartialEq for ColumnValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (ColumnValue::Integer(a), ColumnValue::Integer(b)) => a == b,
            (ColumnValue::Float(a), ColumnValue::Float(b)) => a.to_bits() == b.to_bits(),
            (ColumnValue::String(a), ColumnValue::String(b)) => a == b,
            (ColumnValue::Null, ColumnValue::Null) => true,
            _ => false,
        }
    }
}

impl Eq for ColumnValue {}

impl PartialOrd for ColumnValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (ColumnValue::Integer(a), ColumnValue::Integer(b)) => a.partial_cmp(b),
            (ColumnValue::Float(a), ColumnValue::Float(b)) => a.partial_cmp(b),
            (ColumnValue::String(a), ColumnValue::String(b)) => a.partial_cmp(b),
            (ColumnValue::Null, ColumnValue::Null) => Some(Ordering::Equal),
            (ColumnValue::Integer(_), _) => Some(Ordering::Less),
            (_, ColumnValue::Integer(_)) => Some(Ordering::Greater),
            (ColumnValue::Float(_), _) => Some(Ordering::Less),
            (_, ColumnValue::Float(_)) => Some(Ordering::Greater),
            (ColumnValue::String(_), _) => Some(Ordering::Less),
            (_, ColumnValue::String(_)) => Some(Ordering::Greater),
        }
    }
}

impl Ord for ColumnValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Hash for ColumnValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ColumnValue::Integer(val) => val.hash(state),
            ColumnValue::Float(val) => val.to_bits().hash(state),
            ColumnValue::String(val) => val.hash(state),
            ColumnValue::Null => 0.hash(state),
        }
    }
}

impl Add for ColumnValue {
    type Output = ColumnValue;

    fn add(self, other: ColumnValue) -> ColumnValue {
        match (self, other) {
            (ColumnValue::Integer(a), ColumnValue::Integer(b)) => ColumnValue::Integer(a + b),
            (ColumnValue::Float(a), ColumnValue::Float(b)) => ColumnValue::Float(a + b),
            (ColumnValue::Integer(a), ColumnValue::Float(b)) => ColumnValue::Float(a as f64 + b),
            (ColumnValue::Float(a), ColumnValue::Integer(b)) => ColumnValue::Float(a + b as f64),
            // Handle other combinations or return Null
            _ => ColumnValue::Null,
        }
    }
}

impl AddAssign for ColumnValue {
    fn add_assign(&mut self, other: ColumnValue) {
        *self = self.clone() + other;
    }
}

impl Mul for ColumnValue {
    type Output = ColumnValue;

    fn mul(self, other: ColumnValue) -> ColumnValue {
        match (self, other) {
            (ColumnValue::Integer(a), ColumnValue::Integer(b)) => ColumnValue::Integer(a * b),
            (ColumnValue::Float(a), ColumnValue::Float(b)) => ColumnValue::Float(a * b),
            (ColumnValue::Integer(a), ColumnValue::Float(b)) => ColumnValue::Float(a as f64 * b),
            (ColumnValue::Float(a), ColumnValue::Integer(b)) => ColumnValue::Float(a * b as f64),
            // Handle other combinations or return Null
            _ => ColumnValue::Null,
        }
    }
}

impl Div for ColumnValue {
    type Output = ColumnValue;

    fn div(self, other: ColumnValue) -> ColumnValue {
        match (self, other) {
            (ColumnValue::Integer(a), ColumnValue::Integer(b)) => {
                if b == 0 {
                    ColumnValue::Null
                } else {
                    ColumnValue::Integer(a / b)
                }
            }
            (ColumnValue::Float(a), ColumnValue::Float(b)) => {
                if b == 0.0 {
                    ColumnValue::Null
                } else {
                    ColumnValue::Float(a / b)
                }
            }
            (ColumnValue::Integer(a), ColumnValue::Float(b)) => {
                if b == 0.0 {
                    ColumnValue::Null
                } else {
                    ColumnValue::Float(a as f64 / b)
                }
            }
            (ColumnValue::Float(a), ColumnValue::Integer(b)) => {
                if b == 0 {
                    ColumnValue::Null
                } else {
                    ColumnValue::Float(a / b as f64)
                }
            }
            // Handle other combinations or return Null
            _ => ColumnValue::Null,
        }
    }
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
            ColumnType::Null => ColumnValue::Null,
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

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct AggregateColumn(pub Aggregate, pub Column);

impl From<AggregateColumn> for String {
    fn from(value: AggregateColumn) -> Self {
        let aggregate: &str = value.0.into();
        format!("{}({})", aggregate, value.1.name)
    }
}

pub type QueriedColumns = (Vec<Column>, Vec<AggregateColumn>);

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

pub fn parse_and_validate_queried_columns(
    available_columns: &Vec<Column>,
    queried_columns: &Vec<String>,
) -> io::Result<QueriedColumns> {
    let mut parsed_columns = vec![];
    let mut parsed_aggregate_columns = vec![];

    for queried_column in queried_columns {
        let (aggregate, column) = try_parse_queried_column(queried_column)?;
        let found_column = get_column(available_columns, column)?;
        match aggregate {
            Some(aggregate) => {
                // We add the aggregate column in the columns too since we want to open the files
                // of the aggregated columns too.
                parsed_columns.push(found_column.clone());
                parsed_aggregate_columns.push(AggregateColumn(aggregate, found_column))
            }
            None => parsed_columns.push(found_column),
        };
    }

    Ok((parsed_columns, parsed_aggregate_columns))
}

pub fn parse_and_validate_columns(
    available_columns: &Vec<Column>,
    columns: &Vec<String>,
) -> io::Result<Vec<Column>> {
    let mut parsed_columns = Vec::with_capacity(columns.len());
    for column in columns {
        let found_column = get_column(available_columns, column)?;

        parsed_columns.push(found_column);
    }

    Ok(parsed_columns)
}

fn get_column(available_columns: &Vec<Column>, column: &str) -> io::Result<Column> {
    available_columns
        .into_iter()
        .find(|&c| c.name == *column)
        .ok_or(Error::new(
            ErrorKind::Unsupported,
            "One or more columns do not exist on table",
        ))
        .map(|c| c.clone())
}

fn try_parse_queried_column(queried_column: &str) -> io::Result<(Option<Aggregate>, &str)> {
    let queried_column = queried_column.trim();
    if let Some(open_paren_index) = queried_column.find('(') {
        if let Some(close_paren_index) = queried_column.find(')') {
            let function = (&queried_column[..open_paren_index]).trim();
            let column = (&queried_column[open_paren_index + 1..close_paren_index]).trim();

            if !function.is_empty() && !column.is_empty() {
                return Ok((Some(function.into()), column));
            }
        }
    }

    Ok((None, queried_column))
}
