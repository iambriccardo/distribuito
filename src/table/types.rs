const INTEGER_VALUE_SIZE: usize = std::mem::size_of::<u64>();
const FLOAT_VALUE_SIZE: usize = std::mem::size_of::<f64>();
// For now, we can store strings up to 256 bytes.
const STRING_VALUE_SIZE: usize = 256;

#[derive(Debug, Clone)]
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
