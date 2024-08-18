const INTEGER_VALUE_SIZE: usize = std::mem::size_of::<u64>();
const FLOAT_VALUE_SIZE: usize = std::mem::size_of::<f64>();
// For now, we can store strings up to 256 bytes.
const STRING_VALUE_SIZE: usize = 256;

pub enum ColumnType {
    Integer,
    Float,
    String,
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

impl From<ColumnType> for usize {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Integer => INTEGER_VALUE_SIZE,
            ColumnType::Float => FLOAT_VALUE_SIZE,
            ColumnType::String => STRING_VALUE_SIZE,
        }
    }
}
