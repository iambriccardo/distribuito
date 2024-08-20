use crate::table::column::ColumnType;

pub mod aggregate;
pub mod column;
pub mod cursor;
pub mod table;

pub trait FromDisk {
    fn from(column_type: ColumnType, data: Vec<u8>) -> Self;
}
