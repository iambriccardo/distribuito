use std::fmt::Debug;

use tokio::fs::File;
use tokio::io;

use crate::dio::file::seek;
use crate::table::column::{Column, ColumnType, index_and_timestamp_size};
use crate::table::FromDisk;

#[derive(Debug)]
pub struct Row<T: Debug> {
    index_id: u64,
    timestamp: u64,
    values: Vec<(Column, T)>,
}

impl<T: Debug> Row<T> {
    pub fn from_components(
        index_id: u64,
        timestamp: u64,
        row_components: impl IntoIterator<Item = (Column, T)>,
    ) -> Option<Self> {
        let values = row_components.into_iter().collect();

        Some(Self {
            index_id,
            timestamp,
            values,
        })
    }
}

#[derive(Debug)]
pub struct RowComponent<T> {
    pub index_id: u64,
    pub timestamp: u64,
    pub value: Option<T>,
}

impl<T> RowComponent<T> {
    pub fn new(index_id: u64, timestamp: u64, value: Option<T>) -> Self {
        Self {
            index_id,
            timestamp,
            value,
        }
    }

    pub fn is_index(&self) -> bool {
        self.value.is_none()
    }

    pub fn same_row(&self, other: &RowComponent<T>) -> bool {
        self.index_id == other.index_id && self.timestamp == other.timestamp
    }
}

pub struct ColumnCursor {
    pub column: Column,
    file: File,
    position: u64,
    size: usize,
}

impl ColumnCursor {
    pub fn new(column: Column, file: File) -> Self {
        // We compute the total size of the column data, since we skip data with such size.
        let size = index_and_timestamp_size() + column.size();
        Self {
            column,
            file,
            position: 0,
            size,
        }
    }

    pub async fn read<T: FromDisk + Debug>(&mut self) -> io::Result<RowComponent<T>> {
        let mut index_id = [0u8; ColumnType::Integer.size()];
        let position = self.seek_position();
        seek(&mut self.file, position, &mut index_id).await?;

        let mut timestamp = [0u8; ColumnType::Integer.size()];
        let position = position + (ColumnType::Integer.size() as u64);
        seek(&mut self.file, position, &mut timestamp).await?;

        let mut data: Vec<u8> = Vec::with_capacity(self.column.size());
        for _ in 0..self.column.size() {
            data.push(0u8);
        }
        let position = position + (ColumnType::Integer.size() as u64);
        seek(&mut self.file, position, &mut data[..self.column.size()]).await?;

        Ok(RowComponent::new(
            u64::from_le_bytes(index_id),
            u64::from_le_bytes(timestamp),
            Some(T::from(self.column.ty, data)),
        ))
    }

    pub fn advance(&mut self) {
        self.position += 1
    }

    fn seek_position(&self) -> u64 {
        self.position * (self.size as u64)
    }
}

pub struct IndexCursor {
    file: File,
    position: u64,
    size: usize,
}

impl IndexCursor {
    pub fn new(file: File) -> Self {
        Self {
            file,
            position: 0,
            size: index_and_timestamp_size(),
        }
    }

    pub async fn read<T: FromDisk>(&mut self) -> io::Result<RowComponent<T>> {
        let mut index_id = [0u8; ColumnType::Integer.size()];
        let position = self.seek_position();
        seek(&mut self.file, position, &mut index_id).await?;

        let mut timestamp = [0u8; ColumnType::Integer.size()];
        let position = self.seek_position() + (ColumnType::Integer.size() as u64);
        seek(&mut self.file, position, &mut timestamp).await?;

        Ok(RowComponent::new(
            u64::from_le_bytes(index_id),
            u64::from_le_bytes(timestamp),
            None,
        ))
    }

    pub fn advance(&mut self) {
        self.position += 1
    }

    fn seek_position(&self) -> u64 {
        self.position * (self.size as u64)
    }
}
