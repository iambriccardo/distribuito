use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Div;

use tokio::fs::File;
use tokio::io;

use crate::dio::file::seek;
use crate::table::aggregate::{Aggregable, GroupKey, GroupValue};
use crate::table::column::{AggregateColumn, Column, ColumnType, index_and_timestamp_size};
use crate::table::FromDisk;

#[derive(Debug)]
pub struct AggregatedRow<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    values: Vec<(Column, T)>,
    aggregates: Vec<(AggregateColumn, T, Option<Vec<T>>)>,
}

impl<T> AggregatedRow<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub fn from_group(group_key: GroupKey<T>, group_value: GroupValue<T>) -> Self {
        Self {
            values: group_key.0.into_iter().collect(),
            aggregates: group_value
                .aggregates
                .into_iter()
                .map(|(aggregate_column, aggregate_components)| {
                    let (aggregate_value, aggregate_components) = aggregate_components.compute();
                    (aggregate_column, aggregate_value, aggregate_components)
                })
                .collect(),
        }
    }

    pub fn into_values(self) -> (Vec<T>, Vec<(T, Option<Vec<T>>)>) {
        let values = self.values.into_iter().map(|(_, v)| v).collect();
        let aggregates = self
            .aggregates
            .into_iter()
            .map(|(_, v, c)| (v, c))
            .collect();

        (values, aggregates)
    }

    pub fn columns(&self) -> Vec<Column> {
        self.values.iter().map(|(c, _)| c.clone()).collect()
    }

    pub fn aggregate_columns(&self) -> Vec<(AggregateColumn, &T)> {
        // We have to return `&T` since we will use that to infer the type of the aggregate, which
        // can differ from the type of the `column` on which it is run.
        self.aggregates
            .iter()
            .map(|(c, v, _)| (c.clone(), v))
            .collect()
    }
}

#[derive(Debug)]
pub struct Row<T>
where
    T: Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    index_id: u64,
    timestamp: u64,
    values: Vec<(Column, T)>,
}

impl<T> Row<T>
where
    T: Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub fn from_components(
        index_id: u64,
        timestamp: u64,
        row_components: impl IntoIterator<Item = (Column, T)>,
    ) -> Option<Self> {
        Some(Self {
            index_id,
            timestamp,
            values: row_components.into_iter().collect(),
        })
    }

    pub fn into_values(self) -> Vec<T> {
        self.values.into_iter().map(|(_, v)| v).collect()
    }

    pub fn value(&self, column: &Column) -> Option<&T> {
        self.values
            .iter()
            .find(|(c, _)| c == column)
            .map(|(_, v)| v)
    }

    pub fn take_value(&mut self, column: &Column) -> Option<T> {
        if let Some(pos) = self.values.iter().position(|(c, _)| c == column) {
            Some(self.values.remove(pos).1)
        } else {
            None
        }
    }

    pub fn columns(&self) -> Vec<Column> {
        self.values.iter().map(|(c, _)| c.clone()).collect()
    }

    pub fn group(&self) -> GroupKey<T> {
        let key = self.values.iter().map(|v| v.clone()).collect();

        GroupKey(key)
    }
}

#[derive(Debug)]
pub struct RowComponent<T>
where
    T: Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub index_id: u64,
    pub timestamp: u64,
    pub value: Option<T>,
}

impl<T> RowComponent<T>
where
    T: Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
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

    pub async fn read<T>(&mut self) -> io::Result<RowComponent<T>>
    where
        T: FromDisk + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
    {
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

    pub async fn read<T>(&mut self) -> io::Result<RowComponent<T>>
    where
        T: FromDisk + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
    {
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
