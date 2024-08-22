use std::fmt::Debug;
use std::hash::Hash;
use std::io::SeekFrom;
use std::ops::Div;

use crate::table::aggregate::{Aggregable, GroupKey, GroupValue};
use crate::table::column::{index_and_timestamp_size, AggregateColumn, Column, ColumnType};
use crate::table::FromDisk;
use tokio::fs::File;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufStream};

#[derive(Debug)]
pub struct AggregatedRow<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    values: Vec<(Column, T)>,
    aggregates: Vec<(AggregateColumn, T, Vec<T>)>,
}

impl<T> AggregatedRow<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub fn new(
        values: impl IntoIterator<Item = (Column, T)>,
        aggregates: impl IntoIterator<Item = (AggregateColumn, T, Vec<T>)>,
    ) -> Self {
        Self {
            values: values.into_iter().collect(),
            aggregates: aggregates.into_iter().collect(),
        }
    }

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

    pub fn to_group(self) -> (GroupKey<T>, GroupValue<T>) {
        let group_key = GroupKey(self.values.into_iter().collect());
        let group_value = GroupValue::from_aggregates(
            self.aggregates
                .into_iter()
                .map(|(a, _, c)| (a, c))
                .collect(),
        );

        (group_key, group_value)
    }

    pub fn into_values(self) -> (Vec<T>, Vec<(T, Vec<T>)>) {
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
    #[allow(dead_code)]
    index_id: u64,
    #[allow(dead_code)]
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

    pub fn columns(&self) -> Vec<Column> {
        self.values.iter().map(|(c, _)| c.clone()).collect()
    }

    pub fn group(&self, group_by_columns: &Vec<Column>) -> GroupKey<T> {
        let key = self
            .values
            .iter()
            .filter_map(|(c, v)| {
                group_by_columns
                    .into_iter()
                    .find(|inner_c| **inner_c == *c)?;
                Some((c.clone(), v.clone()))
            })
            .collect();

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

    pub fn same_row(&self, other: &RowComponent<T>) -> bool {
        self.index_id == other.index_id && self.timestamp == other.timestamp
    }
}

pub struct ColumnCursor {
    pub column: Option<Column>,
    file: BufStream<File>,
}

impl ColumnCursor {
    pub fn new(column: Option<Column>, file: BufStream<File>) -> Self {
        Self { column, file }
    }

    pub async fn read<T>(&mut self) -> io::Result<RowComponent<T>>
    where
        T: FromDisk + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
    {
        let total_size = ColumnType::Integer.size() * 2 + self.column_size();
        let mut buffer = vec![0u8; total_size];
        self.file.read_exact(&mut buffer).await?;

        let index_id = u64::from_le_bytes(buffer[..ColumnType::Integer.size()].try_into().unwrap());
        let timestamp = u64::from_le_bytes(
            buffer[ColumnType::Integer.size()..ColumnType::Integer.size() * 2]
                .try_into()
                .unwrap(),
        );
        let Some(column) = &self.column else {
            return Ok(RowComponent::new(index_id, timestamp, None));
        };

        let data = buffer[ColumnType::Integer.size() * 2..].to_vec();
        Ok(RowComponent::new(
            index_id,
            timestamp,
            Some(T::from(column.ty, data)),
        ))
    }

    pub async fn undo(&mut self) -> io::Result<()> {
        // We compute the total size of the column data, since we skip data with such size.
        let size = (index_and_timestamp_size() + self.column_size()) as i64;
        self.file.seek(SeekFrom::Current(-size)).await.map(|_| ())
    }

    fn column_size(&self) -> usize {
        self.column.as_ref().map_or(0, |c| c.size())
    }
}
