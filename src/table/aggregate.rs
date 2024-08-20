use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Div;

use crate::table::column::{AggregateColumn, Column, ColumnValue};
use crate::table::cursor::Row;

#[derive(Debug, Clone)]
pub enum Aggregate {
    Count,
    Sum,
    Avg,
}

impl<'a> From<&'a str> for Aggregate {
    fn from(value: &'a str) -> Self {
        match value.to_lowercase().as_str() {
            "count" => Aggregate::Count,
            "sum" => Aggregate::Sum,
            "avg" => Aggregate::Avg,
            _ => Aggregate::Count,
        }
    }
}

impl<'a> From<Aggregate> for &'a str {
    fn from(value: Aggregate) -> Self {
        match value {
            Aggregate::Count => "count",
            Aggregate::Sum => "sum",
            Aggregate::Avg => "avg",
        }
    }
}

/// Enumerator representing the merging operation to do between aggregate components.
#[derive(Debug, Clone)]
pub enum MergeOp {
    Count,
    Sum,
}

#[derive(Debug)]
pub enum AggregateComponents<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    Count(T),
    Sum(T),
    Avg { sum: T, count: T },
}

impl<T> AggregateComponents<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub fn new(aggregate_column: &AggregateColumn) -> Self {
        match aggregate_column.0 {
            Aggregate::Count => AggregateComponents::Count(T::init(aggregate_column)),
            Aggregate::Sum => AggregateComponents::Sum(T::init(aggregate_column)),
            Aggregate::Avg => AggregateComponents::Avg {
                sum: T::init(aggregate_column),
                count: T::init(aggregate_column),
            },
        }
    }

    pub fn aggregate(&mut self, value: &T) {
        match self {
            AggregateComponents::Count(count) => count.merge(MergeOp::Count, value.clone()),
            AggregateComponents::Sum(sum) => sum.merge(MergeOp::Sum, value.clone()),
            AggregateComponents::Avg { sum, count } => {
                sum.merge(MergeOp::Sum, value.clone());
                count.merge(MergeOp::Count, value.clone());
            }
        }
    }

    pub fn compute(self) -> (T, Option<Vec<T>>) {
        match self {
            AggregateComponents::Count(count) => (count, None),
            AggregateComponents::Sum(sum) => (sum, None),
            AggregateComponents::Avg { sum, count } => {
                (sum.clone() / count.clone(), Some(vec![sum, count]))
            }
        }
    }
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct GroupKey<T>(pub BTreeSet<(Column, T)>)
where
    T: Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash;

#[derive(Debug)]
pub struct GroupValue<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub aggregates: Vec<(AggregateColumn, AggregateComponents<T>)>,
}

impl<T> GroupValue<T>
where
    T: Aggregable<T> + Div<Output = T> + Debug + Clone + Ord + PartialOrd + Eq + PartialEq + Hash,
{
    pub fn new(aggregate_columns: Vec<AggregateColumn>) -> Self {
        Self {
            aggregates: aggregate_columns
                .into_iter()
                .map(|a| {
                    let c = AggregateComponents::new(&a);
                    (a, c)
                })
                .collect(),
        }
    }

    pub fn add(&mut self, row: Row<T>) {
        for (aggregate_column, aggregate_components) in self.aggregates.iter_mut() {
            // TODO: take value out of the array instead of cloning.
            if let Some(value) = row.value(&aggregate_column.1) {
                aggregate_components.aggregate(value);
            }
        }
    }
}

pub trait Aggregable<T> {
    fn init(aggregate_column: &AggregateColumn) -> T;

    fn merge(&mut self, aggregate_op: MergeOp, other: T);
}

impl Aggregable<ColumnValue> for ColumnValue {
    fn init(aggregate_column: &AggregateColumn) -> ColumnValue {
        match aggregate_column.0 {
            Aggregate::Count => ColumnValue::Integer(0),
            Aggregate::Sum => aggregate_column.1.ty.into(),
            Aggregate::Avg => ColumnValue::Float(0.0),
        }
    }

    fn merge(&mut self, merge_op: MergeOp, other: ColumnValue) {
        // TODO: maybe instead of &mut we want to consume the value and return a new one.
        *self = match merge_op {
            MergeOp::Count => self.clone() + ColumnValue::Integer(1),
            MergeOp::Sum => self.clone() + other,
        }
    }
}