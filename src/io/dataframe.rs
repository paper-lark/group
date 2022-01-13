use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Index;
use std::vec::Vec;

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub enum ColumnValue {
    Integer(i64),
    String(String),
    DateTime(DateTime<Utc>),
    None,
}

#[derive(PartialEq, Debug, Deserialize, Clone, Copy)]
pub enum InputAttributeType {
    Integer,
    String,
    DateTime,
}

impl std::string::ToString for ColumnValue {
    fn to_string(&self) -> String {
        match self {
            ColumnValue::Integer(n) => n.to_string(),
            ColumnValue::String(s) => s.clone(),
            ColumnValue::DateTime(d) => d.format("%H:%M:%S%.3f").to_string(),
            ColumnValue::None => String::from(""),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Column {
    pub name: String,
    pub attr_type: InputAttributeType,
    pub values: Vec<ColumnValue>,
}

impl Column {
    pub fn unique(&self) -> HashSet<&ColumnValue> {
        self.values.iter().collect::<HashSet<_>>()
    }
}

impl Index<usize> for Column {
    type Output = ColumnValue;
    fn index(&self, key: usize) -> &ColumnValue {
        &self.values[key]
    }
}

pub trait DataFrame {
    fn len(&self) -> usize;
    fn column_names(&self) -> Vec<&String>;
    fn row(&self, index: usize) -> Vec<ColumnValue>;
    fn raw(&self, index: usize) -> &String;
}

#[derive(PartialEq, Debug)]
pub struct MaterializedDataFrame {
    pub columns: IndexMap<String, Column>,
    raw_values: Vec<String>,
}

impl MaterializedDataFrame {}

impl DataFrame for MaterializedDataFrame {
    fn len(&self) -> usize {
        self.columns[0].values.len()
    }

    fn column_names(&self) -> Vec<&String> {
        self.columns.keys().collect()
    }

    fn row(&self, index: usize) -> Vec<ColumnValue> {
        self.columns.values().map(|c| c[index].clone()).collect()
    }

    fn raw(&self, index: usize) -> &String {
        &self.raw_values[index]
    }
}

impl Index<&String> for MaterializedDataFrame {
    type Output = Column;

    fn index(&self, key: &String) -> &Column {
        &self.columns[key]
    }
}

impl Index<(&String, usize)> for MaterializedDataFrame {
    type Output = ColumnValue;

    fn index(&self, key: (&String, usize)) -> &ColumnValue {
        &self.columns[key.0][key.1]
    }
}

impl MaterializedDataFrame {
    pub fn new(columns: IndexMap<String, Column>, raw_values: Vec<String>) -> MaterializedDataFrame {
        assert!(!columns.is_empty(), "data should have at least one column");
        let row_counts: Vec<usize> = columns.values().map(|c| c.values.len()).collect();
        assert!(!row_counts.is_empty(), "data should have at least one row");
        assert!(row_counts.iter().min() == row_counts.iter().max(), "columns have different number of rows");

        MaterializedDataFrame { columns, raw_values }
    }

    pub fn filter(&self, column_filters: &HashMap<String, ColumnValue>) -> DataFrameFilterView {
        let idx: Vec<usize> = (0..self.len())
            .filter(|i| {
                for c in self.columns.values() {
                    if let Some(expected_value) = column_filters.get(&c.name) {
                        if expected_value != &c[*i] {
                            return false;
                        }
                    }
                }
                true
            })
            .collect();

        DataFrameFilterView { source: self, idx }
    }

    pub fn group_by<'a>(&'a self, columns: &'a [String], extra_columns: &'a [String]) -> DataFrameGroupView {
        let mut row_indices: indexmap::IndexMap<Vec<ColumnValue>, Vec<usize>> = indexmap::IndexMap::new();
        for i in 0..self.len() {
            let row: Vec<ColumnValue> = columns.iter().map(|name| self[name][i].clone()).collect();
            if let Some(group) = row_indices.get_mut(&row) {
                group.push(i);
            } else {
                row_indices.insert(row, vec![i]);
            }
        }

        DataFrameGroupView {
            group_idx: row_indices.into_iter().map(|(_, v)| v).collect(),
            group_columns: columns,
            extra_columns,
            source: self,
        }
    }
}

pub struct DataFrameFilterView<'a> {
    source: &'a MaterializedDataFrame,
    idx: Vec<usize>,
}

impl<'a> DataFrame for DataFrameFilterView<'a> {
    fn len(&self) -> usize {
        self.idx.len()
    }

    fn column_names(&self) -> Vec<&String> {
        self.source.column_names()
    }

    fn row(&self, index: usize) -> Vec<ColumnValue> {
        self.source.columns.values().map(|c| c[self.idx[index]].clone()).collect()
    }

    fn raw(&self, index: usize) -> &String {
        self.source.raw(self.idx[index])
    }
}

impl<'a> Index<&String> for DataFrameFilterView<'a> {
    type Output = Column;

    fn index(&self, key: &String) -> &Column {
        &self.source[key]
    }
}

impl<'a> Index<(&String, usize)> for DataFrameFilterView<'a> {
    type Output = ColumnValue;

    fn index(&self, key: (&String, usize)) -> &ColumnValue {
        &self.source[key.0][self.idx[key.1]]
    }
}

pub struct DataFrameGroupView<'a> {
    source: &'a MaterializedDataFrame,
    group_columns: &'a [String],
    extra_columns: &'a [String],
    group_idx: Vec<Vec<usize>>,
}

impl<'a> DataFrame for DataFrameGroupView<'a> {
    fn len(&self) -> usize {
        self.group_idx.len()
    }

    fn column_names(&self) -> Vec<&String> {
        self.group_columns.iter().chain(self.extra_columns.iter()).collect()
    }

    fn row(&self, index: usize) -> Vec<ColumnValue> {
        self.source.columns.values().map(|c| c[self.group_idx[index][0]].clone()).collect()
    }

    fn raw(&self, index: usize) -> &String {
        self.source.raw(self.group_idx[index][0])
    }
}

impl<'a> Index<&String> for DataFrameGroupView<'a> {
    type Output = Column;

    fn index(&self, key: &String) -> &Column {
        &self.source[key]
    }
}

impl<'a> Index<(&String, usize)> for DataFrameGroupView<'a> {
    type Output = ColumnValue;

    fn index(&self, key: (&String, usize)) -> &ColumnValue {
        &self.source[key.0][self.group_idx[key.1][0]]
    }
}
