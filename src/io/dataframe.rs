use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Index;
use std::vec::Vec;

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub enum ColumnValue {
    Integer(i64),
    Boolean(bool),
    String(String),
    DateTime(DateTime<Utc>),
    None,
}

impl std::string::ToString for ColumnValue {
    fn to_string(&self) -> String {
        match self {
            ColumnValue::Integer(n) => n.to_string(),
            ColumnValue::Boolean(b) => {
                if *b {
                    String::from("+")
                } else {
                    String::from("-")
                }
            }
            ColumnValue::String(s) => s.clone(),
            ColumnValue::DateTime(d) => d.format("%H:%M:%S%.3f").to_string(),
            ColumnValue::None => String::from(""),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Column {
    pub name: String,
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

impl<'a> DataFrameGroupView<'a> {
    pub fn timeline(&self, index: usize, resolution: u16) -> String {
        // get timestamp column
        const SLOTS_PER_UNIT: u16 = 8;
        let column_name = "time";
        let time_column = match self.source.columns.get(column_name) {
            None => return String::from(""),
            Some(c) => c,
        };

        // get time grid
        let slot_count = resolution * SLOTS_PER_UNIT;
        let grid = create_timeline_grid(time_column, slot_count);

        // get timestamps for requested index
        let timestamps: Vec<_> = self.group_idx[index]
            .iter()
            .map(|i| time_column[*i].clone())
            .filter_map(|c| if let ColumnValue::DateTime(ts) = c { Some(ts) } else { None })
            .collect();
        let mut slots: Vec<bool> = vec![false; slot_count.into()];
        for ts in timestamps {
            let slot_index = grid
                .iter()
                .enumerate()
                .filter(|(_, t)| **t <= ts)
                .map(|(i, _)| i)
                .last()
                .unwrap_or(0);
            slots[slot_index] = true;
        }

        // create string
        let mut chars = vec![' '; resolution as usize];
        for i in 0..resolution as usize {
            // get max subindex in current resolution unit
            let max_idx = (0..SLOTS_PER_UNIT as usize)
                .filter(|j| slots[i * SLOTS_PER_UNIT as usize + j])
                .map(|j| j + 1)
                .max()
                .unwrap_or(0);

            // choose character for current resolution unit
            let slot_char = match max_idx {
                0 => ' ',
                1 => '▏',
                2 => '▎',
                3 => '▍',
                4 => '▌',
                5 => '▋',
                6 => '▊',
                7 => '▉',
                _ => '█', // FIXME: ok?
            };
            chars[i] = slot_char;
        }
        chars.iter().collect()
    }
}

fn create_timeline_grid(time_column: &Column, resolution: u16) -> Vec<DateTime<Utc>> {
    let ts: Vec<_> = time_column
        .values
        .iter()
        .filter_map(|c| if let ColumnValue::DateTime(ts) = c { Some(ts) } else { None })
        .collect();

    if let Some(min_ts) = ts.iter().min() {
        if let Some(max_ts) = ts.iter().max() {
            let delta = (**max_ts - **min_ts) / ((resolution - 1).into());
            let mut intervals: Vec<DateTime<Utc>> = Vec::new();
            let mut ts = **min_ts;
            for _ in 0..resolution {
                intervals.push(ts);
                ts = ts + delta;
            }

            return intervals;
        }
    }
    Vec::new()
}
