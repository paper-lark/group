use indexmap::Equivalent;
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Index;
use std::vec::Vec;

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub enum ColumnValue {
    Integer(i64),
    String(String),
    None,
}

#[derive(PartialEq, Debug, Deserialize, Clone, Copy)]
pub enum InputAttributeType {
    Integer,
    String,
}

impl std::string::ToString for ColumnValue {
    fn to_string(&self) -> String {
        match self {
            ColumnValue::Integer(n) => n.to_string(),
            ColumnValue::String(s) => s.clone(),
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

#[derive(PartialEq, Debug)]
pub struct DataFrame {
    pub columns: IndexMap<String, Column>,
    pub group_columns: Vec<String>,
}

impl<Q: ?Sized> Index<&Q> for DataFrame
where
    Q: Hash + Equivalent<String>,
{
    type Output = Column;

    fn index(&self, key: &Q) -> &Column {
        &self.columns[key]
    }
}
