use std::collections::HashSet;
use std::vec::Vec;

#[derive(Hash, PartialEq, Eq, Debug)]
pub enum ColumnValue {
    Integer(i64),
    String(String),
    None,
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

pub type ColumnValueExtractor = fn(value: &serde_json::Value) -> Result<ColumnValue, &'static str>;

pub fn extract_integer_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
    if let serde_json::Value::Number(n) = value {
        if let Some(v) = n.as_i64() {
            Ok(ColumnValue::Integer(v))
        } else {
            Err("value is not integer")
        }
    } else {
        Err("value is not a number")
    }
}

pub fn extract_string_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
    if let serde_json::Value::String(v) = value {
        Ok(ColumnValue::String(v.clone()))
    } else {
        Err("value is not a number")
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub values: Vec<ColumnValue>,
}

impl Column {
    pub fn unique(&self) -> HashSet<&ColumnValue> {
        self.values.iter().collect::<HashSet<_>>()
    }
}

#[derive(Debug)]
pub struct DataFrame {
    pub columns: Vec<Column>,
}
