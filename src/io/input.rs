use crate::configuration::InputSpec;
use crate::io::dataframe::{Column, ColumnValue, DataFrame, InputAttributeType};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::error::Error;
use string_error::into_err;

type JSONInput = HashMap<String, serde_json::Value>;
type ColumnValueExtractor = fn(value: &serde_json::Value) -> Result<ColumnValue, &'static str>;

pub fn read_dataframe(reader: impl std::io::BufRead, spec: &InputSpec, as_single_object: bool) -> Result<DataFrame, Box<dyn Error>> {
    let input: Vec<JSONInput> = if as_single_object {
        serde_json::from_reader(reader)?
    } else {
        let mut result: Vec<JSONInput> = Vec::new();
        let deserializer = serde_json::Deserializer::from_reader(reader);
        for v in deserializer.into_iter::<JSONInput>() {
            result.push(v?);
        }
        result
    };

    let mut columns: IndexMap<String, Column> = IndexMap::new();
    for attr in &spec.attrs {
        let result = extract_column(&attr.name, attr.attr_type, &input, get_extractor_by_attribute_type(attr.attr_type));
        match result {
            Ok(column) => columns.insert(column.name.clone(), column),
            Err(err) => return Err(err),
        };
    }

    Ok(DataFrame {
        columns,
        group_columns: spec.group_by.clone(),
    })
}

fn extract_column(
    name: &str,
    attr_type: InputAttributeType,
    input: &[JSONInput],
    extractor: ColumnValueExtractor,
) -> Result<Column, Box<dyn Error>> {
    let mut values: Vec<ColumnValue> = Vec::new();
    for input_element in input {
        if let Some(input_value) = input_element.get(name) {
            match extractor(input_value) {
                Ok(v) => values.push(v),
                Err(e) => return Err(into_err(format!("failed to parse value={}: {}", input_value, e))),
            }
        } else {
            values.push(ColumnValue::None);
        }
    }

    Ok(Column {
        name: String::from(name),
        attr_type,
        values,
    })
}

fn get_extractor_by_attribute_type(attr_type: InputAttributeType) -> ColumnValueExtractor {
    match attr_type {
        InputAttributeType::Integer => extract_integer_from_json,
        InputAttributeType::String => extract_string_from_json,
    }
}

fn extract_integer_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
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

fn extract_string_from_json(value: &serde_json::Value) -> Result<ColumnValue, &'static str> {
    if let serde_json::Value::String(v) = value {
        Ok(ColumnValue::String(v.clone()))
    } else {
        Err("value is not a number")
    }
}
