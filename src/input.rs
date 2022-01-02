use crate::dataframe::{Column, ColumnValue, ColumnValueExtractor, DataFrame};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::error::Error;
use string_error::into_err;

type JSONInput = HashMap<String, serde_json::Value>;
pub type JSONColumnSpec = (String, ColumnValueExtractor);

pub fn read(reader: impl std::io::BufRead, spec: &[JSONColumnSpec], as_json: bool) -> Result<DataFrame, Box<dyn Error>> {
    let input: Vec<JSONInput> = if as_json {
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
    for (column_name, extractor) in spec {
        let result = extract_column(column_name, &input, *extractor);
        match result {
            Ok(column) => columns.insert(column.name.clone(), column),
            Err(err) => return Err(err),
        };
    }

    Ok(DataFrame {
        columns,
        group_columns: vec![String::from("name"), String::from("static")],
    })
}

fn extract_column(name: &str, input: &[JSONInput], extractor: ColumnValueExtractor) -> Result<Column, Box<dyn Error>> {
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
        values,
    })
}
