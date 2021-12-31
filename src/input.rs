use crate::dataframe::*;
use std::collections::HashMap;
use std::error::Error;
use string_error::into_err;

type JSONInput = Vec<HashMap<String, serde_json::Value>>; 

pub fn read(reader: impl std::io::BufRead, file_spec: HashMap<&str, ColumnValueExtractor>) -> Result<DataFrame, Box<dyn Error>> {
    let input: JSONInput = serde_json::from_reader(reader)?;

    let mut columns: Vec<Column> = Vec::new();
    for (column_name, extractor) in file_spec {
        let result = extract_column(column_name, &input, &extractor);
        match result {
            Ok(column) => columns.push(column),
            Err(err) => return Err(err)
        }
    }
    
    Ok(DataFrame {
        columns: columns,
    })
}

fn extract_column(name: &str, input: &JSONInput, extractor: &ColumnValueExtractor) -> Result<Column, Box<dyn Error>> {
    let mut values: Vec<ColumnValue> = Vec::new();
    for input_element in input {
        if let Some(input_value) = input_element.get(name) {
            match extractor(input_value) {
                Ok(v) => values.push(v),
                Err(e) => return Err(into_err(format!("failed to parse value={}: {}", input_value, e)))
            }
        } else {
            values.push(ColumnValue::None)
        }
    }

    return Ok(Column{
        name: String::from(name),
        values: values,
    });
}


