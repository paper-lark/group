pub mod column {
    use std::vec::Vec;
    use std::collections::HashSet;

    #[derive(Hash,PartialEq,Eq,Debug)]
    pub enum ColumnValue {
        Integer(i64),
        String(String),
        None
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
        pub values: Vec<ColumnValue>
    }

    impl Column {
        pub fn unique(&self) -> HashSet<&ColumnValue> {
           HashSet::from_iter(self.values.iter())
        }
    }

    #[derive(Debug)]
    pub struct DataFrame {
        pub columns: Vec<Column> 
    }
}

use std::fs;
use std::collections::HashMap;
use crate::column::*;

fn extract_column(name: &str, input: &JSONInput, extractor: &ColumnValueExtractor) -> Column {
    let mut values: Vec<ColumnValue> = Vec::new();
    for input_element in input {
        if let Some(input_value) = input_element.get(name) {
            match extractor(input_value) {
                Ok(v) => values.push(v),
                Err(e) => panic!("failed to parse value={}: {}", input_value, e)
            }
        } else {
            values.push(ColumnValue::None)
        }
    }

    return Column{
        name: String::from(name),
        values: values,
    };
}

type JSONInput = Vec<HashMap<String, serde_json::Value>>;

fn main() {
    let filename = "assets/test.json"; // FIXME: get from args
    let mut spec: HashMap<&str, ColumnValueExtractor> = HashMap::new(); // FIXME: get from config
    spec.insert("name", extract_string_from_json);
    spec.insert("value", extract_integer_from_json);

    let f = fs::File::open(filename).expect("failed to open input file");
    let input: JSONInput = serde_json::from_reader(&f).expect("failed to parse JSON file");
    
    let data = DataFrame {
        columns: spec.iter().map(|(column_name, extractor)| -> Column {
            extract_column(column_name, &input, extractor)
        }).collect()
    };

    println!("Read file: {:?}", data);
    println!("Unique names: {:?}", data.columns[0].unique())
}
