pub mod column {
    use std::vec::Vec;

    pub trait ColumnValue : PartialOrd + std::fmt::Display + std::fmt::Debug {
        fn from_value(value: &serde_json::Value) -> Self;
    }

    impl ColumnValue for String {
        fn from_value(value: &serde_json::Value) -> String {
            if let serde_json::Value::String(s) = value {
                s.clone()
            } else {
                panic!("{} is not a string", value);
            }
        }
    }

    impl ColumnValue for u64 {
        fn from_value(value: &serde_json::Value) -> u64 {
            if let serde_json::Value::Number(n) = value {
                n.as_u64().expect("not a u64")
            } else {
                panic!("{} is not a u64", value);
            }
        }
    }

    #[derive(Debug)]
    pub struct Column<T: ColumnValue> {
        pub name: String,
        pub values: Vec<T>
    }

    pub trait AbstractColumn : std::fmt::Debug {}

    impl<T: ColumnValue> AbstractColumn for Column<T> {}

    #[derive(Debug)]
    pub struct DataFrame {
        pub columns: Vec<Box<dyn AbstractColumn>> 
    }
}

use std::fs;
use crate::column::{Column,ColumnValue,DataFrame};

fn extract_column<T: ColumnValue>(name: &str, input: &serde_json::Value) -> Column<T> {
   if let serde_json::Value::Array(input_values) = input {
        let mut values: Vec<T> = Vec::new();
        for input_element in input_values {
            if let serde_json::Value::Object(input_obj) = input_element {
                let input_value = input_obj.get(name).expect("input element missing key");    
                if let serde_json::Value::String(_) = input_value {
                    let t: Column<String> = extract_column(name, input);
                    println!("> {:?}", t);
                }

                values.push(T::from_value(input_value));
            } else {
                panic!("expected input element to be object but got: {}", input_element);
            }
        }

        return Column{
            name: String::from(name),
            values: values,
        };
    } else {
        panic!("expected input to be array");
    }
}

fn main() {
    let filename = "assets/test.json"; // FIXME: get from args
    let 
    let f = fs::File::open(filename).expect("failed to open input file");
    let input: serde_json::Value = serde_json::from_reader(&f).expect("failed to parse JSON file");
    

    let name_col: Column<String> = extract_column("name", &input);
    let value_col: Column<u64> = extract_column("value", &input);
    let data = DataFrame {
        columns: vec!(
            Box::new(name_col),
            Box::new(value_col)
        )
    };
    println!("Read file: {:?}", data);
}
