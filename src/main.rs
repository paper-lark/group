#![warn(clippy::all, clippy::pedantic)]

mod dataframe;
mod input;
mod ui;

use std::collections::HashMap;
use std::fs;
use std::io;

use crate::dataframe::{extract_integer_from_json, extract_string_from_json, ColumnValueExtractor};
use crate::input::read;
use crate::ui::show_dataframe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filename = "assets/test.json"; // FIXME: get from args
    let mut spec: HashMap<&str, ColumnValueExtractor> = HashMap::new(); // FIXME: get from config
    spec.insert("name", extract_string_from_json);
    spec.insert("value", extract_integer_from_json);

    let f = fs::File::open(filename)?;
    let reader = io::BufReader::new(&f);
    let data = read(reader, spec)?;

    println!("Read file: {:?}", data);
    println!("Unique names: {:?}", data.columns[0].unique());

    show_dataframe(&data)?;

    Ok(())
}
