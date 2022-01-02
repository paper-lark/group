#![warn(clippy::all, clippy::pedantic)]

mod dataframe;
mod input;
mod ui;
mod utils;

use std::fs;
use std::io;

use crate::dataframe::{extract_integer_from_json, extract_string_from_json};
use crate::input::read;
use crate::input::JSONColumnSpec;
use crate::ui::show_dataframe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filename = "assets/test.json"; // FIXME: get from args
    let spec: [JSONColumnSpec; 2] = [
        (String::from("name"), extract_string_from_json),
        (String::from("value"), extract_integer_from_json),
    ]; // FIXME: get from config

    let f = fs::File::open(filename)?;
    let reader = io::BufReader::new(&f);
    let data = read(reader, &spec)?;

    println!("Read file: {:?}", data);
    println!("Unique names: {:?}", data.columns[0].unique());

    show_dataframe(&data)?;

    Ok(())
}
