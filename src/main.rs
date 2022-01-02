#![warn(clippy::all, clippy::pedantic)]

mod args;
mod colorizer;
mod dataframe;
mod input;
mod ui;
mod utils;

use std::fs;
use std::io;
use structopt::StructOpt;

use crate::dataframe::{extract_integer_from_json, extract_string_from_json};
use crate::input::read;
use crate::input::JSONColumnSpec;
use crate::ui::show_dataframe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = args::GroupOpts::from_args();
    println!("{:?}", args);

    let spec: [JSONColumnSpec; 3] = [
        (String::from("name"), extract_string_from_json),
        (String::from("value"), extract_integer_from_json),
        (String::from("static"), extract_string_from_json),
    ]; // FIXME: get from config

    let f = io::BufReader::new(fs::File::open(args.input)?);
    let reader = io::BufReader::new(f);
    let data = read(reader, &spec, args.json)?;
    show_dataframe(&data)?;

    Ok(())
}
