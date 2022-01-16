#![warn(clippy::all, clippy::pedantic)]

mod configuration;
mod io;
mod ui;
mod utils;

use std::fs;
use structopt::StructOpt;

use crate::io::input::read_dataframe;
use crate::ui::show_dataframe;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = configuration::GroupOpts::from_args();
    let spec = configuration::InputSpec::read_from_file(args.spec)?;

    let reader = std::io::BufReader::new(fs::File::open(args.input)?);
    let data = read_dataframe(reader, &spec.attrs, args.single)?;
    show_dataframe(&data, &spec.group_by, &spec.show_in_grouped, &spec.timeline_column)?;

    Ok(())
}
