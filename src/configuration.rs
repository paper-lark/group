use crate::io::dataframe::InputAttributeType;
use serde::Deserialize;
use std::collections::HashSet;
use string_error::{into_err, new_err};
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "group", about = "Utility for grouping JSON input objects.")]
pub struct GroupOpts {
    /// Parse input as a single JSON object (default: parse a stream of JSON objects)
    #[structopt(short, long)]
    pub single: bool,

    /// Input file
    #[structopt(parse(from_os_str))]
    pub input: std::path::PathBuf,

    /// Input spec file (default: spec.yml)
    #[structopt(parse(from_os_str), default_value = "spec.yml")]
    pub spec: std::path::PathBuf,
}

#[derive(Deserialize)]
pub struct InputAttributeSpec {
    pub name: String,

    #[serde(rename = "type")]
    pub attr_type: InputAttributeType,
}

#[derive(Deserialize)]
pub struct InputSpec {
    pub attrs: Vec<InputAttributeSpec>,
    pub group_by: Vec<String>,
    pub show_in_grouped: Vec<String>,
}

impl InputSpec {
    pub fn read_from_file(file_name: std::path::PathBuf) -> Result<InputSpec, Box<dyn std::error::Error>> {
        let f = std::io::BufReader::new(std::fs::File::open(file_name)?);
        let spec: InputSpec = serde_yaml::from_reader(f)?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        let attr_names: HashSet<String> = self.attrs.iter().map(|a| a.name.clone()).collect();
        if attr_names.len() != self.attrs.len() {
            return Err(new_err("spec contains duplicates"));
        }

        for attr_name in &self.group_by {
            if !attr_names.contains(attr_name) {
                return Err(into_err(format!("missing grouping attribute {} in spec", attr_name)));
            }
        }
        for attr_name in &self.show_in_grouped {
            if !attr_names.contains(attr_name) {
                return Err(into_err(format!("missing attribute {} requested to show in grouped mode", attr_name)));
            }
        }
        Ok(())
    }
}
