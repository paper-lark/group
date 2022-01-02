use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "group", about = "Utility for grouping JSON input objects.")]
pub struct GroupOpts {
    /// Parse input as a single JSON object (default: parse a stream of JSON objects)
    #[structopt(short, long)]
    pub single: bool,

    /// Input file
    #[structopt(parse(from_os_str))]
    pub input: std::path::PathBuf,
}
