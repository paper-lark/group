use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "group", about = "Utility for grouping JSON input objects.")]
pub struct GroupOpts {
    /// Parse input as JSON object (default: parses input as stream of JSON oobjects)
    #[structopt(long)]
    pub json: bool,

    /// Input file
    #[structopt(parse(from_os_str))]
    pub input: std::path::PathBuf,
}
