use khonsu_tools::{
    anyhow,
    code_coverage::{self, CodeCoverage},
};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Commands {
    GenerateCodeCoverageReport,
}

fn main() -> anyhow::Result<()> {
    let command = Commands::from_args();
    match command {
        Commands::GenerateCodeCoverageReport => CodeCoverage::<CoverageConfig>::execute(),
    }
}

struct CoverageConfig;

impl code_coverage::Config for CoverageConfig {
    fn ignore_paths() -> Vec<String> {
        vec![
            String::from("circulate/examples/*"),
            String::from("pliantdb/examples/*"),
        ]
    }
}
