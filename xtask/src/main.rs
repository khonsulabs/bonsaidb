use khonsu_tools::{
    anyhow,
    code_coverage::{self, CodeCoverage},
};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Commands {
    GenerateCodeCoverageReport {
        #[structopt(long = "install-dependencies")]
        install_dependencies: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let command = Commands::from_args();
    match command {
        Commands::GenerateCodeCoverageReport {
            install_dependencies,
        } => CodeCoverage::<CoverageConfig>::execute(install_dependencies),
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
