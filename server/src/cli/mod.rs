/// Command-line interface for managing the root certificate.
pub mod certificate;
/// Command-line interface for hosting a server.
pub mod serve;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use pliantdb_core::schema::{self, Schematic};
use structopt::StructOpt;

use crate::Server;

/// Command-line interface for `pliantdb server`.
#[derive(StructOpt, Debug)]
pub struct Cli {
    /// The path to the directory where the server should store its data.
    pub server_data_directory: PathBuf,

    /// The command to execute.
    #[structopt(subcommand)]
    pub subcommand: Command,
}

/// Available commands for `pliantdb server`.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Manage the server's root certificate.
    Certificate(certificate::Command),

    /// Execute the server.
    #[structopt(flatten)]
    Serve(serve::Serve),
}

impl Command {
    /// Executes the command.
    pub async fn execute(
        &self,
        database_path: &Path,
        native_schemas: HashMap<schema::Id, Schematic>,
    ) -> anyhow::Result<()> {
        let server = Server::open(database_path, native_schemas).await?;
        match self {
            Self::Certificate(command) => command.execute(server).await,
            Self::Serve(command) => command.execute(server).await,
        }
    }
}
