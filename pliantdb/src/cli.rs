//! `PliantDB` command line tools.
//!
//! Available commands:
//!
//! - [`local-backup`](pliantdb_local::backup)
use std::collections::HashMap;

use pliantdb_core::schema::{self, Schematic};
use structopt::StructOpt;

/// The command line interface for `pliantdb`.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Back up or restore a local database
    LocalBackup(pliantdb_local::backup::Cli),

    /// Execute a `PliantDB` server command.
    Server(pliantdb_server::cli::Cli),
}

impl Command {
    /// Executes the command.
    pub async fn execute(
        self,
        native_schemas: HashMap<schema::Id, Schematic>,
    ) -> anyhow::Result<()> {
        match self {
            Self::LocalBackup(backup) => backup.subcommand.execute(backup.database_path).await,
            Self::Server(server) => {
                server
                    .subcommand
                    .execute(&server.server_data_directory, native_schemas)
                    .await
            }
        }
    }
}
