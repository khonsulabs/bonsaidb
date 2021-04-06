//! `PliantDB` command line tools.
//!
//! Available commands:
//!
//! - [`local-backup`](pliantdb_local::backup)
use pliantdb_server::Server;
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
    pub async fn execute<F: Fn(&Server) + Send>(self, schema_registrar: F) -> anyhow::Result<()> {
        match self {
            Self::LocalBackup(backup) => backup.subcommand.execute(backup.database_path).await,
            Self::Server(server) => {
                server
                    .subcommand
                    .execute(&server.server_data_directory, schema_registrar)
                    .await
            }
        }
    }
}
