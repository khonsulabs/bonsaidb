//! `BonsaiDb` command line tools.
//!
//! Available commands:
//!
//! - [`local-backup`](bonsaidb_local::backup)
use bonsaidb_server::Server;
use structopt::StructOpt;

/// The command line interface for `bonsaidb`.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Back up or restore a local database
    LocalBackup(bonsaidb_local::backup::Cli),

    /// Execute a `BonsaiDb` server command.
    Server(bonsaidb_server::cli::Cli),
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
