//! `BonsaiDb` command line tools.
//!
//! Available commands:
//!
use bonsaidb_server::Server;
use structopt::StructOpt;

/// The command line interface for `bonsaidb`.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Execute a `BonsaiDb` server command.
    Server(bonsaidb_server::cli::Cli),
}

impl Command {
    /// Executes the command.
    pub async fn execute<F: Fn(&Server) + Send>(self, schema_registrar: F) -> anyhow::Result<()> {
        match self {
            Self::Server(server) => {
                server
                    .subcommand
                    .execute(&server.server_data_directory, schema_registrar)
                    .await
            }
        }
    }
}
