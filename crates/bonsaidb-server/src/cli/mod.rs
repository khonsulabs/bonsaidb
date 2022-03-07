/// Command-line interface for managing the root certificate.
pub mod certificate;
/// Command-line interface for hosting a server.
pub mod serve;

use bonsaidb_local::cli::StorageCommand;
use clap::Parser;

use crate::{Backend, CustomServer, Error, NoBackend, ServerConfiguration};

/// Available commands for `bonsaidb server`.
#[derive(Parser, Debug)]
pub enum Command<B: Backend = NoBackend> {
    /// Manage the server's root certificate.
    #[clap(subcommand)]
    Certificate(certificate::Command),

    /// Execute the server.
    Serve(serve::Serve<B>),

    /// Manage the server's storage.
    #[clap(flatten)]
    Storage(StorageCommand),
}

impl<B: Backend> Command<B> {
    /// Executes the command.
    pub async fn execute(&self, configuration: ServerConfiguration) -> Result<(), Error> {
        let server = CustomServer::<B>::open(configuration).await?;
        match self {
            Self::Certificate(command) => command.execute(&server).await,
            Self::Serve(command) => command.execute(&server).await,
            Self::Storage(command) => command.execute_on_async(&server).await.map_err(Error::from),
        }
    }
}
