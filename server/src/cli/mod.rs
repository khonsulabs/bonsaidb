/// Command-line interface for managing the root certificate.
pub mod certificate;
/// Command-line interface for hosting a server.
pub mod serve;

use std::path::Path;

use bonsaidb_local::Storage;
use futures::Future;
use structopt::StructOpt;

use crate::{Backend, Configuration, CustomServer};

/// Available commands for `bonsaidb server`.
#[derive(StructOpt, Debug)]
pub enum Command<B: Backend = ()> {
    /// Manage the server's root certificate.
    Certificate(certificate::Command),

    /// Execute the server.
    Serve(serve::Serve<B>),
}

impl<B: Backend> Command<B> {
    /// Executes the command.
    pub async fn execute<
        F: FnOnce(Storage) -> Fut + Send,
        Fut: Future<Output = anyhow::Result<()>> + Send,
    >(
        &self,
        database_path: &Path,
        schema_registrar: F,
    ) -> anyhow::Result<()> {
        let server = CustomServer::<B>::open(database_path, Configuration::default()).await?;
        schema_registrar(server.storage().clone()).await?;
        match self {
            Self::Certificate(command) => command.execute(server).await,
            Self::Serve(command) => command.execute(server).await,
        }
    }
}
