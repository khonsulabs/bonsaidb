/// Command-line interface for managing the root certificate.
pub mod certificate;
/// Command-line interface for hosting a server.
pub mod serve;

use std::path::Path;

use structopt::StructOpt;

use crate::{config::DefaultPermissions, Backend, Configuration, CustomServer, Error};

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
    pub async fn execute(&self, database_path: &Path) -> Result<(), Error> {
        let server = CustomServer::<B>::open(
            database_path,
            Configuration {
                default_permissions: DefaultPermissions::AllowAll,
                ..Configuration::default()
            },
        )
        .await?;
        match self {
            Self::Certificate(command) => command.execute(server).await,
            Self::Serve(command) => command.execute(server).await,
        }
    }
}
