/// Command-line interface for managing the root certificate.
pub mod certificate;
/// Command-line interface for hosting a server.
pub mod serve;

use structopt::StructOpt;

use crate::{Backend, CustomServer, Error, ServerConfiguration};

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
    pub async fn execute(&self, configuration: ServerConfiguration) -> Result<(), Error> {
        let server = CustomServer::<B>::open(configuration).await?;
        match self {
            Self::Certificate(command) => command.execute(server).await,
            Self::Serve(command) => command.execute(server).await,
        }
    }
}
