//! `BonsaiDb` command line tools.

use std::{fmt::Debug, path::PathBuf};

use bonsaidb_client::{fabruic::Certificate, Client};
use bonsaidb_core::async_trait::async_trait;
use bonsaidb_server::{Backend, CustomServer, NoBackend, ServerConfiguration};
use structopt::{StructOpt, StructOptInternal};
use url::Url;

use crate::AnyServerConnection;

mod admin;

/// All available command line commands.
#[derive(StructOpt, Debug)]
pub enum Command<Cli: CommandLine> {
    /// Executes an admin command.
    Admin(admin::Command),
    /// Execute a `BonsaiDb` server command.
    Server(bonsaidb_server::cli::Command<Cli>),
    /// An external command.
    #[structopt(flatten)]
    External(Cli),
}

impl<Cli: CommandLine> Command<Cli> {
    /// Executes the command.
    // TODO add client builder insetad of server_url
    pub async fn execute(
        self,
        server_url: Option<Url>,
        pinned_certificate: Option<Certificate>,
    ) -> anyhow::Result<()> {
        match self {
            Command::Server(server) => {
                if server_url.is_some() {
                    anyhow::bail!("server url provided for local-only command.")
                }

                server.execute(Cli::configuration().await?).await?;
            }
            other => {
                let connection = if let Some(server_url) = server_url {
                    let mut client =
                        Client::build(server_url).with_custom_api::<<Cli as Backend>::CustomApi>();

                    if let Some(certificate) = pinned_certificate {
                        client = client.with_certificate(certificate);
                    }

                    AnyServerConnection::Networked(client.finish().await?)
                } else {
                    AnyServerConnection::Local(Cli::open_server().await?)
                };
                match other {
                    Command::Admin(admin) => admin.execute(connection).await?,
                    Command::External(external) => {
                        external.execute(connection).await?;
                    }
                    Command::Server(_) => unreachable!(),
                }
            }
        }
        Ok(())
    }
}

/// The command line interface for `bonsaidb`.
#[derive(StructOpt, Debug)]
pub struct Args<Cli: CommandLine> {
    /// A url to a remote server.
    #[structopt(long)]
    pub url: Option<Url>,
    /// A pinned certificate to use when connecting to `url`.
    #[structopt(short("c"), long)]
    pub pinned_certificate: Option<PathBuf>,
    /// The command to execute on the connection specified.
    #[structopt(subcommand)]
    pub command: Command<Cli>,
}

impl<Cli: CommandLine> Args<Cli> {
    /// Executes the command.
    pub async fn execute(self) -> anyhow::Result<()> {
        let pinned_certificate = if let Some(cert_path) = self.pinned_certificate {
            let bytes = tokio::fs::read(cert_path).await?;
            Some(Certificate::from_der(bytes)?)
        } else {
            None
        };
        self.command.execute(self.url, pinned_certificate).await
    }
}

/// A command line interface that can be executed with either a remote or local
/// connection to a server.
#[async_trait]
pub trait CommandLine: StructOpt + StructOptInternal + Backend {
    /// Returns a new server initialized based on the same configuration used
    /// for [`CommandLine`].
    async fn open_server() -> anyhow::Result<CustomServer<Self>> {
        Ok(CustomServer::<Self>::open(Self::configuration().await?).await?)
    }

    /// Returns the server configuration to use when initializing a local server.
    async fn configuration() -> anyhow::Result<ServerConfiguration>;

    /// Execute the command on `connection`.
    async fn execute(self, connection: AnyServerConnection<Self>) -> anyhow::Result<()>;
}

#[async_trait]
impl CommandLine for NoBackend {
    async fn configuration() -> anyhow::Result<ServerConfiguration> {
        Ok(ServerConfiguration::default())
    }

    async fn execute(self, _connection: AnyServerConnection<Self>) -> anyhow::Result<()> {
        unreachable!()
    }
}
