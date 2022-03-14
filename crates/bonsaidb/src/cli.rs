//! BonsaiDb command line tools.

use std::{ffi::OsString, fmt::Debug, path::PathBuf};

use bonsaidb_client::{fabruic::Certificate, Client};
use bonsaidb_core::async_trait::async_trait;
use bonsaidb_server::{Backend, CustomServer, NoBackend, ServerConfiguration};
use clap::{Parser, Subcommand};
use url::Url;

use crate::AnyServerConnection;

mod admin;

/// All available command line commands.
#[derive(Subcommand, Debug)]
pub enum Command<Cli: CommandLine> {
    /// Executes an admin command.
    #[clap(subcommand)]
    Admin(admin::Command),
    /// Execute a BonsaiDb server command.
    #[clap(subcommand)]
    Server(bonsaidb_server::cli::Command<Cli::Backend>),
    /// An external command.
    #[clap(flatten)]
    External(Cli::Subcommand),
}

impl<Cli> Command<Cli>
where
    Cli: CommandLine,
{
    /// Executes the command.
    // TODO add client builder insetad of server_url
    pub async fn execute(
        self,
        server_url: Option<Url>,
        pinned_certificate: Option<Certificate>,
        mut cli: Cli,
    ) -> anyhow::Result<()> {
        match self {
            Command::Server(server) => {
                if server_url.is_some() {
                    anyhow::bail!("server url provided for local-only command.")
                }

                server.execute(cli.configuration().await?).await?;
            }
            other => {
                let connection = if let Some(server_url) = server_url {
                    // TODO how does custom API handling work here?
                    let mut client = Client::build(server_url);

                    if let Some(certificate) = pinned_certificate {
                        client = client.with_certificate(certificate);
                    }

                    AnyServerConnection::Networked(client.finish().await?)
                } else {
                    AnyServerConnection::Local(cli.open_server().await?)
                };
                match other {
                    Command::Admin(admin) => admin.execute(connection).await?,
                    Command::External(external) => {
                        cli.execute(external, connection).await?;
                    }
                    Command::Server(_) => unreachable!(),
                }
            }
        }
        Ok(())
    }
}

/// The command line interface for `bonsaidb`.
#[derive(Parser, Debug)]
pub struct Args<Cli: CommandLine> {
    /// A url to a remote server.
    #[clap(long)]
    pub url: Option<Url>,
    /// A pinned certificate to use when connecting to `url`.
    #[clap(short = 'c', long)]
    pub pinned_certificate: Option<PathBuf>,
    /// The command to execute on the connection specified.
    #[clap(subcommand)]
    pub command: Command<Cli>,
}

impl<Cli: CommandLine> Args<Cli> {
    /// Executes the command.
    pub async fn execute(self, cli: Cli) -> anyhow::Result<()> {
        let pinned_certificate = if let Some(cert_path) = self.pinned_certificate {
            let bytes = tokio::fs::read(cert_path).await?;
            Some(Certificate::from_der(bytes)?)
        } else {
            None
        };
        self.command
            .execute(self.url, pinned_certificate, cli)
            .await
    }
}

/// A command line interface that can be executed with either a remote or local
/// connection to a server.
#[async_trait]
pub trait CommandLine: Sized + Send + Sync {
    /// The Backend for this command line.
    type Backend: Backend;
    /// The [`Subcommand`] which is embedded next to the built-in BonsaiDb
    /// commands.
    type Subcommand: Subcommand + Send + Sync + Debug;

    /// Runs the command-line interface using command-line arguments from the
    /// environment.
    async fn run(self) -> anyhow::Result<()> {
        Args::<Self>::parse().execute(self).await
    }

    /// Runs the command-line interface using the specified list of arguments.
    async fn run_from<I, T>(self, itr: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = T> + Send,
        T: Into<OsString> + Clone + Send,
    {
        Args::<Self>::parse_from(itr).execute(self).await
    }

    /// Returns a new server initialized based on the same configuration used
    /// for [`CommandLine`].
    async fn open_server(&mut self) -> anyhow::Result<CustomServer<Self::Backend>> {
        Ok(CustomServer::<Self::Backend>::open(self.configuration().await?).await?)
    }

    /// Returns the server configuration to use when initializing a local server.
    async fn configuration(&mut self) -> anyhow::Result<ServerConfiguration<Self::Backend>>;

    /// Execute the command on `connection`.
    async fn execute(
        &mut self,
        command: Self::Subcommand,
        connection: AnyServerConnection<Self::Backend>,
    ) -> anyhow::Result<()>;
}

#[async_trait]
impl CommandLine for NoBackend {
    type Backend = Self;
    type Subcommand = Self;

    async fn configuration(&mut self) -> anyhow::Result<ServerConfiguration> {
        Ok(ServerConfiguration::default())
    }

    async fn execute(
        &mut self,
        _command: Self::Subcommand,
        _connection: AnyServerConnection<Self>,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}

/// Runs the command-line interface with only the built-in commands, using
/// `configuration` to launch a server if running a local command.
pub async fn run<B: Backend>(configuration: ServerConfiguration<B>) -> anyhow::Result<()> {
    Args::parse()
        .execute(NoCommandLine::<B> {
            configuration: Some(configuration),
        })
        .await
}

#[derive(Debug)]
struct NoCommandLine<B: Backend> {
    configuration: Option<ServerConfiguration<B>>,
}

#[async_trait]
impl<B: Backend> CommandLine for NoCommandLine<B> {
    type Backend = B;
    type Subcommand = NoBackend;

    async fn configuration(&mut self) -> anyhow::Result<ServerConfiguration<B>> {
        self.configuration
            .take()
            .ok_or_else(|| anyhow::anyhow!("configuration already consumed"))
    }

    async fn execute(
        &mut self,
        _command: Self::Subcommand,
        _connection: AnyServerConnection<B>,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}
