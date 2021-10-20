//! `BonsaiDb` command line tools.
//!
//! Available commands:
//!
use std::{path::PathBuf, str::FromStr};

use bonsaidb_client::Client;
use bonsaidb_core::admin::Admin;
use bonsaidb_local::Storage;
use bonsaidb_server::Backend;
use structopt::StructOpt;
use url::Url;

mod admin;

/// All available command line commands.
#[derive(StructOpt, Debug)]
pub enum Command<B: Backend = ()> {
    /// Executes an admin command.
    Admin(admin::Command),
    /// Execute a `BonsaiDb` server command.
    Server(bonsaidb_server::cli::Command<B>),
}

/// The command line interface for `bonsaidb`.
#[derive(StructOpt, Debug)]
pub struct Args<B: Backend = ()> {
    /// A path to a local database/server or a URL to a remote server.
    pub connection: ConnectionTarget,
    /// The command to execute on the connection specified.
    #[structopt(subcommand)]
    pub command: Command<B>,
}

/// A target to connect to.
#[derive(Debug)]
pub enum ConnectionTarget {
    /// A locally stored database/server.
    Path(PathBuf),
    /// An Url to a server.
    Url(Url),
}

impl FromStr for ConnectionTarget {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(url) = Url::parse(s) {
            Ok(Self::Url(url))
        } else {
            Ok(Self::Path(PathBuf::from_str(s)?))
        }
    }
}

impl ConnectionTarget {
    /// Returns the path of this connection, if it's a path.
    pub fn path(self) -> anyhow::Result<PathBuf> {
        match self {
            ConnectionTarget::Path(path) => Ok(path),
            ConnectionTarget::Url(_) => anyhow::bail!("url provided when path is required"),
        }
    }
}

impl<B: Backend> Args<B> {
    /// Executes the command.
    pub async fn execute(self) -> anyhow::Result<()> {
        match self.command {
            Command::Server(server) => server.execute(&self.connection.path()?).await,
            Command::Admin(command) => match self.connection {
                ConnectionTarget::Path(path) => {
                    let storage = Storage::open_local(
                        &path,
                        bonsaidb_local::config::Configuration::default(),
                    )
                    .await?;
                    let admin = storage.admin().await;

                    command.execute(admin, storage).await
                }
                ConnectionTarget::Url(server_url) => {
                    let client = Client::new(server_url).await?;
                    let admin = client.database::<Admin>("admin").await?;

                    command.execute(admin, client).await
                }
            },
        }
    }
}
