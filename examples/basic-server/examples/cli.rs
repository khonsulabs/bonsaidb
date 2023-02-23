//! Shows how to utilize the built-in CLI helpers.

use std::convert::Infallible;

use bonsaidb::cli::CommandLine;
use bonsaidb::core::actionable::async_trait;
use bonsaidb::core::connection::AsyncStorageConnection;
use bonsaidb::core::schema::{SerializedCollection, SerializedView};
use bonsaidb::local::config::Builder;
use bonsaidb::server::{
    Backend, BackendError, CustomServer, DefaultPermissions, ServerConfiguration,
};
use bonsaidb::AnyServerConnection;
use clap::Subcommand;

mod support;
use support::schema::{Shape, ShapesByNumberOfSides};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CliBackend.run().await
}

#[derive(Debug, Default)]
struct CliBackend;

#[derive(Debug, Subcommand)]
enum Cli {
    Add { sides: u32 },
    Count { sides: Option<u32> },
}

#[async_trait]
impl Backend for CliBackend {
    type ClientData = ();
    type Error = Infallible;

    async fn initialize(&self, server: &CustomServer<Self>) -> Result<(), BackendError> {
        server.create_database::<Shape>("shapes", true).await?;
        Ok(())
    }
}

#[async_trait]
impl CommandLine for CliBackend {
    type Backend = Self;
    type Subcommand = Cli;

    async fn configuration(&mut self) -> anyhow::Result<ServerConfiguration<CliBackend>> {
        Ok(ServerConfiguration::new("cli.bonsaidb")
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<Shape>()?)
    }

    async fn execute(
        &mut self,
        command: Self::Subcommand,
        connection: AnyServerConnection<Self>,
    ) -> anyhow::Result<()> {
        let database = connection.database::<Shape>("shapes").await?;
        match command {
            Cli::Add { sides } => {
                let new_shape = Shape::new(sides).push_into_async(&database).await?;
                println!(
                    "Shape #{} inserted with {} sides",
                    new_shape.header.id, sides
                );
            }
            Cli::Count { sides } => {
                if let Some(sides) = sides {
                    let count = ShapesByNumberOfSides::entries_async(&database)
                        .with_key(&sides)
                        .reduce()
                        .await?;
                    println!("Found {count} shapes with {sides} sides");
                } else {
                    let count = ShapesByNumberOfSides::entries_async(&database)
                        .reduce()
                        .await?;
                    println!("Found {count} shapes with any number of sides");
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test() -> anyhow::Result<()> {
    if std::path::Path::new("cli.bonsaidb").exists() {
        tokio::fs::remove_dir_all("cli.bonsaidb").await?;
    }
    if std::path::Path::new("cli-backup").exists() {
        tokio::fs::remove_dir_all("cli-backup").await?;
    }

    CliBackend
        .run_from(["executable", "server", "certificate", "install-self-signed"])
        .await?;

    // Execute a command locally (no server running).
    CliBackend.run_from(["executable", "add", "3"]).await?;

    // Spawn the server so that we can communicate with it over the network. We
    // need to be able to shut this server down, but since we're launching it
    // through a command-line interface, we have no way to call the shutdown()
    // function. Instead, we'll spawn a thread to run its own runtime, which
    // allows us to fully control the task cleanup as the runtime is cleaned up.
    // This ensures all file locks are dropped.
    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async {
            CliBackend
                .run_from(["executable", "server", "serve", "--listen-on", "[::1]:6004"])
                .await
                .unwrap();
        });
        rt.block_on(async {
            drop(shutdown_receiver.await);
        });
    });

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // Execute the same command as before, but this time use the network.
    CliBackend
        .run_from([
            "executable",
            "--url",
            "bonsaidb://localhost:6004",
            "--pinned-certificate",
            "cli.bonsaidb/pinned-certificate.der",
            "add",
            "3",
        ])
        .await
        .unwrap();

    CliBackend
        .run_from([
            "executable",
            "--url",
            "bonsaidb://localhost:6004",
            "--pinned-certificate",
            "cli.bonsaidb/pinned-certificate.der",
            "count",
        ])
        .await
        .unwrap();

    // Close the server
    shutdown_sender.send(()).unwrap();

    // Back up the database
    CliBackend
        .run_from(["executable", "server", "backup", "path", "cli-backup"])
        .await?;

    // Remove the database and restore from backup.
    tokio::fs::remove_dir_all("cli.bonsaidb").await?;

    // Restore the database
    CliBackend
        .run_from(["executable", "server", "restore", "path", "cli-backup"])
        .await?;

    // Re-open the database and verify the operations were made.
    {
        let server = CliBackend.open_server().await?;
        let database = server.database::<Shape>("shapes").await?;
        assert_eq!(
            ShapesByNumberOfSides::entries_async(&database)
                .reduce()
                .await?,
            2
        );
    }
    tokio::fs::remove_dir_all("cli-backup").await?;

    Ok(())
}
