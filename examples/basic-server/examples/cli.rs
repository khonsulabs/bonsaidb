//! Shows how to utilize the built-in CLI helpers.

use bonsaidb::{
    cli::CommandLine,
    core::{
        actionable::async_trait,
        connection::{Connection, StorageConnection},
        schema::SerializedCollection,
    },
    local::config::Builder,
    server::{Backend, CustomServer, DefaultPermissions, NoDispatcher, ServerConfiguration},
    AnyServerConnection,
};
use clap::Subcommand;

mod support;
use support::schema::{Shape, ShapesByNumberOfSides};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    CliBackend.run().await
}

#[derive(Debug)]
struct CliBackend;

#[derive(Debug, Subcommand)]
enum Cli {
    Add { sides: u32 },
    Count { sides: Option<u32> },
}

#[async_trait]
impl Backend for CliBackend {
    type CustomApi = ();
    type ClientData = ();
    type CustomApiDispatcher = NoDispatcher<Self>;

    async fn initialize(server: &CustomServer<Self>) {
        server
            .create_database::<Shape>("shapes", true)
            .await
            .unwrap();
    }
}

#[async_trait]
impl CommandLine for CliBackend {
    type Backend = Self;
    type Subcommand = Cli;

    async fn configuration(&mut self) -> anyhow::Result<ServerConfiguration> {
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
                let new_shape = Shape::new(sides).push_into(&database).await?;
                println!("Shape #{} inserted with {} sides", new_shape.id, sides);
            }
            Cli::Count { sides } => {
                if let Some(sides) = sides {
                    let count = database
                        .view::<ShapesByNumberOfSides>()
                        .with_key(sides)
                        .reduce()
                        .await?;
                    println!("Found {} shapes with {} sides", count, sides);
                } else {
                    let count = database.view::<ShapesByNumberOfSides>().reduce().await?;
                    println!("Found {} shapes with any number of sides", count);
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

    // Spawn the server so that we can communicate with it over the network
    let server_task = tokio::task::spawn(async {
        CliBackend
            .run_from(["executable", "server", "serve", "--listen-on", "6004"])
            .await
            .unwrap();
    });

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
        .await?;

    CliBackend
        .run_from([
            "executable",
            "--url",
            "bonsaidb://localhost:6004",
            "--pinned-certificate",
            "cli.bonsaidb/pinned-certificate.der",
            "count",
        ])
        .await?;

    // Close the server
    server_task.abort();
    drop(server_task.await);

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
        assert_eq!(database.view::<ShapesByNumberOfSides>().reduce().await?, 2);
    }
    tokio::fs::remove_dir_all("cli-backup").await?;

    Ok(())
}
