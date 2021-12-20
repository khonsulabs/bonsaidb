//! Shows how to utilize the built-in CLI helpers.

use bonsaidb::{
    cli::{Args, CommandLine},
    core::{
        actionable::async_trait,
        connection::{Connection, StorageConnection},
        schema::Collection,
    },
    local::config::Builder,
    server::{Backend, CustomServer, DefaultPermissions, NoDispatcher, ServerConfiguration},
    AnyServerConnection,
};
use structopt::StructOpt;

mod support;
use support::schema::{Shape, ShapesByNumberOfSides};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let command = Args::<CliBackend>::from_args();
    command.execute().await?;
    Ok(())
}

#[derive(Debug, StructOpt)]
enum CliBackend {
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
    async fn configuration() -> anyhow::Result<ServerConfiguration> {
        Ok(ServerConfiguration::new("cli.bonsaidb")
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<Shape>()?)
    }

    async fn execute(self, connection: AnyServerConnection<Self>) -> anyhow::Result<()> {
        let database = connection.database::<Shape>("shapes").await?;
        match self {
            Self::Add { sides } => {
                let new_shape = Shape::new(sides).insert_into(&database).await?;
                println!("Shape #{} inserted with {} sides", new_shape.id, sides);
            }
            Self::Count { sides } => {
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

    Args::<CliBackend>::from_iter(["executable", "server", "certificate", "install-self-signed"])
        .execute()
        .await?;

    // Execute a command locally (no server running).
    Args::<CliBackend>::from_iter(["executable", "add", "3"])
        .execute()
        .await?;

    // Spawn the server so that we can communicate with it over the network
    let server_task = tokio::task::spawn(async {
        Args::<CliBackend>::from_iter(["executable", "server", "serve", "--listen-on", "6004"])
            .execute()
            .await
            .unwrap();
    });

    // Execute the same command as before, but this time use the network.
    Args::<CliBackend>::from_iter([
        "executable",
        "--url",
        "bonsaidb://localhost:6004",
        "--pinned-certificate",
        "cli.bonsaidb/pinned-certificate.der",
        "add",
        "3",
    ])
    .execute()
    .await?;

    Args::<CliBackend>::from_iter([
        "executable",
        "--url",
        "bonsaidb://localhost:6004",
        "--pinned-certificate",
        "cli.bonsaidb/pinned-certificate.der",
        "count",
    ])
    .execute()
    .await?;

    // Close the server
    server_task.abort();
    drop(server_task.await);

    // Re-open the database and verify the operations were made.
    let server = CliBackend::open_server().await?;
    let database = server.database::<Shape>("shapes").await?;
    assert_eq!(database.view::<ShapesByNumberOfSides>().reduce().await?, 2);

    Ok(())
}
