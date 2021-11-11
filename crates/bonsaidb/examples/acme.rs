//! Shows basic usage of a server.

use std::{path::Path, time::Duration};

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        connection::{Connection, ServerConnection},
        schema::Collection,
    },
    server::{Configuration, DefaultPermissions, Server},
};
use bonsaidb_server::{AcmeConfiguration, LETS_ENCRYPT_STAGING_DIRECTORY};
use rand::{thread_rng, Rng};

mod support;
use support::schema::{Shape, ShapesByNumberOfSides};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ANCHOR: setup
    let server = Server::open(
        Path::new("acme-server-data.bonsaidb"),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            acme: AcmeConfiguration {
                primary_domain: String::from("dev.ncog.id"),
                contact_email: None,
                // directory: LETS_ENCRYPT_STAGING_DIRECTORY.to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?;

    server.register_schema::<Shape>().await?;
    server.create_database::<Shape>("my-database", true).await?;
    // ANCHOR_END: setup

    // If websockets are enabled, we'll also listen for websocket traffic. The
    // QUIC-based connection should be overall better to use than WebSockets,
    // but it's much easier to route WebSocket traffic across the internet.
    let task_server = server.clone();
    tokio::spawn(async move {
        task_server.listen_for_https_on("0.0.0.0:5001").await
    });

    // ugly, wait for the certificate
    while server.certificate_chain().await.is_err() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // To allow this example to run both websockets and QUIC, we're going to gather the clients
    // into a collection and use join_all to wait until they finish.
    let mut tasks = Vec::new();
    // To connect over websockets, use the websocket scheme.
    // tasks.push(do_some_database_work(
    //     Client::new(Url::parse("wss://dev.ncog.id")?)
    //         .await?
    //         .database::<Shape>("my-database")
    //         .await?,
    //     "websockets",
    // ));

    // To connect over QUIC, use the bonsaidb scheme.
    tasks.push(do_some_database_work(
        Client::<()>::build(Url::parse("bonsaidb://dev.ncog.id")?)
            .finish()
            .await?
            .database::<Shape>("my-database")
            .await?,
        "bonsaidb",
    ));

    // Wait for the clients to finish
    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}

async fn do_some_database_work<'a, C: Connection>(
    database: C,
    client_name: &str,
) -> anyhow::Result<()> {
    // Insert 50 random shapes
    for _ in 0u32..50 {
        let sides = {
            let mut rng = thread_rng();
            rng.gen_range(3..=10)
        };
        Shape::new(sides).insert_into(&database).await?;
    }

    println!("Client {} finished", client_name);

    // Print a summary of all the shapes
    for result in database
        .view::<ShapesByNumberOfSides>()
        .reduce_grouped()
        .await?
    {
        println!(
            "Number of entries with {:02} sides: {}",
            result.key, result.value
        );
    }

    Ok(())
}
