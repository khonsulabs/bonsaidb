//! Shows basic usage of a server.

use std::{path::Path, time::Duration};

use pliantdb::{
    client::{url::Url, Client},
    core::{
        connection::{Connection, ServerConnection},
        permissions::Permissions,
        schema::Collection,
        Error,
    },
    server::{Configuration, Server},
};
use rand::{thread_rng, Rng};

mod support;
use support::schema::{Shape, ShapesByNumberOfSides};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ANCHOR: setup
    let server = Server::open(
        Path::new("server-data.pliantdb"),
        Configuration {
            default_permissions: Permissions::allow_all(),
            ..Default::default()
        },
    )
    .await?;
    if server.certificate().await.is_err() {
        server
            .install_self_signed_certificate("example-server", true)
            .await?;
    }
    let certificate = server.certificate().await?;
    server.register_schema::<Shape>().await?;
    match server.create_database::<Shape>("my-database").await {
        Ok(()) => {}
        Err(Error::DatabaseNameAlreadyTaken(_)) => {}
        Err(err) => panic!(
            "Unexpected error from server during create_database: {:?}",
            err
        ),
    }
    // ANCHOR_END: setup

    // If websockets are enabled, we'll also listen for websocket traffic. The
    // QUIC-based connection should be overall better to use than WebSockets,
    // but it's much easier to route WebSocket traffic across the internet.
    #[cfg(feature = "websockets")]
    {
        let server = server.clone();
        tokio::spawn(async move {
            server.listen_for_websockets_on("localhost:8080").await
        });
    }

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // To allow this example to run both websockets and QUIC, we're going to gather the clients
    // into a collection and use join_all to wait until they finish.
    let mut tasks = Vec::new();
    #[cfg(feature = "websockets")]
    {
        // To connect over websockets, use the websocket scheme.
        tasks.push(do_some_database_work(
            Client::<()>::new(Url::parse("ws://localhost:8080")?)
                .await?
                .database::<Shape>("my-database")
                .await?,
            "websockets",
        ));
    }

    // To connect over QUIC, use the pliantdb scheme.
    tasks.push(do_some_database_work(
        Client::<()>::new_with_certificate(
            Url::parse("pliantdb://localhost")?,
            Some(certificate),
        )
        .await?
        .database::<Shape>("my-database")
        .await?,
        "pliantdb",
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
