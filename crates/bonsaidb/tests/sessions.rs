//! Tests breaking a connection and detecting it on the Client.

use std::sync::Arc;
use std::time::Duration;

use bonsaidb::client::url::Url;
use bonsaidb::client::Client;
use bonsaidb::core::test_util::{Basic, TestDirectory};
use bonsaidb::local::config::Builder;
use bonsaidb::server::{DefaultPermissions, Server, ServerConfiguration};
use bonsaidb_core::connection::{AsyncStorageConnection, HasSession, SensitiveString};
use bonsaidb_core::schema::SerializedCollection;

#[tokio::test]
async fn sessions() -> anyhow::Result<()> {
    println!("here");
    let dir = TestDirectory::new("sessions.bonsaidb");
    let server = Server::open(
        ServerConfiguration::new(&dir)
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<Basic>()?,
    )
    .await?;
    println!("Installing cert");
    server.install_self_signed_certificate(false).await?;

    let user_id = server.create_user("ecton").await?;
    server
        .set_user_password(user_id, SensitiveString::from("hunter2"))
        .await?;

    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();

    let reboot = Arc::new(tokio::sync::Notify::new());
    let rebooted = Arc::new(tokio::sync::Notify::new());

    tokio::spawn({
        let reboot = reboot.clone();
        let rebooted = rebooted.clone();
        async move {
            // Start listening
            println!("Listening");
            tokio::spawn({
                let server = server.clone();
                async move {
                    server.listen_on(12346).await.unwrap();
                    println!("Stopped listening.");
                }
            });

            // Wait for the client to signal that we can disconnect.
            reboot.notified().await;
            println!("Server Shutting down.");
            // Completely shut the server down and restart it.
            server.shutdown(None).await.unwrap();
            drop(server);
            println!("Server shut down.");
            // Give time for the endpoint to completley close.
            // TODO this is stupid
            tokio::time::sleep(Duration::from_millis(500)).await;
            let server = Server::open(
                ServerConfiguration::new(&dir)
                    .default_permissions(DefaultPermissions::AllowAll)
                    .with_schema::<Basic>()
                    .unwrap(),
            )
            .await
            .unwrap();

            println!("Server listening again.");
            rebooted.notify_one();
            server.listen_on(12346).await.unwrap();
        }
    });

    let client = Client::build(Url::parse("bonsaidb://localhost:12346")?)
        .with_certificate(certificate.clone())
        .finish()?;

    println!("Authenticating.");
    let authenticated = client
        .authenticate_with_password("ecton", SensitiveString::from("hunter2"))
        .await?;
    println!("Creating db");
    let db = client.create_database::<Basic>("basic", true).await?;
    // Verify we have a session at this point.
    authenticated
        .session()
        .unwrap()
        .id
        .expect("session should be present");

    reboot.notify_one();
    rebooted.notified().await;
    // Give the listener a moment to become established.
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("Continuing client.");

    // Get a disconnection error
    assert!(Basic::get_async(&0, &db).await.is_err());
    println!("Reconnecting");
    // Reconnect
    assert!(Basic::get_async(&0, &db).await.unwrap().is_none());
    println!("Checking session");
    // Verify the client recognizes it was de-authenticated
    assert!(client.session().unwrap().id.is_none());

    println!("Done");
    Ok(())
}
