//! Tests breaking a connection and detecting it on the Client.

use std::sync::Arc;
use std::time::Duration;

use bonsaidb::client::url::Url;
use bonsaidb::client::AsyncClient;
use bonsaidb::core::test_util::{Basic, TestDirectory};
use bonsaidb::local::config::Builder;
use bonsaidb::server::{DefaultPermissions, Server, ServerConfiguration};
use bonsaidb_core::connection::{AsyncStorageConnection, HasSession, SensitiveString};
use bonsaidb_core::schema::SerializedCollection;
use bonsaidb_server::BonsaiListenConfig;
use futures::Future;

#[tokio::test]
#[cfg(feature = "websockets")]
async fn websockets() -> anyhow::Result<()> {
    test_sessions(
        "sessions-ws.bonsaidb",
        "ws://localhost:12345",
        |server| async move {
            server
                .listen_for_websockets_on("0.0.0.0:12345", false)
                .await
                .unwrap();
        },
    )
    .await
}

#[tokio::test]
async fn quic() -> anyhow::Result<()> {
    test_sessions(
        "sessions-quic.bonsaidb",
        "bonsaidb://localhost:12346",
        |server| async move {
            server
                .listen_on(BonsaiListenConfig::from(12346).reuse_address(true))
                .await
                .unwrap();
        },
    )
    .await
}

async fn test_sessions<F, Fut>(dir_name: &str, connect_addr: &str, listen: F) -> anyhow::Result<()>
where
    F: Fn(Server) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    drop(env_logger::try_init());
    println!("here");
    let dir = TestDirectory::new(dir_name);
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

            tokio::spawn(listen(server.clone()));

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
            listen(server).await;
        }
    });

    let client = AsyncClient::build(Url::parse(connect_addr)?)
        .with_certificate(certificate.clone())
        .build()?;

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
