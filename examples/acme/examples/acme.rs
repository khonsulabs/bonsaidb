//! Shows how to use ACME to automatically acquire a TLS certificate for your `BonsaiDb` server.

use std::time::Duration;

use bonsaidb::{
    client::{url::Url, Client},
    core::connection::StorageConnection,
    local::config::Builder,
    server::{DefaultPermissions, Server, ServerConfiguration, LETS_ENCRYPT_STAGING_DIRECTORY},
};

const DOMAIN: &str = "example.com";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let server = Server::open(
        ServerConfiguration::new("acme-server-data.bonsaidb")
            .server_name(DOMAIN)
            .default_permissions(DefaultPermissions::AllowAll)
            .acme_contact_email("mailto:netops@example.com")
            .acme_directory(LETS_ENCRYPT_STAGING_DIRECTORY)
            .with_schema::<()>()?,
    )
    .await?;

    // The ACME registration is done via the TLS-ALPN-01 challenge, which occurs
    // on port 443 for LetsEncrypt. With the feature enabled, listening for
    // HTTPS traffic will automatically.
    let task_server = server.clone();
    tokio::spawn(async move {
        // This call is equivalent to listen_for_websockets_on("0.0.0.0:443",
        // true). This example, however, is meant to work with or without
        // websockets.
        task_server
            .listen_for_secure_tcp_on("0.0.0.0:443", ())
            .await
    });

    // Once the ACME process has succeded, the certificate_chain will be able to
    // be retrieved.
    while server.certificate_chain().await.is_err() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // Spawn our QUIC-based protocol listener. This will use the same
    // certificate as the HTTPS port.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the QUIC listener to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Test connecting using both clients.
    let client = Client::build(Url::parse(&format!("bonsaidb://{}", DOMAIN))?)
        .finish()
        .await?;
    client.create_database::<()>("test-database", true).await?;

    #[cfg(feature = "websockets")]
    {
        let websockets = Client::build(Url::parse(&format!("wss://{}", DOMAIN))?)
            .finish()
            .await?;
        websockets
            .create_database::<()>("test-database", true)
            .await?;
    }

    Ok(())
}
