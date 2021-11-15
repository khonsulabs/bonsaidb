//! Shows how to use the axum web framework with BonsaiDb. Any hyper-compatible
//! framework should be usable.

use std::{path::Path, time::Duration};

use async_trait::async_trait;
use axum::{extract, routing::get, AddExtensionLayer, Router};
use bonsaidb::{
    core::{connection::ServerConnection, kv::Kv},
    server::{Backend, Configuration, CustomServer, DefaultPermissions},
};
use bonsaidb_server::NoDispatcher;
use hyper::{server::conn::Http, Body, Request, Response};

use url::Url;

/// The `AxumBackend` implements `Backend` and overrides
/// `handle_http_connection` by serving the response using
/// [`axum`](https://github.com/tokio-rs/axum).
#[derive(Debug)]
pub struct AxumBackend;

#[async_trait]
impl Backend for AxumBackend {
    async fn handle_http_connection<
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    >(
        connection: S,
        peer_address: std::net::SocketAddr,
        server: &CustomServer<Self>,
    ) -> Result<(), S> {
        let server = server.clone();
        let app = Router::new()
            .route("/", get(uptime_handler))
            .route("/ws", get(upgrade_websocket))
            // Attach the server and the remote address as extractable data for the /ws route
            .layer(AddExtensionLayer::new(server))
            .layer(AddExtensionLayer::new(peer_address));

        if let Err(err) = Http::new()
            .serve_connection(connection, app)
            .with_upgrades()
            .await
        {
            eprintln!("[http] error serving {}: {:?}", peer_address, err);
        }

        Ok(())
    }

    type CustomApi = ();
    type CustomApiDispatcher = NoDispatcher<Self>;
    type ClientData = ();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = CustomServer::<AxumBackend>::open(
        Path::new("http-server-data.bonsaidb"),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            ..Default::default()
        },
    )
    .await?;
    server.register_schema::<()>().await?;
    server.create_database::<()>("storage", true).await?;

    #[cfg(all(feature = "client", feature = "websockets"))]
    {
        let client = bonsaidb::client::Client::<()>::new(Url::parse(
            "ws://localhost:8080/ws",
        )?)
        .await?;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                println!("attempting increment");
                let db = client.database::<()>("storage").await.unwrap();
                db.increment_key_by("uptime", 1_u64).await.unwrap();
            }
        });
    }

    server.listen_for_http_on("localhost:8080").await?;

    Ok(())
}

async fn uptime_handler(
    server: extract::Extension<CustomServer<AxumBackend>>,
) -> String {
    let db = server.database::<()>("storage").await.unwrap();
    format!(
        "Current uptime: {} seconds",
        db.get_key("uptime")
            .into_u64()
            .await
            .unwrap()
            .unwrap_or_default()
    )
}

async fn upgrade_websocket(
    server: extract::Extension<CustomServer<AxumBackend>>,
    peer_address: extract::Extension<std::net::SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    server.upgrade_websocket(*peer_address, req).await
}
