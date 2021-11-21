//! Shows how to use the axum web framework with BonsaiDb. Any hyper-compatible
//! framework should be usable.

use std::path::Path;

use async_trait::async_trait;
use axum::{extract, routing::get, AddExtensionLayer, Router};
use bonsaidb::{
    core::{connection::StorageConnection, kv::Kv},
    server::{Configuration, DefaultPermissions, HttpService, Peer, Server, StandardTcpProtocols},
};
use hyper::{server::conn::Http, Body, Request, Response};
#[cfg(feature = "client")]
use ::{std::time::Duration, url::Url};

#[derive(Debug, Clone)]
pub struct AxumService {
    server: Server,
}

#[async_trait]
impl HttpService for AxumService {
    async fn handle_connection<
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        connection: S,
        peer: &Peer<StandardTcpProtocols>,
    ) -> Result<(), S> {
        let server = self.server.clone();
        let app = Router::new()
            .route("/", get(uptime_handler))
            .route("/ws", get(upgrade_websocket))
            // Attach the server and the remote address as extractable data for the /ws route
            .layer(AddExtensionLayer::new(server))
            .layer(AddExtensionLayer::new(peer.address));

        if let Err(err) = Http::new()
            .serve_connection(connection, app)
            .with_upgrades()
            .await
        {
            log::error!("[http] error serving {}: {:?}", peer.address, err);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let server = Server::open(
        Path::new("http-server-data.bonsaidb"),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            ..Default::default()
        },
    )
    .await?;
    server.register_schema::<()>().await?;
    server.create_database::<()>("storage", true).await?;

    #[cfg(all(feature = "client"))]
    {
        // This is silly to do over a websocket connection, because it can
        // easily be done by just using `server` instead. However, this is to
        // demonstrate that websocket connections work in this example.
        let client = bonsaidb::client::Client::build(Url::parse("ws://localhost:8080/ws")?)
            .finish()
            .await?;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let db = client.database::<()>("storage").await.unwrap();
                db.increment_key_by("uptime", 1_u64).await.unwrap();
            }
        });
    }

    server
        .listen_for_tcp_on(
            "localhost:8080",
            AxumService {
                server: server.clone(),
            },
        )
        .await?;

    Ok(())
}

async fn uptime_handler(server: extract::Extension<Server>) -> String {
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
    server: extract::Extension<Server>,
    peer_address: extract::Extension<std::net::SocketAddr>,
    req: Request<Body>,
) -> Response<Body> {
    server.upgrade_websocket(*peer_address, req).await
}
