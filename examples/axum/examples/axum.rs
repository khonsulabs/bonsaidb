//! Shows how to use the axum web framework with BonsaiDb. Any hyper-compatible
//! framework should be usable.

use async_trait::async_trait;
use axum::{extract, routing::get, AddExtensionLayer, Router};
use bonsaidb::{
    core::{connection::StorageConnection, keyvalue::KeyValue},
    local::config::Builder,
    server::{
        DefaultPermissions, HttpService, Peer, Server, ServerConfiguration, StandardTcpProtocols,
    },
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
        ServerConfiguration::new("http-server-data.bonsaidb")
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<()>()?,
    )
    .await?;
    server.create_database::<()>("storage", true).await?;

    #[cfg(feature = "client")]
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

#[tokio::test]
#[cfg_attr(not(feature = "client"), allow(unused_variables))]
async fn test() {
    use axum::body::HttpBody;

    std::thread::spawn(|| main().unwrap());

    let retrieve_uptime = || async {
        let client = hyper::Client::new();
        let mut response = match client.get("http://localhost:8080/".parse().unwrap()).await {
            Ok(response) => response,
            Err(err) if err.is_connect() => {
                return None;
            }
            Err(other) => unreachable!(other),
        };

        assert_eq!(response.status(), 200);

        let body = response
            .body_mut()
            .data()
            .await
            .expect("no response")
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Current uptime: "));
        Some(body)
    };

    let mut retries_left = 5;
    let original_uptime = loop {
        if let Some(uptime) = retrieve_uptime().await {
            break uptime;
        } else if retries_left > 0 {
            println!("Waiting for server to start");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            retries_left -= 1;
        } else {
            unreachable!("Unable to connect to axum server.")
        }
    };

    #[cfg(feature = "client")]
    {
        // If we have the client, we're expecting the uptime to increase every second
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let new_uptime = retrieve_uptime().await.unwrap();
        assert_ne!(original_uptime, new_uptime);
    }
}
