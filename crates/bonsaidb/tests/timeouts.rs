//! Tests request and connection timeouts

use std::net::UdpSocket;
use std::time::{Duration, Instant};

use bonsaidb::client::url::Url;
use bonsaidb::client::AsyncClient;
use bonsaidb_client::fabruic::Certificate;
use bonsaidb_client::{ApiError, BlockingClient};
use bonsaidb_core::api::Api;
use bonsaidb_core::async_trait::async_trait;
use bonsaidb_core::connection::{AsyncStorageConnection, StorageConnection};
use bonsaidb_core::networking;
use bonsaidb_core::test_util::{Basic, TestDirectory};
use bonsaidb_local::config::Builder;
use bonsaidb_server::api::{Handler, HandlerResult, HandlerSession};
use bonsaidb_server::{DefaultPermissions, Server, ServerConfiguration};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[tokio::test]
#[cfg(feature = "websockets")]
async fn ws_connect_timeout() -> anyhow::Result<()> {
    use std::net::TcpListener;

    let start = Instant::now();
    let tcp = TcpListener::bind("0.0.0.0:0")?;
    let port = tcp.local_addr()?.port();
    let client = AsyncClient::build(Url::parse(&format!("ws://127.0.0.1:{port}"))?)
        .with_connect_timeout(Duration::from_secs(1))
        .build()?;

    match tokio::time::timeout(Duration::from_secs(60), client.list_databases()).await {
        Ok(Err(bonsaidb_core::Error::Networking(networking::Error::ConnectTimeout))) => {
            assert!(start.elapsed() < Duration::from_secs(5));
            Ok(())
        }
        other => unreachable!("expected connect timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn quic_connect_timeout() -> anyhow::Result<()> {
    let start = Instant::now();
    let udp = UdpSocket::bind("0.0.0.0:0")?;
    let port = udp.local_addr()?.port();
    let client = AsyncClient::build(Url::parse(&format!("bonsaidb://127.0.0.1:{port}"))?)
        .with_connect_timeout(Duration::from_secs(1))
        .build()?;

    match tokio::time::timeout(Duration::from_secs(60), client.list_databases()).await {
        Ok(Err(bonsaidb_core::Error::Networking(networking::Error::ConnectTimeout))) => {
            assert!(start.elapsed() < Duration::from_secs(5));
            Ok(())
        }
        other => unreachable!("expected connect timeout, got {other:?}"),
    }
}

#[test]
#[cfg(feature = "websockets")]
fn blocking_ws_connect_timeout() -> anyhow::Result<()> {
    use std::net::TcpListener;

    let start = Instant::now();
    let tcp = TcpListener::bind("0.0.0.0:0")?;
    let port = tcp.local_addr()?.port();
    let client = BlockingClient::build(Url::parse(&format!("ws://127.0.0.1:{port}"))?)
        .with_connect_timeout(Duration::from_secs(1))
        .build()?;

    match client.list_databases() {
        Err(bonsaidb_core::Error::Networking(networking::Error::ConnectTimeout)) => {
            assert!(start.elapsed() < Duration::from_secs(5));
            Ok(())
        }
        other => unreachable!("expected connect timeout, got {other:?}"),
    }
}

#[test]
fn blocking_quic_connect_timeout() -> anyhow::Result<()> {
    let start = Instant::now();
    let udp = UdpSocket::bind("0.0.0.0:0")?;
    let port = udp.local_addr()?.port();
    let client = BlockingClient::build(Url::parse(&format!("bonsaidb://127.0.0.1:{port}"))?)
        .with_connect_timeout(Duration::from_secs(1))
        .build()?;

    match client.list_databases() {
        Err(bonsaidb_core::Error::Networking(networking::Error::ConnectTimeout)) => {
            assert!(start.elapsed() < Duration::from_secs(5));
            Ok(())
        }
        other => unreachable!("expected connect timeout, got {other:?}"),
    }
}

#[derive(Api, Debug, Serialize, Deserialize, Clone)]
#[api(name = "long-call")]
struct LongCall;

#[async_trait]
impl Handler<LongCall> for LongCall {
    async fn handle(_session: HandlerSession<'_>, _request: LongCall) -> HandlerResult<LongCall> {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(())
    }
}

fn shared_server() -> &'static Certificate {
    static SHARED_SERVER: Lazy<Certificate> = Lazy::new(|| {
        drop(env_logger::try_init());
        let dir = TestDirectory::new("timeouts.bonsaidb");

        let (server_sender, server_receiver) = tokio::sync::oneshot::channel();

        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let server = Server::open(
                    ServerConfiguration::new(&dir)
                        .default_permissions(DefaultPermissions::AllowAll)
                        .with_schema::<Basic>()
                        .unwrap()
                        .with_api::<LongCall, LongCall>()
                        .unwrap(),
                )
                .await
                .unwrap();
                server.install_self_signed_certificate(false).await.unwrap();
                server_sender
                    .send(
                        server
                            .certificate_chain()
                            .await
                            .unwrap()
                            .into_end_entity_certificate(),
                    )
                    .unwrap();

                #[cfg(feature = "websockets")]
                tokio::task::spawn({
                    let server = server.clone();
                    async move { server.listen_for_websockets_on("0.0.0.0:7023", false).await }
                });

                server.listen_on(7024).await
            })
        });

        server_receiver.blocking_recv().unwrap()
    });

    &SHARED_SERVER
}

#[tokio::test]
#[cfg(feature = "websockets")]
async fn ws_request_timeout() {
    shared_server();
    // Give the server a moment to actually start up.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();
    let client = AsyncClient::build(Url::parse("ws://127.0.0.1:7023").unwrap())
        .with_request_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    match client.send_api_request(&LongCall).await {
        Err(ApiError::Client(bonsaidb_client::Error::Core(bonsaidb_core::Error::Networking(
            networking::Error::RequestTimeout,
        )))) => {
            assert!(start.elapsed() < Duration::from_secs(5));
        }
        other => unreachable!("expected request timeout, got {other:?}"),
    }
}

#[test]
#[cfg(feature = "websockets")]
fn blocking_ws_request_timeout() {
    shared_server();
    // Give the server a moment to actually start up.
    std::thread::sleep(Duration::from_millis(100));

    let start = Instant::now();
    let client = BlockingClient::build(Url::parse("ws://127.0.0.1:7023").unwrap())
        .with_request_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    match client.send_api_request(&LongCall) {
        Err(ApiError::Client(bonsaidb_client::Error::Core(bonsaidb_core::Error::Networking(
            networking::Error::RequestTimeout,
        )))) => {
            assert!(start.elapsed() < Duration::from_secs(5));
        }
        other => unreachable!("expected request timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn quic_request_timeout() {
    let cert_chain = shared_server();
    // Give the server a moment to actually start up.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = Instant::now();
    let client = AsyncClient::build(Url::parse("bonsaidb://127.0.0.1:7024").unwrap())
        .with_request_timeout(Duration::from_secs(1))
        .with_certificate(cert_chain.clone())
        .build()
        .unwrap();
    match client.send_api_request(&LongCall).await {
        Err(ApiError::Client(bonsaidb_client::Error::Core(bonsaidb_core::Error::Networking(
            networking::Error::RequestTimeout,
        )))) => {
            assert!(start.elapsed() < Duration::from_secs(5));
        }
        other => unreachable!("expected request timeout, got {other:?}"),
    }
}

#[test]
fn blocking_quic_request_timeout() {
    let cert_chain = shared_server();
    // Give the server a moment to actually start up.
    std::thread::sleep(Duration::from_millis(100));

    let start = Instant::now();
    let client = BlockingClient::build(Url::parse("bonsaidb://127.0.0.1:7024").unwrap())
        .with_request_timeout(Duration::from_secs(1))
        .with_certificate(cert_chain.clone())
        .build()
        .unwrap();
    match client.send_api_request(&LongCall) {
        Err(ApiError::Client(bonsaidb_client::Error::Core(bonsaidb_core::Error::Networking(
            networking::Error::RequestTimeout,
        )))) => {
            assert!(start.elapsed() < Duration::from_secs(5));
        }
        other => unreachable!("expected request timeout, got {other:?}"),
    }
}
