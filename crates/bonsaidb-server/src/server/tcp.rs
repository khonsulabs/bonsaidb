use std::sync::Arc;

use async_trait::async_trait;
use rustls::server::ResolvesServerCert;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};

use crate::{Backend, CustomServer, Error};

impl<B: Backend> CustomServer<B> {
    /// Listens for HTTP traffic on `port`. This port will also receive
    /// `WebSocket` connections if feature `websockets` is enabled.
    pub async fn listen_for_tcp_on<S: TcpService, T: tokio::net::ToSocketAddrs + Send + Sync>(
        &self,
        addr: T,
        service: S,
    ) -> Result<(), Error> {
        let listener = TcpListener::bind(&addr).await?;
        let (shutdown_sender, shutdown_receiver) = flume::bounded(1);
        {
            let mut shutdown = self.data.tcp_shutdown.write().await;
            *shutdown = Some(shutdown_sender);
        }

        loop {
            tokio::select! {
                _ = shutdown_receiver.recv_async() => {
                    break;
                }
                incoming = listener.accept() => {
                    if incoming.is_err() {
                        continue;
                    }
                    let (connection, remote_addr) = incoming.unwrap();

                    let peer = Peer {
                        address: remote_addr,
                        protocol: <S::ApplicationProtocols as Default>::default(),
                        secure: false,
                    };

                    let task_self = self.clone();
                    let task_service = service.clone();
                    tokio::spawn(async move {
                        if let Err(err) = task_self.handle_tcp_connection(connection, peer, &task_service).await {
                            log::error!("[server] closing connection {}: {:?}", remote_addr, err);
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Listens for HTTPS traffic on `port`. This port will also receive
    /// `WebSocket` connections if feature `websockets` is enabled. If feature
    /// `acme` is enabled, this connection will automatically manage the
    /// server's private key and certificate, which is also used for the
    /// QUIC-based protocol.
    #[cfg_attr(not(feature = "websockets"), allow(unused_variables))]
    #[cfg_attr(not(feature = "acme"), allow(unused_mut))]
    pub async fn listen_for_secure_tcp_on<
        S: TcpService,
        T: tokio::net::ToSocketAddrs + Send + Sync,
    >(
        &self,
        addr: T,
        service: S,
    ) -> Result<(), Error> {
        // We may not have a certificate yet, so we ignore any errors.
        drop(self.refresh_certified_key().await);

        #[cfg(feature = "acme")]
        {
            let task_self = self.clone();
            tokio::task::spawn(async move {
                if let Err(err) = task_self.update_acme_certificates().await {
                    log::error!("[server] acme task error: {0}", err);
                }
            });
        }

        let mut config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(self.clone()));
        config.alpn_protocols = <S::ApplicationProtocols as ApplicationProtocols>::all()
            .iter()
            .map(|proto| proto.to_vec())
            .collect();

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(&addr).await?;
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let acceptor = acceptor.clone();

            let task_self = self.clone();
            let task_service = service.clone();
            tokio::task::spawn(async move {
                let stream = match acceptor.accept(stream).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        log::error!("[server] error during tls handshake: {:?}", err);
                        return;
                    }
                };

                let protocol = stream
                    .get_ref()
                    .1
                    .alpn_protocol()
                    .map(|protocol| {
                        <S::ApplicationProtocols as ApplicationProtocols>::find(protocol)
                    })
                    .unwrap_or_default();
                let peer = Peer {
                    address: peer_addr,
                    secure: true,
                    protocol,
                };
                if let Err(err) = task_self
                    .handle_tcp_connection(stream, peer, &task_service)
                    .await
                {
                    log::error!("[server] error for client {}: {:?}", peer_addr, err);
                }
            });
        }
    }

    #[cfg_attr(not(feature = "websockets"), allow(unused_variables))]
    async fn handle_tcp_connection<
        S: TcpService,
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        connection: C,
        peer: Peer<S::ApplicationProtocols>,
        service: &S,
    ) -> Result<(), Error> {
        if let Err(connection) = service.handle_connection(connection, &peer).await {
            #[cfg(feature = "websockets")]
            if peer.protocol.fallback_to_websockets() {
                if let Err(err) = self
                    .handle_raw_websocket_connection(connection, peer.address)
                    .await
                {
                    log::error!(
                        "[server] error on websocket for {}: {:?}",
                        peer.address,
                        err
                    );
                }
            }
        }

        Ok(())
    }
}

impl<B: Backend> ResolvesServerCert for CustomServer<B> {
    #[cfg_attr(not(feature = "acme"), allow(unused_variables))]
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        #[cfg(feature = "acme")]
        if client_hello
            .alpn()
            .map(|mut iter| iter.any(|n| n == async_acme::acme::ACME_TLS_ALPN_NAME))
            .unwrap_or_default()
        {
            let server_name = client_hello.server_name()?.to_owned();
            let keys = self.data.alpn_keys.lock().unwrap();
            if let Some(key) = keys.get(AsRef::<str>::as_ref(&server_name)) {
                return Some(key.clone());
            }

            return None;
        }

        let cached_key = self.data.primary_tls_key.lock();
        if let Some(key) = cached_key.as_ref() {
            Some(key.clone())
        } else {
            log::error!("[server] inbound tls connection with no certificate installed");
            None
        }
    }
}

/// A service that can handle incoming TCP connections.
#[async_trait]
pub trait TcpService: Clone + Send + Sync + 'static {
    /// The application layer protocols that this service supports.
    type ApplicationProtocols: ApplicationProtocols;

    /// Handle an incoming `connection` for `peer`. Return `Err(connection)` to
    /// have BonsaiDb handle the connection internally.
    async fn handle_connection<
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        connection: S,
        peer: &Peer<Self::ApplicationProtocols>,
    ) -> Result<(), S>;
}

#[async_trait]
impl TcpService for () {
    type ApplicationProtocols = StandardTcpProtocols;

    async fn handle_connection<
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        connection: S,
        _peer: &Peer<Self::ApplicationProtocols>,
    ) -> Result<(), S> {
        Err(connection)
    }
}

/// A collection of supported protocols for a network service.
pub trait ApplicationProtocols: Clone + Default + std::fmt::Debug + Send + Sync {
    /// Returns all supported protocols.
    fn all() -> &'static [&'static [u8]];
    /// Finds the matching protocol.
    fn find(protocol: &[u8]) -> Self;

    /// Return true if this connection should be allowed to switch to websockets
    /// if [`TcpService::handle_connection`] returns an error.
    #[cfg(feature = "websockets")]
    fn fallback_to_websockets(&self) -> bool;
}

/// A connected network peer.
#[derive(Debug, Clone)]
pub struct Peer<P: ApplicationProtocols> {
    /// The remote address of the peer.
    pub address: std::net::SocketAddr,
    /// If true, the connection is secured with TLS.
    pub secure: bool,
    /// The application protocol to use for this connection.
    pub protocol: P,
}

/// TCP [`ApplicationProtocols`] that `BonsaiDb` has some knowledge of.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum StandardTcpProtocols {
    Http1,
    #[cfg(feature = "acme")]
    Acme,
    Other,
}

impl Default for StandardTcpProtocols {
    fn default() -> Self {
        Self::Http1
    }
}

impl ApplicationProtocols for StandardTcpProtocols {
    #[cfg(feature = "acme")]
    fn all() -> &'static [&'static [u8]] {
        &[b"http/1.1", async_acme::acme::ACME_TLS_ALPN_NAME]
    }

    #[cfg(not(feature = "acme"))]
    fn all() -> &'static [&'static [u8]] {
        &[b"http/1.1"]
    }

    fn find(protocol: &[u8]) -> Self {
        if protocol == b"http/1.1" {
            return Self::Http1;
        }

        #[cfg(feature = "acme")]
        if protocol == async_acme::acme::ACME_TLS_ALPN_NAME {
            return Self::Acme;
        }

        Self::Other
    }

    #[cfg(feature = "websockets")]
    fn fallback_to_websockets(&self) -> bool {
        matches!(self, StandardTcpProtocols::Http1)
    }
}
