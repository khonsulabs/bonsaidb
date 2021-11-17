use std::sync::Arc;

use rustls::server::ResolvesServerCert;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};

use crate::{Backend, CustomServer, Error};

impl<B: Backend> CustomServer<B> {
    /// Listens for HTTP traffic on `port`. This port will also receive
    /// `WebSocket` connections if feature `websockets` is enabled.
    #[cfg(feature = "http")]
    pub async fn listen_for_http_on<T: tokio::net::ToSocketAddrs + Send + Sync>(
        &self,
        addr: T,
    ) -> Result<(), Error> {
        let listener = TcpListener::bind(&addr).await?;
        let (shutdown_sender, shutdown_receiver) = flume::bounded(1);
        {
            let mut shutdown = self.data.http_shutdown.write().await;
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

                    let task_self = self.clone();
                    tokio::spawn(async move {
                        if let Err(err) = task_self.handle_http_connection(connection, remote_addr).await {
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
    pub async fn listen_for_https_on<T: tokio::net::ToSocketAddrs + Send + Sync>(
        &self,
        addr: T,
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
        #[cfg(feature = "acme")]
        {
            config.alpn_protocols = vec![
                async_acme::acme::ACME_TLS_ALPN_NAME.to_vec(),
                b"http/1.1".to_vec(),
            ];
        }

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(&addr).await?;
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let acceptor = acceptor.clone();

            let task_self = self.clone();
            tokio::task::spawn(async move {
                let stream = match acceptor.accept(stream).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        log::error!("[server] error during tls handshake: {:?}", err);
                        return;
                    }
                };

                #[cfg(feature = "http")]
                {
                    let protocol = stream.get_ref().1.alpn_protocol();
                    if protocol.is_none() || protocol.unwrap() == b"http/1.1" {
                        if let Err(err) = task_self.handle_http_connection(stream, peer_addr).await
                        {
                            log::error!("[server] error for client {}: {:?}", peer_addr, err);
                        }
                    }
                }
            });
        }
    }

    #[cfg_attr(not(feature = "websockets"), allow(unused_variables))]
    async fn handle_http_connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        &self,
        connection: S,
        peer_address: std::net::SocketAddr,
    ) -> Result<(), Error> {
        if let Err(connection) = B::handle_http_connection(connection, peer_address, self).await {
            #[cfg(feature = "websockets")]
            if let Err(err) = self
                .handle_raw_websocket_connection(connection, peer_address)
                .await
            {
                log::error!(
                    "[server] error on websocket for {}: {:?}",
                    peer_address,
                    err
                );
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
