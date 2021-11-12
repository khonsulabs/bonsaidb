use std::sync::Arc;

use rustls::server::ResolvesServerCert;
#[cfg(feature = "websockets")]
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

use super::{Backend, CustomServer};
use crate::Error;

impl<B: Backend> CustomServer<B> {
    /// Listens for HTTP traffic on `port`. This port will also receive
    /// `WebSocket` connections if feature `websockets` is enabled.
    #[cfg(feature = "websockets")]
    pub async fn listen_for_http_on<T: tokio::net::ToSocketAddrs + Send + Sync>(
        &self,
        addr: T,
    ) -> Result<(), Error> {
        let listener = TcpListener::bind(&addr).await?;
        let (shutdown_sender, shutdown_receiver) = flume::bounded(1);
        {
            let mut shutdown = self.data.websocket_shutdown.write().await;
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
                        if let Err(err) = task_self.handle_websocket_connection(connection, remote_addr).await {
                            eprintln!("[server] closing connection {}: {:?}", remote_addr, err);
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
        #[cfg(feature = "acme")]
        {
            let task_self = self.clone();
            tokio::task::spawn(async move {
                if let Err(err) = task_self.update_acme_certificates().await {
                    eprintln!("[server] acme task error: {0}", err);
                }
            });
        }

        // We may not have a certificate yet, so we ignore any errors.
        drop(self.refresh_certified_key().await);

        let mut config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(self.clone()));
        #[cfg(feature = "acme")]
        {
            config.alpn_protocols = vec![async_acme::acme::ACME_TLS_ALPN_NAME.to_vec()];
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
                        eprintln!("[server] error during tls handshake: {:?}", err);
                        return;
                    }
                };

                #[cfg(feature = "websockets")]
                if stream.get_ref().1.alpn_protocol().is_none() {
                    // Only pass non ALPN traffic on.
                    if let Err(err) = task_self
                        .handle_websocket_connection(stream, peer_addr)
                        .await
                    {
                        eprintln!("[server] error for client {}: {:?}", peer_addr, err);
                    }
                }
            });
        }
    }

    #[cfg(feature = "websockets")]
    async fn handle_websocket_connection<S: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        &self,
        connection: S,
        peer_address: std::net::SocketAddr,
    ) -> Result<(), Error> {
        use bonsaidb_core::{
            custom_api::CustomApi,
            networking::{Payload, Request, Response},
        };
        use futures::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        use crate::Transport;

        let stream = tokio_tungstenite::accept_async(connection).await?;
        let (mut sender, mut receiver) = stream.split();
        let (response_sender, response_receiver) = flume::unbounded();
        let (message_sender, message_receiver) = flume::unbounded();

        let (api_response_sender, api_response_receiver) = flume::unbounded();
        let client = if let Some(client) = self
            .initialize_client(Transport::WebSocket, peer_address, api_response_sender)
            .await
        {
            client
        } else {
            return Ok(());
        };
        let task_sender = response_sender.clone();
        tokio::spawn(async move {
            while let Ok(response) = api_response_receiver.recv_async().await {
                if task_sender
                    .send(Payload {
                        id: None,
                        wrapped: Response::Api(response),
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            while let Ok(response) = message_receiver.recv_async().await {
                sender.send(response).await?;
            }

            Result::<(), Error>::Ok(())
        });

        let task_sender = message_sender.clone();
        tokio::spawn(async move {
            while let Ok(response) = response_receiver.recv_async().await {
                if task_sender
                    .send(Message::Binary(bincode::serialize(&response)?))
                    .is_err()
                {
                    break;
                }
            }

            Result::<(), Error>::Ok(())
        });

        let (request_sender, request_receiver) =
            flume::bounded::<Payload<Request<<B::CustomApi as CustomApi>::Request>>>(
                self.data.client_simultaneous_request_limit,
            );
        let task_self = self.clone();
        tokio::spawn(async move {
            task_self
                .handle_client_requests(client.clone(), request_receiver, response_sender)
                .await;
        });

        while let Some(payload) = receiver.next().await {
            match payload? {
                Message::Binary(binary) => {
                    let payload = bincode::deserialize::<
                        Payload<Request<<B::CustomApi as CustomApi>::Request>>,
                    >(&binary)?;
                    drop(request_sender.send_async(payload).await);
                }
                Message::Close(_) => break,
                Message::Ping(payload) => {
                    drop(message_sender.send(Message::Pong(payload)));
                }
                other => {
                    eprintln!("[server] unexpected message: {:?}", other);
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
            eprintln!("[server] inbound tls connection with no certificate installed");
            None
        }
    }
}
