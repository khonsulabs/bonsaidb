use super::{Backend, CustomServer};
use crate::Error;
use rcgen::{Certificate, CertificateParams, CustomExtension, PKCS_ECDSA_P256_SHA256};
use rustls::{
    server::ResolvesServerCert,
    sign::{any_ecdsa_type, CertifiedKey},
    PrivateKey,
};
use std::net::SocketAddr;

#[cfg(feature = "websockets")]
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "acme")]
use std::sync::Arc;

use tokio::net::TcpListener;

impl<B: Backend> CustomServer<B> {
    /// Listens for HTTP traffic on `port`. This port will also receive
    /// `WebSocket` connections if feature `websockets` is enabled.
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

    #[cfg(feature = "acme")]
    async fn update_acme_certificates(&self) -> Result<(), Error> {
        use std::time::Duration;

        use async_acme::{
            acme::{Account, Auth, Directory, Identifier, Order},
            rustls_helper::OrderError,
        };

        if self.certificate_chain_path().exists() {
            // TODO check cert expiration
            return Ok(());
        }

        let domains = vec![self.data.acme.primary_domain.clone()];
        let mut params = rcgen::CertificateParams::new(domains.clone());
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.alg = &rcgen::PKCS_ECDSA_P256_SHA256;
        let cert = rcgen::Certificate::from_params(params)?;
        let pk = cert.serialize_private_key_der();

        let account = Account::load_or_create(
            Directory::discover(&self.data.acme.directory).await?,
            Some(self.directory().join("acme")),
            self.data.acme.contact_email.as_ref(),
        )
        .await?;

        let mut order = account.new_order(domains).await?;

        loop {
            order = match dbg!(order) {
                Order::Pending {
                    authorizations,
                    finalize,
                } => {
                    'auth: for authorization in authorizations {
                        let (domain, challenge_url) =
                            match account.check_auth(&authorization).await? {
                                Auth::Pending {
                                    identifier,
                                    challenges,
                                } => {
                                    let Identifier::Dns(domain) = identifier;
                                    let (challenge, auth_key) = account.tls_alpn_01(&challenges)?;
                                    let auth_key =
                                        gen_acme_cert(vec![domain.clone()], auth_key.as_ref())?;

                                    let mut auth_keys = self.data.alpn_keys.lock().unwrap();
                                    println!("Inserting new auth key for {}", domain);
                                    auth_keys.insert(domain.clone(), Arc::new(auth_key));
                                    (domain, challenge.url.clone())
                                }
                                Auth::Valid => continue,
                                err => return Err(Error::AcmeOrder(OrderError::BadAuth(err))),
                            };
                        for attempt in 0_u32..5 {
                            tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                            match account.check_auth(&authorization).await? {
                                Auth::Pending { .. } => {
                                    eprintln!("authorization for {} still pending", &domain);
                                    account.trigger_challenge(&challenge_url).await?;
                                }
                                Auth::Valid => continue 'auth,
                                err => return Err(Error::AcmeOrder(OrderError::BadAuth(err))),
                            }
                        }
                        return Err(Error::AcmeOrder(OrderError::TooManyAttemptsAuth(
                            String::default(),
                        )));
                    }

                    Order::Ready { finalize }
                }
                Order::Ready { finalize } => {
                    let csr = cert.serialize_request_der()?;
                    account.send_csr(finalize, csr).await?
                }
                Order::Valid { certificate } => {
                    let certificate = dbg!(account.obtain_certificate(certificate).await)?;

                    dbg!(
                        self.install_pem_certificate(
                            certificate.as_bytes(),
                            pem::encode(&pem::Pem {
                                tag: String::from("BEGIN PRIVATE KEY"),
                                contents: pk
                            })
                            .as_bytes()
                        )
                        .await
                    )?;
                    return Ok(());
                }
                Order::Invalid => {
                    return Err(Error::AcmeOrder(OrderError::BadOrder(Order::Invalid)))
                }
            }
        }
    }

    /// Listens for HTTPS traffic on `port`. This port will also receive
    /// `WebSocket` connections if feature `websockets` is enabled. If feature
    /// `acme` is enabled, this connection will automatically manage the
    /// server's private key and certificate, which is also used for the
    /// QUIC-based protocol.
    pub async fn listen_for_https_on<T: tokio::net::ToSocketAddrs + Send + Sync>(
        &self,
        addr: T,
    ) -> Result<(), Error> {
        #[cfg(feature = "acme")]
        {
            let task_self = self.clone();
            tokio::task::spawn(async move { task_self.update_acme_certificates().await });
        }
        let config = rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(self.clone()));

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind(&addr).await?;
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            println!("new connection");
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
                println!("Accepted tls session");

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
        peer_address: SocketAddr,
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
    fn resolve(
        &self,
        client_hello: rustls::server::ClientHello<'_>,
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        if client_hello.alpn().and_then(|mut iter| iter.next())
            == Some(async_acme::acme::ACME_TLS_ALPN_NAME)
        {
            let server_name = dbg!(client_hello.server_name())?.to_owned();
            println!("Looking for key");
            let keys = self.data.alpn_keys.lock().unwrap();
            if let Some(key) = keys.get(AsRef::<str>::as_ref(&server_name)) {
                println!("Key found");
                return Some(key.clone());
            }

            None
        } else {
            todo!("return the server's certified key")
        }
    }
}

fn gen_acme_cert(
    domains: Vec<String>,
    acme_hash: &[u8],
) -> Result<CertifiedKey, rcgen::RcgenError> {
    let mut params = CertificateParams::new(domains);
    params.alg = &PKCS_ECDSA_P256_SHA256;
    params.custom_extensions = vec![CustomExtension::new_acme_identifier(acme_hash)];
    let cert = Certificate::from_params(params)?;
    let key = any_ecdsa_type(&PrivateKey(cert.serialize_private_key_der())).unwrap();
    Ok(CertifiedKey::new(
        vec![rustls::Certificate(cert.serialize_der()?)],
        key,
    ))
}
