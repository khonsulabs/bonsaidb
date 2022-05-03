use std::{
    collections::{hash_map, HashMap},
    fmt::Debug,
    net::SocketAddr,
    ops::Deref,
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_lock::{Mutex, RwLock};
use async_trait::async_trait;
use bonsaidb_core::{
    admin::{Admin, ADMIN_DATABASE_NAME},
    api,
    api::ApiName,
    arc_bytes::serde::Bytes,
    connection::{
        self, AsyncConnection, AsyncStorageConnection, HasSession, IdentityReference, Session,
        SessionId,
    },
    networking::{self, Payload, CURRENT_PROTOCOL_VERSION},
    permissions::{
        bonsai::{bonsaidb_resource_name, BonsaiAction, ServerAction},
        Permissions,
    },
    schema::{self, Nameable, NamedCollection, Schema},
};
use bonsaidb_local::{config::Builder, AsyncStorage, Storage, StorageNonBlocking};
use bonsaidb_utils::{fast_async_lock, fast_async_read, fast_async_write};
use derive_where::derive_where;
use fabruic::{self, CertificateChain, Endpoint, KeyPair, PrivateKey};
use flume::Sender;
use futures::{Future, StreamExt};
use rustls::sign::CertifiedKey;
use schema::SchemaName;
#[cfg(not(windows))]
use signal_hook::consts::SIGQUIT;
use signal_hook::consts::{SIGINT, SIGTERM};
use tokio::sync::{oneshot, Notify};

#[cfg(feature = "acme")]
use crate::config::AcmeConfiguration;
use crate::{
    api::{AnyHandler, HandlerSession},
    backend::ConnectionHandling,
    dispatch::{register_api_handlers, ServerDispatcher},
    error::Error,
    hosted::{Hosted, SerializablePrivateKey, TlsCertificate, TlsCertificatesByDomain},
    server::shutdown::{Shutdown, ShutdownState},
    Backend, BackendError, NoBackend, ServerConfiguration,
};

#[cfg(feature = "acme")]
pub mod acme;
mod connected_client;
mod database;

mod shutdown;
mod tcp;
#[cfg(feature = "websockets")]
mod websockets;

use self::connected_client::OwnedClient;
pub use self::{
    connected_client::{ConnectedClient, LockedClientDataGuard, Transport},
    database::ServerDatabase,
    tcp::{ApplicationProtocols, HttpService, Peer, StandardTcpProtocols, TcpService},
};

static CONNECTED_CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// A BonsaiDb server.
#[derive(Debug)]
#[derive_where(Clone)]
pub struct CustomServer<B: Backend = NoBackend> {
    data: Arc<Data<B>>,
    pub(crate) storage: AsyncStorage,
}

impl<'a, B: Backend> From<&'a CustomServer<B>> for Storage {
    fn from(server: &'a CustomServer<B>) -> Self {
        Self::from(server.storage.clone())
    }
}

impl<B: Backend> From<CustomServer<B>> for Storage {
    fn from(server: CustomServer<B>) -> Self {
        Self::from(server.storage)
    }
}

/// A BonsaiDb server without a custom backend.
pub type Server = CustomServer<NoBackend>;

#[derive(Debug)]
struct Data<B: Backend = NoBackend> {
    clients: RwLock<HashMap<u32, ConnectedClient<B>>>,
    request_processor: flume::Sender<ClientRequest<B>>,
    default_session: Session,
    endpoint: RwLock<Option<Endpoint>>,
    client_simultaneous_request_limit: usize,
    primary_tls_key: CachedCertifiedKey,
    primary_domain: String,
    custom_apis: parking_lot::RwLock<HashMap<ApiName, Arc<dyn AnyHandler<B>>>>,
    #[cfg(feature = "acme")]
    acme: AcmeConfiguration,
    #[cfg(feature = "acme")]
    alpn_keys: AlpnKeys,
    shutdown: Shutdown,
}

#[derive(Default)]
struct CachedCertifiedKey(parking_lot::Mutex<Option<Arc<CertifiedKey>>>);

impl Debug for CachedCertifiedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CachedCertifiedKey").finish()
    }
}

impl Deref for CachedCertifiedKey {
    type Target = parking_lot::Mutex<Option<Arc<CertifiedKey>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<B: Backend> CustomServer<B> {
    /// Opens a server using `directory` for storage.
    pub async fn open(
        configuration: ServerConfiguration<B>,
    ) -> Result<Self, BackendError<B::Error>> {
        let configuration = register_api_handlers(B::configure(configuration)?)?;
        let (request_sender, request_receiver) = flume::unbounded::<ClientRequest<B>>();
        for _ in 0..configuration.request_workers {
            let request_receiver = request_receiver.clone();
            tokio::task::spawn(async move {
                while let Ok(mut client_request) = request_receiver.recv_async().await {
                    let request = client_request.request.take().unwrap();
                    let session = client_request.session.clone();
                    // TODO we should be able to upgrade a session-less Storage to one with a Session.
                    // The Session needs to be looked up from the client based on the request's session id.
                    let result = match client_request.server.storage.assume_session(session) {
                        Ok(storage) => {
                            let client = HandlerSession {
                                server: &client_request.server,
                                client: &client_request.client,
                                as_client: Self {
                                    data: client_request.server.data.clone(),
                                    storage,
                                },
                            };
                            ServerDispatcher::dispatch_api_request(
                                client,
                                &request.name,
                                request.value.unwrap(),
                            )
                            .await
                            .map_err(bonsaidb_core::Error::from)
                        }
                        Err(err) => Err(err),
                    };
                    drop(client_request.result_sender.send((request.name, result)));
                }
            });
        }

        let storage = AsyncStorage::open(configuration.storage.with_schema::<Hosted>()?).await?;

        storage.create_database::<Hosted>("_hosted", true).await?;

        let default_permissions = Permissions::from(configuration.default_permissions);

        let server = Self {
            storage,
            data: Arc::new(Data {
                clients: RwLock::default(),
                endpoint: RwLock::default(),
                request_processor: request_sender,
                default_session: Session {
                    permissions: default_permissions,
                    ..Session::default()
                },
                client_simultaneous_request_limit: configuration.client_simultaneous_request_limit,
                primary_tls_key: CachedCertifiedKey::default(),
                primary_domain: configuration.server_name,
                custom_apis: parking_lot::RwLock::new(configuration.custom_apis),
                #[cfg(feature = "acme")]
                acme: configuration.acme,
                #[cfg(feature = "acme")]
                alpn_keys: AlpnKeys::default(),
                shutdown: Shutdown::new(),
            }),
        };
        B::initialize(&server).await?;
        Ok(server)
    }

    /// Returns the path to the public pinned certificate, if this server has
    /// one. Note: this function will always succeed, but the file may not
    /// exist.
    #[must_use]
    pub fn pinned_certificate_path(&self) -> PathBuf {
        self.path().join("pinned-certificate.der")
    }

    /// Returns the primary domain configured for this server.
    #[must_use]
    pub fn primary_domain(&self) -> &str {
        &self.data.primary_domain
    }

    /// Returns the administration database.
    pub async fn admin(&self) -> ServerDatabase<B> {
        let db = self.storage.admin().await;
        ServerDatabase {
            server: self.clone(),
            db,
        }
    }

    pub(crate) async fn hosted(&self) -> ServerDatabase<B> {
        let db = self.storage.database::<Hosted>("_hosted").await.unwrap();
        ServerDatabase {
            server: self.clone(),
            db,
        }
    }

    pub(crate) fn custom_api_dispatcher(&self, name: &ApiName) -> Option<Arc<dyn AnyHandler<B>>> {
        let dispatchers = self.data.custom_apis.read();
        dispatchers.get(name).cloned()
    }

    /// Installs an X.509 certificate used for general purpose connections.
    pub async fn install_self_signed_certificate(&self, overwrite: bool) -> Result<(), Error> {
        let keypair = KeyPair::new_self_signed(&self.data.primary_domain);

        if self.certificate_chain().await.is_ok() && !overwrite {
            return Err(Error::Core(bonsaidb_core::Error::other("bonsaidb-server config", "Certificate already installed. Enable overwrite if you wish to replace the existing certificate.")));
        }

        self.install_certificate(keypair.certificate_chain(), keypair.private_key())
            .await?;

        tokio::fs::write(
            self.pinned_certificate_path(),
            keypair.end_entity_certificate().as_ref(),
        )
        .await?;

        Ok(())
    }

    /// Installs a certificate chain and private key used for TLS connections.
    #[cfg(feature = "pem")]
    pub async fn install_pem_certificate(
        &self,
        certificate_chain: &[u8],
        private_key: &[u8],
    ) -> Result<(), Error> {
        let private_key = match pem::parse(private_key) {
            Ok(pem) => PrivateKey::unchecked_from_der(pem.contents),
            Err(_) => PrivateKey::from_der(private_key)?,
        };
        let certificates = pem::parse_many(&certificate_chain)?
            .into_iter()
            .map(|entry| fabruic::Certificate::unchecked_from_der(entry.contents))
            .collect::<Vec<_>>();

        self.install_certificate(
            &CertificateChain::unchecked_from_certificates(certificates),
            &private_key,
        )
        .await
    }

    /// Installs a certificate chain and private key used for TLS connections.
    pub async fn install_certificate(
        &self,
        certificate_chain: &CertificateChain,
        private_key: &PrivateKey,
    ) -> Result<(), Error> {
        let db = self.hosted().await;

        TlsCertificate::entry_async(&self.data.primary_domain, &db)
            .update_with(|cert: &mut TlsCertificate| {
                cert.certificate_chain = certificate_chain.clone();
                cert.private_key = SerializablePrivateKey(private_key.clone());
            })
            .or_insert_with(|| TlsCertificate {
                domains: vec![self.data.primary_domain.clone()],
                private_key: SerializablePrivateKey(private_key.clone()),
                certificate_chain: certificate_chain.clone(),
            })
            .await?;

        self.refresh_certified_key().await?;

        let pinned_certificate_path = self.pinned_certificate_path();
        if pinned_certificate_path.exists() {
            tokio::fs::remove_file(&pinned_certificate_path).await?;
        }

        Ok(())
    }

    async fn refresh_certified_key(&self) -> Result<(), Error> {
        let certificate = self.tls_certificate().await?;

        let mut cached_key = self.data.primary_tls_key.lock();
        let private_key = rustls::PrivateKey(
            fabruic::dangerous::PrivateKey::as_ref(&certificate.private_key.0).to_vec(),
        );
        let private_key = rustls::sign::any_ecdsa_type(&Arc::new(private_key))?;

        let certificates = certificate
            .certificate_chain
            .iter()
            .map(|cert| rustls::Certificate(cert.as_ref().to_vec()))
            .collect::<Vec<_>>();

        let certified_key = Arc::new(CertifiedKey::new(certificates, private_key));
        *cached_key = Some(certified_key);
        Ok(())
    }

    async fn tls_certificate(&self) -> Result<TlsCertificate, Error> {
        let db = self.hosted().await;
        let (_, certificate) = db
            .view::<TlsCertificatesByDomain>()
            .with_key(&self.data.primary_domain)
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .next()
            .ok_or_else(|| {
                Error::Core(bonsaidb_core::Error::other(
                    "bonsaidb-server config",
                    format!("no certificate found for {}", self.data.primary_domain),
                ))
            })?;
        Ok(certificate.contents)
    }

    /// Returns the current certificate chain.
    pub async fn certificate_chain(&self) -> Result<CertificateChain, Error> {
        let db = self.hosted().await;
        if let Some(mapping) = db
            .view::<TlsCertificatesByDomain>()
            .with_key(&self.data.primary_domain)
            .query()
            .await?
            .into_iter()
            .next()
        {
            Ok(mapping.value)
        } else {
            Err(Error::Core(bonsaidb_core::Error::other(
                "bonsaidb-server config",
                format!("no certificate found for {}", self.data.primary_domain),
            )))
        }
    }

    /// Listens for incoming client connections. Does not return until the
    /// server shuts down.
    pub async fn listen_on(&self, port: u16) -> Result<(), Error> {
        let certificate = self.tls_certificate().await?;
        let keypair =
            KeyPair::from_parts(certificate.certificate_chain, certificate.private_key.0)?;
        let mut builder = Endpoint::builder();
        builder.set_protocols([CURRENT_PROTOCOL_VERSION.as_bytes().to_vec()]);
        builder.set_address(([0; 8], port).into());
        builder.set_max_idle_timeout(None)?;
        builder.set_server_key_pair(Some(keypair));
        let mut server = builder.build()?;
        {
            let mut endpoint = fast_async_write!(self.data.endpoint);
            *endpoint = Some(server.clone());
        }

        let mut shutdown_watcher = self
            .data
            .shutdown
            .watcher()
            .await
            .expect("server already shut down");

        while let Some(result) = tokio::select! {
            shutdown_state = shutdown_watcher.wait_for_shutdown() => {
                drop(server.close_incoming());
                if matches!(shutdown_state, ShutdownState::GracefulShutdown) {
                    server.wait_idle().await;
                }
                drop(server.close());
                None
            },
            msg = server.next() => msg
        } {
            let connection = result.accept::<()>().await?;
            let task_self = self.clone();
            tokio::spawn(async move {
                let address = connection.remote_address();
                if let Err(err) = task_self.handle_bonsai_connection(connection).await {
                    log::error!("[server] closing connection {}: {:?}", address, err);
                }
            });
        }

        Ok(())
    }

    /// Returns all of the currently connected clients.
    pub async fn connected_clients(&self) -> Vec<ConnectedClient<B>> {
        let clients = fast_async_read!(self.data.clients);
        clients.values().cloned().collect()
    }

    /// Sends a custom API response to all connected clients.
    pub async fn broadcast<Api: api::Api>(&self, response: &Api::Response) {
        let clients = fast_async_read!(self.data.clients);
        for client in clients.values() {
            // TODO should this broadcast to all sessions too rather than only the global session?
            drop(client.send::<Api>(None, response));
        }
    }

    async fn initialize_client(
        &self,
        transport: Transport,
        address: SocketAddr,
        sender: Sender<(Option<SessionId>, ApiName, Bytes)>,
    ) -> Option<OwnedClient<B>> {
        if !self.data.default_session.allowed_to(
            &bonsaidb_resource_name(),
            &BonsaiAction::Server(ServerAction::Connect),
        ) {
            return None;
        }

        let client = loop {
            let next_id = CONNECTED_CLIENT_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
            let mut clients = fast_async_write!(self.data.clients);
            if let hash_map::Entry::Vacant(e) = clients.entry(next_id) {
                let client = OwnedClient::new(
                    next_id,
                    address,
                    transport,
                    sender,
                    self.clone(),
                    self.data.default_session.clone(),
                );
                e.insert(client.clone());
                break client;
            }
        };

        match B::client_connected(&client, self).await {
            Ok(ConnectionHandling::Accept) => Some(client),
            Ok(ConnectionHandling::Reject) => None,
            Err(err) => {
                log::error!(
                    "[server] Rejecting connection due to error in `client_connected`: {err:?}"
                );
                None
            }
        }
    }

    async fn disconnect_client(&self, id: u32) {
        if let Some(client) = {
            let mut clients = fast_async_write!(self.data.clients);
            clients.remove(&id)
        } {
            if let Err(err) = B::client_disconnected(client, self).await {
                log::error!("[server] Error in `client_disconnected`: {err:?}");
            }
        }
    }

    async fn handle_bonsai_connection(
        &self,
        mut connection: fabruic::Connection<()>,
    ) -> Result<(), Error> {
        if let Some(incoming) = connection.next().await {
            let incoming = match incoming {
                Ok(incoming) => incoming,
                Err(err) => {
                    log::error!("[server] Error establishing a stream: {err:?}");
                    return Ok(());
                }
            };

            match incoming
                .accept::<networking::Payload, networking::Payload>()
                .await
            {
                Ok((sender, receiver)) => {
                    let (api_response_sender, api_response_receiver) = flume::unbounded();
                    if let Some(disconnector) = self
                        .initialize_client(
                            Transport::Bonsai,
                            connection.remote_address(),
                            api_response_sender,
                        )
                        .await
                    {
                        let task_sender = sender.clone();
                        tokio::spawn(async move {
                            while let Ok((session_id, name, bytes)) =
                                api_response_receiver.recv_async().await
                            {
                                if task_sender
                                    .send(&Payload {
                                        id: None,
                                        session_id,
                                        name,
                                        value: Ok(bytes),
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            let _ = connection.close().await;
                        });

                        let task_self = self.clone();
                        tokio::spawn(async move {
                            if let Err(err) = task_self
                                .handle_stream(disconnector, sender, receiver)
                                .await
                            {
                                log::error!("[server] Error handling stream: {err:?}");
                            }
                        });
                    } else {
                        log::error!("[server] Backend rejected connection.");
                        return Ok(());
                    }
                }
                Err(err) => {
                    log::error!("[server] Error accepting incoming stream: {err:?}");
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn handle_client_requests(
        &self,
        client: ConnectedClient<B>,
        request_receiver: flume::Receiver<Payload>,
        response_sender: flume::Sender<Payload>,
    ) {
        let notify = Arc::new(Notify::new());
        let requests_in_queue = Arc::new(AtomicUsize::new(0));
        loop {
            let current_requests = requests_in_queue.load(Ordering::SeqCst);
            if current_requests == self.data.client_simultaneous_request_limit {
                // Wait for requests to finish.
                notify.notified().await;
            } else if requests_in_queue
                .compare_exchange(
                    current_requests,
                    current_requests + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                let payload = match request_receiver.recv_async().await {
                    Ok(payload) => payload,
                    Err(_) => break,
                };
                let session_id = payload.session_id;
                let id = payload.id;
                let task_sender = response_sender.clone();

                let notify = notify.clone();
                let requests_in_queue = requests_in_queue.clone();
                self.handle_request_through_worker(
                    payload,
                    move |name, value| async move {
                        drop(task_sender.send(Payload {
                            session_id,
                            id,
                            name,
                            value,
                        }));

                        requests_in_queue.fetch_sub(1, Ordering::SeqCst);

                        notify.notify_one();

                        Ok(())
                    },
                    client.clone(),
                )
                .await
                .unwrap();
            }
        }
    }

    async fn handle_request_through_worker<
        F: FnOnce(ApiName, Result<Bytes, bonsaidb_core::Error>) -> R + Send + 'static,
        R: Future<Output = Result<(), Error>> + Send,
    >(
        &self,
        request: Payload,
        callback: F,
        client: ConnectedClient<B>,
    ) -> Result<(), Error> {
        let (result_sender, result_receiver) = oneshot::channel();
        let session = client
            .session(request.session_id)
            .unwrap_or_else(|| self.data.default_session.clone());
        self.data
            .request_processor
            .send(ClientRequest::<B>::new(
                request,
                self.clone(),
                client,
                session,
                result_sender,
            ))
            .map_err(|_| Error::InternalCommunication)?;
        tokio::spawn(async move {
            let (name, result) = result_receiver.await?;
            // Map the error into a Response::Error. The jobs system supports
            // multiple receivers receiving output, and wraps Err to avoid
            // requiring the error to be cloneable. As such, we have to unwrap
            // it. Thankfully, we can guarantee nothing else is waiting on a
            // response to a request than the original requestor, so this can be
            // safely unwrapped.
            callback(name, result).await?;
            Result::<(), Error>::Ok(())
        });
        Ok(())
    }

    async fn handle_stream(
        &self,
        client: OwnedClient<B>,
        sender: fabruic::Sender<Payload>,
        mut receiver: fabruic::Receiver<Payload>,
    ) -> Result<(), Error> {
        let (payload_sender, payload_receiver) = flume::unbounded();
        tokio::spawn(async move {
            while let Ok(payload) = payload_receiver.recv_async().await {
                if sender.send(&payload).is_err() {
                    break;
                }
            }
        });

        let (request_sender, request_receiver) =
            flume::bounded::<Payload>(self.data.client_simultaneous_request_limit);
        let task_self = self.clone();
        tokio::spawn(async move {
            task_self
                .handle_client_requests(client.clone(), request_receiver, payload_sender)
                .await;
        });

        while let Some(payload) = receiver.next().await {
            drop(request_sender.send_async(payload?).await);
        }

        Ok(())
    }

    /// Shuts the server down. If a `timeout` is provided, the server will stop
    /// accepting new connections and attempt to respond to any outstanding
    /// requests already being processed. After the `timeout` has elapsed or if
    /// no `timeout` was provided, the server is forcefully shut down.
    pub async fn shutdown(&self, timeout: Option<Duration>) -> Result<(), Error> {
        if let Some(timeout) = timeout {
            self.data.shutdown.graceful_shutdown(timeout).await;
        } else {
            self.data.shutdown.shutdown().await;
        }

        Ok(())
    }

    /// Listens for signals from the operating system that the server should
    /// shut down and attempts to gracefully shut down.
    pub async fn listen_for_shutdown(&self) -> Result<(), Error> {
        const GRACEFUL_SHUTDOWN: usize = 1;
        const TERMINATE: usize = 2;

        enum SignalShutdownState {
            Running,
            ShuttingDown(flume::Receiver<()>),
        }

        let shutdown_state = Arc::new(Mutex::new(SignalShutdownState::Running));
        let flag = Arc::new(AtomicUsize::default());
        let register_hook = |flag: &Arc<AtomicUsize>| {
            signal_hook::flag::register_usize(SIGINT, flag.clone(), GRACEFUL_SHUTDOWN)?;
            signal_hook::flag::register_usize(SIGTERM, flag.clone(), TERMINATE)?;
            #[cfg(not(windows))]
            signal_hook::flag::register_usize(SIGQUIT, flag.clone(), TERMINATE)?;
            Result::<(), std::io::Error>::Ok(())
        };
        if let Err(err) = register_hook(&flag) {
            log::error!("Error installing signals for graceful shutdown: {err:?}");
            tokio::time::sleep(Duration::MAX).await;
        } else {
            loop {
                match flag.load(Ordering::Relaxed) {
                    0 => {
                        // No signal
                    }
                    GRACEFUL_SHUTDOWN => {
                        let mut state = fast_async_lock!(shutdown_state);
                        match *state {
                            SignalShutdownState::Running => {
                                log::error!("Interrupt signal received. Shutting down gracefully.");
                                let task_server = self.clone();
                                let (shutdown_sender, shutdown_receiver) = flume::bounded(1);
                                tokio::task::spawn(async move {
                                    task_server.shutdown(Some(Duration::from_secs(30))).await?;
                                    let _ = shutdown_sender.send(());
                                    Result::<(), Error>::Ok(())
                                });
                                *state = SignalShutdownState::ShuttingDown(shutdown_receiver);
                            }
                            SignalShutdownState::ShuttingDown(_) => {
                                // Two interrupts, go ahead and force the shutdown
                                break;
                            }
                        }
                    }
                    TERMINATE => {
                        log::error!("Quit signal received. Shutting down.");
                        break;
                    }
                    _ => unreachable!(),
                }

                let state = fast_async_lock!(shutdown_state);
                if let SignalShutdownState::ShuttingDown(receiver) = &*state {
                    if receiver.try_recv().is_ok() {
                        // Fully shut down.
                        return Ok(());
                    }
                }

                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            self.shutdown(None).await?;
        }

        Ok(())
    }
}

impl<B: Backend> Deref for CustomServer<B> {
    type Target = AsyncStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

#[derive(Debug)]
struct ClientRequest<B: Backend> {
    request: Option<Payload>,
    client: ConnectedClient<B>,
    session: Session,
    server: CustomServer<B>,
    result_sender: oneshot::Sender<(ApiName, Result<Bytes, bonsaidb_core::Error>)>,
}

impl<B: Backend> ClientRequest<B> {
    pub fn new(
        request: Payload,
        server: CustomServer<B>,
        client: ConnectedClient<B>,
        session: Session,
        result_sender: oneshot::Sender<(ApiName, Result<Bytes, bonsaidb_core::Error>)>,
    ) -> Self {
        Self {
            request: Some(request),
            server,
            client,
            session,
            result_sender,
        }
    }
}

impl<B: Backend> HasSession for CustomServer<B> {
    fn session(&self) -> Option<&Session> {
        self.storage.session()
    }
}

#[async_trait]
impl<B: Backend> AsyncStorageConnection for CustomServer<B> {
    type Database = ServerDatabase<B>;
    type Authenticated = Self;

    async fn admin(&self) -> Self::Database {
        self.database::<Admin>(ADMIN_DATABASE_NAME).await.unwrap()
    }

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage
            .create_database_with_schema(name, schema, only_if_needed)
            .await
    }

    async fn database<DB: Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        let db = self.storage.database::<DB>(name).await?;
        Ok(ServerDatabase {
            server: self.clone(),
            db,
        })
    }

    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        self.storage.delete_database(name).await
    }

    async fn list_databases(&self) -> Result<Vec<connection::Database>, bonsaidb_core::Error> {
        self.storage.list_databases().await
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        self.storage.list_available_schemas().await
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        self.storage.create_user(username).await
    }

    async fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage.delete_user(user).await
    }

    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage.set_user_password(user, password).await
    }

    #[cfg(any(feature = "token-authentication", feature = "password-hashing"))]
    async fn authenticate(
        &self,
        authentication: bonsaidb_core::connection::Authentication,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let storage = self.storage.authenticate(authentication).await?;
        Ok(Self {
            data: self.data.clone(),
            storage,
        })
    }

    async fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let storage = self.storage.assume_identity(identity).await?;
        Ok(Self {
            data: self.data.clone(),
            storage,
        })
    }

    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage
            .add_permission_group_to_user(user, permission_group)
            .await
    }

    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage
            .remove_permission_group_from_user(user, permission_group)
            .await
    }

    async fn add_role_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage.add_role_to_user(user, role).await
    }

    async fn remove_role_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.storage.remove_role_from_user(user, role).await
    }
}

#[derive(Default)]
struct AlpnKeys(Arc<std::sync::Mutex<HashMap<String, Arc<rustls::sign::CertifiedKey>>>>);

impl Debug for AlpnKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AlpnKeys").finish()
    }
}

impl Deref for AlpnKeys {
    type Target = Arc<std::sync::Mutex<HashMap<String, Arc<rustls::sign::CertifiedKey>>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
