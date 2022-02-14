use std::{
    collections::{hash_map, HashMap},
    fmt::Debug,
    marker::PhantomData,
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
    admin::User,
    arc_bytes::serde::Bytes,
    circulate::{Message, Relay, Subscriber},
    connection::{self, AccessPolicy, Connection, QueryKey, Range, Sort, StorageConnection},
    custom_api::{CustomApi, CustomApiResult},
    document::DocumentId,
    keyvalue::{KeyOperation, KeyValue},
    networking::{
        self, CreateDatabaseHandler, DatabaseRequest, DatabaseRequestDispatcher, DatabaseResponse,
        DeleteDatabaseHandler, Payload, Request, RequestDispatcher, Response, ServerRequest,
        ServerRequestDispatcher, ServerResponse, CURRENT_PROTOCOL_VERSION,
    },
    permissions::{
        bonsai::{
            bonsaidb_resource_name, collection_resource_name, database_resource_name,
            document_resource_name, keyvalue_key_resource_name, kv_resource_name,
            pubsub_topic_resource_name, user_resource_name, view_resource_name, BonsaiAction,
            DatabaseAction, DocumentAction, KeyValueAction, PubSubAction, ServerAction,
            TransactionAction, ViewAction,
        },
        Action, Dispatcher, PermissionDenied, Permissions, ResourceName,
    },
    pubsub::database_topic,
    schema::{self, CollectionName, NamedCollection, NamedReference, Schema, ViewName},
    transaction::{Command, Transaction},
};
#[cfg(feature = "password-hashing")]
use bonsaidb_core::{
    connection::{Authenticated, Authentication},
    permissions::bonsai::AuthenticationMethod,
};
use bonsaidb_local::{
    config::Builder,
    jobs::{manager::Manager, Job},
    Database, Storage,
};
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
use tokio::sync::Notify;

#[cfg(feature = "acme")]
use crate::config::AcmeConfiguration;
use crate::{
    backend::{BackendError, ConnectionHandling, CustomApiDispatcher},
    error::Error,
    hosted::{Hosted, SerializablePrivateKey, TlsCertificate, TlsCertificatesByDomain},
    server::shutdown::{Shutdown, ShutdownState},
    Backend, NoBackend, ServerConfiguration,
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
    database::{ServerDatabase, ServerSubscriber},
    tcp::{ApplicationProtocols, HttpService, Peer, StandardTcpProtocols, TcpService},
};

static CONNECTED_CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// A BonsaiDb server.
#[derive(Debug)]
#[derive_where(Clone)]
pub struct CustomServer<B: Backend = NoBackend> {
    data: Arc<Data<B>>,
}

/// A BonsaiDb server without a custom backend.
pub type Server = CustomServer<NoBackend>;

#[derive(Debug)]
struct Data<B: Backend = NoBackend> {
    storage: Storage,
    clients: RwLock<HashMap<u32, ConnectedClient<B>>>,
    request_processor: Manager,
    default_permissions: Permissions,
    authenticated_permissions: Permissions,
    endpoint: RwLock<Option<Endpoint>>,
    client_simultaneous_request_limit: usize,
    primary_tls_key: CachedCertifiedKey,
    primary_domain: String,
    #[cfg(feature = "acme")]
    acme: AcmeConfiguration,
    #[cfg(feature = "acme")]
    alpn_keys: AlpnKeys,
    shutdown: Shutdown,
    relay: Relay,
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    _backend: PhantomData<B>,
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
    pub async fn open(configuration: ServerConfiguration) -> Result<Self, Error> {
        let request_processor = Manager::default();
        for _ in 0..configuration.request_workers {
            request_processor.spawn_worker();
        }

        let storage = Storage::open(configuration.storage.with_schema::<Hosted>()?).await?;

        storage.create_database::<Hosted>("_hosted", true).await?;

        let default_permissions = Permissions::from(configuration.default_permissions);
        let authenticated_permissions = Permissions::from(configuration.authenticated_permissions);

        let server = Self {
            data: Arc::new(Data {
                clients: RwLock::default(),
                storage,
                endpoint: RwLock::default(),
                request_processor,
                default_permissions,
                authenticated_permissions,
                client_simultaneous_request_limit: configuration.client_simultaneous_request_limit,
                primary_tls_key: CachedCertifiedKey::default(),
                primary_domain: configuration.server_name,
                #[cfg(feature = "acme")]
                acme: configuration.acme,
                #[cfg(feature = "acme")]
                alpn_keys: AlpnKeys::default(),
                shutdown: Shutdown::new(),
                relay: Relay::default(),
                subscribers: Arc::default(),
                _backend: PhantomData::default(),
            }),
        };
        B::initialize(&server).await;
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
        let db = self.data.storage.admin().await;
        ServerDatabase {
            server: self.clone(),
            db,
        }
    }

    pub(crate) async fn hosted(&self) -> ServerDatabase<B> {
        let db = self
            .data
            .storage
            .database::<Hosted>("_hosted")
            .await
            .unwrap();
        ServerDatabase {
            server: self.clone(),
            db,
        }
    }

    /// Installs an X.509 certificate used for general purpose connections.
    pub async fn install_self_signed_certificate(&self, overwrite: bool) -> Result<(), Error> {
        let keypair = KeyPair::new_self_signed(&self.data.primary_domain);

        if self.certificate_chain().await.is_ok() && !overwrite {
            return Err(Error::Core(bonsaidb_core::Error::Configuration(String::from("Certificate already installed. Enable overwrite if you wish to replace the existing certificate."))));
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

        TlsCertificate::entry(&self.data.primary_domain, &db)
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
            .with_key(self.data.primary_domain.clone())
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .next()
            .ok_or_else(|| {
                Error::Core(bonsaidb_core::Error::Configuration(format!(
                    "no certificate found for {}",
                    self.data.primary_domain
                )))
            })?;
        Ok(certificate.contents)
    }

    /// Returns the current certificate chain.
    pub async fn certificate_chain(&self) -> Result<CertificateChain, Error> {
        let db = self.hosted().await;
        if let Some(mapping) = db
            .view::<TlsCertificatesByDomain>()
            .with_key(self.data.primary_domain.clone())
            .query()
            .await?
            .into_iter()
            .next()
        {
            Ok(mapping.value)
        } else {
            Err(Error::Core(bonsaidb_core::Error::Configuration(format!(
                "no certificate found for {}",
                self.data.primary_domain
            ))))
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
        builder
            .set_max_idle_timeout(None)
            .map_err(|err| Error::Core(bonsaidb_core::Error::Transport(err.to_string())))?;
        builder.set_server_key_pair(Some(keypair));
        let mut server = builder
            .build()
            .map_err(|err| Error::Core(bonsaidb_core::Error::Transport(err.to_string())))?;
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
    pub async fn broadcast(&self, response: CustomApiResult<B::CustomApi>) {
        let clients = fast_async_read!(self.data.clients);
        for client in clients.values() {
            drop(client.send(response.clone()));
        }
    }

    async fn initialize_client(
        &self,
        transport: Transport,
        address: SocketAddr,
        sender: Sender<CustomApiResult<B::CustomApi>>,
    ) -> Option<OwnedClient<B>> {
        if !self.data.default_permissions.allowed_to(
            &bonsaidb_resource_name(),
            &BonsaiAction::Server(ServerAction::Connect),
        ) {
            return None;
        }

        let client = loop {
            let next_id = CONNECTED_CLIENT_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
            let mut clients = fast_async_write!(self.data.clients);
            if let hash_map::Entry::Vacant(e) = clients.entry(next_id) {
                let client = OwnedClient::new(next_id, address, transport, sender, self.clone());
                e.insert(client.clone());
                break client;
            }
        };

        if matches!(
            B::client_connected(&client, self).await,
            ConnectionHandling::Accept
        ) {
            Some(client)
        } else {
            None
        }
    }

    async fn disconnect_client(&self, id: u32) {
        if let Some(client) = {
            let mut clients = fast_async_write!(self.data.clients);
            clients.remove(&id)
        } {
            B::client_disconnected(client, self).await;
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
                    log::error!("[server] Error establishing a stream: {:?}", err);
                    return Ok(());
                }
            };

            match incoming
            .accept::<networking::Payload<Response<CustomApiResult<B::CustomApi>>>, networking::Payload<Request<<B::CustomApi as CustomApi>::Request>>>()
            .await {
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
                            while let Ok(response) = api_response_receiver.recv_async().await {
                                if task_sender
                                    .send(&Payload {
                                        id: None,
                                        wrapped: Response::Api(response),
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
                                log::error!("[server] Error handling stream: {:?}", err);
                            }
                        });
                    } else {
                        log::error!("[server] Backend rejected connection.");
                        return Ok(());
                    }
                }
                Err(err) => {
                    log::error!("[server] Error accepting incoming stream: {:?}", err);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    async fn handle_client_requests(
        &self,
        client: ConnectedClient<B>,
        request_receiver: flume::Receiver<Payload<Request<<B::CustomApi as CustomApi>::Request>>>,
        response_sender: flume::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
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
                let id = payload.id;
                let task_sender = response_sender.clone();

                let notify = notify.clone();
                let requests_in_queue = requests_in_queue.clone();
                self.handle_request_through_worker(
                    payload.wrapped,
                    move |response| async move {
                        drop(task_sender.send(Payload {
                            id,
                            wrapped: response,
                        }));

                        requests_in_queue.fetch_sub(1, Ordering::SeqCst);

                        notify.notify_one();

                        Ok(())
                    },
                    client.clone(),
                    self.data.subscribers.clone(),
                    response_sender.clone(),
                )
                .await
                .unwrap();
            }
        }
    }

    async fn handle_request_through_worker<
        F: FnOnce(Response<CustomApiResult<B::CustomApi>>) -> R + Send + 'static,
        R: Future<Output = Result<(), Error>> + Send,
    >(
        &self,
        request: Request<<B::CustomApi as CustomApi>::Request>,
        callback: F,
        client: ConnectedClient<B>,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        response_sender: flume::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
    ) -> Result<(), Error> {
        let job = self
            .data
            .request_processor
            .enqueue(ClientRequest::<B>::new(
                request,
                self.clone(),
                client,
                subscribers,
                response_sender,
            ))
            .await;
        tokio::spawn(async move {
            let result = job.receive().await?;
            // Map the error into a Response::Error. The jobs system supports
            // multiple receivers receiving output, and wraps Err to avoid
            // requiring the error to be cloneable. As such, we have to unwrap
            // it. Thankfully, we can guarantee nothing else is waiting on a
            // response to a request than the original requestor, so this can be
            // safely unwrapped.
            let result =
                result.unwrap_or_else(|err| Response::Error(Arc::try_unwrap(err).unwrap().into()));
            callback(result).await?;
            Result::<(), Error>::Ok(())
        });
        Ok(())
    }

    async fn handle_stream(
        &self,
        client: OwnedClient<B>,
        sender: fabruic::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
        mut receiver: fabruic::Receiver<Payload<Request<<B::CustomApi as CustomApi>::Request>>>,
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
            flume::bounded::<Payload<Request<<B::CustomApi as CustomApi>::Request>>>(
                self.data.client_simultaneous_request_limit,
            );
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

    async fn forward_notifications_for(
        &self,
        subscriber_id: u64,
        receiver: flume::Receiver<Arc<Message>>,
        sender: flume::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
    ) {
        while let Ok(message) = receiver.recv_async().await {
            if sender
                .send(Payload {
                    id: None,
                    wrapped: Response::Database(DatabaseResponse::MessageReceived {
                        subscriber_id,
                        topic: message.topic.clone(),
                        payload: Bytes::from(&message.payload[..]),
                    }),
                })
                .is_err()
            {
                break;
            }
        }
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
            log::error!("Error installing signals for graceful shutdown: {:?}", err);
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

    /// Manually authenticates `client` as `user`. `user` can be the user's id
    /// ([`u64`]) or the username ([`String`]/[`str`]). Returns the permissions
    /// that the user now has.
    pub async fn authenticate_client_as<'name, N: Into<NamedReference<'name>> + Send + Sync>(
        &self,
        user: N,
        client: &ConnectedClient<B>,
    ) -> Result<Permissions, Error> {
        let admin = self.data.storage.admin().await;
        let user = User::load(user, &admin)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;

        let permissions = user.contents.effective_permissions(&admin).await?;
        let permissions = Permissions::merged(
            [
                &permissions,
                &self.data.authenticated_permissions,
                &self.data.default_permissions,
            ]
            .into_iter(),
        );
        client
            .logged_in_as(user.header.id, permissions.clone())
            .await;
        Ok(permissions)
    }

    async fn publish_message(&self, database: &str, topic: &str, payload: Vec<u8>) {
        self.data
            .relay
            .publish_message(Message {
                topic: database_topic(database, topic),
                payload,
            })
            .await;
    }

    async fn publish_serialized_to_all(&self, database: &str, topics: &[String], payload: Vec<u8>) {
        self.data
            .relay
            .publish_serialized_to_all(
                topics
                    .iter()
                    .map(|topic| database_topic(database, topic))
                    .collect(),
                payload,
            )
            .await;
    }

    async fn create_subscriber(&self, database: String) -> ServerSubscriber<B> {
        let subscriber = self.data.relay.create_subscriber().await;

        let mut subscribers = fast_async_write!(self.data.subscribers);
        let subscriber_id = subscriber.id();
        let receiver = subscriber.receiver().clone();
        subscribers.insert(subscriber_id, subscriber);

        ServerSubscriber {
            server: self.clone(),
            database,
            receiver,
            id: subscriber_id,
        }
    }

    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        subscriber_id: u64,
        database: &str,
        topic: S,
    ) -> Result<(), bonsaidb_core::Error> {
        let subscribers = fast_async_read!(self.data.subscribers);
        if let Some(subscriber) = subscribers.get(&subscriber_id) {
            subscriber
                .subscribe_to(database_topic(database, &topic.into()))
                .await;
            Ok(())
        } else {
            Err(bonsaidb_core::Error::Server(String::from(
                "invalid subscriber id",
            )))
        }
    }

    async fn unsubscribe_from(
        &self,
        subscriber_id: u64,
        database: &str,
        topic: &str,
    ) -> Result<(), bonsaidb_core::Error> {
        let subscribers = fast_async_read!(self.data.subscribers);
        if let Some(subscriber) = subscribers.get(&subscriber_id) {
            subscriber
                .unsubscribe_from(&database_topic(database, topic))
                .await;
            Ok(())
        } else {
            Err(bonsaidb_core::Error::Server(String::from(
                "invalid subscriber id",
            )))
        }
    }
}

impl<B: Backend> Deref for CustomServer<B> {
    type Target = Storage;

    fn deref(&self) -> &Self::Target {
        &self.data.storage
    }
}

#[derive(Debug)]
struct ClientRequest<B: Backend> {
    request: Option<Request<<B::CustomApi as CustomApi>::Request>>,
    client: ConnectedClient<B>,
    server: CustomServer<B>,
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    sender: flume::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
}

impl<B: Backend> ClientRequest<B> {
    pub fn new(
        request: Request<<B::CustomApi as CustomApi>::Request>,
        server: CustomServer<B>,
        client: ConnectedClient<B>,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        sender: flume::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
    ) -> Self {
        Self {
            request: Some(request),
            server,
            client,
            subscribers,
            sender,
        }
    }
}

#[async_trait]
impl<B: Backend> Job for ClientRequest<B> {
    type Output = Response<CustomApiResult<B::CustomApi>>;
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn execute(&mut self) -> Result<Self::Output, Self::Error> {
        let request = self.request.take().unwrap();
        ServerDispatcher {
            server: &self.server,
            client: &self.client,
            subscribers: &self.subscribers,
            response_sender: &self.sender,
        }
        .dispatch(&self.client.permissions().await, request)
        .await
    }
}

#[async_trait]
impl<B: Backend> StorageConnection for CustomServer<B> {
    type Database = ServerDatabase<B>;

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .create_database_with_schema(name, schema, only_if_needed)
            .await
    }

    async fn database<DB: Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        let db = self.data.storage.database::<DB>(name).await?;
        Ok(ServerDatabase {
            server: self.clone(),
            db,
        })
    }

    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        self.data.storage.delete_database(name).await
    }

    async fn list_databases(&self) -> Result<Vec<connection::Database>, bonsaidb_core::Error> {
        self.data.storage.list_databases().await
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        self.data.storage.list_available_schemas().await
    }

    async fn create_user(&self, username: &str) -> Result<DocumentId, bonsaidb_core::Error> {
        self.data.storage.create_user(username).await
    }

    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data.storage.set_user_password(user, password).await
    }

    #[cfg(feature = "password-hashing")]
    async fn authenticate<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Authenticated, bonsaidb_core::Error> {
        self.data.storage.authenticate(user, authentication).await
    }

    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .add_permission_group_to_user(user, permission_group)
            .await
    }

    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .remove_permission_group_from_user(user, permission_group)
            .await
    }

    async fn add_role_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data.storage.add_role_to_user(user, role).await
    }

    async fn remove_role_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data.storage.remove_role_from_user(user, role).await
    }
}

#[derive(Dispatcher, Debug)]
#[dispatcher(input = Request<<B::CustomApi as CustomApi>::Request>, input = ServerRequest, actionable = bonsaidb_core::actionable)]
struct ServerDispatcher<'s, B>
where
    B: Backend,
{
    server: &'s CustomServer<B>,
    client: &'s ConnectedClient<B>,
    subscribers: &'s Arc<RwLock<HashMap<u64, Subscriber>>>,
    response_sender: &'s flume::Sender<Payload<Response<CustomApiResult<B::CustomApi>>>>,
}

#[async_trait]
impl<'s, B: Backend> RequestDispatcher for ServerDispatcher<'s, B> {
    type Subaction = <B::CustomApi as CustomApi>::Request;
    type Output = Response<CustomApiResult<B::CustomApi>>;
    type Error = Error;

    async fn handle_subaction(
        &self,
        permissions: &Permissions,
        subaction: Self::Subaction,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let dispatcher =
            <B::CustomApiDispatcher as CustomApiDispatcher<B>>::new(self.server, self.client);
        match dispatcher.dispatch(permissions, subaction).await {
            Ok(response) => Ok(Response::Api(Ok(response))),
            Err(err) => match err {
                BackendError::Backend(backend) => Ok(Response::Api(Err(backend))),
                BackendError::Server(server) => Err(server),
            },
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ServerHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        request: ServerRequest,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        ServerRequestDispatcher::dispatch_to_handlers(self, permissions, request).await
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::DatabaseHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        database_name: String,
        request: DatabaseRequest,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let database = self
            .server
            .database_without_schema_internal(&database_name)
            .await?;
        DatabaseDispatcher {
            name: database_name,
            database,
            server_dispatcher: self,
        }
        .dispatch(permissions, request)
        .await
    }
}

impl<'s, B: Backend> ServerRequestDispatcher for ServerDispatcher<'s, B> {
    type Output = Response<CustomApiResult<B::CustomApi>>;
    type Error = Error;
}

#[async_trait]
impl<'s, B: Backend> CreateDatabaseHandler for ServerDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        database: &'a bonsaidb_core::connection::Database,
        _only_if_needed: &'a bool,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(database_resource_name(&database.name))
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::CreateDatabase)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        database: bonsaidb_core::connection::Database,
        only_if_needed: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server
            .create_database_with_schema(&database.name, database.schema, only_if_needed)
            .await?;
        Ok(Response::Server(ServerResponse::DatabaseCreated {
            name: database.name.clone(),
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> DeleteDatabaseHandler for ServerDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self, database: &'a String) -> Result<ResourceName<'a>, Error> {
        Ok(database_resource_name(database))
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::DeleteDatabase)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        name: String,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server.delete_database(&name).await?;
        Ok(Response::Server(ServerResponse::DatabaseDeleted { name }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListDatabasesHandler for ServerDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self) -> Result<ResourceName<'a>, Error> {
        Ok(bonsaidb_resource_name())
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::ListDatabases)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::Databases(
            self.server.list_databases().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListAvailableSchemasHandler
    for ServerDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self) -> Result<ResourceName<'a>, Error> {
        Ok(bonsaidb_resource_name())
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::ListAvailableSchemas)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::AvailableSchemas(
            self.server.list_available_schemas().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CreateUserHandler for ServerDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self, _username: &'a String) -> Result<ResourceName<'a>, Error> {
        Ok(bonsaidb_resource_name())
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::CreateUser)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        username: String,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::UserCreated {
            id: self.server.create_user(&username).await?,
        }))
    }
}

#[cfg(feature = "password-hashing")]
#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::SetUserPasswordHandler for ServerDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        user: &'a NamedReference<'static>,
        _password: &'a bonsaidb_core::connection::SensitiveString,
    ) -> Result<ResourceName<'a>, Error> {
        let id = user
            .id::<User, _>(&self.server.admin().await)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;

        Ok(user_resource_name(id))
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::SetPassword)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static>,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server.set_user_password(username, password).await?;
        Ok(Response::Ok)
    }
}

#[async_trait]
#[cfg(feature = "password-hashing")]
impl<'s, B: Backend> bonsaidb_core::networking::AuthenticateHandler for ServerDispatcher<'s, B> {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        user: &NamedReference<'static>,
        authentication: &Authentication,
    ) -> Result<(), Error> {
        let id = user
            .id::<User, _>(&self.server.admin().await)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;
        match authentication {
            Authentication::Password(_) => {
                permissions.check(
                    user_resource_name(id),
                    &BonsaiAction::Server(ServerAction::Authenticate(
                        AuthenticationMethod::PasswordHash,
                    )),
                )?;
                Ok(())
            }
        }
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        username: NamedReference<'static>,
        authentication: Authentication,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let mut response = self
            .server
            .authenticate(username.clone(), authentication)
            .await?;

        // TODO this should be handled by the storage layer
        response.permissions = Permissions::merged([
            &response.permissions,
            &self.server.data.authenticated_permissions,
            &self.server.data.default_permissions,
        ]);

        self.client
            .logged_in_as(response.user_id, response.permissions.clone())
            .await;
        Ok(Response::Server(ServerResponse::Authenticated(response)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::AlterUserPermissionGroupMembershipHandler
    for ServerDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        user: &'a NamedReference<'static>,
        _group: &'a NamedReference<'static>,
        _should_be_member: &'a bool,
    ) -> Result<ResourceName<'a>, Error> {
        let id = user
            .id::<User, _>(&self.server.admin().await)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;

        Ok(user_resource_name(id))
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::ModifyUserPermissionGroups)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static>,
        group: NamedReference<'static>,
        should_be_member: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if should_be_member {
            self.server
                .add_permission_group_to_user(user, group)
                .await?;
        } else {
            self.server
                .remove_permission_group_from_user(user, group)
                .await?;
        }

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::AlterUserRoleMembershipHandler
    for ServerDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        user: &'a NamedReference<'static>,
        _role: &'a NamedReference<'static>,
        _should_be_member: &'a bool,
    ) -> Result<ResourceName<'a>, Error> {
        let id = user
            .id::<User, _>(&self.server.admin().await)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;

        Ok(user_resource_name(id))
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::ModifyUserRoles)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static>,
        role: NamedReference<'static>,
        should_be_member: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if should_be_member {
            self.server.add_role_to_user(user, role).await?;
        } else {
            self.server.remove_role_from_user(user, role).await?;
        }

        Ok(Response::Ok)
    }
}

#[derive(Dispatcher, Debug)]
#[dispatcher(input = DatabaseRequest, actionable = bonsaidb_core::actionable)]
struct DatabaseDispatcher<'s, B>
where
    B: Backend,
{
    name: String,
    database: Database,
    server_dispatcher: &'s ServerDispatcher<'s, B>,
}

impl<'s, B: Backend> DatabaseRequestDispatcher for DatabaseDispatcher<'s, B> {
    type Output = Response<CustomApiResult<B::CustomApi>>;
    type Error = Error;
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::GetHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        collection: &'a CollectionName,
        id: &'a DocumentId,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(document_resource_name(&self.name, collection, *id))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        id: DocumentId,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let document = self
            .database
            .internal_get_from_collection_id(id, &collection)
            .await?
            .ok_or_else(|| {
                Error::Core(bonsaidb_core::Error::DocumentNotFound(
                    collection,
                    Box::new(id),
                ))
            })?;
        Ok(Response::Database(DatabaseResponse::Documents(vec![
            document,
        ])))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::GetMultipleHandler for DatabaseDispatcher<'s, B> {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        collection: &CollectionName,
        ids: &Vec<DocumentId>,
    ) -> Result<(), Error> {
        for &id in ids {
            let document_name = document_resource_name(&self.name, collection, id);
            let action = BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get));
            permissions.check(&document_name, &action)?;
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Vec<DocumentId>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let documents = self
            .database
            .internal_get_multiple_from_collection_id(&ids, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        collection: &'a CollectionName,
        _ids: &'a Range<DocumentId>,
        _order: &'a Sort,
        _limit: &'a Option<usize>,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(collection_resource_name(&self.name, collection))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Document(DocumentAction::List))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let documents = self
            .database
            .list_from_collection(ids, order, limit, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::QueryHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        view: &'a ViewName,
        _key: &'a Option<QueryKey<Bytes>>,
        _order: &'a Sort,
        _limit: &'a Option<usize>,
        _access_policy: &'a AccessPolicy,
        _with_docs: &'a bool,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(view_resource_name(&self.name, view))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::View(ViewAction::Query))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if with_docs {
            let mappings = self
                .database
                .query_by_name_with_docs(&view, key, order, limit, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                mappings,
            )))
        } else {
            let mappings = self
                .database
                .query_by_name(&view, key, order, limit, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ReduceHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        view: &'a ViewName,
        _key: &'a Option<QueryKey<Bytes>>,
        _access_policy: &'a AccessPolicy,
        _grouped: &'a bool,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(view_resource_name(&self.name, view))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::View(ViewAction::Reduce))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
        grouped: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if grouped {
            let values = self
                .database
                .reduce_grouped_by_name(&view, key, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewGroupedReduction(
                values,
            )))
        } else {
            let value = self
                .database
                .reduce_by_name(&view, key, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewReduction(
                Bytes::from(value),
            )))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ApplyTransactionHandler
    for DatabaseDispatcher<'s, B>
{
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        transaction: &Transaction,
    ) -> Result<(), Error> {
        for op in &transaction.operations {
            let (resource, action) = match &op.command {
                Command::Insert { .. } => (
                    collection_resource_name(&self.name, &op.collection),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Insert)),
                ),
                Command::Update { header, .. } => (
                    document_resource_name(&self.name, &op.collection, header.id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Update)),
                ),
                Command::Overwrite { id, .. } => (
                    document_resource_name(&self.name, &op.collection, *id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Overwrite)),
                ),
                Command::Delete { header } => (
                    document_resource_name(&self.name, &op.collection, header.id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Delete)),
                ),
            };
            permissions.check(&resource, &action)?;
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        transaction: Transaction,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let results = self.database.apply_transaction(transaction).await?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::DeleteDocsHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        view: &'a ViewName,
        _key: &'a Option<QueryKey<Bytes>>,
        _access_policy: &'a AccessPolicy,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(view_resource_name(&self.name, view))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::View(ViewAction::DeleteDocs))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let count = self
            .database
            .delete_docs_by_name(&view, key, access_policy)
            .await?;
        Ok(Response::Database(DatabaseResponse::DocumentsDeleted(
            count,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ListExecutedTransactionsHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        _starting_id: &'a Option<u64>,
        _result_limit: &'a Option<usize>,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(database_resource_name(&self.name))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Transaction(TransactionAction::ListExecuted))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            self.database
                .list_executed_transactions(starting_id, result_limit)
                .await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::LastTransactionIdHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self) -> Result<ResourceName<'a>, Error> {
        Ok(database_resource_name(&self.name))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Transaction(TransactionAction::GetLastId))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            self.database.last_transaction_id().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CreateSubscriberHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self) -> Result<ResourceName<'a>, Error> {
        Ok(database_resource_name(&self.name))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::CreateSuscriber))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let server = self.server_dispatcher.server;
        let subscriber = server.create_subscriber(self.name.clone()).await;
        let subscriber_id = subscriber.id;
        self.server_dispatcher
            .client
            .register_subscriber(subscriber_id)
            .await;

        let task_self = server.clone();
        let response_sender = self.server_dispatcher.response_sender.clone();
        tokio::spawn(async move {
            task_self
                .forward_notifications_for(
                    subscriber.id,
                    subscriber.receiver,
                    response_sender.clone(),
                )
                .await;
        });
        Ok(Response::Database(DatabaseResponse::SubscriberCreated {
            subscriber_id,
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::PublishHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        topic: &'a String,
        _payload: &'a Bytes,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(pubsub_topic_resource_name(&self.name, topic))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::Publish))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        topic: String,
        payload: Bytes,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server_dispatcher
            .server
            .publish_message(&self.name, &topic, payload.into_vec())
            .await;
        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::PublishToAllHandler for DatabaseDispatcher<'s, B> {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        topics: &Vec<String>,
        _payload: &Bytes,
    ) -> Result<(), Error> {
        for topic in topics {
            let topic_name = pubsub_topic_resource_name(&self.name, topic);
            let action = BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::Publish));
            if !permissions.allowed_to(&topic_name, &action) {
                return Err(Error::from(PermissionDenied {
                    resource: topic_name.to_owned(),
                    action: action.name(),
                }));
            }
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        topics: Vec<String>,
        payload: Bytes,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server_dispatcher
            .server
            .publish_serialized_to_all(&self.name, &topics, payload.into_vec())
            .await;
        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::SubscribeToHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        _subscriber_id: &'a u64,
        topic: &'a String,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(pubsub_topic_resource_name(&self.name, topic))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::SubscribeTo))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if self
            .server_dispatcher
            .client
            .owns_subscriber(subscriber_id)
            .await
        {
            self.server_dispatcher
                .server
                .subscribe_to(subscriber_id, &self.name, topic)
                .await
                .map(|_| Response::Ok)
                .map_err(Error::from)
        } else {
            Err(Error::Transport(String::from("invalid subscriber_id")))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::UnsubscribeFromHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        _subscriber_id: &'a u64,
        topic: &'a String,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(pubsub_topic_resource_name(&self.name, topic))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::PubSub(PubSubAction::UnsubscribeFrom))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if self
            .server_dispatcher
            .client
            .owns_subscriber(subscriber_id)
            .await
        {
            self.server_dispatcher
                .server
                .unsubscribe_from(subscriber_id, &self.name, &topic)
                .await
                .map(|_| Response::Ok)
                .map_err(Error::from)
        } else {
            Err(Error::Transport(String::from("invalid subscriber_id")))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::UnregisterSubscriberHandler
    for DatabaseDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if self
            .server_dispatcher
            .client
            .remove_subscriber(subscriber_id)
            .await
        {
            let mut subscribers = fast_async_write!(self.server_dispatcher.subscribers);
            if subscribers.remove(&subscriber_id).is_none() {
                Ok(Response::Error(bonsaidb_core::Error::Server(String::from(
                    "invalid subscriber id",
                ))))
            } else {
                Ok(Response::Ok)
            }
        } else {
            Err(Error::Transport(String::from("invalid subscriber_id")))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ExecuteKeyOperationHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self, op: &'a KeyOperation) -> Result<ResourceName<'a>, Error> {
        Ok(keyvalue_key_resource_name(
            &self.name,
            op.namespace.as_deref(),
            &op.key,
        ))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::KeyValue(KeyValueAction::ExecuteOperation))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        op: KeyOperation,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let result = self.database.execute_key_operation(op).await?;
        Ok(Response::Database(DatabaseResponse::KvOutput(result)))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CompactCollectionHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(
        &'a self,
        collection: &'a CollectionName,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(collection_resource_name(&self.name, collection))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Compact)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.database.compact_collection_by_name(collection).await?;

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CompactKeyValueStoreHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self) -> Result<ResourceName<'a>, Error> {
        Ok(kv_resource_name(&self.name))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Compact)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.database.compact_key_value_store().await?;

        Ok(Response::Ok)
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::CompactHandler for DatabaseDispatcher<'s, B> {
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self) -> Result<ResourceName<'a>, Error> {
        Ok(database_resource_name(&self.name))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Compact)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.database.compact().await?;

        Ok(Response::Ok)
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
