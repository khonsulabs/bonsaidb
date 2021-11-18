use std::{
    collections::{hash_map, HashMap},
    fmt::Debug,
    marker::PhantomData,
    net::SocketAddr,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use bonsaidb_core::{
    admin::{self, User},
    circulate::{Message, Relay, Subscriber},
    connection::{AccessPolicy, Connection, QueryKey, Range, ServerConnection, Sort},
    custodian_password::{
        LoginFinalization, LoginRequest, RegistrationFinalization, RegistrationRequest,
    },
    custom_api::{CustomApi, CustomApiResult},
    kv::KeyOperation,
    networking::{
        self, CreateDatabaseHandler, DatabaseRequest, DatabaseRequestDispatcher, DatabaseResponse,
        DeleteDatabaseHandler, Payload, Request, RequestDispatcher, Response, ServerRequest,
        ServerRequestDispatcher, ServerResponse,
    },
    permissions::{
        bonsai::{
            bonsaidb_resource_name, collection_resource_name, database_resource_name,
            document_resource_name, kv_key_resource_name, kv_resource_name,
            pubsub_topic_resource_name, user_resource_name, view_resource_name, BonsaiAction,
            DatabaseAction, DocumentAction, KvAction, PubSubAction, ServerAction,
            TransactionAction, ViewAction,
        },
        Action, Dispatcher, PermissionDenied, Permissions, ResourceName,
    },
    pubsub::database_topic,
    schema::{self, Collection, CollectionName, NamedCollection, NamedReference, Schema, ViewName},
    transaction::{Command, Transaction},
};
use bonsaidb_local::{
    jobs::{manager::Manager, Job},
    OpenDatabase, Storage,
};
use fabruic::{self, CertificateChain, Endpoint, KeyPair, PrivateKey};
use flume::Sender;
use futures::{Future, StreamExt};
use rustls::sign::CertifiedKey;
use schema::SchemaName;
use signal_hook::{
    consts::{SIGINT, SIGQUIT},
    iterator::Signals,
};
use tokio::sync::{Mutex, RwLock};

#[cfg(feature = "acme")]
use crate::config::AcmeConfiguration;
use crate::{
    backend::{BackendError, ConnectionHandling, CustomApiDispatcher},
    error::Error,
    hosted::{Hosted, SerializablePrivateKey, TlsCertificate, TlsCertificatesByDomain},
    Backend, Configuration,
};

#[cfg(feature = "acme")]
pub mod acme;
mod connected_client;
mod database;

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

/// A `BonsaiDb` server.
#[derive(Debug)]
pub struct CustomServer<B: Backend> {
    data: Arc<Data<B>>,
}

/// A `BonsaiDb` server without a custom backend.
pub type Server = CustomServer<()>;

impl<B: Backend> Clone for CustomServer<B> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

#[derive(Debug)]
struct Data<B: Backend = ()> {
    directory: PathBuf,
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
    tcp_shutdown: RwLock<Option<Sender<()>>>,
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
    pub async fn open(directory: &Path, configuration: Configuration) -> Result<Self, Error> {
        let request_processor = Manager::default();
        for _ in 0..configuration.request_workers {
            request_processor.spawn_worker();
        }

        let storage = Storage::open_local(directory, configuration.storage).await?;

        storage.register_schema::<Hosted>().await?;
        storage.create_database::<Hosted>("_hosted", true).await?;

        let default_permissions = Permissions::from(configuration.default_permissions);
        let authenticated_permissions = Permissions::from(configuration.authenticated_permissions);

        let server = Self {
            data: Arc::new(Data {
                clients: RwLock::default(),
                storage,
                directory: directory.to_owned(),
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
                tcp_shutdown: RwLock::default(),
                relay: Relay::default(),
                subscribers: Arc::default(),
                _backend: PhantomData::default(),
            }),
        };
        B::initialize(&server).await;
        Ok(server)
    }

    /// Returns the path to the directory that stores this server's data.
    #[must_use]
    pub fn directory(&self) -> &'_ PathBuf {
        &self.data.directory
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

    pub(crate) async fn database_without_schema(
        &self,
        name: &str,
    ) -> Result<Box<dyn OpenDatabase>, Error> {
        let db = self.data.storage.database_without_schema(name).await?;
        Ok(db)
    }

    /// Installs an X.509 certificate used for general purpose connections.
    pub async fn install_self_signed_certificate(&self, overwrite: bool) -> Result<(), Error> {
        let keypair = KeyPair::new_self_signed(&self.data.primary_domain);

        if self.certificate_chain().await.is_ok() && !overwrite {
            return Err(Error::Core(bonsaidb_core::Error::Configuration(String::from("Certificate already installed. Enable overwrite if you wish to replace the existing certificate."))));
        }

        self.install_certificate(keypair.certificate_chain(), keypair.private_key())
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
        use fabruic::Certificate;

        let private_key = match pem::parse(private_key) {
            Ok(pem) => PrivateKey::unchecked_from_der(pem.contents),
            Err(_) => PrivateKey::from_der(private_key)?,
        };
        let certificates = pem::parse_many(&certificate_chain)?
            .into_iter()
            .map(|entry| Certificate::unchecked_from_der(entry.contents))
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
        if let Some(mut existing_record) =
            TlsCertificate::load(&self.data.primary_domain, &db).await?
        {
            existing_record.contents.certificate_chain = certificate_chain.clone();
            existing_record.contents.private_key = SerializablePrivateKey(private_key.clone());
            existing_record.update(&db).await?;
        } else {
            TlsCertificate {
                domains: vec![self.data.primary_domain.clone()],
                private_key: SerializablePrivateKey(private_key.clone()),
                certificate_chain: certificate_chain.clone(),
            }
            .insert_into(&db)
            .await?;
        }

        self.refresh_certified_key().await?;

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
        let certificate = db
            .view::<TlsCertificatesByDomain>()
            .with_key(self.data.primary_domain.clone())
            .query_with_docs()
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                Error::Core(bonsaidb_core::Error::Configuration(format!(
                    "no certificate found for {}",
                    self.data.primary_domain
                )))
            })?;
        Ok(certificate.document.contents()?)
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

        let mut server = Endpoint::new_server(port, keypair)?;
        {
            let mut endpoint = self.data.endpoint.write().await;
            *endpoint = Some(server.clone());
        }

        while let Some(result) = server.next().await {
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
        let clients = self.data.clients.read().await;
        clients.values().cloned().collect()
    }

    /// Sends a custom API response to all connected clients.
    pub async fn broadcast(&self, response: CustomApiResult<B::CustomApi>) {
        let clients = self.data.clients.read().await;
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
            let mut clients = self.data.clients.write().await;
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
            let mut clients = self.data.clients.write().await;
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
                .await
            {
                Ok((sender, receiver)) => {
                    let (api_response_sender, api_response_receiver) = flume::unbounded();
                    if let Some(disconnector) = self.initialize_client(Transport::Bonsai, connection.remote_address(), api_response_sender).await {
                        let task_sender = sender.clone();
                        tokio::spawn(async move {
                            while let Ok(response) = api_response_receiver.recv_async().await {
                                if task_sender.send(&Payload {
                                    id: None,
                                    wrapped: Response::Api(response)
                                }).is_err() {
                                    break;
                                }
                            }
                        });

                        let task_self = self.clone();
                        tokio::spawn(async move {
                            if let Err(err) = task_self.handle_stream(disconnector, sender, receiver).await {
                                log::error!("[server] Error handling stream: {:?}", err);
                            }
                        });
                    } else {
                        log::error!("[server] Backend rejected connection.");
                        return Ok(())
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
        let (request_completion_sender, request_completion_receiver) = flume::unbounded::<()>();
        let requests_in_queue = Arc::new(AtomicUsize::new(0));
        loop {
            let current_requests = requests_in_queue.load(Ordering::SeqCst);
            if current_requests == self.data.client_simultaneous_request_limit {
                // Wait for requests to finish.
                let _ = request_completion_receiver.recv_async().await;
                // Clear the queue
                while request_completion_receiver.try_recv().is_ok() {}
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

                let request_completion_sender = request_completion_sender.clone();
                let requests_in_queue = requests_in_queue.clone();
                self.handle_request_through_worker(
                    payload.wrapped,
                    move |response| async move {
                        drop(task_sender.send(Payload {
                            id,
                            wrapped: response,
                        }));

                        requests_in_queue.fetch_sub(1, Ordering::SeqCst);

                        let _ = request_completion_sender.send(());

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
                        payload: message.payload.clone(),
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
        let endpoint = {
            let endpoint = self.data.endpoint.read().await;
            endpoint.clone()
        };

        if let Some(server) = endpoint {
            if let Some(timeout) = timeout {
                server.close_incoming().await?;

                if tokio::time::timeout(timeout, server.wait_idle())
                    .await
                    .is_err()
                {
                    server.close().await;
                }
            } else {
                server.close().await;
            }
        }

        Ok(())
    }

    /// Listens for signals from the operating system that the server should
    /// shut down and attempts to gracefully shut down.
    pub async fn listen_for_shutdown(&self) -> Result<(), Error> {
        let shutdown_state = Arc::new(Mutex::new(ShutdownState::Running));
        match Signals::new(&[SIGINT, SIGQUIT]) {
            Ok(mut signals) => {
                'outer: loop {
                    for signal in signals.pending() {
                        match signal {
                            SIGINT => {
                                let mut state = shutdown_state.lock().await;
                                match *state {
                                    ShutdownState::Running => {
                                        log::error!(
                                            "Interrupt signal received. Shutting down gracefully."
                                        );
                                        let task_server = self.clone();
                                        let (shutdown_sender, shutdown_receiver) =
                                            flume::bounded(1);
                                        tokio::task::spawn(async move {
                                            task_server
                                                .shutdown(Some(Duration::from_secs(30)))
                                                .await?;
                                            let _ = shutdown_sender.send(());
                                            Result::<(), Error>::Ok(())
                                        });
                                        *state = ShutdownState::ShuttingDown(shutdown_receiver);
                                    }
                                    ShutdownState::ShuttingDown(_) => {
                                        // Two interrupts, go ahead and force the shutdown
                                        break 'outer;
                                    }
                                }
                            }
                            SIGQUIT => {
                                log::error!("Quit signal received. Shutting down.");
                                break 'outer;
                            }
                            _ => unreachable!(),
                        }
                    }

                    let state = shutdown_state.lock().await;
                    if let ShutdownState::ShuttingDown(receiver) = &*state {
                        if receiver.try_recv().is_ok() {
                            // Fully shut down.
                            return Ok(());
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                self.shutdown(None).await?;
            }
            Err(err) => {
                log::error!("Error installing signals for graceful shutdown: {:?}", err);
                tokio::time::sleep(Duration::MAX).await;
            }
        }

        Ok(())
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

        let mut subscribers = self.data.subscribers.write().await;
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
        let subscribers = self.data.subscribers.read().await;
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
        let subscribers = self.data.subscribers.read().await;
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
impl<B: Backend> ServerConnection for CustomServer<B> {
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

    async fn list_databases(&self) -> Result<Vec<admin::Database>, bonsaidb_core::Error> {
        self.data.storage.list_databases().await
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        self.data.storage.list_available_schemas().await
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        self.data.storage.create_user(username).await
    }

    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_request: RegistrationRequest,
    ) -> Result<bonsaidb_core::custodian_password::RegistrationResponse, bonsaidb_core::Error> {
        self.data
            .storage
            .set_user_password(user, password_request)
            .await
    }

    async fn finish_set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_finalization: RegistrationFinalization,
    ) -> Result<(), bonsaidb_core::Error> {
        self.data
            .storage
            .finish_set_user_password(user, password_finalization)
            .await
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
#[dispatcher(input = Request<<B::CustomApi as CustomApi>::Request>, input = ServerRequest)]
struct ServerDispatcher<'s, B: Backend> {
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
        let database = self.server.database_without_schema(&database_name).await?;
        DatabaseDispatcher {
            name: database_name,
            database: database.as_ref(),
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
        database: &'a bonsaidb_core::admin::Database,
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
        database: bonsaidb_core::admin::Database,
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

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::LoginWithPasswordHandler
    for ServerDispatcher<'s, B>
{
    type Action = BonsaiAction;
    async fn resource_name<'a>(
        &'a self,
        username: &'a String,
        _password_request: &'a LoginRequest,
    ) -> Result<ResourceName<'a>, Error> {
        let id = NamedReference::from(username.as_str())
            .id::<User, _>(&self.server.admin().await)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;

        Ok(user_resource_name(id))
    }

    fn action() -> Self::Action {
        BonsaiAction::Server(ServerAction::LoginWithPassword)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        username: String,
        password_request: LoginRequest,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let response = self
            .client
            .initiate_login(&username, password_request, self.server)
            .await?;
        Ok(Response::Server(ServerResponse::PasswordLoginResponse {
            response: Box::new(response),
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::FinishPasswordLoginHandler
    for ServerDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        password_request: LoginFinalization,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if let Some((user_id, login)) = self.client.take_pending_password_login().await {
            login.finish(password_request)?;
            let user_id = user_id.expect("logged in without a user_id");
            let admin = self.server.data.storage.admin().await;
            let user = User::get(user_id, &admin)
                .await?
                .ok_or(bonsaidb_core::Error::UserNotFound)?;

            let permissions = user.contents.effective_permissions(&admin).await?;
            let permissions = Permissions::merged(
                [&permissions, &self.server.data.authenticated_permissions].into_iter(),
            );
            self.client.logged_in_as(user_id, permissions.clone()).await;

            Ok(Response::Server(ServerResponse::LoggedIn { permissions }))
        } else {
            // TODO make this a real error
            Err(Error::from(bonsaidb_core::Error::Server(String::from(
                "no login state found",
            ))))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::SetPasswordHandler for ServerDispatcher<'s, B> {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        user: &NamedReference<'static>,
        _password_request: &RegistrationRequest,
    ) -> Result<(), Error> {
        let id = user
            .id::<User, _>(&self.server.admin().await)
            .await?
            .ok_or(bonsaidb_core::Error::UserNotFound)?;

        if self.client.user_id().await == Some(id) {
            // Users can always set their own password
            Ok(())
        } else {
            let user_resource_id = user_resource_name(id);
            if permissions.allowed_to(
                &user_resource_id,
                &BonsaiAction::Server(ServerAction::SetPassword),
            ) {
                Ok(())
            } else {
                Err(Error::from(PermissionDenied {
                    resource: user_resource_id.to_owned(),
                    action: BonsaiAction::Server(ServerAction::SetPassword).name(),
                }))
            }
        }
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static>,
        password_request: RegistrationRequest,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        Ok(Response::Server(ServerResponse::FinishSetPassword {
            password_reponse: Box::new(
                self.server
                    .set_user_password(user, password_request)
                    .await?,
            ),
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::FinishSetPasswordHandler
    for ServerDispatcher<'s, B>
{
    async fn handle(
        &self,
        _permissions: &Permissions,
        user: NamedReference<'static>,
        password_request: RegistrationFinalization,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server
            .finish_set_user_password(user, password_request)
            .await?;
        Ok(Response::Ok)
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
#[dispatcher(input = DatabaseRequest)]
struct DatabaseDispatcher<'s, B>
where
    B: Backend,
{
    name: String,
    database: &'s dyn OpenDatabase,
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
        id: &'a u64,
    ) -> Result<ResourceName<'a>, Error> {
        Ok(document_resource_name(&self.name, collection, *id))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get))
    }

    async fn handle_protected(
        &self,
        permissions: &Permissions,
        collection: CollectionName,
        id: u64,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let document = self
            .database
            .get_from_collection_id(id, &collection, permissions)
            .await?
            .ok_or(Error::Core(bonsaidb_core::Error::DocumentNotFound(
                collection, id,
            )))?;
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
        ids: &Vec<u64>,
    ) -> Result<(), Error> {
        for &id in ids {
            let document_name = document_resource_name(&self.name, collection, id);
            let action = BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Get));
            if !permissions.allowed_to(&document_name, &action) {
                return Err(Error::from(PermissionDenied {
                    resource: document_name.to_owned(),
                    action: action.name(),
                }));
            }
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        permissions: &Permissions,
        collection: CollectionName,
        ids: Vec<u64>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let documents = self
            .database
            .get_multiple_from_collection_id(&ids, &collection, permissions)
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
        _ids: &'a Range<u64>,
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
        permissions: &Permissions,
        collection: CollectionName,
        ids: Range<u64>,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let documents = self
            .database
            .list_from_collection(ids, order, limit, &collection, permissions)
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
        _key: &'a Option<QueryKey<Vec<u8>>>,
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
        permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if with_docs {
            let mappings = self
                .database
                .query_with_docs(&view, key, order, limit, access_policy, permissions)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                mappings,
            )))
        } else {
            let mappings = self
                .database
                .query(&view, key, order, limit, access_policy)
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
        _key: &'a Option<QueryKey<Vec<u8>>>,
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
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        grouped: bool,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        if grouped {
            let values = self
                .database
                .reduce_grouped(&view, key, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewGroupedReduction(
                values,
            )))
        } else {
            let value = self.database.reduce(&view, key, access_policy).await?;
            Ok(Response::Database(DatabaseResponse::ViewReduction(value)))
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
        transaction: &Transaction<'static>,
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
                Command::Delete { header } => (
                    document_resource_name(&self.name, &op.collection, header.id),
                    BonsaiAction::Database(DatabaseAction::Document(DocumentAction::Delete)),
                ),
            };
            if !permissions.allowed_to(&resource, &action) {
                return Err(Error::from(PermissionDenied {
                    resource: resource.to_owned(),
                    action: action.name(),
                }));
            }
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        permissions: &Permissions,
        transaction: Transaction<'static>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        let results = self
            .database
            .apply_transaction(transaction, permissions)
            .await?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
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
        _payload: &'a Vec<u8>,
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
        payload: Vec<u8>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server_dispatcher
            .server
            .publish_message(&self.name, &topic, payload)
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
        _payload: &Vec<u8>,
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
        payload: Vec<u8>,
    ) -> Result<Response<CustomApiResult<B::CustomApi>>, Error> {
        self.server_dispatcher
            .server
            .publish_serialized_to_all(&self.name, &topics, payload)
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
        self.server_dispatcher
            .server
            .subscribe_to(subscriber_id, &self.name, topic)
            .await
            .map(|_| Response::Ok)
            .map_err(Error::from)
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
        self.server_dispatcher
            .server
            .unsubscribe_from(subscriber_id, &self.name, &topic)
            .await
            .map(|_| Response::Ok)
            .map_err(Error::from)
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
        let mut subscribers = self.server_dispatcher.subscribers.write().await;
        if subscribers.remove(&subscriber_id).is_none() {
            Ok(Response::Error(bonsaidb_core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        } else {
            Ok(Response::Ok)
        }
    }
}

#[async_trait]
impl<'s, B: Backend> bonsaidb_core::networking::ExecuteKeyOperationHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = BonsaiAction;

    async fn resource_name<'a>(&'a self, op: &'a KeyOperation) -> Result<ResourceName<'a>, Error> {
        Ok(kv_key_resource_name(
            &self.name,
            op.namespace.as_deref(),
            &op.key,
        ))
    }

    fn action() -> Self::Action {
        BonsaiAction::Database(DatabaseAction::Kv(KvAction::ExecuteOperation))
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
        self.database.compact_collection(collection).await?;

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

enum ShutdownState {
    Running,
    ShuttingDown(flume::Receiver<()>),
}
