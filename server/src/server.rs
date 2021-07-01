use std::{
    collections::{hash_map, HashMap},
    fmt::Debug,
    marker::PhantomData,
    net::SocketAddr,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use cfg_if::cfg_if;
use custodian_password::{LoginRequest, RegistrationRequest};
use fabruic::{self, Certificate, CertificateChain, Endpoint, KeyPair, PrivateKey};
use flume::Sender;
#[cfg(feature = "websockets")]
use futures::SinkExt;
use futures::{Future, StreamExt, TryFutureExt};
#[cfg(feature = "pubsub")]
use pliantdb_core::{
    circulate::{Message, Relay, Subscriber},
    pubsub::database_topic,
};
use pliantdb_core::{
    connection::{self, AccessPolicy, QueryKey, ServerConnection},
    custom_api::CustomApi,
    kv::KeyOperation,
    networking::{
        self, CreateDatabaseHandler, DatabaseRequest, DatabaseRequestDispatcher, DatabaseResponse,
        DeleteDatabaseHandler, Payload, Request, RequestDispatcher, Response, ServerRequest,
        ServerRequestDispatcher, ServerResponse,
    },
    permissions::{
        pliant::{
            collection_resource_name, database_resource_name, document_resource_name,
            kv_key_resource_name, pliantdb_resource_name, pubsub_topic_resource_name,
            view_resource_name, DatabaseAction, DocumentAction, KvAction, PliantAction,
            PubSubAction, ServerAction, TransactionAction, ViewAction,
        },
        Action, Dispatcher, PermissionDenied, Permissions, ResourceName,
    },
    schema,
    schema::{CollectionName, Schema, ViewName},
    transaction::{Command, Transaction},
};
use pliantdb_jobs::{manager::Manager, Job};
use pliantdb_local::{OpenDatabase, Storage};
use schema::SchemaName;
#[cfg(feature = "websockets")]
use tokio::net::TcpListener;
use tokio::{fs::File, sync::RwLock};

use crate::{
    admin::Admin, async_io_util::FileExt, backend::ConnectionHandling, error::Error, Backend,
    Configuration,
};

mod connected_client;
mod database;
use self::connected_client::OwnedClient;
#[cfg(feature = "pubsub")]
pub use self::database::ServerSubscriber;
pub use self::{
    connected_client::{ConnectedClient, Transport},
    database::ServerDatabase,
};

static CONNECTED_CLIENT_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// A `PliantDb` server.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct CustomServer<B: Backend> {
    data: Arc<Data<B>>,
}

/// A `PliantDb` server without a custom bakend.
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
    default_permissions: Arc<Permissions>,
    custom_api: RwLock<Option<B::CustomApiDispatcher>>,
    endpoint: RwLock<Option<Endpoint>>,
    #[cfg(feature = "websockets")]
    websocket_shutdown: RwLock<Option<Sender<()>>>,
    #[cfg(feature = "pubsub")]
    relay: Relay,
    #[cfg(feature = "pubsub")]
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    _backend: PhantomData<B>,
}

impl<B: Backend> CustomServer<B> {
    /// Opens a server using `directory` for storage.
    pub async fn open(directory: &Path, configuration: Configuration) -> Result<Self, Error> {
        let request_processor = Manager::default();
        for _ in 0..configuration.request_workers {
            request_processor.spawn_worker();
        }

        let storage = Storage::open_local(directory, configuration.storage).await?;

        Ok(Self {
            data: Arc::new(Data {
                clients: RwLock::default(),
                storage,
                directory: directory.to_owned(),
                endpoint: RwLock::default(),
                #[cfg(feature = "websockets")]
                websocket_shutdown: RwLock::default(),
                request_processor,
                default_permissions: Arc::new(configuration.default_permissions),
                custom_api: RwLock::default(),
                #[cfg(feature = "pubsub")]
                relay: Relay::default(),
                #[cfg(feature = "pubsub")]
                subscribers: Arc::default(),
                _backend: PhantomData::default(),
            }),
        })
    }

    pub(crate) async fn admin(&self) -> Result<ServerDatabase<'_, B, Admin>, Error> {
        self.database("admin").await
    }

    /// Returns the path to the directory that stores this server's data.
    #[must_use]
    pub fn directory(&self) -> &'_ PathBuf {
        &self.data.directory
    }

    /// Opens a server using `directory` for storage.
    pub async fn set_custom_api_dispatcher(&self, dispatcher: B::CustomApiDispatcher) {
        let mut server_dispatcher = self.data.custom_api.write().await;
        *server_dispatcher = Some(dispatcher);
    }

    /// Retrieves a database. This function only verifies that the database exists.
    pub async fn database<DB: Schema>(
        &self,
        name: &'_ str,
    ) -> Result<ServerDatabase<'_, B, DB>, Error> {
        let db = self.data.storage.database(name).await?;
        Ok(ServerDatabase { server: self, db })
    }

    pub(crate) async fn database_without_schema(
        &self,
        name: &'_ str,
    ) -> Result<Box<dyn OpenDatabase>, Error> {
        let db = self.data.storage.database_without_schema(name).await?;
        Ok(db)
    }

    /// Installs an X.509 certificate used for general purpose connections.
    pub async fn install_self_signed_certificate(
        &self,
        server_name: &str,
        overwrite: bool,
    ) -> Result<(), Error> {
        let keypair = KeyPair::new_self_signed(server_name);

        if self.certificate_path().exists() && !overwrite {
            return Err(Error::Core(pliantdb_core::Error::Configuration(String::from("Certificate already installed. Enable overwrite if you wish to replace the existing certificate."))));
        }

        self.install_certificate(keypair.end_entity_certificate(), keypair.private_key())
            .await?;

        Ok(())
    }

    /// Installs an X.509 certificate used for general purpose connections.
    /// These currently must be in DER binary format, not ASCII PEM format.
    pub async fn install_certificate(
        &self,
        certificate: &Certificate,
        private_key: &PrivateKey,
    ) -> Result<(), Error> {
        File::create(self.certificate_path())
            .and_then(|file| file.write_all(certificate.as_ref()))
            .await
            .map_err(|err| {
                Error::Core(pliantdb_core::Error::Configuration(format!(
                    "Error writing certificate file: {}",
                    err
                )))
            })?;
        File::create(self.private_key_path())
            .and_then(|file| file.write_all(fabruic::dangerous::PrivateKey::as_ref(private_key)))
            .await
            .map_err(|err| {
                Error::Core(pliantdb_core::Error::Configuration(format!(
                    "Error writing private key file: {}",
                    err
                )))
            })?;

        Ok(())
    }

    fn certificate_path(&self) -> PathBuf {
        self.data.directory.join("public-certificate.der")
    }

    /// Returns the current certificate.
    pub async fn certificate(&self) -> Result<Certificate, Error> {
        Ok(File::open(self.certificate_path())
            .and_then(FileExt::read_all)
            .await
            .map(Certificate::unchecked_from_der)
            .map_err(|err| {
                Error::Core(pliantdb_core::Error::Configuration(format!(
                    "Error reading certificate file: {}",
                    err
                )))
            })?)
    }

    fn private_key_path(&self) -> PathBuf {
        self.data.directory.join("private-key.der")
    }

    /// Listens for incoming client connections. Does not return until the
    /// server shuts down.
    pub async fn listen_on(&self, port: u16) -> Result<(), Error> {
        let certificate = self.certificate().await?;
        let private_key = File::open(self.private_key_path())
            .and_then(FileExt::read_all)
            .await
            .map(PrivateKey::from_der)
            .map_err(|err| {
                Error::Core(pliantdb_core::Error::Configuration(format!(
                    "Error reading private key file: {}",
                    err
                )))
            })??;
        let certchain = CertificateChain::from_certificates(vec![certificate])?;
        let keypair = KeyPair::from_parts(certchain, private_key)?;

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
                if let Err(err) = task_self.handle_pliant_connection(connection).await {
                    eprintln!("[server] closing connection {}: {:?}", address, err);
                }
            });
        }

        Ok(())
    }

    /// Listens for `WebSocket` traffic on `port`.
    #[cfg(feature = "websockets")]
    pub async fn listen_for_websockets_on<T: tokio::net::ToSocketAddrs + Send + Sync>(
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
                        if let Err(err) = task_self.handle_websocket_connection(connection).await {
                            eprintln!("[server] closing connection {}: {:?}", remote_addr, err);
                        }
                    });
                }
            }
        }

        Ok(())
    }

    async fn initialize_client(
        &self,
        transport: Transport,
        address: SocketAddr,
        sender: Sender<<B::CustomApi as CustomApi>::Response>,
    ) -> Option<OwnedClient<B>> {
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
            B::client_connected(&client).await,
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
            B::client_disconnected(client).await
        }
    }

    async fn handle_pliant_connection(
        &self,
        mut connection: fabruic::Connection<()>,
    ) -> Result<(), Error> {
        if let Some(incoming) = connection.next().await {
            let incoming = match incoming {
                Ok(incoming) => incoming,
                Err(err) => {
                    eprintln!("[server] Error establishing a stream: {:?}", err);
                    return Ok(());
                }
            };

            match incoming
                .accept::<networking::Payload<Response<<B::CustomApi as CustomApi>::Response>>, networking::Payload<Request<<B::CustomApi as CustomApi>::Request>>>()
                .await
            {
                Ok((sender, receiver)) => {
                    let (api_response_sender, api_response_receiver) = flume::unbounded();
                    if let Some(disconnector) = self.initialize_client(Transport::Pliant, connection.remote_address(), api_response_sender).await {
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
                        tokio::spawn(async move { task_self.handle_stream(disconnector, sender, receiver).await });
                    } else {
                        eprintln!("[server] Backend rejected connection.");
                        return Ok(())
                    }
                }
                Err(err) => {
                    eprintln!("[server] Error accepting incoming stream: {:?}", err);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "websockets")]
    async fn handle_websocket_connection(
        &self,
        connection: tokio::net::TcpStream,
    ) -> Result<(), Error> {
        use tokio_tungstenite::tungstenite::Message;
        let address = connection.peer_addr()?;
        let stream = tokio_tungstenite::accept_async(connection).await?;
        let (mut sender, mut receiver) = stream.split();
        let (response_sender, response_receiver) = flume::unbounded();
        let (message_sender, message_receiver) = flume::unbounded();

        let (api_response_sender, api_response_receiver) = flume::unbounded();
        let client = if let Some(client) = self
            .initialize_client(Transport::WebSocket, address, api_response_sender)
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

            Result::<(), anyhow::Error>::Ok(())
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

            Result::<(), anyhow::Error>::Ok(())
        });

        while let Some(payload) = receiver.next().await {
            match payload? {
                Message::Binary(binary) => {
                    let payload = bincode::deserialize::<
                        Payload<Request<<B::CustomApi as CustomApi>::Request>>,
                    >(&binary)?;
                    let id = payload.id;
                    let task_sender = response_sender.clone();
                    self.handle_request_through_worker(
                        payload.wrapped,
                        move |response| async move {
                            drop(task_sender.send(Payload {
                                id,
                                wrapped: response,
                            }));

                            Ok(())
                        },
                        client.permissions().await,
                        #[cfg(feature = "pubsub")]
                        self.data.subscribers.clone(),
                        #[cfg(feature = "pubsub")]
                        response_sender.clone(),
                    )
                    .await?;
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

    async fn handle_request_through_worker<
        F: FnOnce(Response<<B::CustomApi as CustomApi>::Response>) -> R + Send + 'static,
        R: Future<Output = Result<(), Error>> + Send,
    >(
        &self,
        request: Request<<B::CustomApi as CustomApi>::Request>,
        callback: F,
        permissions: Arc<Permissions>,
        #[cfg(feature = "pubsub")] subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        #[cfg(feature = "pubsub")] response_sender: flume::Sender<
            Payload<Response<<B::CustomApi as CustomApi>::Response>>,
        >,
    ) -> Result<(), Error> {
        let job = self
            .data
            .request_processor
            .enqueue(ClientRequest::<B>::new(
                request,
                self.clone(),
                permissions,
                #[cfg(feature = "pubsub")]
                subscribers,
                #[cfg(feature = "pubsub")]
                response_sender,
            ))
            .await;
        tokio::spawn(async move {
            let result = job
                .receive()
                .await
                .map_err(|_| Error::Request(Arc::new(anyhow::anyhow!("background job aborted"))))?
                .map_err(Error::Request)?;
            callback(result).await?;
            Result::<(), Error>::Ok(())
        });
        Ok(())
    }

    async fn handle_stream(
        &self,
        client: OwnedClient<B>,
        sender: fabruic::Sender<Payload<Response<<B::CustomApi as CustomApi>::Response>>>,
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

        while let Some(payload) = receiver.next().await {
            let Payload { id, wrapped } = payload?;
            let task_sender = payload_sender.clone();
            self.handle_request_through_worker(
                wrapped,
                move |response| async move {
                    drop(task_sender.send(Payload {
                        id,
                        wrapped: response,
                    }));

                    Ok(())
                },
                client.permissions().await,
                #[cfg(feature = "pubsub")]
                self.data.subscribers.clone(),
                #[cfg(feature = "pubsub")]
                payload_sender.clone(),
            )
            .await?;
        }

        Ok(())
    }

    #[cfg(feature = "pubsub")]
    async fn forward_notifications_for(
        &self,
        subscriber_id: u64,
        receiver: flume::Receiver<Arc<Message>>,
        sender: flume::Sender<Payload<Response<<B::CustomApi as CustomApi>::Response>>>,
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

    #[cfg(feature = "pubsub")]
    async fn publish_message(&self, database: &str, topic: &str, payload: Vec<u8>) {
        self.data
            .relay
            .publish_message(Message {
                topic: database_topic(database, topic),
                payload,
            })
            .await
    }

    #[cfg(feature = "pubsub")]
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

    #[cfg(feature = "pubsub")]
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

    #[cfg(feature = "pubsub")]
    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        subscriber_id: u64,
        database: &str,
        topic: S,
    ) -> Result<(), pliantdb_core::Error> {
        let subscribers = self.data.subscribers.read().await;
        if let Some(subscriber) = subscribers.get(&subscriber_id) {
            subscriber
                .subscribe_to(database_topic(database, &topic.into()))
                .await;
            Ok(())
        } else {
            Err(pliantdb_core::Error::Server(String::from(
                "invalid subscriber id",
            )))
        }
    }

    #[cfg(feature = "pubsub")]
    async fn unsubscribe_from(
        &self,
        subscriber_id: u64,
        database: &str,
        topic: &str,
    ) -> Result<(), pliantdb_core::Error> {
        let subscribers = self.data.subscribers.read().await;
        if let Some(subscriber) = subscribers.get(&subscriber_id) {
            subscriber
                .unsubscribe_from(&database_topic(database, topic))
                .await;
            Ok(())
        } else {
            Err(pliantdb_core::Error::Server(String::from(
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
    permissions: Arc<Permissions>,
    server: CustomServer<B>,
    #[cfg(feature = "pubsub")]
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    #[cfg(feature = "pubsub")]
    sender: flume::Sender<Payload<Response<<B::CustomApi as CustomApi>::Response>>>,
}

impl<B: Backend> ClientRequest<B> {
    pub fn new(
        request: Request<<B::CustomApi as CustomApi>::Request>,
        server: CustomServer<B>,
        permissions: Arc<Permissions>,
        #[cfg(feature = "pubsub")] subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        #[cfg(feature = "pubsub")] sender: flume::Sender<
            Payload<Response<<B::CustomApi as CustomApi>::Response>>,
        >,
    ) -> Self {
        Self {
            request: Some(request),
            server,
            permissions,
            #[cfg(feature = "pubsub")]
            subscribers,
            #[cfg(feature = "pubsub")]
            sender,
        }
    }
}

#[async_trait]
impl<B: Backend> Job for ClientRequest<B> {
    type Output = Response<<B::CustomApi as CustomApi>::Response>;

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let request = self.request.take().unwrap();
        Ok(ServerDispatcher {
            server: &self.server,
            #[cfg(feature = "pubsub")]
            subscribers: &self.subscribers,
            #[cfg(feature = "pubsub")]
            response_sender: &self.sender,
        }
        .dispatch(&self.server.data.default_permissions, request)
        .await
        .unwrap_or_else(Response::Error))
    }
}

#[async_trait]
impl<B: Backend> ServerConnection for CustomServer<B> {
    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
    ) -> Result<(), pliantdb_core::Error> {
        self.data
            .storage
            .create_database_with_schema(name, schema)
            .await
    }

    async fn delete_database(&self, name: &str) -> Result<(), pliantdb_core::Error> {
        self.data.storage.delete_database(name).await
    }

    async fn list_databases(&self) -> Result<Vec<connection::Database>, pliantdb_core::Error> {
        self.data.storage.list_databases().await
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, pliantdb_core::Error> {
        self.data.storage.list_available_schemas().await
    }
}

#[derive(Dispatcher, Debug)]
#[dispatcher(input = Request<<B::CustomApi as CustomApi>::Request>, input = ServerRequest)]
struct ServerDispatcher<'s, B: Backend> {
    server: &'s CustomServer<B>,
    #[cfg(feature = "pubsub")]
    subscribers: &'s Arc<RwLock<HashMap<u64, Subscriber>>>,
    #[cfg(feature = "pubsub")]
    response_sender: &'s flume::Sender<Payload<Response<<B::CustomApi as CustomApi>::Response>>>,
}

#[async_trait]
impl<'s, B: Backend> RequestDispatcher for ServerDispatcher<'s, B> {
    type Subaction = <B::CustomApi as CustomApi>::Request;
    type Output = Response<<B::CustomApi as CustomApi>::Response>;
    type Error = pliantdb_core::Error;

    async fn handle_subaction(
        &self,
        permissions: &Permissions,
        subaction: Self::Subaction,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        let dispatcher = self.server.data.custom_api.read().await;
        if let Some(dispatcher) = dispatcher.as_ref() {
            dispatcher
                .dispatch(permissions, subaction)
                .await
                .map(Response::Api)
                .map_err(|err| {
                    pliantdb_core::Error::Server(format!("error executing custom api: {:?}", err))
                })
        } else {
            Err(pliantdb_core::Error::Server(String::from(
                "No dispatcher to handle request",
            )))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::ServerHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        request: ServerRequest,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        ServerRequestDispatcher::dispatch_to_handlers(self, permissions, request).await
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::DatabaseHandler for ServerDispatcher<'s, B> {
    async fn handle(
        &self,
        permissions: &Permissions,
        database_name: String,
        request: DatabaseRequest,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
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
    type Output = Response<<B::CustomApi as CustomApi>::Response>;
    type Error = pliantdb_core::Error;
}

#[async_trait]
impl<'s, B: Backend> CreateDatabaseHandler for ServerDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(
        &self,
        database: &'a pliantdb_core::connection::Database,
    ) -> ResourceName<'a> {
        database_resource_name(&database.name)
    }

    fn action() -> Self::Action {
        PliantAction::Server(ServerAction::CreateDatabase)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        database: pliantdb_core::connection::Database,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        self.server
            .create_database_with_schema(&database.name, database.schema)
            .await?;
        Ok(Response::Server(ServerResponse::DatabaseCreated {
            name: database.name.clone(),
        }))
    }
}

#[async_trait]
impl<'s, B: Backend> DeleteDatabaseHandler for ServerDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(&self, database: &'a String) -> ResourceName<'a> {
        database_resource_name(database)
    }

    fn action() -> Self::Action {
        PliantAction::Server(ServerAction::DeleteDatabase)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        name: String,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        self.server.delete_database(&name).await?;
        Ok(Response::Server(ServerResponse::DatabaseDeleted { name }))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::ListDatabasesHandler for ServerDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name(&self) -> ResourceName<'static> {
        pliantdb_resource_name()
    }

    fn action() -> Self::Action {
        PliantAction::Server(ServerAction::ListDatabases)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        Ok(Response::Server(ServerResponse::Databases(
            self.server.list_databases().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::ListAvailableSchemasHandler
    for ServerDispatcher<'s, B>
{
    type Action = PliantAction;

    fn resource_name(&self) -> ResourceName<'static> {
        pliantdb_resource_name()
    }

    fn action() -> Self::Action {
        PliantAction::Server(ServerAction::ListAvailableSchemas)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        Ok(Response::Server(ServerResponse::AvailableSchemas(
            self.server.list_available_schemas().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::CreateUserHandler for ServerDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name(
        &self,
        _username: &String,
        _password_request: &RegistrationRequest,
    ) -> ResourceName<'static> {
        pliantdb_resource_name()
    }

    fn action() -> Self::Action {
        PliantAction::Server(ServerAction::CreateUser)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        username: String,
        password_request: RegistrationRequest,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        todo!()
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::LoginHandler for ServerDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name(
        &self,
        username: &String,
        password_request: &LoginRequest,
    ) -> ResourceName<'static> {
        pliantdb_resource_name()
    }

    fn action() -> Self::Action {
        PliantAction::Server(ServerAction::Login)
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        username: String,
        password_request: LoginRequest,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        todo!()
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
    type Output = Response<<B::CustomApi as CustomApi>::Response>;
    type Error = pliantdb_core::Error;
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::GetHandler for DatabaseDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(
        &'a self,
        collection: &'a CollectionName,
        id: &'a u64,
    ) -> ResourceName<'a> {
        document_resource_name(&self.name, collection, *id)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::Document(DocumentAction::Get))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        id: u64,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        let document = self
            .database
            .get_from_collection_id(id, &collection)
            .await?
            .ok_or(Error::Core(pliantdb_core::Error::DocumentNotFound(
                collection, id,
            )))?;
        Ok(Response::Database(DatabaseResponse::Documents(vec![
            document,
        ])))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::GetMultipleHandler for DatabaseDispatcher<'s, B> {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        collection: &CollectionName,
        ids: &Vec<u64>,
    ) -> Result<(), pliantdb_core::Error> {
        for &id in ids {
            let document_name = document_resource_name(&self.name, collection, id);
            let action = PliantAction::Database(DatabaseAction::Document(DocumentAction::Get));
            if !permissions.allowed_to(&document_name, &action) {
                return Err(pliantdb_core::Error::from(PermissionDenied {
                    resource: document_name.to_owned(),
                    action: action.name(),
                }));
            }
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        collection: CollectionName,
        ids: Vec<u64>,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        let documents = self
            .database
            .get_multiple_from_collection_id(&ids, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::QueryHandler for DatabaseDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(
        &'a self,
        view: &'a ViewName,
        _key: &'a Option<QueryKey<Vec<u8>>>,
        _access_policy: &'a AccessPolicy,
        _with_docs: &'a bool,
    ) -> ResourceName<'a> {
        view_resource_name(&self.name, view)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::View(ViewAction::Query))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        if with_docs {
            let mappings = self
                .database
                .query_with_docs(&view, key, access_policy)
                .await?;
            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                mappings,
            )))
        } else {
            let mappings = self.database.query(&view, key, access_policy).await?;
            Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::ReduceHandler for DatabaseDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(
        &'a self,
        view: &'a ViewName,
        _key: &'a Option<QueryKey<Vec<u8>>>,
        _access_policy: &'a AccessPolicy,
        _grouped: &'a bool,
    ) -> ResourceName<'a> {
        view_resource_name(&self.name, view)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::View(ViewAction::Reduce))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        view: ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        grouped: bool,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
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
impl<'s, B: Backend> pliantdb_core::networking::ApplyTransactionHandler
    for DatabaseDispatcher<'s, B>
{
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        transaction: &Transaction<'static>,
    ) -> Result<(), pliantdb_core::Error> {
        for op in &transaction.operations {
            let (resource, action) = match &op.command {
                Command::Insert { .. } => (
                    collection_resource_name(&self.name, &op.collection),
                    PliantAction::Database(DatabaseAction::Document(DocumentAction::Insert)),
                ),
                Command::Update { header, .. } => (
                    document_resource_name(&self.name, &op.collection, header.id),
                    PliantAction::Database(DatabaseAction::Document(DocumentAction::Update)),
                ),
                Command::Delete { header } => (
                    document_resource_name(&self.name, &op.collection, header.id),
                    PliantAction::Database(DatabaseAction::Document(DocumentAction::Delete)),
                ),
            };
            if !permissions.allowed_to(&resource, &action) {
                return Err(pliantdb_core::Error::from(PermissionDenied {
                    resource: resource.to_owned(),
                    action: action.name(),
                }));
            }
        }

        Ok(())
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        transaction: Transaction<'static>,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        let results = self.database.apply_transaction(transaction).await?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::ListExecutedTransactionsHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = PliantAction;

    fn resource_name<'a>(
        &'a self,
        _starting_id: &'a Option<u64>,
        _result_limit: &'a Option<usize>,
    ) -> ResourceName<'a> {
        database_resource_name(&self.name)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::Transaction(TransactionAction::ListExecuted))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            self.database
                .list_executed_transactions(starting_id, result_limit)
                .await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::LastTransactionIdHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = PliantAction;

    fn resource_name(&self) -> ResourceName<'_> {
        database_resource_name(&self.name)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::Transaction(TransactionAction::GetLastId))
    }

    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            self.database.last_transaction_id().await?,
        )))
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::CreateSubscriberHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = PliantAction;

    fn resource_name(&self) -> ResourceName<'_> {
        database_resource_name(&self.name)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::PubSub(PubSubAction::CreateSuscriber))
    }

    #[cfg_attr(not(feature = "pubsub"), allow(unused_variables))]
    async fn handle_protected(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "pubsub")] {
                let server = self.server_dispatcher.server;
                let subscriber = server.create_subscriber(self.name.clone()).await;
                let subscriber_id = subscriber.id;

                let task_self = server.clone();
                let response_sender = self.server_dispatcher.response_sender.clone();
                tokio::spawn(async move {
                    task_self
                        .forward_notifications_for(subscriber.id, subscriber.receiver, response_sender.clone())
                        .await
                });
                Ok(Response::Database(DatabaseResponse::SubscriberCreated {
                    subscriber_id,
                }))
            } else {
                Err(pliantdb_core::Error::Server(String::from("pubsub is not enabled on this server")))
            }
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::PublishHandler for DatabaseDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(&'a self, topic: &'a String, _payload: &'a Vec<u8>) -> ResourceName<'a> {
        pubsub_topic_resource_name(&self.name, topic)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::PubSub(PubSubAction::Publish))
    }

    #[cfg_attr(not(feature = "pubsub"), allow(unused_variables))]
    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        topic: String,
        payload: Vec<u8>,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "pubsub")] {
                self
                    .server_dispatcher
                    .server
                    .publish_message(&self.name, &topic, payload)
                    .await;
                Ok(Response::Ok)
            } else {
                Err(pliantdb_core::Error::Server(String::from("pubsub is not enabled on this server")))
            }
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::PublishToAllHandler for DatabaseDispatcher<'s, B> {
    async fn verify_permissions(
        &self,
        permissions: &Permissions,
        topics: &Vec<String>,
        _payload: &Vec<u8>,
    ) -> Result<(), pliantdb_core::Error> {
        for topic in topics {
            let topic_name = pubsub_topic_resource_name(&self.name, topic);
            let action = PliantAction::Database(DatabaseAction::PubSub(PubSubAction::Publish));
            if !permissions.allowed_to(&topic_name, &action) {
                return Err(pliantdb_core::Error::from(PermissionDenied {
                    resource: topic_name.to_owned(),
                    action: action.name(),
                }));
            }
        }

        Ok(())
    }

    #[cfg_attr(not(feature = "pubsub"), allow(unused_variables))]
    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        topics: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "pubsub")] {
                self
                    .server_dispatcher
                    .server
                    .publish_serialized_to_all(
                        &self.name,
                        &topics,
                        payload,
                    )
                    .await;
                Ok(Response::Ok)
            } else {
                Err(pliantdb_core::Error::Server(String::from("pubsub is not enabled on this server")))
            }
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::SubscribeToHandler for DatabaseDispatcher<'s, B> {
    type Action = PliantAction;

    fn resource_name<'a>(&'a self, _subscriber_id: &'a u64, topic: &'a String) -> ResourceName<'a> {
        pubsub_topic_resource_name(&self.name, topic)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::PubSub(PubSubAction::SubscribeTo))
    }

    #[cfg_attr(not(feature = "pubsub"), allow(unused_variables))]
    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "pubsub")] {
                self.server_dispatcher.server.subscribe_to(subscriber_id, &self.name, topic).await.map(|_| Response::Ok)
            } else {
                Err(pliantdb_core::Error::Server(String::from("pubsub is not enabled on this server")))
            }
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::UnsubscribeFromHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = PliantAction;

    fn resource_name<'a>(&'a self, _subscriber_id: &'a u64, topic: &'a String) -> ResourceName<'a> {
        pubsub_topic_resource_name(&self.name, topic)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::PubSub(PubSubAction::UnsubscribeFrom))
    }

    #[cfg_attr(not(feature = "pubsub"), allow(unused_variables))]
    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
        topic: String,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "pubsub")] {
                self.server_dispatcher.server.unsubscribe_from(subscriber_id, &self.name, &topic).await.map(|_| Response::Ok)
            } else {
                Err(pliantdb_core::Error::Server(String::from("pubsub is not enabled on this server")))
            }
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::UnregisterSubscriberHandler
    for DatabaseDispatcher<'s, B>
{
    #[cfg_attr(not(feature = "pubsub"), allow(unused_variables))]
    async fn handle(
        &self,
        _permissions: &Permissions,
        subscriber_id: u64,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "pubsub")] {
                let mut subscribers = self.server_dispatcher.subscribers.write().await;
                if subscribers.remove(&subscriber_id).is_none() {
                    Ok(Response::Error(pliantdb_core::Error::Server(String::from(
                        "invalid subscriber id",
                    ))))
                } else {
                    Ok(Response::Ok)
                }
            } else {
                Err(pliantdb_core::Error::Server(String::from("pubsub is not enabled on this server")))
            }
        }
    }
}

#[async_trait]
impl<'s, B: Backend> pliantdb_core::networking::ExecuteKeyOperationHandler
    for DatabaseDispatcher<'s, B>
{
    type Action = PliantAction;

    fn resource_name<'a>(&'a self, op: &'a KeyOperation) -> ResourceName<'a> {
        kv_key_resource_name(&self.name, op.namespace.as_deref(), &op.key)
    }

    fn action() -> Self::Action {
        PliantAction::Database(DatabaseAction::Kv(KvAction::ExecuteOperation))
    }

    #[cfg_attr(not(feature = "keyvalue"), allow(unused_variables))]
    async fn handle_protected(
        &self,
        _permissions: &Permissions,
        op: KeyOperation,
    ) -> Result<Response<<B::CustomApi as CustomApi>::Response>, pliantdb_core::Error> {
        cfg_if! {
            if #[cfg(feature = "keyvalue")] {
                let result = self.database.execute_key_operation(op).await?;
                Ok(Response::Database(DatabaseResponse::KvOutput(result)))
            } else {
                Err(pliantdb_core::Error::Server(String::from("keyvalue is not enabled on this server")))
            }
        }
    }
}
