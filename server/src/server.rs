#[cfg(feature = "pubsub")]
use std::collections::HashMap;
use std::{
    fmt::Debug,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use cfg_if::cfg_if;
#[cfg(feature = "websockets")]
use flume::Sender;
#[cfg(feature = "websockets")]
use futures::SinkExt;
use futures::{Future, StreamExt, TryFutureExt};
#[cfg(feature = "keyvalue")]
use pliantdb_core::kv::KeyOperation;
#[cfg(feature = "pubsub")]
use pliantdb_core::{
    circulate::{Message, Relay, Subscriber},
    pubsub::database_topic,
};
use pliantdb_core::{
    connection::{self, AccessPolicy, QueryKey, ServerConnection},
    networking::{
        self,
        fabruic::{self, Certificate, CertificateChain, Endpoint, KeyPair, PrivateKey},
        DatabaseRequest, DatabaseResponse, Payload, Request, Response, ServerRequest,
        ServerResponse,
    },
    schema,
    schema::{CollectionName, Schema, ViewName},
    transaction::Transaction,
};
use pliantdb_jobs::{manager::Manager, Job};
use pliantdb_local::{Database, OpenDatabase, Storage};
use schema::SchemaName;
#[cfg(feature = "websockets")]
use tokio::net::TcpListener;
use tokio::{fs::File, sync::RwLock};

use crate::{async_io_util::FileExt, error::Error, Configuration};

/// A `PliantDb` server.
#[derive(Clone, Debug)]
pub struct Server {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    endpoint: RwLock<Option<Endpoint>>,
    #[cfg(feature = "websockets")]
    websocket_shutdown: RwLock<Option<Sender<()>>>,
    directory: PathBuf,
    storage: Storage,
    request_processor: Manager,
    #[cfg(feature = "pubsub")]
    relay: Relay,
}

impl Server {
    /// Opens a server using `directory` for storage.
    pub async fn open(directory: &Path, configuration: Configuration) -> Result<Self, Error> {
        let request_processor = Manager::default();
        for _ in 0..configuration.request_workers {
            request_processor.spawn_worker();
        }

        let storage = Storage::open_local(directory, &configuration.storage).await?;

        Ok(Self {
            data: Arc::new(Data {
                storage,
                directory: directory.to_owned(),
                endpoint: RwLock::default(),
                #[cfg(feature = "websockets")]
                websocket_shutdown: RwLock::default(),
                request_processor,
                #[cfg(feature = "pubsub")]
                relay: Relay::default(),
            }),
        })
    }

    /// Retrieves a database. This function only verifies that the database exists
    pub async fn database<DB: Schema>(&self, name: &'_ str) -> Result<Database<DB>, Error> {
        let db = self.data.storage.database(name).await?;
        Ok(db)
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

        // TODO switch to logging
        println!("Listening on {}", server.local_address()?);

        while let Some(result) = server.next().await {
            let connection = result.accept::<()>().await?;
            let task_self = self.clone();
            tokio::spawn(async move { task_self.handle_connection(connection).await });
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
                    println!("[server] new connection from {}", remote_addr);

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

    async fn handle_connection(
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

            println!(
                "[server] incoming stream from: {}",
                connection.remote_address()
            );

            match incoming
                .accept::<networking::Payload<Response>, networking::Payload<Request>>()
                .await
            {
                Ok((sender, receiver)) => {
                    let task_self = self.clone();
                    tokio::spawn(async move { task_self.handle_stream(sender, receiver).await });
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
        let stream = tokio_tungstenite::accept_async(connection).await?;

        let (mut sender, mut receiver) = stream.split();
        let (response_sender, response_receiver) = flume::unbounded();
        let (message_sender, message_receiver) = flume::unbounded();
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

        #[cfg(feature = "pubsub")]
        let subscribers: Arc<RwLock<HashMap<u64, Subscriber>>> = Arc::default();

        while let Some(payload) = receiver.next().await {
            match payload? {
                Message::Binary(binary) => {
                    let payload = bincode::deserialize::<Payload<Request>>(&binary)?;
                    let id = payload.id;
                    let task_sender = response_sender.clone();
                    self.handle_request_through_worker(
                        payload.wrapped,
                        move |response| async move {
                            let _ = task_sender.send(Payload {
                                id,
                                wrapped: response,
                            });

                            Ok(())
                        },
                        #[cfg(feature = "pubsub")]
                        subscribers.clone(),
                        #[cfg(feature = "pubsub")]
                        response_sender.clone(),
                    )
                    .await?;
                }
                Message::Close(_) => break,
                Message::Ping(payload) => {
                    let _ = message_sender.send(Message::Pong(payload));
                }
                other => {
                    eprintln!("[server] unexpected message: {:?}", other);
                }
            }
        }

        Ok(())
    }

    async fn handle_request_through_worker<
        F: FnOnce(Response) -> R + Send + 'static,
        R: Future<Output = Result<(), Error>> + Send,
    >(
        &self,
        request: Request,
        callback: F,
        #[cfg(feature = "pubsub")] subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        #[cfg(feature = "pubsub")] response_sender: flume::Sender<Payload<Response>>,
    ) -> Result<(), Error> {
        let job = self
            .data
            .request_processor
            .enqueue(ClientRequest::new(
                request,
                self.clone(),
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
        sender: fabruic::Sender<Payload<Response>>,
        mut receiver: fabruic::Receiver<Payload<Request>>,
    ) -> Result<(), Error> {
        #[cfg(feature = "pubsub")]
        let subscribers: Arc<RwLock<HashMap<u64, Subscriber>>> = Arc::default();
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
                    let _ = task_sender.send(Payload {
                        id,
                        wrapped: response,
                    });

                    Ok(())
                },
                #[cfg(feature = "pubsub")]
                subscribers.clone(),
                #[cfg(feature = "pubsub")]
                payload_sender.clone(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn handle_request(
        &self,
        request: Request,
        #[cfg(feature = "pubsub")] subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        #[cfg(feature = "pubsub")] response_sender: &flume::Sender<Payload<Response>>,
    ) -> Result<Response, Error> {
        match request {
            Request::Server(request) => self.handle_server_request(request).await,
            Request::Database { database, request } => {
                self.handle_database_request(
                    database,
                    request,
                    #[cfg(feature = "pubsub")]
                    subscribers,
                    #[cfg(feature = "pubsub")]
                    response_sender,
                )
                .await
            }
        }
    }

    async fn handle_server_request(&self, request: ServerRequest) -> Result<Response, Error> {
        match request {
            ServerRequest::CreateDatabase(database) => {
                self.create_database_with_schema(&database.name, database.schema)
                    .await?;
                Ok(Response::Server(ServerResponse::DatabaseCreated {
                    name: database.name.clone(),
                }))
            }
            ServerRequest::DeleteDatabase { name } => {
                self.delete_database(&name).await?;
                Ok(Response::Server(ServerResponse::DatabaseDeleted { name }))
            }
            ServerRequest::ListDatabases => Ok(Response::Server(ServerResponse::Databases(
                self.list_databases().await?,
            ))),
            ServerRequest::ListAvailableSchemas => Ok(Response::Server(
                ServerResponse::AvailableSchemas(self.list_available_schemas().await?),
            )),
        }
    }

    #[allow(clippy::too_many_lines)]
    #[cfg_attr(not(feature = "pubsub"), allow(clippy::match_same_arms))]
    async fn handle_database_request(
        &self,
        database: String,
        request: DatabaseRequest,
        #[cfg(feature = "pubsub")] subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        #[cfg(feature = "pubsub")] response_sender: &flume::Sender<Payload<Response>>,
    ) -> Result<Response, Error> {
        match request {
            DatabaseRequest::Get { collection, id } => {
                self.handle_database_get_request(database, collection, id)
                    .await
            }
            DatabaseRequest::GetMultiple { collection, ids } => {
                self.handle_database_get_multiple_request(database, collection, ids)
                    .await
            }
            DatabaseRequest::Query {
                view,
                key,
                access_policy,
                with_docs,
            } => {
                self.handle_database_query(database, &view, key, access_policy, with_docs)
                    .await
            }
            DatabaseRequest::Reduce {
                view,
                key,
                access_policy,
                grouped,
            } => {
                self.handle_database_reduce(database, &view, key, access_policy, grouped)
                    .await
            }

            DatabaseRequest::ApplyTransaction { transaction } => {
                self.handle_database_apply_transaction(database, transaction)
                    .await
            }

            DatabaseRequest::ListExecutedTransactions {
                starting_id,
                result_limit,
            } => {
                self.handle_database_list_executed_transactions(database, starting_id, result_limit)
                    .await
            }
            DatabaseRequest::LastTransactionId => {
                self.handle_database_last_transaction_id(database).await
            }

            DatabaseRequest::CreateSubscriber => {
                cfg_if! {
                    if #[cfg(feature = "pubsub")] {
                        self.handle_database_create_subscriber(subscribers, response_sender)
                        .await
                    } else {
                        Err(Error::Request(Arc::new(anyhow::anyhow!("pubsub is not enabled on this server"))))
                    }
                }
            }
            DatabaseRequest::Publish { topic, payload } => {
                cfg_if! {
                    if #[cfg(feature = "pubsub")] {
                        self.handle_database_publish(database, topic, payload).await
                    } else {
                        drop((topic, payload));
                        Err(Error::Request(Arc::new(anyhow::anyhow!("pubsub is not enabled on this server"))))
                    }
                }
            }
            DatabaseRequest::PublishToAll { topics, payload } => {
                cfg_if! {
                    if #[cfg(feature = "pubsub")] {
                        self.handle_database_publish_to_all(database, topics, payload)
                            .await
                    } else {
                        drop((topics, payload));
                        Err(Error::Request(Arc::new(anyhow::anyhow!("pubsub is not enabled on this server"))))
                    }
                }
            }
            DatabaseRequest::SubscribeTo {
                subscriber_id,
                topic,
            } => {
                cfg_if! {
                    if #[cfg(feature = "pubsub")] {
                        self.handle_database_subscribe_to(subscriber_id, topic, subscribers)
                            .await
                    } else {
                        let _ = (subscriber_id, topic);
                        Err(Error::Request(Arc::new(anyhow::anyhow!("pubsub is not enabled on this server"))))
                    }
                }
            }
            DatabaseRequest::UnsubscribeFrom {
                subscriber_id,
                topic,
            } => {
                cfg_if! {
                    if #[cfg(feature = "pubsub")] {
                        self.handle_database_unsubscribe_from(subscriber_id, topic, subscribers)
                            .await
                    } else {
                        let _ = (subscriber_id, topic);
                        Err(Error::Request(Arc::new(anyhow::anyhow!("pubsub is not enabled on this server"))))
                    }
                }
            }
            DatabaseRequest::UnregisterSubscriber { subscriber_id } => {
                cfg_if! {
                    if #[cfg(feature = "pubsub")] {
                        self.handle_database_unregister_subscriber(subscriber_id, subscribers)
                            .await
                    } else {
                        let _ = subscriber_id;
                        Err(Error::Request(Arc::new(anyhow::anyhow!("pubsub is not enabled on this server"))))
                    }
                }
            }
            DatabaseRequest::ExecuteKeyOperation(op) => {
                cfg_if! {
                    if #[cfg(feature = "keyvalue")] {
                        self.handle_database_kv_request(database, op).await
                    } else {
                        let _ = op;
                        Err(Error::Request(Arc::new(anyhow::anyhow!("keyvalue is not enabled on this server"))))
                    }
                }
            }
        }
    }

    async fn handle_database_get_request(
        &self,
        database: String,
        collection: CollectionName,
        id: u64,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        let document = db
            .get_from_collection_id(id, &collection)
            .await?
            .ok_or(Error::Core(pliantdb_core::Error::DocumentNotFound(
                collection, id,
            )))?;
        Ok(Response::Database(DatabaseResponse::Documents(vec![
            document,
        ])))
    }

    async fn handle_database_get_multiple_request(
        &self,
        database: String,
        collection: CollectionName,
        ids: Vec<u64>,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        let documents = db
            .get_multiple_from_collection_id(&ids, &collection)
            .await?;
        Ok(Response::Database(DatabaseResponse::Documents(documents)))
    }

    async fn handle_database_query(
        &self,
        database: String,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        with_docs: bool,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        if with_docs {
            let mappings = db.query_with_docs(view, key, access_policy).await?;
            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                mappings,
            )))
        } else {
            let mappings = db.query(view, key, access_policy).await?;
            Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
        }
    }

    async fn handle_database_reduce(
        &self,
        database: String,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        grouped: bool,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        if grouped {
            let values = db.reduce_grouped(view, key, access_policy).await?;
            Ok(Response::Database(DatabaseResponse::ViewGroupedReduction(
                values,
            )))
        } else {
            let value = db.reduce(view, key, access_policy).await?;
            Ok(Response::Database(DatabaseResponse::ViewReduction(value)))
        }
    }

    async fn handle_database_apply_transaction(
        &self,
        database: String,
        transaction: Transaction<'static>,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        let results = db.apply_transaction(transaction).await?;
        Ok(Response::Database(DatabaseResponse::TransactionResults(
            results,
        )))
    }

    async fn handle_database_list_executed_transactions(
        &self,
        database: String,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            db.list_executed_transactions(starting_id, result_limit)
                .await?,
        )))
    }

    async fn handle_database_last_transaction_id(
        &self,
        database: String,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            db.last_transaction_id().await?,
        )))
    }

    #[cfg(feature = "pubsub")]
    async fn handle_database_create_subscriber(
        &self,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        response_sender: &flume::Sender<Payload<Response>>,
    ) -> Result<Response, Error> {
        let subscriber = self.data.relay.create_subscriber().await;

        let mut subscribers = subscribers.write().await;
        let subscriber_id = subscriber.id();
        let receiver = subscriber.receiver().clone();
        subscribers.insert(subscriber_id, subscriber);

        let task_self = self.clone();
        let response_sender = response_sender.clone();
        tokio::spawn(async move {
            task_self
                .forward_notifications_for(subscriber_id, receiver, response_sender.clone())
                .await
        });

        Ok(Response::Database(DatabaseResponse::SubscriberCreated {
            subscriber_id,
        }))
    }

    #[cfg(feature = "pubsub")]
    async fn handle_database_publish(
        &self,
        database: String,
        topic: String,
        payload: Vec<u8>,
    ) -> Result<Response, Error> {
        self.data
            .relay
            .publish_message(Message {
                topic: database_topic(&database, &topic),
                payload,
            })
            .await;
        Ok(Response::Ok)
    }

    #[cfg(feature = "pubsub")]
    async fn handle_database_publish_to_all(
        &self,
        database: String,
        topics: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<Response, Error> {
        self.data
            .relay
            .publish_serialized_to_all(
                topics
                    .iter()
                    .map(|topic| database_topic(&database, topic))
                    .collect(),
                payload,
            )
            .await;
        Ok(Response::Ok)
    }

    #[cfg(feature = "pubsub")]
    async fn handle_database_subscribe_to(
        &self,
        subscriber_id: u64,
        topic: String,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    ) -> Result<Response, Error> {
        let subscribers = subscribers.read().await;
        if let Some(subscriber) = subscribers.get(&subscriber_id) {
            subscriber.subscribe_to(topic).await;
            Ok(Response::Ok)
        } else {
            Ok(Response::Error(pliantdb_core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        }
    }

    #[cfg(feature = "pubsub")]
    async fn handle_database_unsubscribe_from(
        &self,
        subscriber_id: u64,
        topic: String,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    ) -> Result<Response, Error> {
        let subscribers = subscribers.read().await;
        if let Some(subscriber) = subscribers.get(&subscriber_id) {
            subscriber.unsubscribe_from(&topic).await;
            Ok(Response::Ok)
        } else {
            Ok(Response::Error(pliantdb_core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        }
    }

    #[cfg(feature = "pubsub")]
    async fn handle_database_unregister_subscriber(
        &self,
        subscriber_id: u64,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    ) -> Result<Response, Error> {
        let mut subscribers = subscribers.write().await;
        if subscribers.remove(&subscriber_id).is_none() {
            Ok(Response::Error(pliantdb_core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        } else {
            Ok(Response::Ok)
        }
    }

    #[cfg(feature = "keyvalue")]
    async fn handle_database_kv_request(
        &self,
        database: String,
        op: KeyOperation,
    ) -> Result<Response, Error> {
        let db = self.database_without_schema(&database).await?;
        let result = db.execute_key_operation(op).await?;
        Ok(Response::Database(DatabaseResponse::KvOutput(result)))
    }

    #[cfg(feature = "pubsub")]
    async fn forward_notifications_for(
        &self,
        subscriber_id: u64,
        receiver: flume::Receiver<Arc<Message>>,
        sender: flume::Sender<Payload<Response>>,
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
}

impl Deref for Server {
    type Target = Storage;

    fn deref(&self) -> &Self::Target {
        &self.data.storage
    }
}

#[derive(Debug)]
struct ClientRequest {
    request: Option<Request>,
    server: Server,
    #[cfg(feature = "pubsub")]
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    #[cfg(feature = "pubsub")]
    sender: flume::Sender<Payload<Response>>,
}
impl ClientRequest {
    pub const fn new(
        request: Request,
        server: Server,
        #[cfg(feature = "pubsub")] subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        #[cfg(feature = "pubsub")] sender: flume::Sender<Payload<Response>>,
    ) -> Self {
        Self {
            request: Some(request),
            server,
            #[cfg(feature = "pubsub")]
            subscribers,
            #[cfg(feature = "pubsub")]
            sender,
        }
    }
}

#[async_trait]
impl Job for ClientRequest {
    type Output = Response;

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let request = self.request.take().unwrap();
        Ok(self
            .server
            .handle_request(
                request,
                #[cfg(feature = "pubsub")]
                self.subscribers.clone(),
                #[cfg(feature = "pubsub")]
                &self.sender,
            )
            .await
            .unwrap_or_else(|err| Response::Error(err.into())))
    }
}

#[async_trait]
impl ServerConnection for Server {
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
