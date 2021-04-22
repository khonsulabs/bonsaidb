use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use admin::database::{self, Database};
use async_trait::async_trait;
#[cfg(feature = "websockets")]
use flume::Sender;
#[cfg(feature = "websockets")]
use futures::SinkExt;
use futures::{Future, StreamExt, TryFutureExt};
use itertools::Itertools;
use pliantdb_core::{
    self as core,
    circulate::{Message, Relay, Subscriber},
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    kv::{KeyOperation, Kv, Output},
    networking::{
        self,
        fabruic::{self, Certificate, Endpoint, PrivateKey},
        DatabaseRequest, DatabaseResponse, Payload, Request, Response, ServerConnection as _,
        ServerRequest, ServerResponse,
    },
    schema,
    schema::{
        view::map::{self, MappedValue},
        CollectionName, Schema, Schematic, ViewName,
    },
    transaction::{Executed, OperationResult, Transaction},
};
use pliantdb_jobs::{manager::Manager, Job};
use pliantdb_local::{Internal, Storage};
use schema::SchemaName;
#[cfg(feature = "websockets")]
use tokio::net::TcpListener;
use tokio::{fs::File, sync::RwLock};

use crate::{
    admin::{self, database::ByName, Admin},
    async_io_util::FileExt,
    error::Error,
    hosted, Configuration,
};

/// A `PliantDB` server.
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
    admin: Storage<Admin>,
    schemas: RwLock<HashMap<SchemaName, Box<dyn DatabaseOpener>>>,
    open_databases: RwLock<HashMap<String, Arc<Box<dyn OpenDatabase>>>>,
    available_databases: RwLock<HashMap<String, SchemaName>>,
    request_processor: Manager,
    storage_configuration: pliantdb_local::Configuration,
    relay: Relay,
}

impl Server {
    /// Creates or opens a [`Server`] with its data stored in `directory`.
    /// `schemas` is a collection of [`SchemaName`] to [`Schematic`] pairs. [`SchemaName`]s are used as an identifier of a specific `Schema`, which the Server uses to
    pub async fn open(directory: &Path, configuration: Configuration) -> Result<Self, Error> {
        let admin =
            Storage::open_local(directory.join("admin.pliantdb"), &configuration.storage).await?;

        let available_databases = admin
            .view::<ByName>()
            .query()
            .await?
            .into_iter()
            .map(|map| (map.key, map.value))
            .collect();

        let request_processor = Manager::default();
        for _ in 0..configuration.request_workers {
            request_processor.spawn_worker();
        }

        Ok(Self {
            data: Arc::new(Data {
                admin,
                directory: directory.to_owned(),
                available_databases: RwLock::new(available_databases),
                schemas: RwLock::default(),
                endpoint: RwLock::default(),
                #[cfg(feature = "websockets")]
                websocket_shutdown: RwLock::default(),
                open_databases: RwLock::default(),
                request_processor,
                storage_configuration: configuration.storage,
                relay: Relay::default(),
            }),
        })
    }

    /// Registers a schema for use within the server.
    pub async fn register_schema<DB: Schema>(&self) -> Result<(), Error> {
        let mut schemas = self.data.schemas.write().await;
        if schemas
            .insert(
                DB::schema_name()?,
                Box::new(ServerSchemaOpener::<DB>::new(self.clone())?),
            )
            .is_none()
        {
            Ok(())
        } else {
            Err(Error::SchemaAlreadyRegistered(DB::schema_name()?))
        }
    }

    /// Retrieves a database. This function only verifies that the database exists
    pub async fn database<'a, DB: Schema>(
        &self,
        name: &'a str,
    ) -> Result<hosted::Database<'_, 'a, DB>, Error> {
        let available_databases = self.data.available_databases.read().await;

        if let Some(stored_schema) = available_databases.get(name) {
            if stored_schema == &DB::schema_name()? {
                Ok(hosted::Database::new(self, name))
            } else {
                Err(Error::SchemaMismatch {
                    database_name: name.to_owned(),
                    schema: DB::schema_name()?,
                    stored_schema: stored_schema.clone(),
                })
            }
        } else {
            Err(Error::DatabaseNotFound(name.to_owned()))
        }
    }

    pub(crate) async fn open_database_without_schema(
        &self,
        name: &str,
    ) -> Result<Arc<Box<dyn OpenDatabase>>, Error> {
        // If we have an open database return it
        {
            let open_databases = self.data.open_databases.read().await;
            if let Some(db) = open_databases.get(name) {
                return Ok(db.clone());
            }
        }

        // Open the database.
        let mut open_databases = self.data.open_databases.write().await;
        // Check that we weren't in a race to initialize this database.
        if let Some(db) = open_databases.get(name) {
            return Ok(db.clone());
        }

        let schema = match self
            .data
            .admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .first()
        {
            Some(entry) => entry.value.clone(),
            None => return Err(Error::DatabaseNotFound(name.to_string())),
        };

        let schemas = self.data.schemas.read().await;
        if let Some(schema) = schemas.get(&schema) {
            let db = schema.open(name).await?;
            open_databases.insert(name.to_string(), db.clone());
            Ok(db)
        } else {
            Err(Error::Core(pliantdb_core::Error::Networking(
                networking::Error::SchemaNotRegistered(schema),
            )))
        }
    }

    pub(crate) async fn open_database<DB: Schema>(&self, name: &str) -> Result<Storage<DB>, Error> {
        let db = self.open_database_without_schema(name).await?;
        let storage = db
            .as_any()
            .downcast_ref::<Storage<DB>>()
            .expect("schema did not match");
        Ok(storage.clone())
    }

    pub(crate) fn relay(&self) -> &'_ Relay {
        &self.data.relay
    }

    fn validate_name(name: &str) -> Result<(), Error> {
        if name
            .chars()
            .enumerate()
            .all(|(index, c)| c.is_ascii_alphanumeric() || (index > 0 && (c == '.' || c == '-')))
        {
            Ok(())
        } else {
            Err(Error::InvalidDatabaseName(name.to_owned()))
        }
    }

    fn database_path(&self, name: &str) -> PathBuf {
        self.data.directory.join(format!("{}.pliantdb", name))
    }

    /// Installs an X.509 certificate used for general purpose connections.
    #[cfg(any(test, feature = "certificate-generation"))]
    pub async fn install_self_signed_certificate(
        &self,
        server_name: &str,
        overwrite: bool,
    ) -> Result<(), Error> {
        let (certificate, private_key) = fabruic::generate_self_signed(server_name);

        if self.certificate_path().exists() && !overwrite {
            return Err(Error::Core(core::Error::Configuration(String::from("Certificate already installed. Enable overwrite if you wish to replace the existing certificate."))));
        }

        self.install_certificate(&certificate, &private_key).await?;

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
                Error::Core(core::Error::Configuration(format!(
                    "Error writing certificate file: {}",
                    err
                )))
            })?;
        File::create(self.private_key_path())
            .and_then(|file| file.write_all(fabruic::Dangerous::as_ref(private_key)))
            .await
            .map_err(|err| {
                Error::Core(core::Error::Configuration(format!(
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
                Error::Core(core::Error::Configuration(format!(
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
                Error::Core(core::Error::Configuration(format!(
                    "Error reading private key file: {}",
                    err
                )))
            })??;

        let mut server = Endpoint::new_server(port, &certificate, &private_key)?;
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
            println!(
                "[server] incoming stream from: {}",
                connection.remote_address()
            );

            let (sender, receiver) = incoming
                .accept::<networking::Payload<Response>, networking::Payload<Request>>()
                .await?;
            let task_self = self.clone();
            tokio::spawn(async move { task_self.handle_stream(sender, receiver).await });
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
                        subscribers.clone(),
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
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        response_sender: flume::Sender<Payload<Response>>,
    ) -> Result<(), Error> {
        let job = self
            .data
            .request_processor
            .enqueue(ClientRequest::new(
                request,
                self.clone(),
                subscribers,
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
            let Payload { id, wrapped } = payload;
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
                subscribers.clone(),
                payload_sender.clone(),
            )
            .await?;
        }

        Ok(())
    }

    pub(crate) async fn handle_request(
        &self,
        request: Request,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        response_sender: &flume::Sender<Payload<Response>>,
    ) -> Result<Response, Error> {
        match request {
            Request::Server(request) => self.handle_server_request(request).await,
            Request::Database { database, request } => {
                self.handle_database_request(database, request, subscribers, response_sender)
                    .await
            }
        }
    }

    async fn handle_server_request(&self, request: ServerRequest) -> Result<Response, Error> {
        match request {
            ServerRequest::CreateDatabase(database) => {
                self.create_database(&database.name, database.schema)
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

    async fn handle_database_request(
        &self,
        database: String,
        request: DatabaseRequest,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        response_sender: &flume::Sender<Payload<Response>>,
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
                self.handle_database_create_subscriber(subscribers, response_sender)
                    .await
            }
            DatabaseRequest::Publish(message) => self.handle_database_publish(message).await,
            DatabaseRequest::SubscribeTo {
                subscriber_id,
                topic,
            } => {
                self.handle_database_subscribe_to(subscriber_id, topic, subscribers)
                    .await
            }
            DatabaseRequest::UnsubscribeFrom {
                subscriber_id,
                topic,
            } => {
                self.handle_database_unsubscribe_from(subscriber_id, topic, subscribers)
                    .await
            }
            DatabaseRequest::UnregisterSubscriber { subscriber_id } => {
                self.handle_database_unregister_subscriber(subscriber_id, subscribers)
                    .await
            }
            DatabaseRequest::ExecuteKeyOperation(op) => {
                self.handle_database_kv_request(database, op).await
            }
        }
    }

    async fn handle_database_get_request(
        &self,
        database: String,
        collection: CollectionName,
        id: u64,
    ) -> Result<Response, Error> {
        let db = self.open_database_without_schema(&database).await?;
        let document = db
            .get_from_collection_id(id, &collection)
            .await?
            .ok_or(Error::Core(core::Error::DocumentNotFound(collection, id)))?;
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
        let db = self.open_database_without_schema(&database).await?;
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
        let db = self.open_database_without_schema(&database).await?;
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
        let db = self.open_database_without_schema(&database).await?;
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
        let db = self.open_database_without_schema(&database).await?;
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
        let db = self.open_database_without_schema(&database).await?;
        Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
            db.list_executed_transactions(starting_id, result_limit)
                .await?,
        )))
    }

    async fn handle_database_last_transaction_id(
        &self,
        database: String,
    ) -> Result<Response, Error> {
        let db = self.open_database_without_schema(&database).await?;
        Ok(Response::Database(DatabaseResponse::LastTransactionId(
            db.last_transaction_id().await?,
        )))
    }

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

    async fn handle_database_publish(&self, message: Message) -> Result<Response, Error> {
        self.data.relay.publish_message(message).await;
        Ok(Response::Ok)
    }

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
            Ok(Response::Error(core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        }
    }

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
            Ok(Response::Error(core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        }
    }

    async fn handle_database_unregister_subscriber(
        &self,
        subscriber_id: u64,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    ) -> Result<Response, Error> {
        let mut subscribers = subscribers.write().await;
        if subscribers.remove(&subscriber_id).is_none() {
            Ok(Response::Error(core::Error::Server(String::from(
                "invalid subscriber id",
            ))))
        } else {
            Ok(Response::Ok)
        }
    }

    async fn handle_database_kv_request(
        &self,
        database: String,
        op: KeyOperation,
    ) -> Result<Response, Error> {
        let db = self.open_database_without_schema(&database).await?;
        let result = db.execute_key_operation(op).await?;
        Ok(Response::Database(DatabaseResponse::KvOutput(result)))
    }

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
                        message: message.as_ref().clone(),
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

        // Close all databases by dropping them.
        let mut open_databases = self.data.open_databases.write().await;
        open_databases.clear();

        Ok(())
    }
}

#[async_trait]
impl networking::ServerConnection for Server {
    async fn create_database(
        &self,
        name: &str,
        schema: SchemaName,
    ) -> Result<(), pliantdb_core::Error> {
        Self::validate_name(name)?;

        {
            let schemas = self.data.schemas.read().await;
            if !schemas.contains_key(&schema) {
                return Err(pliantdb_core::Error::Networking(
                    networking::Error::SchemaNotRegistered(schema),
                ));
            }
        }

        let mut available_databases = self.data.available_databases.write().await;
        if !self
            .data
            .admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .is_empty()
        {
            return Err(pliantdb_core::Error::Networking(
                networking::Error::DatabaseNameAlreadyTaken(name.to_string()),
            ));
        }

        self.data
            .admin
            .collection::<Database>()
            .push(&networking::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .await?;
        available_databases.insert(name.to_string(), schema);

        Ok(())
    }

    async fn delete_database(&self, name: &str) -> Result<(), pliantdb_core::Error> {
        let mut open_databases = self.data.open_databases.write().await;
        open_databases.remove(name);

        let mut available_databases = self.data.available_databases.write().await;

        let file_path = self.database_path(name);
        let files_existed = file_path.exists();
        if file_path.exists() {
            tokio::fs::remove_dir_all(file_path)
                .await
                .map_err(|err| core::Error::Io(err.to_string()))?;
        }

        if let Some(entry) = self
            .data
            .admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query_with_docs()
            .await?
            .first()
        {
            self.data.admin.delete(&entry.document).await?;
            available_databases.remove(name);

            Ok(())
        } else if files_existed {
            // There's no way to atomically remove files AND ensure the admin
            // database is updated. Instead, if this method is called again and
            // the folder is still found, we will try to delete it again without
            // returning an error. Thus, if you get an error from
            // delete_database, you can try again until you get a success
            // returned.
            Ok(())
        } else {
            return Err(pliantdb_core::Error::Networking(
                networking::Error::DatabaseNotFound(name.to_string()),
            ));
        }
    }

    async fn list_databases(&self) -> Result<Vec<networking::Database>, pliantdb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases
            .iter()
            .map(|(name, schema)| networking::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .collect())
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, pliantdb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases.values().unique().cloned().collect())
    }
}

#[async_trait]
pub trait OpenDatabase: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &'_ dyn Any;

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error>;

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error>;

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, pliantdb_core::Error>;

    async fn query(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, pliantdb_core::Error>;

    async fn query_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<networking::MappedDocument>, pliantdb_core::Error>;

    async fn reduce(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error>;

    async fn reduce_grouped(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, pliantdb_core::Error>;

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, pliantdb_core::Error>;

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error>;

    async fn execute_key_operation(&self, op: KeyOperation)
        -> Result<Output, pliantdb_core::Error>;
}

#[async_trait]
impl<DB> OpenDatabase for Storage<DB>
where
    DB: Schema,
{
    fn as_any(&self) -> &'_ dyn Any {
        self
    }

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
        Internal::get_from_collection_id(self, id, collection).await
    }

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
        Internal::get_multiple_from_collection_id(self, ids, collection).await
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, core::Error> {
        <Self as Connection>::apply_transaction(self, transaction).await
    }

    async fn query(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, pliantdb_core::Error> {
        if let Some(view) = self.schematic().view_by_name(view) {
            let mut results = Vec::new();
            self.for_each_in_view(view, key, access_policy, |key, entry| {
                for mapping in entry.mappings {
                    results.push(map::Serialized {
                        source: mapping.source,
                        key: key.to_vec(),
                        value: mapping.value,
                    });
                }
                Ok(())
            })
            .await?;

            Ok(results)
        } else {
            Err(pliantdb_core::Error::CollectionNotFound)
        }
    }

    async fn query_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<networking::MappedDocument>, pliantdb_core::Error> {
        let results = OpenDatabase::query(self, view, key, access_policy).await?;
        let view = self.schematic().view_by_name(view).unwrap(); // query() will fail if it's not present

        let mut documents = Internal::get_multiple_from_collection_id(
            self,
            &results.iter().map(|m| m.source).collect::<Vec<_>>(),
            &view.collection()?,
        )
        .await?
        .into_iter()
        .map(|doc| (doc.header.id, doc))
        .collect::<HashMap<_, _>>();

        Ok(results
            .into_iter()
            .filter_map(|map| {
                if let Some(source) = documents.remove(&map.source) {
                    Some(networking::MappedDocument {
                        key: map.key,
                        value: map.value,
                        source,
                    })
                } else {
                    None
                }
            })
            .collect())
    }

    async fn reduce(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error> {
        self.reduce_in_view(view, key, access_policy).await
    }

    async fn reduce_grouped(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, pliantdb_core::Error> {
        self.grouped_reduce_in_view(view, key, access_policy).await
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, pliantdb_core::Error> {
        Connection::list_executed_transactions(self, starting_id, result_limit).await
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error> {
        Connection::last_transaction_id(self).await
    }

    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, pliantdb_core::Error> {
        Kv::execute_key_operation(self, op).await
    }
}

#[test]
fn name_validation_tests() {
    assert!(matches!(Server::validate_name("azAZ09.-"), Ok(())));
    assert!(matches!(
        Server::validate_name(".alphaunmericfirstrequired"),
        Err(Error::InvalidDatabaseName(_))
    ));
    assert!(matches!(
        Server::validate_name("-alphaunmericfirstrequired"),
        Err(Error::InvalidDatabaseName(_))
    ));
    assert!(matches!(
        Server::validate_name("\u{2661}"),
        Err(Error::InvalidDatabaseName(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn opening_databases_test() -> Result<(), Error> {
    Ok(())
}

#[async_trait]
trait DatabaseOpener: Send + Sync + Debug {
    fn schematic(&self) -> &'_ Schematic;
    async fn open(&self, name: &str) -> Result<Arc<Box<dyn OpenDatabase>>, Error>;
}

#[derive(Debug)]
struct ServerSchemaOpener<DB: Schema> {
    server: Server,
    schematic: Schematic,
    _phantom: PhantomData<DB>,
}

impl<DB> ServerSchemaOpener<DB>
where
    DB: Schema,
{
    fn new(server: Server) -> Result<Self, Error> {
        let schematic = DB::schematic()?;
        Ok(Self {
            server,
            schematic,
            _phantom: PhantomData::default(),
        })
    }
}

#[async_trait]
impl<DB> DatabaseOpener for ServerSchemaOpener<DB>
where
    DB: Schema,
{
    fn schematic(&self) -> &'_ Schematic {
        &self.schematic
    }

    async fn open(&self, name: &str) -> Result<Arc<Box<dyn OpenDatabase>>, Error> {
        let db = Storage::<DB>::open_local(
            self.server.database_path(name),
            &self.server.data.storage_configuration,
        )
        .await?;
        Ok(Arc::new(Box::new(db)))
    }
}

#[derive(Debug)]
struct ClientRequest {
    request: Option<Request>,
    server: Server,
    subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
    sender: flume::Sender<Payload<Response>>,
}
impl ClientRequest {
    pub const fn new(
        request: Request,
        server: Server,
        subscribers: Arc<RwLock<HashMap<u64, Subscriber>>>,
        sender: flume::Sender<Payload<Response>>,
    ) -> Self {
        Self {
            request: Some(request),
            server,
            subscribers,
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
            .handle_request(request, self.subscribers.clone(), &self.sender)
            .await
            .unwrap_or_else(|err| Response::Error(err.into())))
    }
}
