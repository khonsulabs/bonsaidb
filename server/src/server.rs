use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use admin::database::{self, Database};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use pliantdb_core::{
    connection::{AccessPolicy, QueryKey},
    networking::{
        self,
        fabruic::{self, Certificate, Endpoint, PrivateKey},
        Api, DatabaseRequest, DatabaseResponse, Payload, Request, Response, ServerConnection as _,
        ServerRequest, ServerResponse,
    },
    schema,
    transaction::Executed,
};
use pliantdb_local::{
    core::{
        self,
        connection::Connection,
        document::Document,
        schema::{collection, map, Schema, Schematic},
        transaction::{OperationResult, Transaction},
    },
    Configuration, Storage,
};
use tokio::{fs::File, sync::RwLock};

use crate::{
    admin::{self, database::ByName, Admin},
    async_io_util::FileExt,
    error::Error,
    hosted,
};

/// A `PliantDB` server.
#[derive(Clone, Debug)]
pub struct Server {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    endpoint: RwLock<Option<Endpoint>>,
    directory: PathBuf,
    admin: Storage<Admin>,
    schemas: RwLock<HashMap<schema::Id, Box<dyn DatabaseOpener>>>,
    open_databases: RwLock<HashMap<String, Arc<Box<dyn OpenDatabase>>>>,
    available_databases: RwLock<HashMap<String, schema::Id>>,
}

impl Server {
    /// Creates or opens a [`Server`] with its data stored in `directory`.
    /// `schemas` is a collection of [`schema::Id`] to [`Schematic`] pairs. [`schema::Id`]s are used as an identifier of a specific `Schema`, which the Server uses to
    pub async fn open(directory: &Path) -> Result<Self, Error> {
        let admin =
            Storage::open_local(directory.join("admin.pliantdb"), &Configuration::default())
                .await?;

        let available_databases = admin
            .view::<ByName>()
            .query()
            .await?
            .into_iter()
            .map(|map| (map.key, map.value))
            .collect();

        Ok(Self {
            data: Arc::new(Data {
                admin,
                directory: directory.to_owned(),
                available_databases: RwLock::new(available_databases),
                schemas: RwLock::default(),
                endpoint: RwLock::default(),
                open_databases: RwLock::default(),
            }),
        })
    }

    /// Registers a schema for use within the server.
    pub async fn register_schema<DB: Schema>(&self) -> Result<(), Error> {
        let mut schemas = self.data.schemas.write().await;
        if schemas
            .insert(
                DB::schema_id(),
                Box::new(ServerSchemaOpener::<DB>::new(self.clone())),
            )
            .is_none()
        {
            Ok(())
        } else {
            Err(Error::SchemaAlreadyRegistered(DB::schema_id()))
        }
    }

    /// Retrieves a database. This function only verifies that the database exists
    pub async fn database<'a, DB: Schema>(
        &self,
        name: &'a str,
    ) -> Result<hosted::Database<'_, 'a, DB>, Error> {
        let available_databases = self.data.available_databases.read().await;

        if let Some(stored_schema) = available_databases.get(name) {
            if stored_schema == &DB::schema_id() {
                Ok(hosted::Database::new(self, name))
            } else {
                Err(Error::SchemaMismatch {
                    database_name: name.to_owned(),
                    schema: DB::schema_id(),
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
            todo!()
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
    #[cfg(feature = "certificate")]
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
            let connection = result?;
            let task_self = self.clone();
            tokio::spawn(async move { task_self.handle_connection(connection).await });
        }

        Ok(())
    }

    async fn handle_connection(&self, mut connection: fabruic::Connection) -> Result<(), Error> {
        // TODO limit number of streams open for a client -- allowing for streams to shut down and be reclaimed.
        while let Some(incoming) = connection.next().await {
            println!("New incoming stream from: {}", connection.remote_address());

            let (sender, receiver) = incoming.accept_stream::<networking::Payload<'_>>();
            let task_self = self.clone();
            tokio::spawn(async move { task_self.handle_stream(sender, receiver).await });
        }
        Ok(())
    }

    async fn handle_stream(
        &self,
        sender: fabruic::Sender<Payload<'static>>,
        mut receiver: fabruic::Receiver<Payload<'static>>,
    ) -> Result<(), Error> {
        while let Some(payload) = receiver.next().await {
            let payload = payload?;
            let request = match payload.api {
                Api::Request(request) => request,
                Api::Response(..) => {
                    todo!("fabruic should have separate types for send/receive")
                }
            };
            let response = self
                .handle_request(request)
                .await
                .unwrap_or_else(|err| Response::Error(err.into()));
            sender.send(&Payload {
                id: payload.id,
                api: Api::Response(response),
            })?;
        }
        Ok(())
    }

    pub(crate) async fn handle_request(
        &self,
        request: Request<'static>,
    ) -> Result<Response<'static>, Error> {
        match request {
            Request::Server(request) => match request {
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
            },
            Request::Database { database, request } => {
                let db = self.open_database_without_schema(&database).await?;
                match request {
                    DatabaseRequest::Get { collection, id } => {
                        let document = db
                            .get_from_collection_id(id, &collection)
                            .await?
                            .ok_or(Error::Core(core::Error::DocumentNotFound(collection, id)))?;
                        Ok(Response::Database(DatabaseResponse::Documents(vec![
                            document,
                        ])))
                    }
                    DatabaseRequest::GetMultiple { collection, ids } => {
                        let documents = db
                            .get_multiple_from_collection_id(&ids, &collection)
                            .await?;
                        Ok(Response::Database(DatabaseResponse::Documents(documents)))
                    }
                    DatabaseRequest::Query {
                        view,
                        key,
                        access_policy,
                        with_docs,
                    } => {
                        if with_docs {
                            let mappings = db.query_with_docs(&view, key, access_policy).await?;
                            Ok(Response::Database(DatabaseResponse::ViewMappingsWithDocs(
                                mappings,
                            )))
                        } else {
                            let mappings = db.query(&view, key, access_policy).await?;
                            Ok(Response::Database(DatabaseResponse::ViewMappings(mappings)))
                        }
                    }
                    DatabaseRequest::Reduce {
                        view,
                        key,
                        access_policy,
                    } => {
                        let value = db.reduce(&view, key, access_policy).await?;
                        Ok(Response::Database(DatabaseResponse::ViewReduction(value)))
                    }

                    DatabaseRequest::ApplyTransaction { transaction } => {
                        let results = db.apply_transaction(transaction).await?;
                        Ok(Response::Database(DatabaseResponse::TransactionResults(
                            results,
                        )))
                    }

                    DatabaseRequest::ListExecutedTransactions {
                        starting_id,
                        result_limit,
                    } => Ok(Response::Database(DatabaseResponse::ExecutedTransactions(
                        db.list_executed_transactions(starting_id, result_limit)
                            .await?,
                    ))),
                    DatabaseRequest::LastTransactionId => Ok(Response::Database(
                        DatabaseResponse::LastTransactionId(db.last_transaction_id().await?),
                    )),
                }
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

                let idle = server.wait_idle().then(|_| async { Ok(()) });
                let timeout = tokio::time::sleep(timeout).then(|_| async { Err::<(), ()>(()) });
                if futures::try_join!(idle, timeout).is_err() {
                    server.close().await?;
                }
            } else {
                server.close().await?;
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
        schema: schema::Id,
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
                name: Cow::Borrowed(name),
                schema: schema.clone(),
            })
            .await?;
        available_databases.insert(name.to_owned(), schema);

        Ok(())
    }

    async fn delete_database(&self, name: &str) -> Result<(), pliantdb_core::Error> {
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

    async fn list_databases(
        &self,
    ) -> Result<Vec<networking::Database<'static>>, pliantdb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases
            .iter()
            .map(|(name, schema)| networking::Database {
                name: Cow::Owned(name.to_owned()),
                schema: schema.clone(),
            })
            .collect())
    }

    async fn list_available_schemas(&self) -> Result<Vec<schema::Id>, pliantdb_core::Error> {
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
        collection: &collection::Id,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error>;

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &collection::Id,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error>;

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, pliantdb_core::Error>;

    async fn query(
        &self,
        view: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, pliantdb_core::Error>;

    async fn query_with_docs(
        &self,
        view: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<networking::MappedDocument>, pliantdb_core::Error>;

    async fn reduce(
        &self,
        view: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error>;

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, pliantdb_core::Error>;

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error>;
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
        collection: &collection::Id,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error> {
        self.get_from_collection_id(id, collection).await
    }

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &collection::Id,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error> {
        self.get_multiple_from_collection_id(ids, collection).await
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, core::Error> {
        <Self as Connection>::apply_transaction(self, transaction).await
    }

    async fn query(
        &self,
        view: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, pliantdb_core::Error> {
        if let Some(view) = self.schema.view_by_name(view) {
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
        view: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<networking::MappedDocument>, pliantdb_core::Error> {
        let results = OpenDatabase::query(self, view, key, access_policy).await?;
        let view = self.schema.view_by_name(view).unwrap(); // query() will fail if it's not present

        let mut documents = self
            .get_multiple_from_collection_id(
                &results.iter().map(|m| m.source).collect::<Vec<_>>(),
                &view.collection(),
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
        view: &str,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error> {
        self.reduce_in_view(view, key, access_policy).await
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
    fn new(server: Server) -> Self {
        let schematic = DB::schematic();
        Self {
            server,
            schematic,
            _phantom: PhantomData::default(),
        }
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
        let db =
            Storage::<DB>::open_local(self.server.database_path(name), &Configuration::default())
                .await?;
        Ok(Arc::new(Box::new(db)))
    }
}
