use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    net::ToSocketAddrs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use admin::database::{self, Database};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use pliantdb_core::schema;
use pliantdb_local::{
    core::{
        self,
        connection::Connection,
        document::Document,
        schema::{collection, Schema, Schematic},
    },
    Configuration, Storage,
};
use pliantdb_networking::{
    fabruic::{self, Certificate, Endpoint, PrivateKey},
    DatabaseRequest, DatabaseResponse, Payload, Request, Response, ServerRequest, ServerResponse,
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

    /// Creates a database named `name` using the [`schema::Id`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken]: `name` was already used for a
    ///   previous database name. Database names are case insensitive.
    pub async fn create_database(&self, name: &str, schema: schema::Id) -> Result<(), Error> {
        Self::validate_name(name)?;

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
            return Err(Error::DatabaseNameAlreadyTaken(name.to_string()));
        }
        self.data
            .admin
            .collection::<Database>()
            .push(&pliantdb_networking::Database {
                name: Cow::Borrowed(name),
                schema: schema.clone(),
            })
            .await?;
        available_databases.insert(name.to_owned(), schema);

        Ok(())
    }

    /// Deletes a database named `name`.
    ///
    /// ## Errors
    ///
    /// * [`Error::DatabaseNotFound`]: database `name` does not exist.
    /// * [`Error::Io`]: an error occurred while deleting files.
    pub async fn delete_database(&self, name: &str) -> Result<(), Error> {
        let mut available_databases = self.data.available_databases.write().await;

        let file_path = self.database_path(name);
        let files_existed = file_path.exists();
        if file_path.exists() {
            tokio::fs::remove_dir_all(file_path).await?;
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
            return Err(Error::DatabaseNotFound(name.to_string()));
        }
    }

    /// Lists the databases on this server.
    pub async fn list_databases(&self) -> Vec<pliantdb_networking::Database<'static>> {
        let available_databases = self.data.available_databases.read().await;
        available_databases
            .iter()
            .map(|(name, schema)| pliantdb_networking::Database {
                name: Cow::Owned(name.to_owned()),
                schema: schema.clone(),
            })
            .collect()
    }

    /// Lists the [`schema::Id`]s on this server.
    pub async fn list_available_schemas(&self) -> Vec<schema::Id> {
        let available_databases = self.data.available_databases.read().await;
        available_databases.values().unique().cloned().collect()
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
    pub async fn install_self_signed_certificate(
        &self,
        server_name: &str,
        overwrite: bool,
    ) -> Result<(), Error> {
        let (certificate, private_key) = fabruic::generate_self_signed(server_name);

        if self.certificate_path().exists() && !overwrite {
            return Err(Error::Configuration(String::from("Certificate already installed. Enable overwrite if you wish to replace the existing certificate.")));
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
            .and_then(|file| file.write_all(&certificate.0))
            .await
            .map_err(|err| {
                Error::Configuration(format!("Error writing certificate file: {}", err))
            })?;
        File::create(self.private_key_path())
            .and_then(|file| file.write_all(&private_key.0))
            .await
            .map_err(|err| {
                Error::Configuration(format!("Error writing private key file: {}", err))
            })?;

        Ok(())
    }

    fn certificate_path(&self) -> PathBuf {
        self.data.directory.join("public-certificate.der")
    }

    fn private_key_path(&self) -> PathBuf {
        self.data.directory.join("public-certificate.der")
    }

    /// Listens for incoming client connections. Does not return until the
    /// server shuts down.
    pub async fn listen_on<Addrs: ToSocketAddrs + Send + Sync>(
        &self,
        addrs: Addrs,
    ) -> Result<(), Error> {
        let certificate = File::open(self.certificate_path())
            .and_then(FileExt::read_all)
            .await
            .map(Certificate)
            .map_err(|err| {
                Error::Configuration(format!("Error reading certificate file: {}", err))
            })?;
        let private_key = File::open(self.private_key_path())
            .and_then(FileExt::read_all)
            .await
            .map(PrivateKey)
            .map_err(|err| {
                Error::Configuration(format!("Error reading private key file: {}", err))
            })?;

        let mut server = Endpoint::new_server(addrs, &certificate, &private_key)?;
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
        // TODO this is ugly
        while let Some(incoming) = connection.next().await {
            let incoming = incoming?;
            println!("New incoming stream from: {}", connection.remote_address());

            let (sender, receiver) = incoming.accept_stream::<pliantdb_networking::Payload<'_>>();
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
        // TODO this is ugly
        while let Some(payload) = receiver.next().await {
            let payload = payload?;
            let request = match payload {
                Payload::Request(request) => request,
                Payload::Response(..) => {
                    todo!("fabruic should have separate types for send/receive")
                }
            };
            let response = self
                .handle_request(request)
                .await
                .unwrap_or_else(|err| Response::Error(err.to_string()));
            sender.send(&Payload::Response(response))?;
        }
        Ok(())
    }

    pub(crate) async fn handle_request(
        &self,
        request: Request<'static>,
    ) -> Result<Response<'static>, Error> {
        match request {
            Request::Server { request } => match request {
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
                    self.list_databases().await,
                ))),
                ServerRequest::ListAvailableSchemas => Ok(Response::Server(
                    ServerResponse::AvailableSchemas(self.list_available_schemas().await),
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
pub trait OpenDatabase: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &'_ dyn Any;

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &collection::Id,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error>;
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
