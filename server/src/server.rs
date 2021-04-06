use std::{
    any::Any,
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    net::ToSocketAddrs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use admin::database::{self, Database};
use futures::{FutureExt, StreamExt, TryFutureExt};
use pliantdb_core::schema;
use pliantdb_local::{
    core::{
        connection::Connection,
        schema::{Schema, Schematic},
    },
    Configuration, Storage,
};
use pliantdb_networking::{
    fabruic::{self, Certificate, Endpoint, PrivateKey},
    Payload, Request, Response, ServerRequest, ServerResponse,
};
use tokio::{fs::File, sync::RwLock};

use crate::{
    admin::{self, database::ByName, Admin},
    async_io_util::FileExt,
    error::Error,
    hosted,
};

mod shutdown;

/// A `PliantDB` server.
#[derive(Clone, Debug)]
pub struct Server {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    endpoint: RwLock<Option<Endpoint>>,
    shutdown: shutdown::Signal,
    directory: PathBuf,
    admin: Storage<Admin>,
    schemas: HashMap<schema::Id, Schematic>,
    open_databases: RwLock<HashMap<String, Box<dyn OpenDatabase>>>,
    available_databases: RwLock<HashMap<String, schema::Id>>,
}

impl Server {
    /// Creates or opens a [`Server`] with its data stored in `directory`.
    /// `schemas` is a collection of [`schema::Id`] to [`Schematic`] pairs. [`schema::Id`]s are used as an identifier of a specific `Schema`, which the Server uses to
    pub async fn open(
        directory: &Path,
        schemas: HashMap<schema::Id, Schematic>,
    ) -> Result<Self, Error> {
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
                schemas,
                shutdown: shutdown::Signal::new(),
                available_databases: RwLock::new(available_databases),
                endpoint: RwLock::default(),
                open_databases: RwLock::default(),
            }),
        })
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

    pub(crate) async fn open_database<DB: Schema>(&self, name: &str) -> Result<Storage<DB>, Error> {
        // If we have an open database return it
        {
            let open_databases = self.data.open_databases.read().await;
            if let Some(db) = open_databases.get(name) {
                let storage = db
                    .as_any()
                    .downcast_ref::<Storage<DB>>()
                    .expect("schema did not match");
                return Ok(storage.clone());
            }
        }

        // Open the database.
        let mut open_databases = self.data.open_databases.write().await;
        if self
            .data
            .admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .is_empty()
        {
            return Err(Error::DatabaseNotFound(name.to_string()));
        }

        let db = Storage::<DB>::open_local(
            self.data.directory.join(format!("{}.pliantdb", name)),
            &Configuration::default(),
        )
        .await?;
        open_databases.insert(name.to_owned(), Box::new(db.clone()));
        Ok(db)
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
        while let Some(incoming) = futures::try_join!(
            async { Ok(connection.next().await) },
            self.wait_for_shutdown()
        )
        .unwrap_or_default()
        .0
        {
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
        while let Some(payload) = futures::try_join!(
            async { Ok(receiver.next().await) },
            self.wait_for_shutdown()
        )
        .unwrap_or_default()
        .0
        {
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

    async fn wait_for_shutdown(&self) -> Result<(), Error> {
        self.data
            .shutdown
            .wait()
            .then(|_| async { Err::<(), Error>(Error::ShuttingDown) })
            .await
    }

    async fn handle_request(&self, request: Request<'static>) -> Result<Response<'static>, Error> {
        match request {
            Request::Server { request } => match request {
                ServerRequest::CreateDatabase(database) => {
                    self.create_database(database.name.as_ref(), database.schema)
                        .await?;
                    Ok(Response::Server(ServerResponse::DatabaseCreated {
                        name: database.name.clone(),
                    }))
                }
                // ServerRequest::DeleteDatabase { name } => {},
                // ServerRequest::ListDatabases => todo!(),
                // ServerRequest::ListAvailableSchemas => todo!(),
            },
            // Request::Database { database, request } => todo!(),
        }
    }

    /// Shuts the server down.
    pub async fn shutdown(&self, timeout: Option<Duration>) -> Result<(), Error> {
        self.data.shutdown.shutdown();

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

        Ok(())
    }
}

trait OpenDatabase: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &'_ dyn Any;
}

impl<DB> OpenDatabase for Storage<DB>
where
    DB: Schema,
{
    fn as_any(&self) -> &'_ dyn Any {
        self
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
