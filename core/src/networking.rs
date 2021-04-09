#![allow(missing_docs)] // TODO
use std::borrow::Cow;

use async_trait::async_trait;
pub use fabruic;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    connection::{AccessPolicy, QueryKey},
    document::Document,
    schema::{
        self, collection,
        map::{self},
        view, Key,
    },
    transaction::{OperationResult, Transaction},
};

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Payload<'a> {
    pub id: u64,
    pub api: Api<'a>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Api<'a> {
    Request(Request<'a>),
    Response(Response<'a>),
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum Request<'a> {
    Server(ServerRequest<'a>),
    Database {
        database: Cow<'a, str>,
        request: DatabaseRequest<'a>,
    },
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum ServerRequest<'a> {
    CreateDatabase(Database<'a>),
    DeleteDatabase { name: Cow<'a, str> },
    ListDatabases,
    ListAvailableSchemas,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub enum DatabaseRequest<'a> {
    Get {
        collection: collection::Id,
        id: u64,
    },
    GetMultiple {
        collection: collection::Id,
        ids: Vec<u64>,
    },
    Query {
        view: Cow<'a, str>,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
        with_docs: bool,
    },
    ApplyTransaction {
        transaction: Transaction<'a>,
    },
}
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Response<'a> {
    Server(ServerResponse<'a>),
    Database(DatabaseResponse<'a>),
    Error(crate::Error),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ServerResponse<'a> {
    DatabaseCreated { name: Cow<'a, str> },
    DatabaseDeleted { name: Cow<'a, str> },
    Databases(Vec<Database<'a>>),
    AvailableSchemas(Vec<schema::Id>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum DatabaseResponse<'a> {
    Documents(Vec<Document<'a>>),
    TransactionResults(Vec<OperationResult>),
    ViewMappings(Vec<map::Serialized>),
    ViewMappingsWithDocs(Vec<MappedDocument>),
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedDocument {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub source: Document<'static>,
}

impl MappedDocument {
    pub fn deserialized<K: Key, V: Serialize + DeserializeOwned>(
        self,
    ) -> Result<map::MappedDocument<K, V>, crate::Error> {
        let key = Key::from_big_endian_bytes(&self.key)
            .map_err(|err| crate::Error::Storage(view::Error::KeySerialization(err).to_string()))?;
        let value = serde_cbor::from_slice(&self.value)
            .map_err(|err| crate::Error::Storage(view::Error::from(err).to_string()))?;

        Ok(map::MappedDocument {
            document: self.source,
            key,
            value,
        })
    }
}

#[derive(Clone, PartialEq, Deserialize, Serialize, Debug)]
pub struct Database<'a> {
    pub name: Cow<'a, str>,
    pub schema: schema::Id,
}

#[async_trait]
pub trait ServerConnection: Send + Sync {
    /// Creates a database named `name` using the [`schema::Id`] `schema`.
    ///
    /// ## Errors
    ///
    /// * [`Error::InvalidDatabaseName`]: `name` must begin with an alphanumeric
    ///   character (`[a-zA-Z0-9]`), and all remaining characters must be
    ///   alphanumeric, a period (`.`), or a hyphen (`-`).
    /// * [`Error::DatabaseNameAlreadyTaken]: `name` was already used for a
    ///   previous database name. Database names are case insensitive.
    async fn create_database(&self, name: &str, schema: schema::Id) -> Result<(), crate::Error>;

    /// Deletes a database named `name`.
    ///
    /// ## Errors
    ///
    /// * [`Error::DatabaseNotFound`]: database `name` does not exist.
    /// * [`Error::Core(core::Error::Io)`]: an error occurred while deleting files.
    async fn delete_database(&self, name: &str) -> Result<(), crate::Error>;

    /// Lists the databases on this server.
    async fn list_databases(&self) -> Result<Vec<Database<'static>>, crate::Error>;

    /// Lists the [`schema::Id`]s on this server.
    async fn list_available_schemas(&self) -> Result<Vec<schema::Id>, crate::Error>;
}

#[derive(Clone, thiserror::Error, Debug, Serialize, Deserialize)]
pub enum Error {
    /// An invalid database name was specified. See
    /// [`ServerConnection::create_database()`] for database name requirements.
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(String),

    /// The database name given was not found.
    #[error("database '{0}' was not found")]
    DatabaseNotFound(String),

    /// The database name already exists.
    #[error("a database with name '{0}' already exists")]
    DatabaseNameAlreadyTaken(String),

    /// The server responded with a message that wasn't expected for the request
    /// sent.
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Disconnected,

    /// The database named `database_name` was created with a different schema
    /// (`stored_schema`) than provided (`schema`).
    #[error(
        "database '{database_name}' was created with schema '{stored_schema}', not '{schema}'"
    )]
    SchemaMismatch {
        /// The name of the database being accessed.
        database_name: String,

        /// The schema provided for the database.
        schema: schema::Id,

        /// The schema stored for the database.
        stored_schema: schema::Id,
    },

    /// The [`schema::Id`] returned has already been registered with this server.
    #[error("schema '{0}' was already registered")]
    SchemaAlreadyRegistered(schema::Id),

    /// The [`schema::Id`] requested was not registered with this server.
    #[error("schema '{0}' is not registered with this server")]
    SchemaNotRegistered(schema::Id),
}
