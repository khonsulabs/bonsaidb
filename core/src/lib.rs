//! Core functionality and types for `PliantDb`.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
)]
pub use num_traits;

/// Types for creating and validating permissions.
pub mod permissions;

/// Types for interacting with `PliantDb`.
pub mod connection;
/// Types for interacting with `Document`s.
pub mod document;
/// Limits used within `PliantDb`.
pub mod limits;
/// Types for defining database schema.
pub mod schema;
/// Types for executing transactions.
pub mod transaction;

/// Types for utilizing a lightweight atomic Key-Value store.
pub mod kv;

#[cfg(feature = "networking")]
pub use fabruic;
#[cfg(feature = "networking")]
/// Types for implementing the `PliantDb` network protocol.
pub mod networking;

#[cfg(feature = "pubsub")]
/// Types for Publish/Subscribe (`PubSub`) messaging.
pub mod pubsub;
#[cfg(feature = "pubsub")]
pub use circulate;
use schema::{CollectionName, SchemaName, ViewName};
use serde::{Deserialize, Serialize};

/// an enumeration of errors that this crate can produce
#[derive(Clone, thiserror::Error, Debug, Serialize, Deserialize)]
pub enum Error {
    /// The database named `database_name` was created with a different schema
    /// (`stored_schema`) than provided (`schema`).
    #[error(
        "database '{database_name}' was created with schema '{stored_schema}', not '{schema}'"
    )]
    SchemaMismatch {
        /// The name of the database being accessed.
        database_name: String,

        /// The schema provided for the database.
        schema: SchemaName,

        /// The schema stored for the database.
        stored_schema: SchemaName,
    },

    /// The [`SchemaName`] returned has already been registered with this server.
    #[error("schema '{0}' was already registered")]
    SchemaAlreadyRegistered(SchemaName),

    /// The [`SchemaName`] requested was not registered with this server.
    #[error("schema '{0}' is not registered with this server")]
    SchemaNotRegistered(SchemaName),

    /// An invalid database name was specified. See
    /// [`ServerConnection::create_database()`](connection::ServerConnection::create_database)
    /// for database name requirements.
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(String),

    /// The database name given was not found.
    #[error("database '{0}' was not found")]
    DatabaseNotFound(String),

    /// The database name already exists.
    #[error("a database with name '{0}' already exists")]
    DatabaseNameAlreadyTaken(String),

    /// An error from interacting with local storage.
    #[error("error from storage: {0}")]
    Database(String),

    /// An error from interacting with a server.
    #[error("error from server: {0}")]
    Server(String),

    /// An error occurred from the QUIC transport layer.
    #[error("a transport error occurred: '{0}'")]
    Transport(String),

    /// An error occurred from the websocket transport layer.
    #[cfg(feature = "websockets")]
    #[error("a websocket error occurred: '{0}'")]
    Websocket(String),

    /// An error occurred from networking.
    #[cfg(feature = "networking")]
    #[error("a networking error occurred: '{0}'")]
    Networking(networking::Error),

    /// An error occurred from IO.
    #[error("an io error occurred: '{0}'")]
    Io(String),

    /// An error occurred with the provided configuration options.
    #[error("a configuration error occurred: '{0}'")]
    Configuration(String),

    /// An error occurred inside of the client.
    #[error("an io error in the client: '{0}'")]
    Client(String),

    /// An attempt to use a `Collection` with a `Database` that it wasn't defined within.
    #[error("attempted to access a collection not registered with this schema")]
    CollectionNotFound,

    /// An attempt to update a document that doesn't exist.
    #[error("the requested document id {1} from collection {0} was not found")]
    DocumentNotFound(CollectionName, u64),

    /// When updating a document, if a situation is detected where the contents have changed on the server since the `Revision` provided, a Conflict error will be returned.
    #[error("a conflict was detected while updating document id {1} from collection {0}")]
    DocumentConflict(CollectionName, u64),

    /// When saving a document in a collection with unique views, a document emits a key that is already emitted by an existing ocument, this error is returned.
    #[error("a unique key violation occurred: document `{existing_document_id}` already has the same key as `{conflicting_document_id}` for {view}")]
    UniqueKeyViolation {
        /// The name of the view that the unique key violation occurred.
        view: ViewName,
        /// The document that caused the violation.
        conflicting_document_id: u64,
        /// The document that already uses the same key.
        existing_document_id: u64,
    },

    /// An invalid name was specified during schema creation.
    #[error("an invalid name was used in a schema: {0}")]
    InvalidName(#[from] schema::InvalidNameError),

    /// Permission was denied.
    #[error("permission error: {0}")]
    PermissionDenied(#[from] actionable::PermissionDenied),
}

impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Self {
        Self::Database(err.to_string())
    }
}

/// Shared schemas and utilities used for unit testing.
#[cfg(any(feature = "test-util", test))]
#[allow(missing_docs)]
pub mod test_util;
