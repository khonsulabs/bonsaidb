//! Core functionality and types for BonsaiDb.

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
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

/// Types for creating and validating permissions.
pub mod permissions;

/// Database administration types and functionality.
pub mod admin;
/// Types for interacting with BonsaiDb.
pub mod connection;
/// Types for interacting with `Document`s.
pub mod document;
/// Limits used within BonsaiDb.
pub mod limits;
/// Types for defining database schema.
pub mod schema;
/// Types for executing transactions.
pub mod transaction;

/// Types for utilizing a lightweight atomic Key-Value store.
pub mod keyvalue;

/// Traits for tailoring a server.
pub mod custom_api;

#[cfg(feature = "networking")]
/// Types for implementing the BonsaiDb network protocol.
pub mod networking;

/// Types for Publish/Subscribe (`PubSub`) messaging.
pub mod pubsub;

use std::string::FromUtf8Error;

pub use actionable;
pub use arc_bytes;
pub use async_trait;
pub use circulate;
pub use num_traits;
pub use ordered_varint;
use schema::{view, CollectionName, SchemaName, ViewName};
use serde::{Deserialize, Serialize};
pub use transmog;
pub use transmog_pot;

use crate::{document::Header, schema::InsertError};

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

    /// The [`SchemaName`] returned has already been registered.
    #[error("schema '{0}' was already registered")]
    SchemaAlreadyRegistered(SchemaName),

    /// The [`SchemaName`] requested was not registered.
    #[error("schema '{0}' is not registered")]
    SchemaNotRegistered(SchemaName),

    /// An invalid database name was specified. See
    /// [`StorageConnection::create_database()`](connection::StorageConnection::create_database)
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

    /// An error serializing data.
    #[error("error serializing: {0}")]
    Serialization(String),

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

    /// A `Collection` being added already exists. This can be caused by a collection name not being unique.
    #[error("attempted to define a collection that already has been defined")]
    CollectionAlreadyDefined,

    /// An attempt to update a document that doesn't exist.
    #[error("the requested document id {1} from collection {0} was not found")]
    DocumentNotFound(CollectionName, u64),

    /// When updating a document, if a situation is detected where the contents have changed on the server since the `Revision` provided, a Conflict error will be returned.
    #[error("a conflict was detected while updating document id {1} from collection {0}")]
    DocumentConflict(CollectionName, u64),

    /// When saving a document in a collection with unique views, a document emits a key that is already emitted by an existing ocument, this error is returned.
    #[error("a unique key violation occurred: document `{existing_document}` already has the same key as `{conflicting_document}` for {view}")]
    UniqueKeyViolation {
        /// The name of the view that the unique key violation occurred.
        view: ViewName,
        /// The document that caused the violation.
        conflicting_document: Header,
        /// The document that already uses the same key.
        existing_document: Header,
    },

    /// An invalid name was specified during schema creation.
    #[error("an invalid name was used in a schema: {0}")]
    InvalidName(#[from] schema::InvalidNameError),

    /// Permission was denied.
    #[error("permission error: {0}")]
    PermissionDenied(#[from] actionable::PermissionDenied),

    /// An internal error handling passwords was encountered.
    #[error("error with password: {0}")]
    Password(String),

    /// The user specified was not found. This will not be returned in response
    /// to an invalid username being used during login. It will be returned in
    /// other APIs that operate upon users.
    #[error("user not found")]
    UserNotFound,

    /// An error occurred converting from bytes to Utf-8.
    #[error("invalid string: {0}")]
    InvalidUnicode(String),

    /// The credentials specified are not valid.
    #[error("invalid credentials")]
    InvalidCredentials,

    /// Returned when the a view's reduce() function is unimplemented.
    #[error("reduce is unimplemented")]
    ReduceUnimplemented,

    /// A floating point operation yielded Not a Number.
    #[error("floating point operation yielded NaN")]
    NotANumber,
}

impl From<pot::Error> for Error {
    fn from(err: pot::Error) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl<T> From<InsertError<T>> for Error {
    fn from(err: InsertError<T>) -> Self {
        err.error
    }
}

impl From<view::Error> for Error {
    fn from(err: view::Error) -> Self {
        Self::Database(err.to_string())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Self::InvalidUnicode(err.to_string())
    }
}

/// Shared schemas and utilities used for unit testing.
#[cfg(any(feature = "test-util", test))]
#[allow(missing_docs)]
pub mod test_util;

/// When true, encryption was enabled at build-time.
#[cfg(feature = "encryption")]
pub const ENCRYPTION_ENABLED: bool = true;

/// When true, encryption was enabled at build-time.
#[cfg(not(feature = "encryption"))]
pub const ENCRYPTION_ENABLED: bool = false;

/// A type that implements [`Error`](std::error::Error) and is threadsafe.
pub trait AnyError: std::error::Error + Send + Sync + 'static {}

impl<T> AnyError for T where T: std::error::Error + Send + Sync + 'static {}
