//! Core functionality and types for `PliantDB`.

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

pub use circulate;
#[cfg(feature = "networking")]
pub use fabruic;

/// Types for interacting with a database.
pub mod connection;
/// Types for interacting with `Document`s.
pub mod document;
/// Limits used within `PliantDB`.
pub mod limits;
#[cfg(feature = "networking")]
/// Types for networked communications.
pub mod networking;
/// Types for defining database schema.
pub mod schema;
/// Types for executing transactions.
pub mod transaction;

/// Types for Publish/Subscribe (`PubSub`) messaging.
pub mod pubsub;

use schema::CollectionName;
use serde::{Deserialize, Serialize};

/// an enumeration of errors that this crate can produce
#[derive(Clone, thiserror::Error, Debug, Serialize, Deserialize)]
pub enum Error {
    /// An error from interacting with local storage.
    #[error("error from storage: {0}")]
    Storage(String),

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

    /// An invalid name was specified during schema creation.
    #[error("an invalid name was used in a schema: {0}")]
    InvalidName(#[from] schema::InvalidNameError),
}

impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Self {
        Self::Storage(err.to_string())
    }
}

/// Shared schemas and utilities used for unit testing.
#[cfg(any(feature = "test-util", test))]
#[allow(missing_docs)]
pub mod test_util;
