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
#![cfg_attr(doc, warn(rustdoc))]
#![allow(
    clippy::missing_errors_doc, // TODO
    // clippy::missing_panics_doc, // not on stable yet
    clippy::option_if_let_else,
)]

/// Types for interacting with a database.
pub mod connection;
/// Types for interacting with `Document`s.
pub mod document;
/// Limits used within `PliantDB`.
pub mod limits;
/// Types for defining database schema.
pub mod schema;
/// Types for executing transactions.
pub mod transaction;

use schema::collection;

/// an enumeration of errors that this crate can produce
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error from interacting with local storage.
    #[error("error from storage: {0}")]
    Storage(String),

    /// An error from interacting with a server.
    #[error("error from server: {0}")]
    Server(String),

    /// An attempt to use a `Collection` with a `Database` that it wasn't defined within.
    #[error("attempted to access a collection not registered with this schema")]
    CollectionNotFound,

    // TODO consider moving these to something like a document::Error
    /// An attempt to update a document that doesn't exist.
    #[error("the requested document id {1} from collection {0} was not found")]
    DocumentNotFound(collection::Id, u64),

    /// When updating a document, if a situation is detected where the contents have changed on the server since the `Revision` provided, a Conflict error will be returned.
    #[error("a conflict was detected while updating document id {1} from collection {0}")]
    DocumentConflict(collection::Id, u64),
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
