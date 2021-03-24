//! pliantdb (name not set in stone) is a programmable document database inspired by `CouchDB` written in Rust.

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

use schema::collection;
use uuid::Uuid;

/// types for interacting with a database
pub mod connection;
/// types for interacting with `Document`s
pub mod document;
/// types for defining database schema
pub mod schema;
/// types for interacting with a local, file-based database
pub mod storage;
/// types for executing transactions
pub mod transaction;

/// an enumeration of errors that this crate can produce
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// an error from serializing or deserializing from a `Document`
    #[error("error working with storage: {0}")]
    Storage(#[from] storage::Error),

    /// an attempt to use a `Collection` with a `Database` that it wasn't defined within
    #[error("attempted to access a collection not registered with this schema")]
    CollectionNotFound,

    // TODO consider moving these to something like a document::Error
    /// an attempt to update a document that doesn't exist
    #[error("the requested document id {1} from collection {0} was not found")]
    DocumentNotFound(collection::Id, Uuid),

    /// when updating a document, if a situation is detected where the contents have changed on the server since the `Revision` provided, a Conflict error will be returned.
    #[error("a conflict was detected while updating document id {1} from collection {0}")]
    DocumentConflict(collection::Id, Uuid),
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Self::Storage(storage::Error::from(err))
    }
}

impl From<serde_cbor::Error> for Error {
    fn from(err: serde_cbor::Error) -> Self {
        Self::Storage(storage::Error::from(err))
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Self::Storage(storage::Error::from(err))
    }
}

#[cfg(test)]
mod test_util;
