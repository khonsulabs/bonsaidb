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
    /// an error that occurred while interacting with a `Connection`
    #[error("error on connection: {0}")]
    Connection(#[from] connection::Error),

    /// an error from serializing or deserializing from a `Document`
    #[error("error serializing: {0}")]
    Serialization(#[from] serde_cbor::Error),
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Self::Connection(connection::Error::from(err))
    }
}

#[cfg(test)]
mod test_util;
