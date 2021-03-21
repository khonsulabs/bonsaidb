#![forbid(unsafe_code)]
#![warn(
    // TODO clippy::cargo,
    // TODO missing_docs,
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

pub mod connection;
pub mod schema;
pub mod storage;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error on connection: {0}")]
    Connection(#[from] connection::Error),
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
