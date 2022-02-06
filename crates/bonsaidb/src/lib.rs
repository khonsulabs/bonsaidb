#![doc = include_str!("../crate-docs.md")]
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
    clippy::multiple_crate_versions, // TODO custodian-password deps + x25119 deps
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

#[cfg(feature = "client")]
pub use bonsaidb_client as client;
pub use bonsaidb_core as core;
#[cfg(feature = "local")]
pub use bonsaidb_local as local;
#[cfg(feature = "server")]
pub use bonsaidb_server as server;
#[cfg(feature = "cli")]
pub mod cli;

/// `VaultKeyStorage` implementors.
#[cfg(feature = "keystorage-s3")]
pub mod keystorage {
    pub use bonsaidb_keystorage_s3 as s3;
}

#[cfg(all(feature = "client", feature = "server"))]
mod any_connection;
#[cfg(all(feature = "client", feature = "server"))]
pub use any_connection::{AnyDatabase, AnyServerConnection};
