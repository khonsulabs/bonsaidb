#![doc = include_str!(".crate-docs.md")]
#![cfg_attr(not(feature = "included-from-omnibus"), doc = include_str!("../server-feature-flags.md"))]
#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::missing_panics_doc, // TODO clippy::missing_panics_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

/// Types for defining API handlers.
pub mod api;
mod backend;
/// Command-line interface for the server.
#[cfg(feature = "cli")]
pub mod cli;
mod config;
mod dispatch;
mod error;
pub(crate) mod hosted;
mod server;

#[cfg(feature = "acme")]
pub use config::{
    AcmeConfiguration, LETS_ENCRYPT_PRODUCTION_DIRECTORY, LETS_ENCRYPT_STAGING_DIRECTORY,
};

pub use self::backend::{Backend, BackendError, ConnectionHandling, NoBackend};
pub use self::config::{BonsaiListenConfig, DefaultPermissions, ServerConfiguration};
pub use self::error::Error;
pub use self::server::{
    ApplicationProtocols, ConnectedClient, CustomServer, HttpService, LockedClientDataGuard, Peer,
    Server, ServerDatabase, StandardTcpProtocols, TcpService, Transport,
};

#[cfg(test)]
mod tests;

#[cfg(any(feature = "test-util", test))]
pub mod test_util;

#[cfg(not(feature = "included-from-omnibus"))]
pub use bonsaidb_core as core;
#[cfg(not(feature = "included-from-omnibus"))]
pub use bonsaidb_local as local;
pub use fabruic;
