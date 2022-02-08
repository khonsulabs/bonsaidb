//! The `BonsaiDb` Server.

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
    clippy::missing_panics_doc, // TODO clippy::missing_panics_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

mod backend;
/// Command-line interface for the server.
#[cfg(feature = "cli")]
pub mod cli;
mod config;
mod error;
pub(crate) mod hosted;
mod server;

#[cfg(feature = "acme")]
pub use config::{
    AcmeConfiguration, LETS_ENCRYPT_PRODUCTION_DIRECTORY, LETS_ENCRYPT_STAGING_DIRECTORY,
};

pub use self::{
    backend::{
        Backend, BackendError, ConnectionHandling, CustomApiDispatcher, NoBackend, NoDispatcher,
    },
    config::{DefaultPermissions, ServerConfiguration},
    error::Error,
    server::{
        ApplicationProtocols, ConnectedClient, CustomServer, HttpService, LockedClientDataGuard,
        Peer, Server, ServerDatabase, ServerSubscriber, StandardTcpProtocols, TcpService,
        Transport,
    },
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
