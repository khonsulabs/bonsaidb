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
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::missing_panics_doc, // TODO clippy::missing_panics_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

mod async_io_util;
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
    backend::{Backend, BackendError, ConnectionHandling, CustomApiDispatcher, NoDispatcher},
    config::{Configuration, DefaultPermissions, StorageConfiguration},
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

pub use bonsaidb_local as local;
pub use fabruic;
