//! The `PliantDb` Server.

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
)]

mod async_io_util;
mod backend;
/// Command-line interface for the server.
#[cfg(feature = "cli")]
pub mod cli;
mod config;
mod error;
mod server;

pub use self::{
    backend::Backend,
    config::Configuration,
    error::Error,
    server::{CustomServer, Server},
};

#[cfg(test)]
mod tests;

#[cfg(any(feature = "test-util", test))]
pub mod test_util;

pub use pliantdb_local as local;
