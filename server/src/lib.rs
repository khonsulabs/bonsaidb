//! The `PliantDB` Server.

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
    clippy::missing_errors_doc, // TODO
    clippy::missing_panics_doc, // TODO
    clippy::option_if_let_else,
)]

mod admin;
mod async_io_util;
/// Command-line interface for the server.
#[cfg(feature = "cli")]
pub mod cli;
mod error;
mod hosted;
mod server;

pub use self::{error::Error, server::Server};

#[cfg(test)]
mod tests;
