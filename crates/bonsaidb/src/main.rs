//! A programmable document database inspired by `CouchDB` written in Rust.
//!
//! The `bonsaidb` executable

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
    clippy::option_if_let_else,
    clippy::multiple_crate_versions, // TODO custodian-password deps + x25119 deps
)]

mod any_connection;
mod cli;
pub use any_connection::*;
use bonsaidb_server::{NoBackend, ServerConfiguration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run::<NoBackend>(ServerConfiguration::default()).await
}
