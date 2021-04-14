//! A programmable document database inspired by `CouchDB` written in Rust.
//!
//! The `pliantdb` executable

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
    clippy::option_if_let_else,
)]

mod cli;

use structopt::StructOpt;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let command = cli::Command::from_args();
    command.execute(|_| {}).await
}
