//! Local database tool to dump and load databases into plain an easy-to-consume
//! filesystem structure.

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

mod config;
mod dump;
mod error;
mod open_trees;
mod storage;
mod tasks;
mod views;

pub use error::Error;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = dump::Cli::from_args();
    args.command.execute(args.database_path).await
}
