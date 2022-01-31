//! Local storage backend for `BonsaiDb`.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
    clippy::module_name_repetitions,
)]

/// Command-line interface helpers.
#[cfg(feature = "cli")]
pub mod cli;
/// Configuration options.
pub mod config;
mod database;
mod error;
pub mod jobs;
mod open_trees;
mod storage;
mod tasks;
#[cfg(feature = "encryption")]
pub mod vault;
mod views;

#[cfg(feature = "password-hashing")]
pub use argon2;
#[cfg(not(feature = "included-from-omnibus"))]
pub use bonsaidb_core as core;

pub use self::{
    database::{pubsub::Subscriber, Database},
    error::Error,
    storage::{BackupLocation, Storage, StorageId},
};

#[cfg(test)]
mod tests;
