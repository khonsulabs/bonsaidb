//! Local storage backend for `PliantDB`.

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
    // clippy::missing_panics_doc, // not on stable yet
    clippy::option_if_let_else,
)]

/// Configuration options.
pub mod config;
mod error;
mod open_trees;
mod storage;
mod tasks;
mod views;

#[doc(inline)]
pub use pliantdb_core as core;

pub use self::{config::Configuration, error::Error, storage::Storage};

#[cfg(feature = "cli")]
pub mod backup;

#[cfg(test)]
mod tests;
