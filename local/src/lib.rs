//! local storage backend for `PliantDB`

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

mod error;
mod open_trees;
mod storage;
pub use self::{
    error::Error,
    storage::{Storage, LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
};

#[cfg(test)]
mod tests;
