//! Aysnc jobs management for `PliantDB`.

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
    clippy::option_if_let_else,
)]

/// Types related to the job [`Manager`](manager::Manager).
pub mod manager;
/// Types related to defining [`Job`]s.
pub mod task;
mod traits;

pub use self::traits::{Job, Keyed};
