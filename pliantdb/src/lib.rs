//! A programmable document database inspired by `CouchDB` written in Rust.

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

#[doc(inline)]
pub use pliantdb_core as core;
#[cfg(feature = "local")]
#[doc(inline)]
pub use pliantdb_local as local;
#[cfg(feature = "server")]
#[doc(inline)]
pub use pliantdb_server as server;
#[cfg(feature = "cli")]
pub mod cli;
