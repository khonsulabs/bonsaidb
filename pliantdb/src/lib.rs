//! A programmable document database inspired by `CouchDB` written in Rust.
//!
//! This crate provides a convenient way to access all functionality of
//! `PliantDB`. The crates that are re-exported are:
//!
//! - [`pliantdb-core`](https://docs.rs/pliantdb-core): Common types and traits
//!   used when interacting with `PliantDB`.
//! - [`pliantdb-local`](https://docs.rs/pliantdb-local): Local, file-based
//!   database implementation.
//! - [`pliantdb-server`](https://docs.rs/pliantdb-server): Multi-database
//!   networked server implementation.
//! - [`pliantdb-client`](https://docs.rs/pliantdb-client): Client to access a
//!   `PliantDB` server.
//!
//! ## Feature Flags
//!
//! By default, `cli` is enabled, which also enables `full`.
//!
//! - `full`: Enables `local`, `server`, `client`, `websockets`, `trusted-dns`
//! - `cli`: Enables the `pliantdb` executable, as well as `StructOpt` structures
//!   for embedding into your own command-line interface.
//! - `local`: Enables the [`local`] module, which re-exports the crate
//!   `pliantdb-local`.
//! - `server`: Enables the [`server`] module, which re-exports the crate
//!   `pliantdb-server`.
//! - `client`: Enables the [`client`] module, which re-exports the crate
//!   `pliantdb-client`.
//! - `websockets`: Enables `WebSocket` support for `server` and `client`.
//! - `trusted-dns`: Enables using [trust-dns](https://lib.rs/trust-dns) for DNS
//!   resolution within `pliantdb-client`.

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
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
)]

#[cfg(feature = "client")]
#[doc(inline)]
pub use pliantdb_client as client;
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
