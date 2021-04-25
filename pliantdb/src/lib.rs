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
//! No feature flags are enabled by default in the `pliantdb` crate. This is
//! because in most Rust executables, you will only need a subset of the
//! functionality. If you'd prefer to enable everything, you can use the `full`
//! feature:
//!
//! ```toml
//! [dependencies]
//! pliantdb = { version = "*", default-features = false, features = "full" }
//! ```
//!
//! - `full`: Enables `local-full`, `server-full`, and `client-full`.
//! - `cli`: Enables the `pliantdb` executable.
//!
//! ### Local databases only
//!
//! ```toml
//! [dependencies]
//! pliantdb = { version = "*", default-features = false, features = "local-full" }
//! ```
//!
//! - `local-full`: Enables `local`, `local-cli`, `local-keyvalue`, and
//!   `local-pubsub`
//! - `local`: Enables the [`local`] module, which re-exports the crate
//!   `pliantdb-local`.
//! - `local-cli`: Enables the `StructOpt` structures for embedding database
//!   management commands into your own command-line interface.
//! - `local-pubsub`: Enables `PubSub` for `pliantdb-local`.
//! - `local-keyvalue`: Enables the key-value store for `pliantdb-local`.
//!
//! ### `PliantDB` server
//!
//! ```toml
//! [dependencies]
//! pliantdb = { version = "*", default-features = false, features = "server-full" }
//! ```
//!
//! - `server-full`: Enables `server`, `server-websockets`, `server-keyvalue`,
//!   and `server-pubsub`
//! - `server`: Enables the [`server`] module, which re-exports the crate
//!   `pliantdb-server`.
//! - `server-websockets`: Enables `WebSocket` support for `pliantdb-server`.
//! - `server-pubsub`: Enables `PubSub` for `pliantdb-server`.
//! - `server-keyvalue`: Enables the key-value store for `pliantdb-server`.
//!
//! ### Client for accessing a `PliantDB` server.
//!
//! ```toml
//! [dependencies]
//! pliantdb = { version = "*", default-features = false, features = "client-full" }
//! ```
//!
//! - `client-full`: Enables `client`, `client-trusted-dns`,
//!   `client-websockets`, `client-keyvalue`, and `client-pubsub`
//! - `client`: Enables the [`client`] module, which re-exports the crate
//!   `pliantdb-client`.
//! - `client-trusted-dns`: Enables using trust-dns for DNS resolution. If not
//!   enabled, all DNS resolution is done with the OS's default name resolver.
//! - `client-websockets`: Enables `WebSocket` support for `pliantdb-client`.
//! - `client-pubsub`: Enables `PubSub` for `pliantdb-client`.
//! - `client-keyvalue`: Enables the key-value store for `pliantdb-client`.

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
