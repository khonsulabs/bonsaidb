#![doc = include_str!("../crate-docs.md")]
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
#![cfg_attr(doc, deny(rustdoc::all))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
    clippy::multiple_crate_versions, // TODO custodian-password deps + x25119 deps
)]

#[cfg(feature = "client")]
#[doc(inline)]
pub use bonsaidb_client as client;
#[doc(inline)]
pub use bonsaidb_core as core;
#[cfg(feature = "local")]
#[doc(inline)]
pub use bonsaidb_local as local;
#[cfg(feature = "server")]
#[doc(inline)]
pub use bonsaidb_server as server;
#[cfg(all(feature = "client", feature = "server"))]
mod any_connection;
#[cfg(feature = "cli")]
pub mod cli;

/// `VaultKeyStorage` implementors.
#[cfg(feature = "keystorage-s3")]
pub mod keystorage {
    #[cfg(feature = "keystorage-s3")]
    #[doc(inline)]
    pub use bonsaidb_keystorage_s3 as s3;
}
#[cfg(all(feature = "client", feature = "server"))]
pub use any_connection::*;
