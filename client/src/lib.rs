//! Client for `bonsaidb-server`.

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
)]

pub use url;

mod builder;
mod client;
mod error;

pub use fabruic;

#[cfg(feature = "pubsub")]
pub use self::client::RemoteSubscriber;
pub use self::{
    builder::Builder,
    client::{Client, CustomApiCallback, RemoteDatabase},
    error::Error,
};
