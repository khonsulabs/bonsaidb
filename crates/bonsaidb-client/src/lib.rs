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
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

pub use url;

mod builder;
mod client;
mod error;

#[cfg(not(target_arch = "wasm32"))]
pub use fabruic;

pub use self::{
    builder::Builder,
    client::{Client, CustomApiCallback, RemoteDatabase, RemoteSubscriber},
    error::Error,
};
