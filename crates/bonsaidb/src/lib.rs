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
#[cfg(feature = "files")]
#[doc(inline)]
pub use bonsaidb_files as files;
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

#[test]
fn struct_sizes() {
    fn print_size<T: Sized>() {
        println!(
            "{}: {} bytes",
            std::any::type_name::<T>(),
            std::mem::size_of::<T>()
        );
    }
    #[cfg(feature = "client")]
    {
        print_size::<client::Client>();
        print_size::<client::RemoteDatabase>();
        print_size::<client::RemoteSubscriber>();
    }
    #[cfg(feature = "local")]
    {
        print_size::<local::Storage>();
        print_size::<local::Database>();
        print_size::<local::Subscriber>();
        #[cfg(feature = "local-async")]
        {
            print_size::<local::AsyncStorage>();
            print_size::<local::AsyncDatabase>();
        }
    }
    #[cfg(feature = "server")]
    {
        print_size::<server::Server>();
        print_size::<server::ServerDatabase<server::NoBackend>>();
    }
}
