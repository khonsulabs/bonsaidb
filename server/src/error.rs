use std::sync::Arc;

use bonsaidb_local::core::{self, schema};
use schema::InvalidNameError;

/// An error occurred while interacting with a [`Server`](crate::Server).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An error occurred from the QUIC transport layer.
    #[error("a networking error occurred: '{0}'")]
    Transport(String),

    #[cfg(feature = "websockets")]
    /// An error occurred from the Websocket transport layer.
    #[error("a websocket error occurred: '{0}'")]
    Websocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// An error occurred from IO
    #[error("a networking error occurred: '{0}'")]
    Io(#[from] tokio::io::Error),

    /// An error occurred while processing a request
    #[error("an error occurred processing a request: '{0}'")]
    Request(Arc<anyhow::Error>),

    /// An error occurred from within the schema.
    #[error("error from core {0}")]
    Core(#[from] core::Error),

    /// An error occurred while interacting with a local database.
    #[error("an error occurred interacting with a database: {0}")]
    Database(#[from] bonsaidb_local::Error),
}

impl From<Error> for core::Error {
    fn from(other: Error) -> Self {
        // without it, there's no way to get this to_string() easily.
        match other {
            Error::Database(storage) => Self::Database(storage.to_string()),
            Error::Core(core) => core,
            Error::Io(io) => Self::Io(io.to_string()),
            Error::Transport(networking) => Self::Transport(networking),
            #[cfg(feature = "websockets")]
            Error::Websocket(err) => Self::Websocket(err.to_string()),
            Error::Request(err) => Self::Server(err.to_string()),
        }
    }
}

impl From<InvalidNameError> for Error {
    fn from(err: InvalidNameError) -> Self {
        Self::Core(core::Error::InvalidName(err))
    }
}

pub trait ResultExt<R> {
    fn map_err_to_core(self) -> Result<R, core::Error>
    where
        Self: Sized;
}

impl<R> ResultExt<R> for Result<R, Error> {
    fn map_err_to_core(self) -> Result<R, core::Error>
    where
        Self: Sized,
    {
        self.map_err(core::Error::from)
    }
}

#[cfg(feature = "websockets")]
impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Core(core::Error::Websocket(format!(
            "error deserializing message: {:?}",
            other
        )))
    }
}

macro_rules! impl_from_fabruic {
    ($error:ty) => {
        impl From<$error> for Error {
            fn from(other: $error) -> Self {
                Self::Core(bonsaidb_core::Error::Transport(other.to_string()))
            }
        }
    };
}

impl_from_fabruic!(fabruic::error::CertificateChain);
impl_from_fabruic!(fabruic::error::Receiver);
impl_from_fabruic!(fabruic::error::Connecting);
impl_from_fabruic!(fabruic::error::PrivateKey);
impl_from_fabruic!(fabruic::error::KeyPair);
impl_from_fabruic!(fabruic::error::Connection);
impl_from_fabruic!(fabruic::error::Incoming);
impl_from_fabruic!(fabruic::error::AlreadyClosed);
