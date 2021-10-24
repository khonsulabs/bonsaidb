use std::sync::Arc;

use actionable::PermissionDenied;
use bonsaidb_local::core::{self, schema, AnyError};
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
    Request(Arc<dyn AnyError>),

    /// An error occurred from within the schema.
    #[error("error from core {0}")]
    Core(#[from] core::Error),

    /// An internal error occurred while waiting for a message.
    #[error("error while waiting for a message: {0}")]
    InternalCommunication(#[from] flume::RecvError),

    /// An error occurred while interacting with a local database.
    #[error("an error occurred interacting with a database: {0}")]
    Database(#[from] bonsaidb_local::Error),

    /// An error occurred with a certificate.
    #[error("a certificate error: {0}")]
    Certificate(#[from] fabruic::error::Certificate),
    /// An error occurred with handling opaque-ke.
    #[error("an opaque-ke error: {0}")]
    Password(#[from] core::custodian_password::Error),
}

impl From<Error> for core::Error {
    fn from(other: Error) -> Self {
        // without it, there's no way to get this to_string() easily.
        match other {
            Error::Database(storage) => Self::Database(storage.to_string()),
            Error::Core(core) => core,
            Error::Io(io) => Self::Io(io.to_string()),
            Error::Transport(networking) => Self::Transport(networking),
            Error::InternalCommunication(err) => Self::Server(err.to_string()),
            Error::Certificate(err) => Self::Server(err.to_string()),
            #[cfg(feature = "websockets")]
            Error::Websocket(err) => Self::Websocket(err.to_string()),
            Error::Request(err) => Self::Server(err.to_string()),
            Error::Password(err) => Self::Server(err.to_string()),
        }
    }
}

impl From<PermissionDenied> for Error {
    fn from(err: PermissionDenied) -> Self {
        Self::Core(core::Error::PermissionDenied(err))
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
