use std::sync::Arc;

use bonsaidb_core::{permissions::PermissionDenied, schema, schema::InsertError, AnyError};
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
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// An error occurred from IO
    #[error("a networking error occurred: '{0}'")]
    Io(#[from] tokio::io::Error),

    /// An error occurred while processing a request
    #[error("an error occurred processing a request: '{0}'")]
    Request(Arc<dyn AnyError>),

    /// An error occurred from within the schema.
    #[error("error from core {0}")]
    Core(#[from] bonsaidb_core::Error),

    /// An internal error occurred while waiting for a message.
    #[error("error while waiting for a message: {0}")]
    InternalCommunication(#[from] flume::RecvError),

    /// An error occurred while interacting with a local database.
    #[error("an error occurred interacting with a database: {0}")]
    Database(#[from] bonsaidb_local::Error),

    /// An error occurred with a certificate.
    #[error("a certificate error: {0}")]
    Certificate(#[from] fabruic::error::Certificate),

    /// An error occurred parsing a PEM file.
    #[error("an invalid PEM file: {0}")]
    #[cfg(feature = "pem")]
    Pem(#[from] pem::PemError),

    /// An error occurred with handling opaque-ke.
    #[error("an opaque-ke error: {0}")]
    Password(#[from] bonsaidb_core::custodian_password::Error),

    /// An error occurred requesting an ACME certificate.
    #[error("an error requesting an ACME certificate: {0}")]
    #[cfg(feature = "acme")]
    Acme(#[from] async_acme::acme::AcmeError),

    /// An error occurred while processing an ACME order.
    #[error("an error occurred while processing an ACME order: {0}")]
    #[cfg(feature = "acme")]
    AcmeOrder(#[from] async_acme::rustls_helper::OrderError),

    /// An error occurred during tls signing.
    #[error("an error occurred during tls signing")]
    TlsSigningError,
}

impl From<Error> for bonsaidb_core::Error {
    fn from(other: Error) -> Self {
        // without it, there's no way to get this to_string() easily.
        match other {
            Error::Database(storage) => Self::Database(storage.to_string()),
            Error::Core(core) => core,
            Error::Io(io) => Self::Io(io.to_string()),
            Error::Transport(networking) => Self::Transport(networking),
            #[cfg(feature = "websockets")]
            Error::WebSocket(err) => Self::Websocket(err.to_string()),
            err => Self::Server(err.to_string()),
        }
    }
}

impl From<PermissionDenied> for Error {
    fn from(err: PermissionDenied) -> Self {
        Self::Core(bonsaidb_core::Error::PermissionDenied(err))
    }
}

impl From<InvalidNameError> for Error {
    fn from(err: InvalidNameError) -> Self {
        Self::Core(bonsaidb_core::Error::InvalidName(err))
    }
}

impl From<rustls::sign::SignError> for Error {
    fn from(_: rustls::sign::SignError) -> Self {
        Self::TlsSigningError
    }
}

impl<T> From<InsertError<T>> for Error {
    fn from(error: InsertError<T>) -> Self {
        Self::from(error.error)
    }
}

pub trait ResultExt<R> {
    fn map_err_to_core(self) -> Result<R, bonsaidb_core::Error>
    where
        Self: Sized;
}

impl<R> ResultExt<R> for Result<R, Error> {
    fn map_err_to_core(self) -> Result<R, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        self.map_err(bonsaidb_core::Error::from)
    }
}

#[cfg(feature = "websockets")]
impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Core(bonsaidb_core::Error::Websocket(format!(
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
impl_from_fabruic!(fabruic::error::Config);
