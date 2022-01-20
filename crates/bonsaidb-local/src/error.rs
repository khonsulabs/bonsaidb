use std::{convert::Infallible, str::Utf8Error, string::FromUtf8Error, sync::Arc};

use bonsaidb_core::{
    permissions::PermissionDenied,
    schema::{view, InvalidNameError},
    AnyError,
};
use nebari::AbortError;

/// Errors that can occur from interacting with storage.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred interacting with the storage layer, `nebari`.
    #[error("error from storage: {0}")]
    Nebari(#[from] nebari::Error),

    /// An error occurred serializing the underlying database structures.
    #[error("error while serializing internal structures: {0}")]
    InternalSerialization(#[from] bincode::Error),

    /// An error occurred serializing the contents of a `Document` or results of a `View`.
    #[error("error while serializing: {0}")]
    Serialization(#[from] pot::Error),

    /// An internal error occurred while waiting for or sending a message.
    #[error("error while communicating internally")]
    InternalCommunication,

    /// An error occurred while executing a view
    #[error("error from view: {0}")]
    View(#[from] view::Error),

    /// An error occurred in the secrets storage layer.
    #[error("a vault error occurred: {0}")]
    #[cfg(feature = "encryption")]
    Vault(#[from] crate::vault::Error),

    /// A collection requested to be encrypted, but encryption is disabled.
    #[error("encryption is disabled, but a collection is requesting encryption")]
    #[cfg(not(feature = "encryption"))]
    EncryptionDisabled,

    /// An core error occurred.
    #[error("a core error occurred: {0}")]
    Core(#[from] bonsaidb_core::Error),

    /// A tokio task failed to execute.
    #[error("a concurrency error ocurred: {0}")]
    TaskJoin(#[from] tokio::task::JoinError),

    /// A tokio task failed to execute.
    #[error("an IO error occurred: {0}")]
    Io(#[from] tokio::io::Error),

    /// An error occurred from a job and couldn't be unwrapped due to clones.
    #[error("an error from a job occurred: {0}")]
    Job(Arc<Error>),

    /// An error occurred from backing up or restoring.
    #[error("a backup error: {0}")]
    Backup(Box<dyn AnyError>),
}

impl From<flume::RecvError> for Error {
    fn from(_: flume::RecvError) -> Self {
        Self::InternalCommunication
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Self::InternalCommunication
    }
}

impl From<tokio::sync::oneshot::error::TryRecvError> for Error {
    fn from(_: tokio::sync::oneshot::error::TryRecvError) -> Self {
        Self::InternalCommunication
    }
}

impl From<Error> for bonsaidb_core::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::View(view::Error::Core(core)) | Error::Core(core) => core,
            other => Self::Database(other.to_string()),
        }
    }
}

impl From<Arc<Error>> for Error {
    fn from(err: Arc<Error>) -> Self {
        match Arc::try_unwrap(err) {
            Ok(err) => err,
            Err(still_wrapped) => Error::Job(still_wrapped),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Self::Core(bonsaidb_core::Error::InvalidUnicode(err.to_string()))
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Self::Core(bonsaidb_core::Error::InvalidUnicode(err.to_string()))
    }
}

impl From<InvalidNameError> for Error {
    fn from(err: InvalidNameError) -> Self {
        Self::Core(bonsaidb_core::Error::from(err))
    }
}

impl From<AbortError<Infallible>> for Error {
    fn from(err: AbortError<Infallible>) -> Self {
        match err {
            AbortError::Nebari(error) => Self::Nebari(error),
            AbortError::Other(_) => unreachable!(),
        }
    }
}

impl From<AbortError<Error>> for Error {
    fn from(err: AbortError<Error>) -> Self {
        match err {
            AbortError::Nebari(error) => Self::Nebari(error),
            AbortError::Other(error) => error,
        }
    }
}

impl From<PermissionDenied> for Error {
    fn from(err: PermissionDenied) -> Self {
        Self::Core(bonsaidb_core::Error::from(err))
    }
}

#[test]
fn test_converting_error() {
    use serde::ser::Error as _;
    let err: bonsaidb_core::Error = Error::Serialization(pot::Error::custom("mymessage")).into();
    match err {
        bonsaidb_core::Error::Database(storage_error) => {
            assert!(storage_error.contains("mymessage"));
        }
        _ => unreachable!(),
    }
}
