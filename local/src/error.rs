use std::sync::Arc;

use pliantdb_core::schema::{view, InvalidNameError};

/// Errors that can occur from interacting with storage.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred interacting with `sled`.
    #[error("error from storage: {0}")]
    Sled(#[from] sled::Error),

    /// An error occurred serializing the underlying database structures.
    #[error("error while serializing internal structures: {0}")]
    InternalSerialization(#[from] bincode::Error),

    /// An error occurred serializing the contents of a `Document` or results of a `View`.
    #[error("error while serializing: {0}")]
    Serialization(#[from] serde_cbor::Error),

    /// An internal error occurred while waiting for a message.
    #[error("error while waiting for a message: {0}")]
    InternalCommunication(#[from] flume::RecvError),

    /// An error occurred while executing a view
    #[error("error from view: {0}")]
    View(#[from] view::Error),

    /// An internal error occurred while waiting for a message.
    #[error("error while waiting for a message: {0}")]
    Core(#[from] pliantdb_core::Error),

    /// An internal error occurred while waiting for a message.
    #[error("an unexpected error occurred: {0}")]
    Other(#[from] Arc<anyhow::Error>),
}

impl From<Error> for pliantdb_core::Error {
    fn from(err: Error) -> Self {
        Self::Storage(err.to_string())
    }
}

impl From<InvalidNameError> for Error {
    fn from(err: InvalidNameError) -> Self {
        Self::Core(pliantdb_core::Error::from(err))
    }
}

pub trait ResultExt {
    type Output;
    fn map_err_to_core(self) -> Result<Self::Output, pliantdb_core::Error>;
}

impl<T, E> ResultExt for Result<T, E>
where
    E: Into<Error>,
{
    type Output = T;

    fn map_err_to_core(self) -> Result<Self::Output, pliantdb_core::Error> {
        self.map_err(|err| pliantdb_core::Error::Storage(err.into().to_string()))
    }
}

#[test]
fn test_converting_error() {
    use serde::ser::Error as _;
    let err: pliantdb_core::Error =
        Error::Serialization(serde_cbor::Error::custom("mymessage")).into();
    match err {
        pliantdb_core::Error::Storage(storage_error) => {
            assert!(storage_error.contains("mymessage"))
        }
        _ => unreachable!(),
    }
}
