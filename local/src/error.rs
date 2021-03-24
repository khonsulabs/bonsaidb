/// errors that can occur from interacting with storage
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// an error occurred interacting with `sled`
    #[error("error from storage: {0}")]
    Sled(#[from] sled::Error),

    /// an error occurred serializing the underlying database structures
    #[error("error while serializing internal structures: {0}")]
    InternalSerialization(#[from] bincode::Error),

    /// an error occurred serializing the contents of a `Document` or results of a `View`
    #[error("error while serializing: {0}")]
    Serialization(#[from] serde_cbor::Error),
}

impl Into<pliantdb_core::Error> for Error {
    fn into(self) -> pliantdb_core::Error {
        pliantdb_core::Error::Storage(self.to_string())
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
