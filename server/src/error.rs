use pliantdb_local::core;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("an error occurred interacting with a database: {0}")]
    Storage(#[from] pliantdb_local::Error),

    #[error("invalid database name: {0}")]
    InvalidDatabaseName(String),

    #[error("database '{0}' was not found")]
    DatabaseNotFound(String),

    #[error("a database with name '{0}' already exists")]
    DatabaseNameAlreadyTaken(String),

    #[error("error from core {0}")]
    Core(#[from] core::Error),
}

impl From<Error> for core::Error {
    fn from(other: Error) -> Self {
        match other {
            Error::Storage(storage) => Self::Storage(storage.to_string()),
            Error::Core(core) => core,
            other => Self::Server(other.to_string()),
        }
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
