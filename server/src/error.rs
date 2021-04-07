use pliantdb_local::core::{self, schema};
use pliantdb_networking::fabruic;

/// An error occurred while interacting with a [`Server`](crate::Server).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An invalid database name was specified. See
    /// [`ServerConnection::create_database()`](pliantdb_networking::ServerConnection::create_database)
    /// for database name requirements.
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(String),

    /// The database name given was not found.
    #[error("database '{0}' was not found")]
    DatabaseNotFound(String),

    /// The database name already exists.
    #[error("a database with name '{0}' already exists")]
    DatabaseNameAlreadyTaken(String),

    /// An error occurred from the QUIC transport layer.
    #[error("a networking error occurred: '{0}'")]
    Transport(#[from] fabruic::Error),

    /// An error occurred from IO
    #[error("a networking error occurred: '{0}'")]
    Io(#[from] tokio::io::Error),

    /// The database named `database_name` was created with a different schema
    /// (`stored_schema`) than provided (`schema`).
    #[error(
        "database '{database_name}' was created with schema '{stored_schema}', not '{schema}'"
    )]
    SchemaMismatch {
        /// The name of the database being accessed.
        database_name: String,

        /// The schema provided for the database.
        schema: schema::Id,

        /// The schema stored for the database.
        stored_schema: schema::Id,
    },

    /// The [`schema::Id`] returned has already been registered with this server.
    #[error("schema '{0}' was already registered")]
    SchemaAlreadyRegistered(schema::Id),

    /// An error occurred from within the schema.
    #[error("error from core {0}")]
    Core(#[from] core::Error),

    /// An error occurred while interacting with a local database.
    #[error("an error occurred interacting with a database: {0}")]
    Storage(#[from] pliantdb_local::Error),
}

impl From<Error> for core::Error {
    fn from(other: Error) -> Self {
        match other {
            Error::Storage(storage) => Self::Storage(storage.to_string()),
            Error::Core(core) => core,
            Error::Io(io) => Self::Io(io.to_string()),
            Error::Transport(networking) => Self::Transport(networking.to_string()),
            other => Self::Server(other.to_string()),
        }
    }
}

impl From<Error> for pliantdb_networking::Error {
    fn from(other: Error) -> Self {
        match other {
            Error::InvalidDatabaseName(name) => Self::InvalidDatabaseName(name),
            Error::DatabaseNotFound(name) => Self::DatabaseNotFound(name),
            Error::DatabaseNameAlreadyTaken(name) => Self::DatabaseNameAlreadyTaken(name),
            Error::SchemaMismatch {
                database_name,
                schema,
                stored_schema,
            } => Self::SchemaMismatch {
                database_name,
                schema,
                stored_schema,
            },
            Error::SchemaAlreadyRegistered(id) => Self::SchemaAlreadyRegistered(id),
            other => Self::Core(other.into()),
        }
    }
}

pub trait ResultExt<R> {
    fn map_err_to_core(self) -> Result<R, core::Error>
    where
        Self: Sized;
    fn map_err_to_net(self) -> Result<R, pliantdb_networking::Error>
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
    fn map_err_to_net(self) -> Result<R, pliantdb_networking::Error>
    where
        Self: Sized,
    {
        self.map_err(pliantdb_networking::Error::from)
    }
}
