use std::fmt::Display;

use thiserror::Error;

/// An error from [`Roots`](crate::Roots).
#[derive(Debug, Error)]
pub enum Error {
    /// An error has occurred. The string contains human-readable error message.
    /// This error is only used in situations where a user is not expected to be
    /// able to recover automatically from the error.
    #[error("{0}")]
    Message(String),
    /// An error occurred while performing IO.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// An unrecoverable data integrity error was encountered.
    #[error("an unrecoverable error with the data on disk has been found: {0}")]
    DataIntegrity(Box<Self>),
}

impl Error {
    /// Returns a new [`Error::Message`] instance with the message provided.
    pub fn message<S: Display>(message: S) -> Self {
        Self::Message(message.to_string())
    }
}
