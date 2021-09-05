use std::fmt::Display;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("an unrecoverable error with the data on disk has been found: {0}")]
    DataIntegrity(Box<Self>),
}

impl Error {
    pub fn message<S: Display>(message: S) -> Self {
        Self::Message(message.to_string())
    }
}
