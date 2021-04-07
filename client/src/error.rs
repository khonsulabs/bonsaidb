use pliantdb_networking::fabruic;

/// Errors related to working with [`Client`](crate::Client)
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred from the QUIC transport layer.
    #[error("a transport error occurred: '{0}'")]
    Transport(#[from] fabruic::Error),

    /// An error occurred from networking.
    #[error("a networking error occurred: '{0}'")]
    Network(#[from] pliantdb_networking::Error),

    /// An invalid Url was provided.
    #[error("invalid url: '{0}'")]
    InvalidUrl(String),

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Disconnected,
}

impl<T> From<flume::SendError<T>> for Error {
    fn from(_: flume::SendError<T>) -> Self {
        Self::Disconnected
    }
}

impl From<flume::RecvError> for Error {
    fn from(_: flume::RecvError) -> Self {
        Self::Disconnected
    }
}

impl From<Error> for pliantdb_networking::Error {
    fn from(other: Error) -> Self {
        match other {
            Error::Transport(err) => Self::Core(pliantdb_core::Error::Transport(err.to_string())),
            Error::Network(err) => err,
            Error::InvalidUrl(err) => Self::Core(pliantdb_core::Error::Configuration(format!(
                "invalid url: {:?}",
                err
            ))),
            Error::Disconnected => Self::Disconnected,
        }
    }
}
