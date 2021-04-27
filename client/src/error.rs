use pliantdb_core as core;
use pliantdb_core::networking::fabruic;

/// Errors related to working with [`Client`](crate::Client)
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[cfg(feature = "websockets")]
    /// An error occurred from the WebSocket transport layer.
    #[error("a transport error occurred: '{0}'")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// An error occurred from networking.
    #[error("a networking error occurred: '{0}'")]
    Network(#[from] pliantdb_core::networking::Error),

    /// An invalid Url was provided.
    #[error("invalid url: '{0}'")]
    InvalidUrl(String),

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Disconnected,

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Core(#[from] core::Error),
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

impl From<Error> for core::Error {
    fn from(other: Error) -> Self {
        match other {
            Error::Core(err) => err,
            other => Self::Client(other.to_string()),
        }
    }
}

#[cfg(feature = "websockets")]
impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Core(core::Error::Websocket(format!(
            "error decoding websocket message: {:?}",
            other
        )))
    }
}

macro_rules! impl_from_fabruic {
    ($error:ty) => {
        impl From<$error> for Error {
            fn from(other: $error) -> Self {
                Self::Core(pliantdb_core::Error::Transport(other.to_string()))
            }
        }
    };
}

impl_from_fabruic!(fabruic::error::Sender);
impl_from_fabruic!(fabruic::error::Receiver);
impl_from_fabruic!(fabruic::error::Stream);
impl_from_fabruic!(fabruic::error::Connecting);
impl_from_fabruic!(fabruic::error::Connect);
