use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::schema::Name;

/// Errors related to working with the BonsaiDb client.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[cfg(feature = "websockets")]
    /// An error occurred from the WebSocket transport layer.
    #[error("a transport error occurred: '{0}'")]
    WebSocket(crate::client::WebSocketError),

    /// An error occurred from networking.
    #[error("a networking error occurred: '{0}'")]
    Network(#[from] bonsaidb_core::networking::Error),

    /// An invalid Url was provided.
    #[error("invalid url: '{0}'")]
    InvalidUrl(String),

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Disconnected,

    /// The connection was interrupted.
    #[error("unexpected disconnection")]
    Core(#[from] bonsaidb_core::Error),

    /// An error from a `Api`. The actual error is still serialized, as it
    /// could be any type.
    #[error("api {name} error")]
    Api {
        /// The unique name of the api that responded with an error
        name: Name,
        /// The serialized bytes of the error type.
        error: Bytes,
    },

    /// The server is incompatible with this version of the client.
    #[error("server incompatible with client protocol version")]
    ProtocolVersionMismatch,
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

impl From<Error> for bonsaidb_core::Error {
    fn from(other: Error) -> Self {
        match other {
            Error::Core(err) => err,
            other => Self::other("bonsaidb-client", other),
        }
    }
}

#[cfg(feature = "websockets")]
impl From<bincode::Error> for Error {
    fn from(other: bincode::Error) -> Self {
        Self::Core(bonsaidb_core::Error::other("bincode", other))
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod fabruic_impls {
    macro_rules! impl_from_fabruic {
        ($error:ty) => {
            impl From<$error> for $crate::Error {
                fn from(other: $error) -> Self {
                    Self::Core(bonsaidb_core::Error::other("quic", other))
                }
            }
        };
    }

    impl_from_fabruic!(fabruic::error::Sender);
    impl_from_fabruic!(fabruic::error::Receiver);
    impl_from_fabruic!(fabruic::error::Stream);
    impl_from_fabruic!(fabruic::error::Connecting);
    impl_from_fabruic!(fabruic::error::Connect);
}

#[cfg(feature = "websockets")]
impl From<crate::client::WebSocketError> for Error {
    #[cfg(not(target_arch = "wasm32"))]
    fn from(err: crate::client::WebSocketError) -> Self {
        if let crate::client::WebSocketError::Http(response) = &err {
            if response.status() == 406 {
                return Self::ProtocolVersionMismatch;
            }
        }

        Self::WebSocket(err)
    }

    #[cfg(target_arch = "wasm32")]
    fn from(err: crate::client::WebSocketError) -> Self {
        Self::WebSocket(err)
    }
}

impl From<pot::Error> for Error {
    fn from(err: pot::Error) -> Self {
        Self::from(bonsaidb_core::Error::from(err))
    }
}

/// An error returned from an api request.
#[derive(thiserror::Error, Debug)]
pub enum ApiError<T> {
    /// The API returned its own error type.
    #[error("api error: {0}")]
    Api(T),
    /// An error from BonsaiDb occurred.
    #[error("client error: {0}")]
    Client(#[from] Error),
}

impl From<ApiError<Self>> for bonsaidb_core::Error {
    fn from(error: ApiError<Self>) -> Self {
        match error {
            ApiError::Api(err) => err,
            ApiError::Client(err) => Self::from(err),
        }
    }
}
