use bonsaidb_core::custom_api::CustomApiError;

/// Errors related to working with [`Client`](crate::Client)
#[derive(thiserror::Error, Debug)]
pub enum Error<ApiError: CustomApiError> {
    #[cfg(feature = "websockets")]
    /// An error occurred from the WebSocket transport layer.
    #[error("a transport error occurred: '{0}'")]
    WebSocket(#[from] crate::client::WebSocketError),

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

    /// An error from the custom API.
    #[error("api error: {0}")]
    Api(ApiError),

    /// The server is incompatible with this version of the client.
    #[error("server incompatible with client protocol version")]
    ProtocolVersionMismatch,
}

impl<T, ApiError: CustomApiError> From<flume::SendError<T>> for Error<ApiError> {
    fn from(_: flume::SendError<T>) -> Self {
        Self::Disconnected
    }
}

impl<ApiError: CustomApiError> From<flume::RecvError> for Error<ApiError> {
    fn from(_: flume::RecvError) -> Self {
        Self::Disconnected
    }
}

impl<ApiError: CustomApiError> From<Error<ApiError>> for bonsaidb_core::Error {
    fn from(other: Error<ApiError>) -> Self {
        match other {
            Error::Core(err) => err,
            other => Self::Client(other.to_string()),
        }
    }
}

#[cfg(feature = "websockets")]
impl<ApiError: CustomApiError> From<bincode::Error> for Error<ApiError> {
    fn from(other: bincode::Error) -> Self {
        Self::Core(bonsaidb_core::Error::Websocket(format!(
            "error decoding websocket message: {:?}",
            other
        )))
    }
}

#[cfg(not(target_arch = "wasm32"))]
mod fabruic_impls {
    macro_rules! impl_from_fabruic {
        ($error:ty) => {
            impl<ApiError: bonsaidb_core::custom_api::CustomApiError> From<$error>
                for $crate::Error<ApiError>
            {
                fn from(other: $error) -> Self {
                    Self::Core(bonsaidb_core::Error::Transport(other.to_string()))
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
