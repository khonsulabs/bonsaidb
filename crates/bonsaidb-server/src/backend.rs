use std::fmt::Debug;

use async_trait::async_trait;
use bonsaidb_core::{
    api::{ApiError, Infallible},
    connection::Session,
    permissions::PermissionDenied,
    schema::{InsertError, InvalidNameError},
};

use crate::{server::ConnectedClient, CustomServer, Error};

/// Tailors the behavior of a server to your needs.
#[async_trait]
pub trait Backend: Debug + Send + Sync + Sized + 'static {
    /// The type of data that can be stored in
    /// [`ConnectedClient::set_client_data`]. This allows state to be stored
    /// associated with each connected client.
    type ClientData: Send + Sync + Debug;

    /// Invoked once upon the server starting up.
    #[allow(unused_variables)]
    async fn initialize(server: &CustomServer<Self>) {}

    /// A client disconnected from the server. This is invoked before authentication has been performed.
    #[allow(unused_variables)]
    #[must_use]
    async fn client_connected(
        client: &ConnectedClient<Self>,
        server: &CustomServer<Self>,
    ) -> ConnectionHandling {
        log::info!(
            "{:?} client connected from {:?}",
            client.transport(),
            client.address()
        );

        ConnectionHandling::Accept
    }

    /// A client disconnected from the server.
    #[allow(unused_variables)]
    async fn client_disconnected(client: ConnectedClient<Self>, server: &CustomServer<Self>) {
        log::info!(
            "{:?} client disconnected ({:?})",
            client.transport(),
            client.address()
        );
    }

    /// A client successfully authenticated.
    #[allow(unused_variables)]
    async fn client_authenticated(
        client: ConnectedClient<Self>,
        session: &Session,
        server: &CustomServer<Self>,
    ) {
        log::info!(
            "{:?} client authenticated as user: {:?}",
            client.transport(),
            session.identity
        );
    }
}

/// A [`Backend`] with no custom functionality.
#[cfg_attr(feature = "cli", derive(clap::Subcommand))]
#[derive(Debug)]
pub enum NoBackend {}

impl Backend for NoBackend {
    type ClientData = ();
}

/// Controls how a server should handle a connection.
pub enum ConnectionHandling {
    /// The server should accept this connection.
    Accept,
    /// The server should reject this connection.
    Reject,
}

/// An error that can occur inside of a [`Backend`] function.
#[derive(thiserror::Error, Debug)]
pub enum BackendError<E: ApiError = Infallible> {
    /// A backend-related error.
    #[error("backend error: {0}")]
    Backend(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Server(#[from] Error),
}

impl<E: ApiError> From<PermissionDenied> for BackendError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Server(Error::from(permission_denied))
    }
}

impl<E: ApiError> From<bonsaidb_core::Error> for BackendError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E: ApiError> From<InvalidNameError> for BackendError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Server(Error::from(err))
    }
}

#[cfg(feature = "websockets")]
impl<E: ApiError> From<bincode::Error> for BackendError<E> {
    fn from(other: bincode::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E: ApiError> From<pot::Error> for BackendError<E> {
    fn from(other: pot::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<T, E> From<InsertError<T>> for BackendError<E>
where
    E: ApiError,
{
    fn from(error: InsertError<T>) -> Self {
        Self::Server(Error::from(error.error))
    }
}
