use std::convert::Infallible;
use std::fmt::Debug;

use async_trait::async_trait;
use bonsaidb_core::connection::Session;
use bonsaidb_core::permissions::PermissionDenied;
use bonsaidb_core::schema::{InsertError, InvalidNameError};

use crate::server::ConnectedClient;
use crate::{CustomServer, Error, ServerConfiguration};

/// Tailors the behavior of a server to your needs.
#[async_trait]
pub trait Backend: Debug + Send + Sync + Sized + 'static {
    /// The error type that can be returned from the backend functions. If a
    /// backend doesn't need an error type, [`Infallible`] can be used.
    type Error: std::error::Error + Send + Sync;
    /// The type of data that can be stored in
    /// [`ConnectedClient::set_client_data`]. This allows state to be stored
    /// associated with each connected client.
    type ClientData: Send + Sync + Debug;

    /// Invoked once before the server is initialized.
    #[allow(unused_variables)]
    fn configure(
        config: ServerConfiguration<Self>,
    ) -> Result<ServerConfiguration<Self>, BackendError<Self::Error>> {
        Ok(config)
    }

    /// Invoked once after initialization during
    /// [`Server::open`/`CustomServer::open`](CustomServer::open).
    #[allow(unused_variables)]
    async fn initialize(
        &self,
        server: &CustomServer<Self>,
    ) -> Result<(), BackendError<Self::Error>> {
        Ok(())
    }

    /// A client disconnected from the server. This is invoked before authentication has been performed.
    #[allow(unused_variables)]
    #[must_use]
    async fn client_connected(
        &self,
        client: &ConnectedClient<Self>,
        server: &CustomServer<Self>,
    ) -> Result<ConnectionHandling, BackendError<Self::Error>> {
        log::info!(
            "{:?} client connected from {:?}",
            client.transport(),
            client.address()
        );

        Ok(ConnectionHandling::Accept)
    }

    /// A client disconnected from the server.
    #[allow(unused_variables)]
    async fn client_disconnected(
        &self,
        client: ConnectedClient<Self>,
        server: &CustomServer<Self>,
    ) -> Result<(), BackendError<Self::Error>> {
        log::info!(
            "{:?} client disconnected ({:?})",
            client.transport(),
            client.address()
        );
        Ok(())
    }

    /// A client successfully authenticated.
    #[allow(unused_variables)]
    async fn client_authenticated(
        &self,
        client: ConnectedClient<Self>,
        session: &Session,
        server: &CustomServer<Self>,
    ) -> Result<(), BackendError<Self::Error>> {
        log::info!(
            "{:?} client authenticated as user: {:?}",
            client.transport(),
            session.authentication
        );
        Ok(())
    }

    /// A client's session has ended.
    ///
    /// If `disconnecting` is true, the session is ending because the client is
    /// in the process of disconnecting.
    #[allow(unused_variables)]
    async fn client_session_ended(
        &self,
        session: Session,
        client: &ConnectedClient<Self>,
        disconnecting: bool,
        server: &CustomServer<Self>,
    ) -> Result<(), BackendError<Self::Error>> {
        log::info!(
            "{:?} client session ended {:?}",
            client.transport(),
            session.authentication
        );
        Ok(())
    }
}

/// A [`Backend`] with no custom functionality.
#[derive(Debug, Default)]
pub struct NoBackend;

impl Backend for NoBackend {
    type ClientData = ();
    type Error = Infallible;
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
pub enum BackendError<E = Infallible> {
    /// A backend-related error.
    #[error("backend error: {0}")]
    Backend(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Server(#[from] Error),
}

impl<E> From<PermissionDenied> for BackendError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Server(Error::from(permission_denied))
    }
}

impl<E> From<bonsaidb_core::Error> for BackendError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E> From<bonsaidb_local::Error> for BackendError<E> {
    fn from(err: bonsaidb_local::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E> From<std::io::Error> for BackendError<E> {
    fn from(err: std::io::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E> From<InvalidNameError> for BackendError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Server(Error::from(err))
    }
}

#[cfg(feature = "websockets")]
impl<E> From<bincode::Error> for BackendError<E> {
    fn from(other: bincode::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E> From<pot::Error> for BackendError<E> {
    fn from(other: pot::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<T, E> From<InsertError<T>> for BackendError<E> {
    fn from(error: InsertError<T>) -> Self {
        Self::Server(Error::from(error.error))
    }
}
