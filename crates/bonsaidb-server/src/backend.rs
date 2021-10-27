use std::fmt::Debug;

use actionable::PermissionDenied;
use async_trait::async_trait;
use bonsaidb_core::{
    custom_api::{CustomApi, CustomApiError, Infallible},
    permissions::Dispatcher,
    schema::{InsertError, InvalidNameError},
};

use crate::{server::ConnectedClient, CustomServer, Error};

/// Tailors the behavior of a server to your needs.
#[async_trait]
pub trait Backend: Debug + Send + Sync + Sized + 'static {
    /// The custom API definition. If you do not wish to have an API, `()` may be provided.
    type CustomApi: CustomApi;

    /// The type that implements the
    /// [`Dispatcher`](bonsaidb_core::permissions::Dispatcher) trait.
    type CustomApiDispatcher: Dispatcher<
            <Self::CustomApi as CustomApi>::Request,
            Result = BackendApiResult<Self::CustomApi>,
        > + Debug;

    /// Returns a dispatcher to handle custom api requests. The `server` and
    /// `client` parameters are provided to allow the dispatcher to have access
    /// to them when handling the individual actions.
    fn dispatcher_for(
        server: &CustomServer<Self>,
        client: &ConnectedClient<Self>,
    ) -> Self::CustomApiDispatcher;

    /// Invoked once upon the server starting up.
    #[allow(unused_variables)]
    async fn initialize(server: &CustomServer<Self>) {}

    // TODO: add client connections events, client errors, etc.
    /// A client disconnected from the server. This is invoked before authentication has been performed.
    #[allow(unused_variables)]
    #[must_use]
    async fn client_connected(
        client: &ConnectedClient<Self>,
        server: &CustomServer<Self>,
    ) -> ConnectionHandling {
        println!(
            "{:?} client connected from {:?}",
            client.transport(),
            client.address()
        );

        ConnectionHandling::Accept
    }

    /// A client disconnected from the server.
    #[allow(unused_variables)]
    async fn client_disconnected(client: ConnectedClient<Self>, server: &CustomServer<Self>) {
        println!(
            "{:?} client disconnected ({:?})",
            client.transport(),
            client.address()
        );
    }

    /// A client successfully authenticated.
    #[allow(unused_variables)]
    async fn client_authenticated(client: ConnectedClient<Self>, server: &CustomServer<Self>) {
        println!(
            "{:?} client authenticated as user: {}",
            client.transport(),
            client.user_id().await.unwrap()
        );
    }
}

impl Backend for () {
    type CustomApi = ();
    type CustomApiDispatcher = NoDispatcher;

    fn dispatcher_for(
        _server: &CustomServer<Self>,
        _client: &ConnectedClient<Self>,
    ) -> Self::CustomApiDispatcher {
        NoDispatcher
    }
}

/// Defines a no-op dispatcher for a backend with no custom api.
#[derive(Debug)]
pub struct NoDispatcher;

#[async_trait]
impl actionable::Dispatcher<()> for NoDispatcher {
    type Result = Result<(), BackendError>;

    async fn dispatch(&self, _permissions: &actionable::Permissions, _request: ()) -> Self::Result {
        Ok(())
    }
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
pub enum BackendError<E: CustomApiError = Infallible> {
    /// A backend-related error.
    #[error("backend error: {0}")]
    Backend(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Server(#[from] Error),
}

impl<E: CustomApiError> From<PermissionDenied> for BackendError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Server(Error::from(permission_denied))
    }
}

impl<E: CustomApiError> From<bonsaidb_core::Error> for BackendError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E: CustomApiError> From<InvalidNameError> for BackendError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Server(Error::from(err))
    }
}

#[cfg(feature = "websockets")]
impl<E: CustomApiError> From<bincode::Error> for BackendError<E> {
    fn from(other: bincode::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E: CustomApiError> From<pot::Error> for BackendError<E> {
    fn from(other: pot::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<T, E> From<InsertError<T>> for BackendError<E>
where
    E: CustomApiError,
{
    fn from(error: InsertError<T>) -> Self {
        Self::Server(Error::from(error.error))
    }
}

pub type BackendApiResult<Api> =
    Result<<Api as CustomApi>::Response, BackendError<<Api as CustomApi>::Error>>;
