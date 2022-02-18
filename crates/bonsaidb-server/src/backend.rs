use std::{fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use bonsaidb_core::{
    actionable,
    custom_api::{CustomApi, CustomApiError, Infallible},
    permissions::{Dispatcher, PermissionDenied},
    schema::{InsertError, InvalidNameError},
};

use crate::{server::ConnectedClient, CustomServer, Error};

/// Tailors the behavior of a server to your needs.
#[async_trait]
pub trait Backend: Debug + Send + Sync + Sized + 'static {
    /// The custom API definition. If you do not wish to have an API, `()` may be provided.
    type CustomApi: CustomApi;

    /// The type of data that can be stored in
    /// [`ConnectedClient::set_client_data`]. This allows state to be stored
    /// associated with each connected client.
    type ClientData: Send + Sync + Debug;

    /// The type that implements the
    /// [`Dispatcher`](bonsaidb_core::permissions::Dispatcher) trait.
    type CustomApiDispatcher: CustomApiDispatcher<Self>;

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
    async fn client_authenticated(client: ConnectedClient<Self>, server: &CustomServer<Self>) {
        log::info!(
            "{:?} client authenticated as user: {}",
            client.transport(),
            client.user_id().await.unwrap()
        );
    }
}

/// A trait that can dispatch requests for a [`CustomApi`].
pub trait CustomApiDispatcher<B: Backend>:
    Dispatcher<<B::CustomApi as CustomApi>::Request, Result = BackendApiResult<B::CustomApi>> + Debug
{
    /// Returns a dispatcher to handle custom api requests. The `server` and
    /// `client` parameters are provided to allow the dispatcher to have access
    /// to them when handling the individual actions.
    fn new(server: &CustomServer<B>, client: &ConnectedClient<B>) -> Self;
}

/// A [`Backend`] with no custom functionality.
#[cfg_attr(feature = "cli", derive(clap::Subcommand))]
#[derive(Debug)]
pub enum NoBackend {}

impl Backend for NoBackend {
    type CustomApi = ();
    type CustomApiDispatcher = NoDispatcher<Self>;
    type ClientData = ();
}

/// Defines a no-op dispatcher for a backend with no custom api.
#[derive(Debug)]
pub struct NoDispatcher<B: Backend>(PhantomData<B>);

impl<B: Backend<CustomApi = ()>> CustomApiDispatcher<B> for NoDispatcher<B> {
    fn new(_server: &CustomServer<B>, _client: &ConnectedClient<B>) -> Self {
        Self(PhantomData)
    }
}

#[async_trait]
impl<B: Backend<CustomApi = ()>> actionable::Dispatcher<()> for NoDispatcher<B> {
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
