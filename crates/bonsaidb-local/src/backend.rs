use std::{fmt::Debug, marker::PhantomData};

use bonsaidb_core::{
    actionable,
    custom_api::{CustomApi, CustomApiError, Infallible},
    permissions::{Dispatcher, PermissionDenied},
    schema::{InsertError, InvalidNameError},
};

use crate::{Error, Storage};

/// Tailors the behavior of [`Storage`] to your needs.
pub trait Backend: Debug + Send + Sync + Sized + 'static {
    /// The custom API definition. If you do not wish to have an API, [`NoBackend`] may be provided.
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
    fn initialize(server: &Storage<Self>) {}

    // TODO replace with session_created or something similar.
    // /// A client successfully authenticated.
    // #[allow(unused_variables)]
    // async fn client_authenticated(client: ConnectedClient<Self>, server: &Storage<Self>) {
    //     log::info!(
    //         "{:?} client authenticated as user: {}",
    //         client.transport(),
    //         client.user_id().await.unwrap()
    //     );
    // }
}

/// A trait that can dispatch requests for a [`CustomApi`].
pub trait CustomApiDispatcher<B: Backend>:
    Dispatcher<<B::CustomApi as CustomApi>::Request, Result = BackendApiResult<B::CustomApi>> + Debug
{
    /// Returns a dispatcher to handle custom api requests. The `storage`
    /// instance is provided to allow the dispatcher to have access during
    /// dispatched calls.
    fn new(storage: &Storage<B>) -> Self;
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
    fn new(_server: &Storage<B>) -> Self {
        Self(PhantomData)
    }
}

impl<B: Backend<CustomApi = ()>> actionable::Dispatcher<()> for NoDispatcher<B> {
    type Result = Result<(), BackendError>;

    fn dispatch(&self, _permissions: &actionable::Permissions, _request: ()) -> Self::Result {
        Ok(())
    }
}

/// An error that can occur inside of a [`Backend`] function.
#[derive(thiserror::Error, Debug)]
pub enum BackendError<E: CustomApiError = Infallible> {
    /// A backend-related error.
    #[error("backend error: {0}")]
    Backend(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Storage(#[from] Error),
}

impl<E: CustomApiError> From<PermissionDenied> for BackendError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Storage(Error::from(permission_denied))
    }
}

impl<E: CustomApiError> From<bonsaidb_core::Error> for BackendError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Storage(Error::from(err))
    }
}

impl<E: CustomApiError> From<InvalidNameError> for BackendError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Storage(Error::from(err))
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
        BackendError::Storage(Error::from(other))
    }
}

impl<T, E> From<InsertError<T>> for BackendError<E>
where
    E: CustomApiError,
{
    fn from(error: InsertError<T>) -> Self {
        Self::Storage(Error::from(error.error))
    }
}

pub type BackendApiResult<Api> =
    Result<<Api as CustomApi>::Response, BackendError<<Api as CustomApi>::Error>>;
