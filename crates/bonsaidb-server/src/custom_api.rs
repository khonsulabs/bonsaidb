use std::{fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    custom_api::{CustomApi, CustomApiError, Infallible},
    permissions::{Dispatcher, PermissionDenied, Permissions},
    schema::{InsertError, InvalidNameError},
};

use crate::{Backend, ConnectedClient, CustomServer, Error};

/// A trait that can dispatch requests for a [`CustomApi`].
pub trait CustomApiDispatcher<B: Backend>: Send + Sync {
    type Dispatcher: Dispatcher<Self::Api, Result = BackendApiResult<Self::Api>>;
    type Api: CustomApi;
    /// Returns a dispatcher to handle custom api requests. The `storage`
    /// instance is provided to allow the dispatcher to have access during
    /// dispatched calls.
    fn dispatcher(server: &CustomServer<B>, client: &ConnectedClient<B>) -> Self::Dispatcher;
}

#[async_trait]
pub trait AnyCustomApiDispatcher<B: Backend>: Send + Sync + Debug {
    async fn dispatch(
        &self,
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        permissions: &Permissions,
        request: &[u8],
    ) -> Result<Bytes, Error>;
}

pub(crate) struct AnyWrapper<D: CustomApiDispatcher<B>, B: Backend>(
    pub(crate) D,
    pub(crate) PhantomData<B>,
);

impl<D, B> Debug for AnyWrapper<D, B>
where
    D: CustomApiDispatcher<B>,
    B: Backend,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyWrapper").finish()
    }
}

#[async_trait]
impl<T, B> AnyCustomApiDispatcher<B> for AnyWrapper<T, B>
where
    B: Backend,
    T: CustomApiDispatcher<B>,
{
    async fn dispatch(
        &self,
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        permissions: &Permissions,
        request: &[u8],
    ) -> Result<Bytes, Error> {
        let request = pot::from_slice(request)?;
        let dispatcher = T::dispatcher(server, client);
        let response = match dispatcher.dispatch(permissions, request).await {
            Ok(response) => Ok(response),
            Err(DispatchError::Api(api)) => Err(api),
            Err(DispatchError::Storage(err)) => return Err(err),
        };
        Ok(Bytes::from(pot::to_vec(&response)?))
    }
}

/// An error that can occur inside of a [`Backend`] function.
#[derive(thiserror::Error, Debug)]
pub enum DispatchError<E: CustomApiError = Infallible> {
    /// An api-related error.
    #[error("api error: {0}")]
    Api(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Storage(#[from] Error),
}

impl<E: CustomApiError> From<PermissionDenied> for DispatchError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Storage(Error::from(permission_denied))
    }
}

impl<E: CustomApiError> From<bonsaidb_core::Error> for DispatchError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Storage(Error::from(err))
    }
}

impl<E: CustomApiError> From<InvalidNameError> for DispatchError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Storage(Error::from(err))
    }
}

#[cfg(feature = "websockets")]
impl<E: CustomApiError> From<bincode::Error> for DispatchError<E> {
    fn from(other: bincode::Error) -> Self {
        Self::Storage(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E: CustomApiError> From<pot::Error> for DispatchError<E> {
    fn from(other: pot::Error) -> Self {
        Self::Storage(Error::from(other))
    }
}

impl<T, E> From<InsertError<T>> for DispatchError<E>
where
    E: CustomApiError,
{
    fn from(error: InsertError<T>) -> Self {
        Self::Storage(Error::from(error.error))
    }
}

pub type BackendApiResult<Api> =
    Result<<Api as CustomApi>::Response, DispatchError<<Api as CustomApi>::Error>>;
