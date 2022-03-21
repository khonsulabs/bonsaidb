use std::{fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use bonsaidb_core::{
    api::{self, Api, ApiError, Infallible},
    arc_bytes::serde::Bytes,
    permissions::PermissionDenied,
    schema::{InsertError, InvalidNameError},
};

use crate::{Backend, ConnectedClient, CustomServer, Error};

/// A trait that can dispatch requests for a [`Api`].
#[async_trait]
pub trait CustomApiHandler<B: Backend, Api: api::Api>: Send + Sync {
    /// Returns a dispatcher to handle custom api requests. The parameters are
    /// provided so that they can be cloned if needed during the processing of
    /// requests.
    async fn handle(
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        request: Api,
    ) -> DispatcherResult<Api>;
}

#[async_trait]
pub(crate) trait AnyCustomApiHandler<B: Backend>: Send + Sync + Debug {
    async fn handle(
        &self,
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        request: &[u8],
    ) -> Result<Bytes, Error>;
}

pub(crate) struct AnyWrapper<D: CustomApiHandler<B, A>, B: Backend, A: Api>(
    pub(crate) PhantomData<(D, B, A)>,
);

impl<D, B, A> Debug for AnyWrapper<D, B, A>
where
    D: CustomApiHandler<B, A>,
    B: Backend,
    A: Api,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyWrapper").finish()
    }
}

#[async_trait]
impl<T, B, A> AnyCustomApiHandler<B> for AnyWrapper<T, B, A>
where
    B: Backend,
    T: CustomApiHandler<B, A>,
    A: Api,
{
    async fn handle(
        &self,
        server: &CustomServer<B>,
        client: &ConnectedClient<B>,
        request: &[u8],
    ) -> Result<Bytes, Error> {
        let request = pot::from_slice(request)?;
        let response = T::handle(server, client, request).await;
        Ok(Bytes::from(pot::to_vec(&response)?))
    }
}

/// An error that can occur inside of a [`Backend`] function.
#[derive(thiserror::Error, Debug)]
pub enum DispatchError<E: ApiError = Infallible> {
    /// An api-related error.
    #[error("api error: {0}")]
    Api(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Storage(#[from] Error),
}

impl<E: ApiError> From<PermissionDenied> for DispatchError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Storage(Error::from(permission_denied))
    }
}

impl<E: ApiError> From<bonsaidb_core::Error> for DispatchError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Storage(Error::from(err))
    }
}

impl<E: ApiError> From<InvalidNameError> for DispatchError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Storage(Error::from(err))
    }
}

#[cfg(feature = "websockets")]
impl<E: ApiError> From<bincode::Error> for DispatchError<E> {
    fn from(other: bincode::Error) -> Self {
        Self::Storage(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E: ApiError> From<pot::Error> for DispatchError<E> {
    fn from(other: pot::Error) -> Self {
        Self::Storage(Error::from(other))
    }
}

impl<T, E> From<InsertError<T>> for DispatchError<E>
where
    E: ApiError,
{
    fn from(error: InsertError<T>) -> Self {
        Self::Storage(Error::from(error.error))
    }
}

/// The return type from a [`CustomApiDispatcher`]'s
/// [`dispatch()`](Dispatcher::dispatch) function.
pub type DispatcherResult<Api> = Result<<Api as api::Api>::Response, <Api as api::Api>::Error>;
