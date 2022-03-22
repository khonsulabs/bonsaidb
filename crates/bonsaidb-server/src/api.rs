use std::{fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use bonsaidb_core::{
    api::{self, Api, ApiError, Infallible},
    arc_bytes::serde::Bytes,
    permissions::PermissionDenied,
    schema::{InsertError, InvalidNameError},
};

use crate::{Backend, ConnectedClient, CustomServer, Error, NoBackend};

/// A trait that can dispatch requests for a [`Api`].
#[async_trait]
pub trait Handler<B: Backend, Api: api::Api>: Send + Sync {
    /// Returns a dispatcher to handle custom api requests. The parameters are
    /// provided so that they can be cloned if needed during the processing of
    /// requests.
    async fn handle(session: HandlerSession<'_, B>, request: Api) -> HandlerResult<Api>;
}

/// A session for a [`Handler`], providing ways to access the server and
/// connected client.
pub struct HandlerSession<'a, B: Backend = NoBackend> {
    /// The [`Handler`]'s server reference. This server instance is not limited
    /// to the permissions of the connected user.
    pub server: &'a CustomServer<B>,
    /// The connected client's server reference. This server instance will
    /// reject any database operations that the connected client is not
    /// explicitly authorized to perform based on its authentication state.
    pub as_client: CustomServer<B>,
    /// The connected client making the API request.
    pub client: &'a ConnectedClient<B>,
}

#[async_trait]
pub(crate) trait AnyHandler<B: Backend>: Send + Sync + Debug {
    async fn handle(&self, session: HandlerSession<'_, B>, request: &[u8]) -> Result<Bytes, Error>;
}

pub(crate) struct AnyWrapper<D: Handler<B, A>, B: Backend, A: Api>(
    pub(crate) PhantomData<(D, B, A)>,
);

impl<D, B, A> Debug for AnyWrapper<D, B, A>
where
    D: Handler<B, A>,
    B: Backend,
    A: Api,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyWrapper").finish()
    }
}

#[async_trait]
impl<T, B, A> AnyHandler<B> for AnyWrapper<T, B, A>
where
    B: Backend,
    T: Handler<B, A>,
    A: Api,
{
    async fn handle(&self, client: HandlerSession<'_, B>, request: &[u8]) -> Result<Bytes, Error> {
        let request = pot::from_slice(request)?;
        let response = match T::handle(client, request).await {
            Ok(response) => Ok(response),
            Err(HandlerError::Api(err)) => Err(err),
            Err(HandlerError::Server(err)) => return Err(err),
        };
        Ok(Bytes::from(pot::to_vec(&response)?))
    }
}

/// An error that can occur inside of a [`Backend`] function.
#[derive(thiserror::Error, Debug)]
pub enum HandlerError<E: ApiError = Infallible> {
    /// An api-related error.
    #[error("api error: {0}")]
    Api(E),
    /// A server-related error.
    #[error("server error: {0}")]
    Server(#[from] Error),
}

impl<E: ApiError> From<PermissionDenied> for HandlerError<E> {
    fn from(permission_denied: PermissionDenied) -> Self {
        Self::Server(Error::from(permission_denied))
    }
}

impl<E: ApiError> From<bonsaidb_core::Error> for HandlerError<E> {
    fn from(err: bonsaidb_core::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E: ApiError> From<bonsaidb_local::Error> for HandlerError<E> {
    fn from(err: bonsaidb_local::Error) -> Self {
        Self::Server(Error::from(err))
    }
}

impl<E: ApiError> From<InvalidNameError> for HandlerError<E> {
    fn from(err: InvalidNameError) -> Self {
        Self::Server(Error::from(err))
    }
}

#[cfg(feature = "websockets")]
impl<E: ApiError> From<bincode::Error> for HandlerError<E> {
    fn from(other: bincode::Error) -> Self {
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E: ApiError> From<pot::Error> for HandlerError<E> {
    fn from(other: pot::Error) -> Self {
        Self::Server(Error::from(other))
    }
}

impl<T, E> From<InsertError<T>> for HandlerError<E>
where
    E: ApiError,
{
    fn from(error: InsertError<T>) -> Self {
        Self::Server(Error::from(error.error))
    }
}

/// The return type from a [`Handler`]'s [`handle()`](Handler::handle)
/// function.
pub type HandlerResult<Api> =
    Result<<Api as api::Api>::Response, HandlerError<<Api as api::Api>::Error>>;
