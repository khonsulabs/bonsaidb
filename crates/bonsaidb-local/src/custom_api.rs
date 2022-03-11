use std::fmt::Debug;

use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    custom_api::{CustomApi, CustomApiError, Infallible},
    permissions::{Dispatcher, PermissionDenied, Permissions},
    schema::{InsertError, InvalidNameError},
};

use crate::{Error, Storage};

/// A trait that can dispatch requests for a [`CustomApi`].
pub trait CustomApiDispatcher:
    Dispatcher<<Self::Api as CustomApi>::Request, Result = BackendApiResult<Self::Api>> + Debug
{
    type Api: CustomApi;
    /// Returns a dispatcher to handle custom api requests. The `storage`
    /// instance is provided to allow the dispatcher to have access during
    /// dispatched calls.
    fn new(storage: &Storage) -> Self;
}

pub trait AnyCustomApiDispatcher: Send + Sync + Debug {
    fn dispatch(&self, permissions: &Permissions, request: &[u8]) -> Result<Bytes, Error>;
}

impl<T> AnyCustomApiDispatcher for T
where
    T: CustomApiDispatcher,
{
    fn dispatch(&self, permissions: &Permissions, request: &[u8]) -> Result<Bytes, Error> {
        let request = pot::from_slice(request)?;
        let response = match self.dispatch(permissions, request) {
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
        Self::Server(Error::from(bonsaidb_local::Error::from(other)))
    }
}

impl<E: CustomApiError> From<pot::Error> for DispatchError<E> {
    fn from(other: pot::Error) -> Self {
        DispatchError::Storage(Error::from(other))
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
