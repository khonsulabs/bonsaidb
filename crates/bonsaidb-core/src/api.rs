use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::schema::ApiName;

/// An API request type.
pub trait Api: Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug + 'static {
    /// The type that represents an API response. This type will be sent to clients from the server.
    type Response: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug;
    /// The error type that this  can return.
    type Error: ApiError;

    /// Returns the unique name of this api.
    fn name() -> ApiName;
}
/// An Error type that can be used in within an [`Api`] definition.
///
/// The reason `std::convert::Infallible` can't be used is because `Api`
/// errors must be able to be serialized across a network connection. While a
/// value will never be present when this is Infallible, the associated type
/// still must be declared as Serializable.
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
#[error("an unreachable error")]
pub enum Infallible {}

/// An error that can be used within a [`Api`] definition.
pub trait ApiError:
    std::fmt::Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug
{
}

impl<T> ApiError for T where
    T: std::fmt::Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug
{
}

/// The result of executing a custom API call.
pub type ApiResult<Api> = Result<<Api as self::Api>::Response, <Api as self::Api>::Error>;
