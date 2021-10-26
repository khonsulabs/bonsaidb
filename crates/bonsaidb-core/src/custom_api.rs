use std::fmt::Debug;

use serde::{Deserialize, Serialize};

/// A definition of a custom API. The `Request` associated type is what a
/// connected client can send. The `Response` associated type is what your API
/// is expecetd to send from the server to the client. The `Dispatcher` is the
/// type that will handle the custom API requests.
///
/// This feature is powered by [`actionable`] through these derives:
///
/// * [`Action`](crate::permissions::Action) (trait and derive macro)
/// * [`Actionable`](crate::permissions::Actionable) (derive macro)
pub trait CustomApi: Debug + Send + Sync + 'static {
    /// The type that represents an API request. This type is what clients will send to the server.
    type Request: Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug;
    /// The type that represents an API response. This type will be sent to clients from the server.
    type Response: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug;
    /// The error type that this Api instance can return.
    type Error: CustomApiError;
}

impl CustomApi for () {
    type Request = ();
    type Response = ();
    type Error = Infallible;
}

/// An Error type that can be used in within a [`CustomApi`] definition.
///
/// The reason `std::convert::Infallible` can't be used is because `CustomApi`
/// errors must be able to be serialized across a network connection. While a
/// value will never be present when this is Infallible, the associated type
/// still must be declared as Serializable.
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
#[error("an unreachable error")]
pub struct Infallible;

/// An error that can be used within a [`CustomApi`] definition.
pub trait CustomApiError:
    std::fmt::Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug
{
}

impl<T> CustomApiError for T where
    T: std::fmt::Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug
{
}

/// The result of executing a custom API call.
pub type CustomApiResult<Api> = Result<<Api as CustomApi>::Response, <Api as CustomApi>::Error>;
