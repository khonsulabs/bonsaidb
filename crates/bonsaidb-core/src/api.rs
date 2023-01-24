use std::fmt::{Debug, Display};
use std::ops::Deref;

pub use bonsaidb_macros::Api;
use serde::{Deserialize, Serialize};

use crate::schema::{Authority, Name, Qualified, QualifiedName};

/// An API request type. This trait is used by BonsaiDb's server to allow a
/// client to send a request of this type, and the server can respond with a
/// `Result<`[`Api::Response`]`,`[`Api::Error`]`>`.
///
/// # Deriving this trait
///
/// This trait can be derived. The only required attribute is `name`:
///
/// - `name = "api-name"` or `name = "api-name", authority = "api-authority"`:
///   Configures the Api's fully qualified name. This name must be unique across
///   all other Apis installed. When creating an Api that is meant to be reused,
///   ensure that a unique authority is used to prevent name collisions.
/// - `response = ResponseType`: Configures the [`Api::Response`] associated
///   type. This is the type that the handler will return upon success. If not
///   specified, `()` is used.
/// - `error = ErrorType`: Configures the [`Api::Error`] associated type. This
///   is the type that the handler will return upon error. If not specified,
///   [`Infallible`] is used.
///
/// ```rust
/// use bonsaidb_core::api::Api;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Api, Debug, Serialize, Deserialize)]
/// #[api(name = "list-records", response = Vec<Record>)]
/// # #[api(core = bonsaidb_core)]
/// struct ListRecords {
///     starting_id: u64,
/// }
///
/// #[derive(Debug, Serialize, Deserialize, Clone)]
/// struct Record {
///     title: String,
/// }
/// ```
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

/// The qualified name of an [`Api`](crate::api::Api).
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(transparent)]
pub struct ApiName(QualifiedName);

impl Qualified for ApiName {
    fn new<A: Into<Authority>, N: Into<Name>>(authority: A, name: N) -> Self {
        Self(QualifiedName::new(authority, name))
    }
}

impl Display for ApiName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Deref for ApiName {
    type Target = QualifiedName;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
