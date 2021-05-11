use std::fmt::Debug;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::permissions::Dispatcher;

/// Tailors the behavior of a server to your needs.
#[async_trait]
pub trait Backend: Debug + Send + Sync + 'static {
    /// The custom API definition. If you do not wish to have an API, `()` may be provided.
    type CustomApi: CustomApi;

    // TODO: add client connections events, client errors, etc.
}

/// A definition of a custom API. The `Request` associated type is what a
/// connected client can send. The `Response` associated type is what your API
/// is expecetd to send from the server to the client. The `Dispatcher` is the
/// type that will handle the custom API requests.
///
/// This feature is powered by [`actionable`] through these derives:
///
/// * [`Action`](crate::permissions::Action) (trait and derive macro)
/// * [`Dispatcher`](crate::permissions::Dispatcher) (trait and derive macro)
/// * [`Actionable`](crate::permissions::Actionable) (derive macro)
pub trait CustomApi: Debug + Send + Sync {
    /// The type that represents an API request. This type is what clients will send to the server.
    type Request: Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug;
    /// The type that represents an API response. This type will be sent to clients from the server.
    type Response: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug;

    /// The type that implements the [`Dispatcher`] trait.
    type Dispatcher: Dispatcher<Self::Request, Result = anyhow::Result<Self::Response>> + Debug;
}

impl Backend for () {
    type CustomApi = ();
}

impl CustomApi for () {
    type Request = ();
    type Response = ();
    type Dispatcher = NoDispatcher;
}

// This needs to be pub because of the impl, but the user doesn't need to know
// about this type.
#[doc(hidden)]
#[derive(Debug)]
pub struct NoDispatcher;

#[async_trait]
impl actionable::Dispatcher<()> for NoDispatcher {
    type Result = anyhow::Result<()>;

    async fn dispatch(&self, _permissions: &actionable::Permissions, _request: ()) -> Self::Result {
        Ok(())
    }
}
