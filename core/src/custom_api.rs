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
}

impl CustomApi for () {
    type Request = ();
    type Response = ();
}
