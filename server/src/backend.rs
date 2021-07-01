use std::fmt::Debug;

use async_trait::async_trait;
use pliantdb_core::{custom_api::CustomApi, permissions::Dispatcher};

use crate::server::ConnectedClient;

/// Tailors the behavior of a server to your needs.
#[async_trait]
pub trait Backend: Debug + Send + Sync + Sized + 'static {
    /// The custom API definition. If you do not wish to have an API, `()` may be provided.
    type CustomApi: CustomApi;

    /// The type that implements the [`Dispatcher`](pliantdb_core::permissions::Dispatcher) trait.
    type CustomApiDispatcher: Dispatcher<
            <Self::CustomApi as CustomApi>::Request,
            Result = anyhow::Result<<Self::CustomApi as CustomApi>::Response>,
        > + Debug;

    // TODO: add client connections events, client errors, etc.
    /// A client disconnected from the server. This is invoked before authentication has been performed.
    #[allow(unused_variables)]
    #[must_use]
    async fn client_connected(client: &ConnectedClient<Self>) -> ConnectionHandling {
        println!(
            "{:?} client connected from {:?}",
            client.transport(),
            client.address()
        );

        ConnectionHandling::Accept
    }

    /// A client disconnected from the server.
    #[allow(unused_variables)]
    async fn client_disconnected(client: ConnectedClient<Self>) {
        println!(
            "{:?} client disconnected ({:?})",
            client.transport(),
            client.address()
        );
    }
}

impl Backend for () {
    type CustomApi = ();
    type CustomApiDispatcher = NoDispatcher;
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

pub enum ConnectionHandling {
    Accept,
    Reject,
}
