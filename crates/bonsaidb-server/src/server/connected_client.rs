use std::{
    collections::HashSet,
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use async_lock::{Mutex, MutexGuard, RwLock};
use bonsaidb_core::{custom_api::CustomApiResult, document::DocumentId, permissions::Permissions};
use bonsaidb_utils::{fast_async_lock, fast_async_read, fast_async_write};
use derive_where::derive_where;
use flume::Sender;

use crate::{Backend, CustomServer, NoBackend};

/// The ways a client can be connected to the server.
#[derive(Debug, PartialEq, Eq)]
pub enum Transport {
    /// A connection over BonsaiDb's QUIC-based protocol.
    Bonsai,
    /// A connection over WebSockets.
    #[cfg(feature = "websockets")]
    WebSocket,
}

/// A connected database client.
#[derive(Debug)]
#[derive_where(Clone)]
pub struct ConnectedClient<B: Backend = NoBackend> {
    data: Arc<Data<B>>,
}

#[derive(Debug)]
struct Data<B: Backend = NoBackend> {
    id: u32,
    address: SocketAddr,
    transport: Transport,
    response_sender: Sender<CustomApiResult<B::CustomApi>>,
    auth_state: RwLock<AuthenticationState>,
    client_data: Mutex<Option<B::ClientData>>,
    subscriber_ids: Mutex<HashSet<u64>>,
}

#[derive(Debug, Default)]
struct AuthenticationState {
    user_id: Option<DocumentId>,
    permissions: Permissions,
}

impl<B: Backend> ConnectedClient<B> {
    /// Returns the address of the connected client.
    #[must_use]
    pub fn address(&self) -> &SocketAddr {
        &self.data.address
    }

    /// Returns the transport method the client is connected via.
    #[must_use]
    pub fn transport(&self) -> &Transport {
        &self.data.transport
    }

    /// Returns the current permissions for this client. Will reflect the
    /// current state of authentication.
    pub async fn permissions(&self) -> Permissions {
        let auth_state = fast_async_read!(self.data.auth_state);
        auth_state.permissions.clone()
    }

    /// Returns the unique id of the user this client is connected as. Returns
    /// None if the connection isn't authenticated.
    pub async fn user_id(&self) -> Option<DocumentId> {
        let auth_state = fast_async_read!(self.data.auth_state);
        auth_state.user_id
    }

    pub(crate) async fn logged_in_as(&self, user_id: DocumentId, new_permissions: Permissions) {
        let mut auth_state = fast_async_write!(self.data.auth_state);
        auth_state.user_id = Some(user_id);
        auth_state.permissions = new_permissions;
    }

    pub(crate) async fn owns_subscriber(&self, subscriber_id: u64) -> bool {
        let subscriber_ids = fast_async_lock!(self.data.subscriber_ids);
        subscriber_ids.contains(&subscriber_id)
    }

    pub(crate) async fn register_subscriber(&self, subscriber_id: u64) {
        let mut subscriber_ids = fast_async_lock!(self.data.subscriber_ids);
        subscriber_ids.insert(subscriber_id);
    }

    pub(crate) async fn remove_subscriber(&self, subscriber_id: u64) -> bool {
        let mut subscriber_ids = fast_async_lock!(self.data.subscriber_ids);
        subscriber_ids.remove(&subscriber_id)
    }

    /// Sends a custom API response to the client.
    pub fn send(
        &self,
        response: CustomApiResult<B::CustomApi>,
    ) -> Result<(), flume::SendError<CustomApiResult<B::CustomApi>>> {
        self.data.response_sender.send(response)
    }

    /// Returns a locked reference to the stored client data.
    pub async fn client_data(&self) -> LockedClientDataGuard<'_, B::ClientData> {
        LockedClientDataGuard(fast_async_lock!(self.data.client_data))
    }

    /// Sets the associated data for this client.
    pub async fn set_client_data(&self, data: B::ClientData) {
        let mut client_data = fast_async_lock!(self.data.client_data);
        *client_data = Some(data);
    }
}

/// A locked reference to associated client data.
pub struct LockedClientDataGuard<'client, ClientData>(MutexGuard<'client, Option<ClientData>>);

impl<'client, ClientData> Deref for LockedClientDataGuard<'client, ClientData> {
    type Target = Option<ClientData>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'client, ClientData> DerefMut for LockedClientDataGuard<'client, ClientData> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct OwnedClient<B: Backend> {
    client: ConnectedClient<B>,
    runtime: tokio::runtime::Handle,
    server: Option<CustomServer<B>>,
}

impl<B: Backend> OwnedClient<B> {
    pub(crate) fn new(
        id: u32,
        address: SocketAddr,
        transport: Transport,
        response_sender: Sender<CustomApiResult<B::CustomApi>>,
        server: CustomServer<B>,
    ) -> Self {
        Self {
            client: ConnectedClient {
                data: Arc::new(Data {
                    id,
                    address,
                    transport,
                    response_sender,
                    auth_state: RwLock::new(AuthenticationState {
                        permissions: server.data.default_permissions.clone(),
                        user_id: None,
                    }),
                    client_data: Mutex::default(),
                    subscriber_ids: Mutex::default(),
                }),
            },
            runtime: tokio::runtime::Handle::current(),
            server: Some(server),
        }
    }

    pub fn clone(&self) -> ConnectedClient<B> {
        self.client.clone()
    }
}

impl<B: Backend> Drop for OwnedClient<B> {
    fn drop(&mut self) {
        let id = self.client.data.id;
        let server = self.server.take().unwrap();
        self.runtime.spawn(async move {
            server.disconnect_client(id).await;
        });
    }
}

impl<B: Backend> Deref for OwnedClient<B> {
    type Target = ConnectedClient<B>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
