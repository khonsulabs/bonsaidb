use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use async_lock::{Mutex, MutexGuard, RwLock};
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::Session,
    custom_api::{CustomApi, CustomApiResult},
    networking::Response,
    permissions::Permissions,
};
use bonsaidb_utils::{fast_async_lock, fast_async_read, fast_async_write};
use derive_where::derive_where;
use flume::Sender;

use crate::{Backend, CustomServer, Error, NoBackend};

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
    sessions: RwLock<HashMap<Option<u64>, Session>>,
    address: SocketAddr,
    transport: Transport,
    response_sender: Sender<(Option<u64>, Response)>,
    client_data: Mutex<Option<B::ClientData>>,
    subscriber_ids: Mutex<HashSet<u64>>,
}

#[derive(Debug, Default)]
struct AuthenticationState {
    user_id: Option<u64>,
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

    pub(crate) async fn logged_in_as(&self, session: Session) {
        let mut sessions = fast_async_write!(self.data.sessions);
        sessions.insert(session.id, session);
    }

    /// Sends a custom API response to the client.
    pub fn send<Api: CustomApi>(
        &self,
        session: Option<&Session>,
        response: &CustomApiResult<Api>,
    ) -> Result<(), Error> {
        let encoded = pot::to_vec(&response)?;
        self.data.response_sender.send((
            session.and_then(|session| session.id),
            Response::Api {
                name: Api::name(),
                response: Bytes::from(encoded),
            },
        ))?;
        Ok(())
    }

    /// Returns a locked reference to the stored client data.
    pub async fn client_data(&self) -> LockedClientDataGuard<'_, B::ClientData> {
        LockedClientDataGuard(fast_async_lock!(self.data.client_data))
    }

    pub async fn session(&self, session_id: Option<u64>) -> Session {
        let sessions = fast_async_read!(self.data.sessions);
        sessions.get(&session_id).cloned().unwrap_or_default()
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
        response_sender: Sender<(Option<u64>, Response)>,
        server: CustomServer<B>,
    ) -> Self {
        Self {
            client: ConnectedClient {
                data: Arc::new(Data {
                    id,
                    address,
                    transport,
                    response_sender,
                    sessions: RwLock::default(),
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
