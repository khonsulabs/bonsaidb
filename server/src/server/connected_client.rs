use std::{net::SocketAddr, ops::Deref, sync::Arc};

use actionable::Permissions;
use flume::Sender;
use pliantdb_core::custom_api::CustomApi;
use tokio::sync::RwLock;

use crate::{Backend, CustomServer};

/// The ways a client can be connected to the server.
#[derive(Debug, PartialEq, Eq)]
pub enum Transport {
    /// A connection over `PliantDb`'s QUIC-based protocol.
    Pliant,
    /// A connection over WebSockets.
    #[cfg(feature = "websockets")]
    WebSocket,
}

/// A connected database client.
#[derive(Debug)]
pub struct ConnectedClient<B: Backend = ()> {
    data: Arc<Data<B>>,
}

#[derive(Debug)]
struct Data<B: Backend = ()> {
    id: u32,
    address: SocketAddr,
    transport: Transport,
    response_sender: Sender<<B::CustomApi as CustomApi>::Response>,
    permissions: RwLock<Permissions>,
}

impl<B: Backend> Clone for ConnectedClient<B> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
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
        let permissions = self.data.permissions.read().await;
        permissions.clone()
    }

    pub(crate) async fn set_permissions(&self, new_permissions: Permissions) {
        let mut permissions = self.data.permissions.write().await;
        *permissions = new_permissions;
    }

    /// Sends a custom API response to the client.
    pub fn send(
        &self,
        response: <B::CustomApi as CustomApi>::Response,
    ) -> Result<(), flume::SendError<<B::CustomApi as CustomApi>::Response>> {
        self.data.response_sender.send(response)
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
        response_sender: Sender<<B::CustomApi as CustomApi>::Response>,
        server: CustomServer<B>,
    ) -> Self {
        Self {
            client: ConnectedClient {
                data: Arc::new(Data {
                    id,
                    address,
                    transport,
                    response_sender,
                    permissions: RwLock::new(server.data.default_permissions.clone()),
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
        self.runtime
            .spawn(async move { server.disconnect_client(id).await });
    }
}

impl<B: Backend> Deref for OwnedClient<B> {
    type Target = ConnectedClient<B>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
