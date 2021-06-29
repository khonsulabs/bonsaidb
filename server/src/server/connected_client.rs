use std::{net::SocketAddr, sync::Arc};

use flume::Sender;
use pliantdb_core::custom_api::CustomApi;

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
}

impl<B: Backend> Clone for ConnectedClient<B> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

impl<B: Backend> ConnectedClient<B> {
    pub(crate) fn new(
        id: u32,
        address: SocketAddr,
        transport: Transport,
        response_sender: Sender<<B::CustomApi as CustomApi>::Response>,
        server: CustomServer<B>,
    ) -> (Self, Disconnector<B>) {
        (
            Self {
                data: Arc::new(Data {
                    id,
                    address,
                    transport,
                    response_sender,
                }),
            },
            Disconnector::new(id, server),
        )
    }

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

    /// Sends a custom API response to the client.
    pub fn send(
        &self,
        response: <B::CustomApi as CustomApi>::Response,
    ) -> Result<(), flume::SendError<<B::CustomApi as CustomApi>::Response>> {
        self.data.response_sender.send(response)
    }
}

#[derive(Debug)]
pub struct Disconnector<B: Backend> {
    runtime: tokio::runtime::Handle,
    server: Option<CustomServer<B>>,
    id: u32,
}

impl<B: Backend> Disconnector<B> {
    fn new(id: u32, server: CustomServer<B>) -> Self {
        Self {
            id,
            runtime: tokio::runtime::Handle::current(),
            server: Some(server),
        }
    }
}

impl<B: Backend> Drop for Disconnector<B> {
    fn drop(&mut self) {
        let id = self.id;
        let server = self.server.take().unwrap();
        self.runtime
            .spawn(async move { server.disconnect_client(id).await });
    }
}
