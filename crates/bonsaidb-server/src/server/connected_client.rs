use std::{net::SocketAddr, ops::Deref, sync::Arc};

use actionable::Permissions;
use bonsaidb_core::{
    custodian_password::{LoginRequest, LoginResponse, ServerLogin},
    custom_api::CustomApiResult,
};
use flume::Sender;
use tokio::sync::{Mutex, RwLock};

use crate::{Backend, CustomServer};

/// The ways a client can be connected to the server.
#[derive(Debug, PartialEq, Eq)]
pub enum Transport {
    /// A connection over `BonsaiDb`'s QUIC-based protocol.
    Bonsai,
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
    response_sender: Sender<CustomApiResult<B::CustomApi>>,
    auth_state: RwLock<AuthenticationState>,
    pending_password_login: Mutex<Option<(Option<u64>, ServerLogin)>>,
}

#[derive(Debug, Default)]
struct AuthenticationState {
    user_id: Option<u64>,
    permissions: Permissions,
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
        let auth_state = self.data.auth_state.read().await;
        auth_state.permissions.clone()
    }

    /// Returns the unique id of the user this client is connected as. Returns
    /// None if the connection isn't authenticated.
    pub async fn user_id(&self) -> Option<u64> {
        let auth_state = self.data.auth_state.read().await;
        auth_state.user_id
    }

    /// Initiates a login request for this client.
    pub async fn initiate_login(
        &self,
        username: &str,
        password_request: LoginRequest,
        server: &CustomServer<B>,
    ) -> Result<LoginResponse, bonsaidb_core::Error> {
        let (user_id, login, response) = server
            .internal_login_with_password(username, password_request)
            .await?;
        self.set_pending_password_login(user_id, login).await;
        Ok(response)
    }

    pub(crate) async fn logged_in_as(&self, user_id: u64, new_permissions: Permissions) {
        let mut auth_state = self.data.auth_state.write().await;
        auth_state.user_id = Some(user_id);
        auth_state.permissions = new_permissions;
    }

    pub(crate) async fn set_pending_password_login(
        &self,
        user_id: Option<u64>,
        new_state: ServerLogin,
    ) {
        let mut pending_password_login = self.data.pending_password_login.lock().await;
        *pending_password_login = Some((user_id, new_state));
    }

    pub(crate) async fn take_pending_password_login(&self) -> Option<(Option<u64>, ServerLogin)> {
        let mut pending_password_login = self.data.pending_password_login.lock().await;
        pending_password_login.take()
    }

    /// Sends a custom API response to the client.
    pub fn send(
        &self,
        response: CustomApiResult<B::CustomApi>,
    ) -> Result<(), flume::SendError<CustomApiResult<B::CustomApi>>> {
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
                    pending_password_login: Mutex::default(),
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
