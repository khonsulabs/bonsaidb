use std::any::TypeId;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
#[cfg(feature = "test-util")]
use std::sync::atomic::AtomicBool;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bonsaidb_core::admin::{Admin, ADMIN_DATABASE_NAME};
use bonsaidb_core::api::{self, Api, ApiName};
use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::arc_bytes::OwnedBytes;
use bonsaidb_core::connection::{
    AsyncStorageConnection, Database, HasSession, IdentityReference, Session,
};
use bonsaidb_core::networking::{
    AlterUserPermissionGroupMembership, AlterUserRoleMembership, AssumeIdentity, CreateDatabase,
    CreateUser, DeleteDatabase, DeleteUser, ListAvailableSchemas, ListDatabases, LogOutSession,
    MessageReceived, Payload, UnregisterSubscriber, CURRENT_PROTOCOL_VERSION,
};
use bonsaidb_core::permissions::Permissions;
use bonsaidb_core::schema::{Nameable, Schema, SchemaName, SchemaSummary, Schematic};
use bonsaidb_utils::fast_async_lock;
use flume::Sender;
use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use parking_lot::Mutex;
#[cfg(not(target_arch = "wasm32"))]
use tokio::{runtime::Handle, task::JoinHandle};
use url::Url;

pub use self::remote_database::{AsyncRemoteDatabase, AsyncRemoteSubscriber};
#[cfg(not(target_arch = "wasm32"))]
pub use self::sync::{BlockingClient, BlockingRemoteDatabase, BlockingRemoteSubscriber};
use crate::builder::Async;
use crate::error::Error;
use crate::{ApiError, Builder};

#[cfg(not(target_arch = "wasm32"))]
mod quic_worker;
mod remote_database;
#[cfg(not(target_arch = "wasm32"))]
mod sync;
#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
mod tungstenite_worker;
#[cfg(all(feature = "websockets", target_arch = "wasm32"))]
mod wasm_websocket_worker;

#[derive(Debug, Clone, Default)]
pub struct SubscriberMap(Arc<Mutex<HashMap<u64, flume::Sender<Message>>>>);

impl SubscriberMap {
    pub fn clear(&self) {
        let mut data = self.lock();
        data.clear();
    }
}

impl Deref for SubscriberMap {
    type Target = Mutex<HashMap<u64, flume::Sender<Message>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

use bonsaidb_core::circulate::Message;

#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
pub type WebSocketError = tokio_tungstenite::tungstenite::Error;

#[cfg(all(feature = "websockets", target_arch = "wasm32"))]
pub type WebSocketError = wasm_websocket_worker::WebSocketError;

/// Client for connecting to a BonsaiDb server.
///
/// ## How this type automatically reconnects
///
/// This type is designed to automatically reconnect if the underlying network
/// connection has been lost. When a disconnect happens, the error that caused
/// the disconnection will be returned to at least one requestor. If multiple
/// pending requests are outstanding, all remaining pending requests will have
/// an [`Error::Disconnected`] returned. This allows the application to detect
/// when a networking issue has arisen.
///
/// If the disconnect happens while the client is completely idle, the next
/// request will report the disconnection error. The subsequent request will
/// cause the client to begin reconnecting again.
///
/// When unauthenticated, this reconnection behavior is mostly transparent --
/// disconnection errors can be shown to the user, and service will be restored
/// automatically. However, when dealing with authentication, the client does
/// not store credentials to be able to send them again when reconnecting. This
/// means that the existing client handles will lose their authentication when
/// the network connection is broken. The current authentication status can be
/// checked using [`HasSession::session()`].
///
/// ## Connecting via QUIC
///
/// The URL scheme to connect via QUIC is `bonsaidb`. If no port is specified,
/// port 5645 is assumed.
///
/// ### With a valid TLS certificate
///
/// ```rust
/// # use bonsaidb_client::{AsyncClient, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = AsyncClient::build(Url::parse("bonsaidb://my-server.com")?).build()?;
/// # Ok(())
/// # }
/// ```
///
/// ### With a Self-Signed Pinned Certificate
///
/// When using `install_self_signed_certificate()`, clients will need the
/// contents of the `pinned-certificate.der` file within the database. It can be
/// specified when building the client:
///
/// ```rust
/// # use bonsaidb_client::{AsyncClient, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let certificate =
///     Certificate::from_der(std::fs::read("mydb.bonsaidb/pinned-certificate.der")?)?;
/// let client = AsyncClient::build(Url::parse("bonsaidb://localhost")?)
///     .with_certificate(certificate)
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// ## Connecting via WebSockets
///
/// WebSockets are built atop the HTTP protocol. There are two URL schemes for
/// WebSockets:
///
/// - `ws`: Insecure WebSockets. Port 80 is assumed if no port is specified.
/// - `wss`: Secure WebSockets. Port 443 is assumed if no port is specified.
///
/// ### Without TLS
///
/// ```rust
/// # use bonsaidb_client::{AsyncClient, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = AsyncClient::build(Url::parse("ws://localhost")?).build()?;
/// # Ok(())
/// # }
/// ```
///
/// ### With TLS
///
/// ```rust
/// # use bonsaidb_client::{AsyncClient, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = AsyncClient::build(Url::parse("wss://my-server.com")?).build()?;
/// # Ok(())
/// # }
/// ```
///
/// ## Using a `Api`
///
/// Our user guide has a [section on creating and
/// using](https://dev.bonsaidb.io/release/guide/about/access-models/custom-api-server.html)
/// an [`Api`](api::Api).
///
/// ```rust
/// # use bonsaidb_client::{AsyncClient, fabruic::Certificate, url::Url};
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_client::core`.
/// use bonsaidb_core::api::{Api, ApiName, Infallible};
/// use bonsaidb_core::schema::Qualified;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug)]
/// pub struct Ping;
///
/// #[derive(Serialize, Deserialize, Clone, Debug)]
/// pub struct Pong;
///
/// impl Api for Ping {
///     type Error = Infallible;
///     type Response = Pong;
///
///     fn name() -> ApiName {
///         ApiName::private("ping")
///     }
/// }
///
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = AsyncClient::build(Url::parse("bonsaidb://localhost")?).build()?;
/// let Pong = client.send_api_request(&Ping).await?;
/// # Ok(())
/// # }
/// ```
///
/// ### Receiving out-of-band messages from the server
///
/// If the server sends a message that isn't in response to a request, the
/// client will invoke it's [api callback](Builder::with_api_callback):
///
/// ```rust
/// # use bonsaidb_client::{AsyncClient, ApiCallback, fabruic::Certificate, url::Url};
/// # // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_client::core`.
/// # use bonsaidb_core::{api::{Api, Infallible, ApiName}, schema::{Qualified}};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Serialize, Deserialize, Debug)]
/// # pub struct Ping;
/// # #[derive(Serialize, Deserialize, Clone, Debug)]
/// # pub struct Pong;
/// # impl Api for Ping {
/// #     type Response = Pong;
/// #     type Error = Infallible;
/// #
/// #     fn name() -> ApiName {
/// #         ApiName::private("ping")
/// #     }
/// # }
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = AsyncClient::build(Url::parse("bonsaidb://localhost")?)
///     .with_api_callback(ApiCallback::<Ping>::new(|result: Pong| async move {
///         println!("Received out-of-band Pong");
///     }))
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct AsyncClient {
    pub(crate) data: Arc<Data>,
    session: ClientSession,
}

impl Drop for AsyncClient {
    fn drop(&mut self) {
        if self.session_is_current() && Arc::strong_count(&self.session.session) == 1 {
            if let Some(session_id) = self.session.session.id {
                // Final reference to an authenticated session
                drop(self.invoke_blocking_api_request(&LogOutSession(session_id)));
            }
        }
    }
}

impl PartialEq for AsyncClient {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
}

#[derive(Debug)]
pub struct Data {
    request_sender: Sender<PendingRequest>,
    #[cfg(not(target_arch = "wasm32"))]
    _worker: CancellableHandle<Result<(), Error>>,
    effective_permissions: Mutex<Option<Permissions>>,
    schemas: Mutex<HashMap<TypeId, Arc<Schematic>>>,
    connection_counter: Arc<AtomicU32>,
    request_id: AtomicU32,
    subscribers: SubscriberMap,
    #[cfg(feature = "test-util")]
    background_task_running: Arc<AtomicBool>,
}

impl AsyncClient {
    /// Returns a builder for a new client connecting to `url`.
    pub fn build(url: Url) -> Builder<Async> {
        Builder::new(url)
    }

    /// Initialize a client connecting to `url`. This client can be shared by
    /// cloning it. All requests are done asynchronously over the same
    /// connection.
    ///
    /// If the client has an error connecting, the first request made will
    /// present that error. If the client disconnects while processing requests,
    /// all requests being processed will exit and return
    /// [`Error::Disconnected`]. The client will automatically try reconnecting.
    ///
    /// The goal of this design of this reconnection strategy is to make it
    /// easier to build resilliant apps. By allowing existing Client instances
    /// to recover and reconnect, each component of the apps built can adopt a
    /// "retry-to-recover" design, or "abort-and-fail" depending on how critical
    /// the database is to operation.
    pub fn new(url: Url) -> Result<Self, Error> {
        Self::new_from_parts(
            url,
            CURRENT_PROTOCOL_VERSION,
            HashMap::default(),
            #[cfg(not(target_arch = "wasm32"))]
            None,
            #[cfg(not(target_arch = "wasm32"))]
            Handle::try_current().ok(),
        )
    }

    /// Initialize a client connecting to `url` with `certificate` being used to
    /// validate and encrypt the connection. This client can be shared by
    /// cloning it. All requests are done asynchronously over the same
    /// connection.
    ///
    /// If the client has an error connecting, the first request made will
    /// present that error. If the client disconnects while processing requests,
    /// all requests being processed will exit and return
    /// [`Error::Disconnected`]. The client will automatically try reconnecting.
    ///
    /// The goal of this design of this reconnection strategy is to make it
    /// easier to build resilliant apps. By allowing existing Client instances
    /// to recover and reconnect, each component of the apps built can adopt a
    /// "retry-to-recover" design, or "abort-and-fail" depending on how critical
    /// the database is to operation.
    pub(crate) fn new_from_parts(
        url: Url,
        protocol_version: &'static str,
        mut custom_apis: HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
        #[cfg(not(target_arch = "wasm32"))] certificate: Option<fabruic::Certificate>,
        #[cfg(not(target_arch = "wasm32"))] tokio: Option<Handle>,
    ) -> Result<Self, Error> {
        let subscribers = SubscriberMap::default();
        let callback_subscribers = subscribers.clone();
        custom_apis.insert(
            MessageReceived::name(),
            Some(Arc::new(ApiCallback::<MessageReceived>::new(
                move |message: MessageReceived| {
                    let callback_subscribers = callback_subscribers.clone();
                    async move {
                        let mut subscribers = callback_subscribers.lock();
                        if let Some(sender) = subscribers.get(&message.subscriber_id) {
                            if sender
                                .send(bonsaidb_core::circulate::Message {
                                    topic: OwnedBytes::from(message.topic.into_vec()),
                                    payload: OwnedBytes::from(message.payload.into_vec()),
                                })
                                .is_err()
                            {
                                subscribers.remove(&message.subscriber_id);
                            }
                        }
                    }
                },
            ))),
        );
        match url.scheme() {
            #[cfg(not(target_arch = "wasm32"))]
            "bonsaidb" => Ok(Self::new_bonsai_client(
                url,
                protocol_version,
                certificate,
                custom_apis,
                tokio,
                subscribers,
            )),
            #[cfg(feature = "websockets")]
            "wss" | "ws" => Ok(Self::new_websocket_client(
                url,
                protocol_version,
                custom_apis,
                #[cfg(not(target_arch = "wasm32"))]
                tokio,
                subscribers,
            )),
            other => Err(Error::InvalidUrl(format!("unsupported scheme {other}"))),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn new_bonsai_client(
        url: Url,
        protocol_version: &'static str,
        certificate: Option<fabruic::Certificate>,
        custom_apis: HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
        tokio: Option<Handle>,
        subscribers: SubscriberMap,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();
        let connection_counter = Arc::new(AtomicU32::default());

        let worker = sync::spawn_client(
            quic_worker::reconnecting_client_loop(
                url,
                protocol_version,
                certificate,
                request_receiver,
                Arc::new(custom_apis),
                subscribers.clone(),
                connection_counter.clone(),
            ),
            tokio,
        );

        #[cfg(feature = "test-util")]
        let background_task_running = Arc::new(AtomicBool::new(true));

        Self {
            data: Arc::new(Data {
                request_sender,
                _worker: CancellableHandle {
                    worker,
                    #[cfg(feature = "test-util")]
                    background_task_running: background_task_running.clone(),
                },
                schemas: Mutex::default(),
                connection_counter,
                request_id: AtomicU32::default(),
                effective_permissions: Mutex::default(),
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
            session: ClientSession::default(),
        }
    }

    #[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
    fn new_websocket_client(
        url: Url,
        protocol_version: &'static str,
        custom_apis: HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
        tokio: Option<Handle>,
        subscribers: SubscriberMap,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();
        let connection_counter = Arc::new(AtomicU32::default());

        let worker = sync::spawn_client(
            tungstenite_worker::reconnecting_client_loop(
                url,
                protocol_version,
                request_receiver,
                Arc::new(custom_apis),
                subscribers.clone(),
                connection_counter.clone(),
            ),
            tokio,
        );

        #[cfg(feature = "test-util")]
        let background_task_running = Arc::new(AtomicBool::new(true));

        Self {
            data: Arc::new(Data {
                request_sender,
                #[cfg(not(target_arch = "wasm32"))]
                _worker: CancellableHandle {
                    worker,
                    #[cfg(feature = "test-util")]
                    background_task_running: background_task_running.clone(),
                },
                schemas: Mutex::default(),
                request_id: AtomicU32::default(),
                connection_counter,
                effective_permissions: Mutex::default(),
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
            session: ClientSession::default(),
        }
    }

    #[cfg(all(feature = "websockets", target_arch = "wasm32"))]
    fn new_websocket_client(
        url: Url,
        protocol_version: &'static str,
        custom_apis: HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
        subscribers: SubscriberMap,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();
        let connection_counter = Arc::new(AtomicU32::default());

        wasm_websocket_worker::spawn_client(
            Arc::new(url),
            protocol_version,
            request_receiver,
            Arc::new(custom_apis),
            subscribers.clone(),
            connection_counter.clone(),
            None,
        );

        #[cfg(feature = "test-util")]
        let background_task_running = Arc::new(AtomicBool::new(true));

        Self {
            data: Arc::new(Data {
                request_sender,
                #[cfg(not(target_arch = "wasm32"))]
                worker: CancellableHandle {
                    worker,
                    #[cfg(feature = "test-util")]
                    background_task_running: background_task_running.clone(),
                },
                schemas: Mutex::default(),
                request_id: AtomicU32::default(),
                connection_counter,
                effective_permissions: Mutex::default(),
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
            session: ClientSession::default(),
        }
    }

    fn send_request_without_confirmation(
        &self,
        name: ApiName,
        bytes: Bytes,
    ) -> Result<flume::Receiver<Result<Bytes, Error>>, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        let id = self.data.request_id.fetch_add(1, Ordering::SeqCst);
        self.data.request_sender.send(PendingRequest {
            request: Payload {
                session_id: self.session.session.id,
                id: Some(id),
                name,
                value: Ok(bytes),
            },
            responder: result_sender,
        })?;

        Ok(result_receiver)
    }

    async fn send_request_async(&self, name: ApiName, bytes: Bytes) -> Result<Bytes, Error> {
        let result_receiver = self.send_request_without_confirmation(name, bytes)?;

        result_receiver.recv_async().await?
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn send_request(&self, name: ApiName, bytes: Bytes) -> Result<Bytes, Error> {
        let result_receiver = self.send_request_without_confirmation(name, bytes)?;

        result_receiver.recv()?
    }

    /// Sends an api `request`.
    pub async fn send_api_request<Api: api::Api>(
        &self,
        request: &Api,
    ) -> Result<Api::Response, ApiError<Api::Error>> {
        let request = Bytes::from(pot::to_vec(request).map_err(Error::from)?);
        let response = self.send_request_async(Api::name(), request).await?;
        let response =
            pot::from_slice::<Result<Api::Response, Api::Error>>(&response).map_err(Error::from)?;
        response.map_err(ApiError::Api)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn send_blocking_api_request<Api: api::Api>(
        &self,
        request: &Api,
    ) -> Result<Api::Response, ApiError<Api::Error>> {
        let request = Bytes::from(pot::to_vec(request).map_err(Error::from)?);
        let response = self.send_request(Api::name(), request)?;

        let response =
            pot::from_slice::<Result<Api::Response, Api::Error>>(&response).map_err(Error::from)?;
        response.map_err(ApiError::Api)
    }

    fn invoke_blocking_api_request<Api: api::Api>(&self, request: &Api) -> Result<(), Error> {
        let request = Bytes::from(pot::to_vec(request).map_err(Error::from)?);
        self.send_request_without_confirmation(Api::name(), request)
            .map(|_| ())
    }

    /// Returns the current effective permissions for the client. Returns None
    /// if unauthenticated.
    #[must_use]
    pub fn effective_permissions(&self) -> Option<Permissions> {
        let effective_permissions = self.data.effective_permissions.lock();
        effective_permissions.clone()
    }

    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn background_task_running(&self) -> Arc<AtomicBool> {
        self.data.background_task_running.clone()
    }

    pub(crate) fn register_subscriber(&self, id: u64, sender: flume::Sender<Message>) {
        let mut subscribers = self.data.subscribers.lock();
        subscribers.insert(id, sender);
    }

    pub(crate) async fn unregister_subscriber_async(&self, database: String, id: u64) {
        drop(
            self.send_api_request(&UnregisterSubscriber {
                database,
                subscriber_id: id,
            })
            .await,
        );
        let mut subscribers = self.data.subscribers.lock();
        subscribers.remove(&id);
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn unregister_subscriber(&self, database: String, id: u64) {
        drop(self.send_blocking_api_request(&UnregisterSubscriber {
            database,
            subscriber_id: id,
        }));
        let mut subscribers = self.data.subscribers.lock();
        subscribers.remove(&id);
    }

    fn remote_database<DB: bonsaidb_core::schema::Schema>(
        &self,
        name: &str,
    ) -> Result<AsyncRemoteDatabase, bonsaidb_core::Error> {
        let mut schemas = self.data.schemas.lock();
        let type_id = TypeId::of::<DB>();
        let schematic = if let Some(schematic) = schemas.get(&type_id) {
            schematic.clone()
        } else {
            let schematic = Arc::new(DB::schematic()?);
            schemas.insert(type_id, schematic.clone());
            schematic
        };
        Ok(AsyncRemoteDatabase::new(
            self.clone(),
            name.to_string(),
            schematic,
        ))
    }

    fn session_is_current(&self) -> bool {
        self.session.session.id.is_none()
            || self.data.connection_counter.load(Ordering::SeqCst) == self.session.connection_id
    }
}

impl HasSession for AsyncClient {
    fn session(&self) -> Option<&Session> {
        self.session_is_current().then_some(&self.session.session)
    }
}

#[async_trait]
impl AsyncStorageConnection for AsyncClient {
    type Authenticated = Self;
    type Database = AsyncRemoteDatabase;

    async fn admin(&self) -> Self::Database {
        self.remote_database::<Admin>(ADMIN_DATABASE_NAME).unwrap()
    }

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&CreateDatabase {
            database: Database {
                name: name.to_string(),
                schema,
            },
            only_if_needed,
        })
        .await?;
        Ok(())
    }

    async fn database<DB: Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        self.remote_database::<DB>(name)
    }

    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&DeleteDatabase {
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<Database>, bonsaidb_core::Error> {
        Ok(self.send_api_request(&ListDatabases).await?)
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaSummary>, bonsaidb_core::Error> {
        Ok(self.send_api_request(&ListAvailableSchemas).await?)
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        Ok(self
            .send_api_request(&CreateUser {
                username: username.to_string(),
            })
            .await?)
    }

    async fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        Ok(self
            .send_api_request(&DeleteUser {
                user: user.name()?.into_owned(),
            })
            .await?)
    }

    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        Ok(self
            .send_api_request(&bonsaidb_core::networking::SetUserPassword {
                user: user.name()?.into_owned(),
                password,
            })
            .await?)
    }

    #[cfg(any(feature = "token-authentication", feature = "password-hashing"))]
    async fn authenticate(
        &self,
        authentication: bonsaidb_core::connection::Authentication,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session = self
            .send_api_request(&bonsaidb_core::networking::Authenticate { authentication })
            .await?;
        Ok(Self {
            data: self.data.clone(),
            session: ClientSession {
                session: Arc::new(session),
                connection_id: self.data.connection_counter.load(Ordering::SeqCst),
            },
        })
    }

    async fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session = self
            .send_api_request(&AssumeIdentity(identity.into_owned()))
            .await?;
        Ok(Self {
            data: self.data.clone(),
            session: ClientSession {
                session: Arc::new(session),
                connection_id: self.data.connection_counter.load(Ordering::SeqCst),
            },
        })
    }

    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserPermissionGroupMembership {
            user: user.name()?.into_owned(),
            group: permission_group.name()?.into_owned(),
            should_be_member: true,
        })
        .await?;
        Ok(())
    }

    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserPermissionGroupMembership {
            user: user.name()?.into_owned(),
            group: permission_group.name()?.into_owned(),
            should_be_member: false,
        })
        .await?;
        Ok(())
    }

    async fn add_role_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserRoleMembership {
            user: user.name()?.into_owned(),
            role: role.name()?.into_owned(),
            should_be_member: true,
        })
        .await?;
        Ok(())
    }

    async fn remove_role_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request(&AlterUserRoleMembership {
            user: user.name()?.into_owned(),
            role: role.name()?.into_owned(),
            should_be_member: false,
        })
        .await?;
        Ok(())
    }
}

type OutstandingRequestMap = HashMap<u32, PendingRequest>;
type OutstandingRequestMapHandle = Arc<async_lock::Mutex<OutstandingRequestMap>>;
type PendingRequestResponder = Sender<Result<Bytes, Error>>;

#[derive(Debug)]
pub struct PendingRequest {
    request: Payload,
    responder: PendingRequestResponder,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct CancellableHandle<T> {
    worker: JoinHandle<T>,
    #[cfg(feature = "test-util")]
    background_task_running: Arc<AtomicBool>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> Drop for CancellableHandle<T> {
    fn drop(&mut self) {
        self.worker.abort();
        #[cfg(feature = "test-util")]
        self.background_task_running.store(false, Ordering::Release);
    }
}

async fn process_response_payload(
    payload: Payload,
    outstanding_requests: &OutstandingRequestMapHandle,
    custom_apis: &HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
) {
    if let Some(payload_id) = payload.id {
        if let Some(outstanding_request) = {
            let mut outstanding_requests = fast_async_lock!(outstanding_requests);
            outstanding_requests.remove(&payload_id)
        } {
            drop(
                outstanding_request
                    .responder
                    .send(payload.value.map_err(Error::from)),
            );
        }
    } else if let (Some(custom_api_callback), Ok(value)) = (
        custom_apis.get(&payload.name).and_then(Option::as_ref),
        payload.value,
    ) {
        custom_api_callback.response_received(value).await;
    } else {
        log::warn!("unexpected api response received ({})", payload.name);
    }
}

trait ApiWrapper<Response>: Send + Sync {
    fn invoke(&self, response: Response) -> BoxFuture<'static, ()>;
}

/// A callback that is invoked when an [`Api::Response`](Api::Response)
/// value is received out-of-band (not in reply to a request).
pub struct ApiCallback<Api: api::Api> {
    generator: Box<dyn ApiWrapper<Api::Response>>,
}

/// The trait bounds required for the function wrapped in a [`ApiCallback`].
pub trait ApiCallbackFn<Request, F>: Fn(Request) -> F + Send + Sync + 'static {}

impl<T, Request, F> ApiCallbackFn<Request, F> for T where T: Fn(Request) -> F + Send + Sync + 'static
{}

struct ApiFutureBoxer<Response: Send + Sync, F: Future<Output = ()> + Send + Sync>(
    Box<dyn ApiCallbackFn<Response, F>>,
);

impl<Response: Send + Sync, F: Future<Output = ()> + Send + Sync + 'static> ApiWrapper<Response>
    for ApiFutureBoxer<Response, F>
{
    fn invoke(&self, response: Response) -> BoxFuture<'static, ()> {
        self.0(response).boxed()
    }
}

impl<Api: api::Api> ApiCallback<Api> {
    /// Returns a new instance wrapping the provided function.
    pub fn new<
        F: ApiCallbackFn<Api::Response, Fut>,
        Fut: Future<Output = ()> + Send + Sync + 'static,
    >(
        callback: F,
    ) -> Self {
        Self {
            generator: Box::new(ApiFutureBoxer::<Api::Response, Fut>(Box::new(callback))),
        }
    }

    /// Returns a new instance wrapping the provided function, passing a clone
    /// of `context` as the second parameter. This is just a convenience wrapper
    /// around `new()` that produces more readable code when needing to access
    /// external information inside of the callback.
    pub fn new_with_context<
        Context: Send + Sync + Clone + 'static,
        F: Fn(Api::Response, Context) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + Sync + 'static,
    >(
        context: Context,
        callback: F,
    ) -> Self {
        Self {
            generator: Box::new(ApiFutureBoxer::<Api::Response, Fut>(Box::new(
                move |request| {
                    let context = context.clone();
                    callback(request, context)
                },
            ))),
        }
    }
}

#[async_trait]
pub trait AnyApiCallback: Send + Sync + 'static {
    /// An out-of-band `response` was received. This happens when the server
    /// sends a response that isn't in response to a request.
    async fn response_received(&self, response: Bytes);
}

#[async_trait]
impl<Api: api::Api> AnyApiCallback for ApiCallback<Api> {
    async fn response_received(&self, response: Bytes) {
        match pot::from_slice::<Result<Api::Response, Api::Error>>(&response) {
            Ok(response) => self.generator.invoke(response.unwrap()).await,
            Err(err) => {
                log::error!("error deserializing api: {err}");
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ClientSession {
    session: Arc<Session>,
    connection_id: u32,
}

async fn disconnect_pending_requests(
    outstanding_requests: &OutstandingRequestMapHandle,
    pending_error: &mut Option<Error>,
) {
    let mut outstanding_requests = fast_async_lock!(outstanding_requests);
    for (_, pending) in outstanding_requests.drain() {
        drop(
            pending
                .responder
                .send(Err(pending_error.take().unwrap_or(Error::Disconnected))),
        );
    }
}
