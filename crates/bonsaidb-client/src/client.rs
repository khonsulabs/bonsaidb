#[cfg(feature = "test-util")]
use std::sync::atomic::AtomicBool;
use std::{
    any::TypeId,
    collections::HashMap,
    fmt::Debug,
    ops::Deref,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::Mutex;
use async_trait::async_trait;
#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::Authentication;
use bonsaidb_core::{
    admin::{Admin, ADMIN_DATABASE_NAME},
    api::{self, Api},
    arc_bytes::{serde::Bytes, OwnedBytes},
    connection::{AsyncStorageConnection, Database, Identity, Session},
    networking::{
        AlterUserPermissionGroupMembership, AlterUserRoleMembership, AssumeIdentity,
        CreateDatabase, CreateUser, DeleteDatabase, DeleteUser, ListAvailableSchemas,
        ListDatabases, MessageReceived, Payload, UnregisterSubscriber, CURRENT_PROTOCOL_VERSION,
    },
    permissions::Permissions,
    schema::{Name, Nameable, Schema, SchemaName, Schematic},
};
use bonsaidb_utils::fast_async_lock;
use flume::Sender;
use futures::{future::BoxFuture, Future, FutureExt};
#[cfg(not(target_arch = "wasm32"))]
use tokio::{runtime::Handle, task::JoinHandle};
use url::Url;

pub use self::remote_database::{RemoteDatabase, RemoteSubscriber};
use crate::{error::Error, ApiError, Builder};

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
pub struct SubscriberMap(Arc<Mutex<HashMap<u64, flume::Sender<Arc<Message>>>>>);

impl SubscriberMap {
    pub async fn clear(&self) {
        let mut data = fast_async_lock!(self);
        data.clear();
    }
}

impl Deref for SubscriberMap {
    type Target = Mutex<HashMap<u64, flume::Sender<Arc<Message>>>>;

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
///
///
/// ## Connecting via QUIC
///
/// The URL scheme to connect via QUIC is `bonsaidb`. If no port is specified,
/// port 5645 is assumed.
///
/// ### With a valid TLS certificate
///
/// ```rust
/// # use bonsaidb_client::{Client, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = Client::build(Url::parse("bonsaidb://my-server.com")?)
///     .finish()
///     .await?;
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
/// # use bonsaidb_client::{Client, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let certificate =
///     Certificate::from_der(std::fs::read("mydb.bonsaidb/pinned-certificate.der")?)?;
/// let client = Client::build(Url::parse("bonsaidb://localhost")?)
///     .with_certificate(certificate)
///     .finish()
///     .await?;
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
/// # use bonsaidb_client::{Client, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = Client::build(Url::parse("ws://localhost")?)
///     .finish()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ### With TLS
///
/// ```rust
/// # use bonsaidb_client::{Client, fabruic::Certificate, url::Url};
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = Client::build(Url::parse("wss://my-server.com")?)
///     .finish()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ## Using a `Api`
///
/// Our user guide has a [section on creating and using a
/// `Api`](https://dev.bonsaidb.io/release/guide/about/access-models/custom-api-server.html).
///
/// ```rust
/// # use bonsaidb_client::{Client, fabruic::Certificate, url::Url};
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_client::core`.
/// use bonsaidb_core::custom_api::{CustomApi, Infallible};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug)]
/// pub enum Request {
///     Ping,
/// }
///
/// #[derive(Serialize, Deserialize, Clone, Debug)]
/// pub enum Response {
///     Pong,
/// }
///
/// #[derive(Debug)]
/// pub enum MyApi {}
///
/// impl CustomApi for MyApi {
///     type Request = Request;
///     type Response = Response;
///     type Error = Infallible;
/// }
///
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = Client::build(Url::parse("bonsaidb://localhost")?)
///     .with_api::<MyApi>()
///     .finish()
///     .await?;
/// let Response::Pong = client.send_api_request(Request::Ping).await?;
/// # Ok(())
/// # }
/// ```
///
/// ### Receiving out-of-band messages from the server
///
/// If the server sends a message that isn't in response to a request, the
/// client will invoke it's [custom api
/// callback](Builder::with_custom_api_callback):
///
/// ```rust
/// # use bonsaidb_client::{Client, fabruic::Certificate, url::Url};
/// # // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_client::core`.
/// # use bonsaidb_core::custom_api::{CustomApi, Infallible};
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Serialize, Deserialize, Debug)]
/// # pub enum Request {
/// #     Ping
/// # }
/// # #[derive(Serialize, Deserialize, Clone, Debug)]
/// # pub enum Response {
/// #     Pong
/// # }
/// # #[derive(Debug)]
/// # pub enum MyApi {}
/// # impl CustomApi for MyApi {
/// #     type Request = Request;
/// #     type Response = Response;
/// #     type Error = Infallible;
/// # }
/// # async fn test_fn() -> anyhow::Result<()> {
/// let client = Client::build(Url::parse("bonsaidb://localhost")?)
///     .with_api_callback::<MyApi, _>(|result: Result<Response, Infallible>| {
///         let Response::Pong = result.unwrap();
///     })
///     .finish()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    pub(crate) data: Arc<Data>,
    session: Session,
}

impl PartialEq for Client {
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
    #[cfg(not(target_arch = "wasm32"))]
    tokio: Option<Handle>,
    schemas: Mutex<HashMap<TypeId, Arc<Schematic>>>,
    request_id: AtomicU32,
    subscribers: SubscriberMap,
    #[cfg(feature = "test-util")]
    background_task_running: Arc<AtomicBool>,
}

impl Client {
    /// Returns a builder for a new client connecting to `url`.
    pub fn build(url: Url) -> Builder {
        Builder::new(url)
    }
}

impl Client {
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
        mut custom_apis: HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>,
        #[cfg(not(target_arch = "wasm32"))] certificate: Option<fabruic::Certificate>,
        #[cfg(not(target_arch = "wasm32"))] tokio: Option<Handle>,
    ) -> Result<Self, Error> {
        let subscribers = SubscriberMap::default();
        let callback_subscribers = subscribers.clone();
        custom_apis.insert(
            MessageReceived::name(),
            Some(Arc::new(CustomApiCallback::<MessageReceived>::new(
                move |message: MessageReceived| {
                    let callback_subscribers = callback_subscribers.clone();
                    async move {
                        let mut subscribers = fast_async_lock!(callback_subscribers);
                        if let Some(sender) = subscribers.get(&message.subscriber_id) {
                            if sender
                                .send(std::sync::Arc::new(bonsaidb_core::circulate::Message {
                                    topic: message.topic,
                                    payload: OwnedBytes::from(message.payload.into_vec()),
                                }))
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
            other => {
                return Err(Error::InvalidUrl(format!("unsupported scheme {}", other)));
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn new_bonsai_client(
        url: Url,
        protocol_version: &'static str,
        certificate: Option<fabruic::Certificate>,
        custom_apis: HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>,
        tokio: Option<Handle>,
        subscribers: SubscriberMap,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        let worker = tokio::task::spawn(quic_worker::reconnecting_client_loop(
            url,
            protocol_version,
            certificate,
            request_receiver,
            Arc::new(custom_apis),
            subscribers.clone(),
        ));

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
                request_id: AtomicU32::default(),
                effective_permissions: Mutex::default(),
                subscribers,
                tokio,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
            session: Session::default(),
        }
    }

    #[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
    fn new_websocket_client(
        url: Url,
        protocol_version: &'static str,
        custom_apis: HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>,
        tokio: Option<Handle>,
        subscribers: SubscriberMap,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        let worker = tokio::task::spawn(tungstenite_worker::reconnecting_client_loop(
            url,
            protocol_version,
            request_receiver,
            Arc::new(custom_apis),
            subscribers.clone(),
        ));

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
                effective_permissions: Mutex::default(),
                subscribers,
                tokio,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
            session: Session::default(),
        }
    }

    #[cfg(all(feature = "websockets", target_arch = "wasm32"))]
    fn new_websocket_client(
        url: Url,
        protocol_version: &'static str,
        custom_apis: HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>,
        subscribers: SubscriberMap,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        wasm_websocket_worker::spawn_client(
            Arc::new(url),
            protocol_version,
            request_receiver,
            Arc::new(custom_apis),
            subscribers.clone(),
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
                effective_permissions: Mutex::default(),
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
            session: Session::default(),
        }
    }

    async fn send_request_async(&self, name: Name, bytes: Bytes) -> Result<Bytes, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        let id = self.data.request_id.fetch_add(1, Ordering::SeqCst);
        self.data.request_sender.send(PendingRequest {
            request: Payload {
                session_id: self.session.id,
                id: Some(id),
                name,
                value: Ok(bytes),
            },
            responder: result_sender,
        })?;

        result_receiver.recv_async().await?
    }

    fn send_request(&self, name: Name, bytes: Bytes) -> Result<Bytes, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        let id = self.data.request_id.fetch_add(1, Ordering::SeqCst);
        self.data.request_sender.send(PendingRequest {
            request: Payload {
                session_id: self.session.id,
                id: Some(id),
                name,
                value: Ok(bytes),
            },
            responder: result_sender,
        })?;

        result_receiver.recv()?
    }

    /// Sends an api `request`.
    pub async fn send_api_request_async<Api: api::Api>(
        &self,
        request: &Api,
    ) -> Result<Api::Response, ApiError<Api::Error>> {
        let request = Bytes::from(pot::to_vec(request).map_err(Error::from)?);
        let response = self.send_request_async(Api::name(), request).await?;
        let response =
            pot::from_slice::<Result<Api::Response, Api::Error>>(&response).map_err(Error::from)?;
        response.map_err(ApiError::Api)
    }

    /// Sends an api `request`.
    pub fn send_api_request<Api: api::Api>(
        &self,
        request: &Api,
    ) -> Result<Api::Response, ApiError<Api::Error>> {
        let request = Bytes::from(pot::to_vec(request).map_err(Error::from)?);
        let response = self.send_request(Api::name(), request)?;

        let response =
            pot::from_slice::<Result<Api::Response, Api::Error>>(&response).map_err(Error::from)?;
        response.map_err(ApiError::Api)
    }

    /// Returns the current effective permissions for the client. Returns None
    /// if unauthenticated.
    pub async fn effective_permissions(&self) -> Option<Permissions> {
        let effective_permissions = fast_async_lock!(self.data.effective_permissions);
        effective_permissions.clone()
    }

    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn background_task_running(&self) -> Arc<AtomicBool> {
        self.data.background_task_running.clone()
    }

    pub(crate) async fn register_subscriber(&self, id: u64, sender: flume::Sender<Arc<Message>>) {
        let mut subscribers = fast_async_lock!(self.data.subscribers);
        subscribers.insert(id, sender);
    }

    pub(crate) async fn unregister_subscriber(&self, database: String, id: u64) {
        drop(
            self.send_api_request_async(&UnregisterSubscriber {
                database,
                subscriber_id: id,
            })
            .await,
        );
        let mut subscribers = fast_async_lock!(self.data.subscribers);
        subscribers.remove(&id);
    }
}

#[async_trait]
impl AsyncStorageConnection for Client {
    type Database = RemoteDatabase;
    type Authenticated = Self;

    fn session(&self) -> Option<&Session> {
        Some(&self.session)
    }

    async fn admin(&self) -> Self::Database {
        self.database::<Admin>(ADMIN_DATABASE_NAME).await.unwrap()
    }

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request_async(&CreateDatabase {
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
        let mut schemas = fast_async_lock!(self.data.schemas);
        let type_id = TypeId::of::<DB>();
        let schematic = if let Some(schematic) = schemas.get(&type_id) {
            schematic.clone()
        } else {
            let schematic = Arc::new(DB::schematic()?);
            schemas.insert(type_id, schematic.clone());
            schematic
        };
        Ok(RemoteDatabase::new(
            self.clone(),
            name.to_string(),
            schematic,
        ))
    }

    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        self.send_api_request_async(&DeleteDatabase {
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<Database>, bonsaidb_core::Error> {
        Ok(self.send_api_request_async(&ListDatabases).await?)
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        Ok(self.send_api_request_async(&ListAvailableSchemas).await?)
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        Ok(self
            .send_api_request_async(&CreateUser {
                username: username.to_string(),
            })
            .await?)
    }

    async fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        Ok(self
            .send_api_request_async(&DeleteUser {
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
            .send_api_request_async(&bonsaidb_core::networking::SetUserPassword {
                user: user.name()?.into_owned(),
                password,
            })
            .await?)
    }

    #[cfg(feature = "password-hashing")]
    async fn authenticate<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session = self
            .send_api_request_async(&bonsaidb_core::networking::Authenticate {
                user: user.name()?.into_owned(),
                authentication,
            })
            .await?;
        Ok(Self {
            data: self.data.clone(),
            session,
        })
    }

    async fn assume_identity(
        &self,
        identity: Identity,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let session = self
            .send_api_request_async(&AssumeIdentity(identity))
            .await?;
        Ok(Self {
            data: self.data.clone(),
            session,
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
        self.send_api_request_async(&AlterUserPermissionGroupMembership {
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
        self.send_api_request_async(&AlterUserPermissionGroupMembership {
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
        self.send_api_request_async(&AlterUserRoleMembership {
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
        self.send_api_request_async(&AlterUserRoleMembership {
            user: user.name()?.into_owned(),
            role: role.name()?.into_owned(),
            should_be_member: false,
        })
        .await?;
        Ok(())
    }
}

type OutstandingRequestMap = HashMap<u32, PendingRequest>;
type OutstandingRequestMapHandle = Arc<Mutex<OutstandingRequestMap>>;
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
    custom_apis: &HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>,
) {
    if let Some(payload_id) = payload.id {
        let request = {
            let mut outstanding_requests = fast_async_lock!(outstanding_requests);
            outstanding_requests
                .remove(&payload_id)
                .expect("missing responder")
        };
        drop(request.responder.send(payload.value.map_err(Error::from)));
    } else if let (Some(custom_api_callback), Ok(value)) = (
        custom_apis.get(&payload.name).and_then(Option::as_ref),
        payload.value,
    ) {
        // if let Some(custom_api_callback) = custom_api_callback {
        custom_api_callback.response_received(value).await;
        // }
    } else {
        log::warn!("unexpected api response received ({})", payload.name);
    }
}

trait CustomApiWrapper<Response>: Send + Sync {
    fn invoke(&self, response: Response) -> BoxFuture<'static, ()>;
}

/// A callback that is invoked when an [`Api::Response`](CustomApi::Response)
/// value is received out-of-band (not in reply to a request).
pub struct CustomApiCallback<Api: api::Api> {
    generator: Box<dyn CustomApiWrapper<Api::Response>>,
}

/// The trait bounds required for the function wrapped in a
/// [`CustomApiCallback`].
pub trait CustomApiCallbackFn<Request, F>: Fn(Request) -> F + Send + Sync + 'static {}

impl<T, Request, F> CustomApiCallbackFn<Request, F> for T where
    T: Fn(Request) -> F + Send + Sync + 'static
{
}

struct CustomApiFutureBoxer<Response: Send + Sync, F: Future<Output = ()> + Send + Sync>(
    Box<dyn CustomApiCallbackFn<Response, F>>,
);

impl<Response: Send + Sync, F: Future<Output = ()> + Send + Sync + 'static>
    CustomApiWrapper<Response> for CustomApiFutureBoxer<Response, F>
{
    fn invoke(&self, response: Response) -> BoxFuture<'static, ()> {
        (&self.0)(response).boxed()
    }
}

impl<Api: api::Api> CustomApiCallback<Api> {
    /// Returns a new instance wrapping the provided function.
    pub fn new<
        F: CustomApiCallbackFn<Api::Response, Fut>,
        Fut: Future<Output = ()> + Send + Sync + 'static,
    >(
        callback: F,
    ) -> Self {
        Self {
            generator: Box::new(CustomApiFutureBoxer::<Api::Response, Fut>(Box::new(
                callback,
            ))),
        }
    }
}

#[async_trait]
pub trait AnyCustomApiCallback: Send + Sync + 'static {
    /// An out-of-band `response` was received. This happens when the server
    /// sends a response that isn't in response to a request.
    async fn response_received(&self, response: Bytes);
}

#[async_trait]
impl<Api: api::Api> AnyCustomApiCallback for CustomApiCallback<Api> {
    async fn response_received(&self, response: Bytes) {
        match pot::from_slice::<Result<Api::Response, Api::Error>>(&response) {
            Ok(response) => self.generator.invoke(response.unwrap()).await,
            Err(err) => {
                log::error!("error deserializing api: {err}");
            }
        }
    }
}
