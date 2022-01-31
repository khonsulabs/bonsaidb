#[cfg(feature = "test-util")]
use std::sync::atomic::AtomicBool;
use std::{
    any::TypeId,
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::Mutex;
use async_trait::async_trait;
#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::{Authenticated, Authentication};
use bonsaidb_core::{
    connection::{Database, StorageConnection},
    custom_api::{CustomApi, CustomApiResult},
    networking::{
        self, Payload, Request, Response, ServerRequest, ServerResponse, CURRENT_PROTOCOL_VERSION,
    },
    permissions::Permissions,
    schema::{NamedReference, Schema, SchemaName, Schematic},
};
use bonsaidb_utils::fast_async_lock;
use derive_where::derive_where;
use flume::Sender;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;
use url::Url;

pub use self::remote_database::{RemoteDatabase, RemoteSubscriber};
use crate::{error::Error, Builder};

#[cfg(not(target_arch = "wasm32"))]
mod quic_worker;
mod remote_database;
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

use bonsaidb_core::{circulate::Message, networking::DatabaseRequest};

#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
pub type WebSocketError = tokio_tungstenite::tungstenite::Error;

#[cfg(all(feature = "websockets", target_arch = "wasm32"))]
pub type WebSocketError = wasm_websocket_worker::WebSocketError;

/// Client for connecting to a `BonsaiDb` server.
#[derive(Debug)]
#[derive_where(Clone)]
pub struct Client<A: CustomApi = ()> {
    pub(crate) data: Arc<Data<A>>,
}

impl<A> PartialEq for Client<A>
where
    A: CustomApi,
{
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }
}

#[derive(Debug)]
pub struct Data<A: CustomApi> {
    request_sender: Sender<PendingRequest<A>>,
    #[cfg(not(target_arch = "wasm32"))]
    _worker: CancellableHandle<Result<(), Error<A::Error>>>,
    effective_permissions: Mutex<Option<Permissions>>,
    schemas: Mutex<HashMap<TypeId, Arc<Schematic>>>,
    request_id: AtomicU32,
    subscribers: SubscriberMap,
    #[cfg(feature = "test-util")]
    background_task_running: Arc<AtomicBool>,
}

impl Client<()> {
    /// Returns a builder for a new client connecting to `url`.
    pub fn build(url: Url) -> Builder<()> {
        Builder::new(url)
    }
}

impl<A: CustomApi> Client<A> {
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
    pub async fn new(url: Url) -> Result<Self, Error<A::Error>> {
        Self::new_from_parts(
            url,
            CURRENT_PROTOCOL_VERSION,
            #[cfg(not(target_arch = "wasm32"))]
            None,
            None,
        )
        .await
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
    pub(crate) async fn new_from_parts(
        url: Url,
        protocol_version: &'static str,
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
        #[cfg(not(target_arch = "wasm32"))] certificate: Option<fabruic::Certificate>,
    ) -> Result<Self, Error<A::Error>> {
        match url.scheme() {
            #[cfg(not(target_arch = "wasm32"))]
            "bonsaidb" => Ok(Self::new_bonsai_client(
                url,
                protocol_version,
                certificate,
                custom_api_callback,
            )),
            #[cfg(feature = "websockets")]
            "wss" | "ws" => {
                Self::new_websocket_client(url, protocol_version, custom_api_callback).await
            }
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
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        let subscribers = SubscriberMap::default();
        let worker = tokio::task::spawn(quic_worker::reconnecting_client_loop(
            url,
            protocol_version,
            certificate,
            request_receiver,
            custom_api_callback,
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
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
        }
    }

    #[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
    async fn new_websocket_client(
        url: Url,
        protocol_version: &'static str,
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    ) -> Result<Self, Error<A::Error>> {
        let (request_sender, request_receiver) = flume::unbounded();

        let subscribers = SubscriberMap::default();

        let worker = tokio::task::spawn(tungstenite_worker::reconnecting_client_loop(
            url,
            protocol_version,
            request_receiver,
            custom_api_callback,
            subscribers.clone(),
        ));

        #[cfg(feature = "test-util")]
        let background_task_running = Arc::new(AtomicBool::new(true));

        let client = Self {
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
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
        };

        Ok(client)
    }

    #[cfg(all(feature = "websockets", target_arch = "wasm32"))]
    async fn new_websocket_client(
        url: Url,
        protocol_version: &'static str,
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    ) -> Result<Self, Error<A::Error>> {
        let (request_sender, request_receiver) = flume::unbounded();

        let subscribers = SubscriberMap::default();

        wasm_websocket_worker::spawn_client(
            Arc::new(url),
            protocol_version,
            request_receiver,
            custom_api_callback.clone(),
            subscribers.clone(),
        );

        #[cfg(feature = "test-util")]
        let background_task_running = Arc::new(AtomicBool::new(true));

        let client = Self {
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
        };

        Ok(client)
    }

    async fn send_request(
        &self,
        request: Request<<A as CustomApi>::Request>,
    ) -> Result<Response<CustomApiResult<A>>, Error<A::Error>> {
        let (result_sender, result_receiver) = flume::bounded(1);
        let id = self.data.request_id.fetch_add(1, Ordering::SeqCst);
        self.data.request_sender.send(PendingRequest {
            request: Payload {
                id: Some(id),
                wrapped: request,
            },
            responder: result_sender.clone(),
            _phantom: PhantomData,
        })?;

        result_receiver.recv_async().await?
    }

    /// Sends an api `request`.
    pub async fn send_api_request(
        &self,
        request: <A as CustomApi>::Request,
    ) -> Result<A::Response, Error<A::Error>> {
        match self.send_request(Request::Api(request)).await? {
            Response::Api(response) => response.map_err(Error::Api),
            Response::Error(err) => Err(Error::Core(err)),
            other => Err(Error::Network(networking::Error::UnexpectedResponse(
                format!("{:?}", other),
            ))),
        }
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
            self.send_request(Request::Database {
                database,
                request: DatabaseRequest::UnregisterSubscriber { subscriber_id: id },
            })
            .await,
        );
        let mut subscribers = fast_async_lock!(self.data.subscribers);
        subscribers.remove(&id);
    }
}

#[async_trait]
impl<A: CustomApi> StorageConnection for Client<A> {
    type Database = RemoteDatabase<A>;

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::CreateDatabase {
                database: Database {
                    name: name.to_string(),
                    schema,
                },
                only_if_needed,
            }))
            .await?
        {
            Response::Server(ServerResponse::DatabaseCreated { .. }) => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
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
        match self
            .send_request(Request::Server(ServerRequest::DeleteDatabase {
                name: name.to_string(),
            }))
            .await?
        {
            Response::Server(ServerResponse::DatabaseDeleted { .. }) => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn list_databases(&self) -> Result<Vec<Database>, bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::ListDatabases))
            .await?
        {
            Response::Server(ServerResponse::Databases(databases)) => Ok(databases),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::ListAvailableSchemas))
            .await?
        {
            Response::Server(ServerResponse::AvailableSchemas(schemas)) => Ok(schemas),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::CreateUser {
                username: username.to_string(),
            }))
            .await?
        {
            Response::Server(ServerResponse::UserCreated { id }) => Ok(id),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::SetUserPassword {
                user: user.into().into_owned(),
                password,
            }))
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    #[cfg(feature = "password-hashing")]
    async fn authenticate<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Authenticated, bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::Authenticate {
                user: user.into().into_owned(),
                authentication,
            }))
            .await?
        {
            Response::Server(ServerResponse::Authenticated(response)) => Ok(response),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(
                ServerRequest::AlterUserPermissionGroupMembership {
                    user: user.into().into_owned(),
                    group: permission_group.into().into_owned(),
                    should_be_member: true,
                },
            ))
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(
                ServerRequest::AlterUserPermissionGroupMembership {
                    user: user.into().into_owned(),
                    group: permission_group.into().into_owned(),
                    should_be_member: false,
                },
            ))
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn add_role_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::AlterUserRoleMembership {
                user: user.into().into_owned(),
                role: role.into().into_owned(),
                should_be_member: true,
            }))
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn remove_role_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::AlterUserRoleMembership {
                user: user.into().into_owned(),
                role: role.into().into_owned(),
                should_be_member: false,
            }))
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }
}

type OutstandingRequestMap<Api> = HashMap<u32, PendingRequest<Api>>;
type OutstandingRequestMapHandle<Api> = Arc<Mutex<OutstandingRequestMap<Api>>>;
type PendingRequestResponder<Api> =
    Sender<Result<Response<CustomApiResult<Api>>, Error<<Api as CustomApi>::Error>>>;

#[derive(Debug)]
pub struct PendingRequest<Api: CustomApi> {
    request: Payload<Request<Api::Request>>,
    responder: PendingRequestResponder<Api>,
    _phantom: PhantomData<Api>,
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

async fn process_response_payload<A: CustomApi>(
    payload: Payload<Response<CustomApiResult<A>>>,
    outstanding_requests: &OutstandingRequestMapHandle<A>,
    custom_api_callback: Option<&dyn CustomApiCallback<A>>,
    subscribers: &SubscriberMap,
) {
    if let Some(payload_id) = payload.id {
        if let Response::Api(response) = &payload.wrapped {
            if let Some(custom_api_callback) = custom_api_callback {
                custom_api_callback
                    .request_response_received(response)
                    .await;
            }
        }

        let request = {
            let mut outstanding_requests = fast_async_lock!(outstanding_requests);
            outstanding_requests
                .remove(&payload_id)
                .expect("missing responder")
        };
        drop(request.responder.send(Ok(payload.wrapped)));
    } else {
        match payload.wrapped {
            Response::Api(response) => {
                if let Some(custom_api_callback) = custom_api_callback {
                    custom_api_callback.response_received(response).await;
                }
            }
            Response::Database(bonsaidb_core::networking::DatabaseResponse::MessageReceived {
                subscriber_id,
                topic,
                payload,
            }) => {
                let mut subscribers = fast_async_lock!(subscribers);
                if let Some(sender) = subscribers.get(&subscriber_id) {
                    if sender
                        .send(std::sync::Arc::new(bonsaidb_core::circulate::Message {
                            topic,
                            payload: payload.into_vec(),
                        }))
                        .is_err()
                    {
                        subscribers.remove(&subscriber_id);
                    }
                }
            }
            _ => {
                log::error!("unexpected adhoc response");
            }
        }
    }
}

/// A handler of [`CustomApi`] responses.
#[async_trait]
pub trait CustomApiCallback<A: CustomApi>: Send + Sync + 'static {
    /// An out-of-band `response` was received. This happens when the server
    /// sends a response that isn't in response to a request.
    async fn response_received(&self, response: CustomApiResult<A>);

    /// A response was received. Unlike in `response_received` this response
    /// will be returned to the original requestor. This is invoked before the
    /// requestor recives the response.
    #[allow(unused_variables)]
    async fn request_response_received(&self, response: &CustomApiResult<A>) {
        // This is provided in case you'd like to see a response always, even if
        // it is also being handled by the code that made the request.
    }
}

#[async_trait]
impl<F, T> CustomApiCallback<T> for F
where
    F: Fn(CustomApiResult<T>) + Send + Sync + 'static,
    T: CustomApi,
{
    async fn response_received(&self, response: CustomApiResult<T>) {
        self(response);
    }
}

#[async_trait]
impl<T> CustomApiCallback<T> for ()
where
    T: CustomApi,
{
    async fn response_received(&self, _response: CustomApiResult<T>) {}
}
