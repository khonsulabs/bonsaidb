#[cfg(feature = "test-util")]
use std::sync::atomic::AtomicBool;
use std::{
    any::TypeId,
    collections::HashMap,
    fmt::Debug,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::Mutex;
use async_trait::async_trait;
use bonsaidb_core::{
    connection::{Database, PasswordResult, ServerConnection},
    custodian_password::{
        ClientConfig, ClientFile, ClientLogin, LoginFinalization, LoginRequest, LoginResponse,
    },
    custom_api::{CustomApi, CustomApiResult},
    networking::{self, Payload, Request, Response, ServerRequest, ServerResponse},
    permissions::Permissions,
    schema::{NamedReference, Schema, SchemaName, Schematic},
    PASSWORD_CONFIG,
};
use flume::Sender;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;
use url::Url;

pub use self::remote_database::RemoteDatabase;
#[cfg(feature = "pubsub")]
pub use self::remote_database::RemoteSubscriber;
use crate::{error::Error, Builder};

#[cfg(not(target_arch = "wasm32"))]
mod quic_worker;
mod remote_database;
#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
mod tungstenite_worker;
#[cfg(all(feature = "websockets", target_arch = "wasm32"))]
mod wasm_websocket_worker;

#[cfg(feature = "pubsub")]
type SubscriberMap = Arc<Mutex<HashMap<u64, flume::Sender<Arc<Message>>>>>;

#[cfg(feature = "pubsub")]
use bonsaidb_core::{circulate::Message, networking::DatabaseRequest};

#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
pub type WebSocketError = tokio_tungstenite::tungstenite::Error;

#[cfg(all(feature = "websockets", target_arch = "wasm32"))]
pub type WebSocketError = wasm_websocket_worker::WebSocketError;

/// Client for connecting to a `BonsaiDb` server.
#[derive(Debug)]
pub struct Client<A: CustomApi = ()> {
    pub(crate) data: Arc<Data<A>>,
}

impl<A: CustomApi> Clone for Client<A> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

#[allow(type_alias_bounds)] // Causes compilation errors without it
type BackendPendingRequest<A: CustomApi> =
    PendingRequest<<A as CustomApi>::Request, CustomApiResult<A>>;

#[derive(Debug)]
pub struct Data<A: CustomApi> {
    request_sender: Sender<BackendPendingRequest<A>>,
    #[cfg(not(target_arch = "wasm32"))]
    worker: CancellableHandle<Result<(), Error>>,
    effective_permissions: Mutex<Option<Permissions>>,
    schemas: Mutex<HashMap<TypeId, Arc<Schematic>>>,
    request_id: AtomicU32,
    #[cfg(feature = "pubsub")]
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
    pub async fn new(url: Url) -> Result<Self, Error> {
        Self::new_from_parts(
            url,
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
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
        #[cfg(not(target_arch = "wasm32"))] certificate: Option<fabruic::Certificate>,
    ) -> Result<Self, Error> {
        match url.scheme() {
            #[cfg(not(target_arch = "wasm32"))]
            "bonsaidb" => Ok(Self::new_bonsai_client(
                url,
                certificate,
                custom_api_callback,
            )),
            #[cfg(feature = "websockets")]
            "wss" | "ws" => Self::new_websocket_client(url, custom_api_callback).await,
            other => {
                return Err(Error::InvalidUrl(format!("unsupported scheme {}", other)));
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn new_bonsai_client(
        url: Url,
        certificate: Option<fabruic::Certificate>,
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    ) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();
        let worker = tokio::task::spawn(quic_worker::reconnecting_client_loop(
            url,
            certificate,
            request_receiver,
            custom_api_callback,
            #[cfg(feature = "pubsub")]
            subscribers.clone(),
        ));

        #[cfg(feature = "test-util")]
        let background_task_running = Arc::new(AtomicBool::new(true));

        Self {
            data: Arc::new(Data {
                request_sender,
                worker: CancellableHandle {
                    worker,
                    #[cfg(feature = "test-util")]
                    background_task_running: background_task_running.clone(),
                },
                schemas: Mutex::default(),
                request_id: AtomicU32::default(),
                effective_permissions: Mutex::default(),
                #[cfg(feature = "pubsub")]
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
        }
    }

    #[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
    async fn new_websocket_client(
        url: Url,
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    ) -> Result<Self, Error> {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();

        let worker = tokio::task::spawn(tungstenite_worker::reconnecting_client_loop(
            url,
            request_receiver,
            custom_api_callback,
            #[cfg(feature = "pubsub")]
            subscribers.clone(),
        ));

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
                #[cfg(feature = "pubsub")]
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
        custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    ) -> Result<Self, Error> {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();

        wasm_websocket_worker::spawn_client(
            Arc::new(url),
            request_receiver,
            custom_api_callback.clone(),
            #[cfg(feature = "pubsub")]
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
                #[cfg(feature = "pubsub")]
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
        };

        Ok(client)
    }

    /// Returns a structure representing a remote database. No validations are
    /// done when this method is executed. The server will validate the schema
    /// and database name when a [`Connection`](bonsaidb_core::connection::Connection) function is called.
    pub async fn database<DB: Schema>(&self, name: &str) -> Result<RemoteDatabase<DB, A>, Error> {
        let mut schemas = self.data.schemas.lock().await;
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

    /// Logs in as a user with a password, using `custodian-password` to login using `OPAQUE-PAKE`.
    pub async fn login_with_password(
        &self,
        username: &str,
        login_request: LoginRequest,
    ) -> Result<LoginResponse, bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::LoginWithPassword {
                username: username.to_string(),
                login_request,
            }))
            .await?
        {
            Response::Server(ServerResponse::PasswordLoginResponse { response }) => Ok(*response),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    /// Finishes setting a user's password by finishing the `OPAQUE-PAKE`
    /// login.
    pub async fn finish_login_with_password(
        &self,
        login_finalization: LoginFinalization,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::FinishPasswordLogin {
                login_finalization,
            }))
            .await?
        {
            Response::Server(ServerResponse::LoggedIn { permissions }) => {
                let mut effective_permissions = self.data.effective_permissions.lock().await;
                *effective_permissions = Some(permissions);
                Ok(())
            }
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    /// Authenticates as a user with a provided password. The password provided
    /// will never leave the machine that is calling this function. Internally
    /// uses `login_with_password` and `finish_login_with_password` in
    /// conjunction with `custodian-password`.
    pub async fn login_with_password_str(
        &self,
        username: &str,
        password: &str,
        previous_file: Option<ClientFile>,
    ) -> Result<PasswordResult, bonsaidb_core::Error> {
        let (login, request) = ClientLogin::login(
            &ClientConfig::new(PASSWORD_CONFIG, None)?,
            previous_file,
            password,
        )?;
        let response = self.login_with_password(username, request).await?;
        let (file, login_finalization, export_key) = login.finish(response)?;
        self.finish_login_with_password(login_finalization).await?;
        Ok(PasswordResult { file, export_key })
    }

    async fn send_request(
        &self,
        request: Request<<A as CustomApi>::Request>,
    ) -> Result<Response<CustomApiResult<A>>, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        let id = self.data.request_id.fetch_add(1, Ordering::SeqCst);
        self.data.request_sender.send(PendingRequest {
            request: Payload {
                id: Some(id),
                wrapped: request,
            },
            responder: result_sender.clone(),
        })?;

        result_receiver.recv_async().await?
    }

    /// Sends an api `request`.
    pub async fn send_api_request(
        &self,
        request: <A as CustomApi>::Request,
    ) -> Result<CustomApiResult<A>, Error> {
        match self.send_request(Request::Api(request)).await? {
            Response::Api(response) => Ok(response),
            Response::Error(err) => Err(Error::Core(err)),
            other => Err(Error::Network(networking::Error::UnexpectedResponse(
                format!("{:?}", other),
            ))),
        }
    }

    /// Returns the current effective permissions for the client. Returns None
    /// if unauthenticated.
    pub async fn effective_permissions(&self) -> Option<Permissions> {
        let effective_permissions = self.data.effective_permissions.lock().await;
        effective_permissions.clone()
    }

    #[cfg(feature = "test-util")]
    #[doc(hidden)]
    #[must_use]
    pub fn background_task_running(&self) -> Arc<AtomicBool> {
        self.data.background_task_running.clone()
    }

    #[cfg(feature = "pubsub")]
    pub(crate) async fn register_subscriber(&self, id: u64, sender: flume::Sender<Arc<Message>>) {
        let mut subscribers = self.data.subscribers.lock().await;
        subscribers.insert(id, sender);
    }

    #[cfg(feature = "pubsub")]
    pub(crate) async fn unregister_subscriber(&self, database: String, id: u64) {
        drop(
            self.send_request(Request::Database {
                database,
                request: DatabaseRequest::UnregisterSubscriber { subscriber_id: id },
            })
            .await,
        );
        let mut subscribers = self.data.subscribers.lock().await;
        subscribers.remove(&id);
    }
}

#[async_trait]
impl ServerConnection for Client {
    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::CreateDatabase(Database {
                name: name.to_string(),
                schema,
            })))
            .await?
        {
            Response::Server(ServerResponse::DatabaseCreated { .. }) => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
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

    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_request: bonsaidb_core::custodian_password::RegistrationRequest,
    ) -> Result<bonsaidb_core::custodian_password::RegistrationResponse, bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::SetPassword {
                user: user.into().into_owned(),
                password_request,
            }))
            .await?
        {
            Response::Server(ServerResponse::FinishSetPassword { password_reponse }) => {
                Ok(*password_reponse)
            }
            Response::Error(err) => Err(err),
            other => Err(bonsaidb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn finish_set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_finalization: bonsaidb_core::custodian_password::RegistrationFinalization,
    ) -> Result<(), bonsaidb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::FinishSetPassword {
                user: user.into().into_owned(),
                password_finalization,
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

type OutstandingRequestMap<R, O> = HashMap<u32, PendingRequest<R, O>>;
type OutstandingRequestMapHandle<R, O> = Arc<Mutex<OutstandingRequestMap<R, O>>>;

#[derive(Debug)]
pub struct PendingRequest<R, O> {
    request: Payload<Request<R>>,
    responder: Sender<Result<Response<O>, Error>>,
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
    outstanding_requests: &OutstandingRequestMapHandle<A::Request, CustomApiResult<A>>,
    custom_api_callback: Option<&dyn CustomApiCallback<A>>,
    #[cfg(feature = "pubsub")] subscribers: &SubscriberMap,
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
            let mut outstanding_requests = outstanding_requests.lock().await;
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
            #[cfg(feature = "pubsub")]
            Response::Database(bonsaidb_core::networking::DatabaseResponse::MessageReceived {
                subscriber_id,
                topic,
                payload,
            }) => {
                let mut subscribers = subscribers.lock().await;
                if let Some(sender) = subscribers.get(&subscriber_id) {
                    if sender
                        .send(std::sync::Arc::new(bonsaidb_core::circulate::Message {
                            topic,
                            payload,
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
