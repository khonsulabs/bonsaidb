#[cfg(feature = "test-util")]
use std::sync::atomic::AtomicBool;
use std::{
    any::TypeId,
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use async_lock::Mutex;
use async_trait::async_trait;
use flume::Sender;
use pliantdb_core::{
    connection::{Database, ServerConnection},
    custodian_password::{
        ClientConfig, ClientFile, ClientLogin, LoginFinalization, LoginRequest, LoginResponse,
    },
    custom_api::CustomApi,
    networking::{self, Payload, Request, Response, ServerRequest, ServerResponse},
    permissions::Permissions,
    schema::{Schema, SchemaName, Schematic},
    PASSWORD_CONFIG,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::JoinHandle;
use url::Url;

pub use self::remote_database::RemoteDatabase;
#[cfg(feature = "pubsub")]
pub use self::remote_database::RemoteSubscriber;
use crate::error::Error;

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
use pliantdb_core::{circulate::Message, networking::DatabaseRequest};

#[cfg(all(feature = "websockets", not(target_arch = "wasm32")))]
pub type WebSocketError = tokio_tungstenite::tungstenite::Error;

#[cfg(all(feature = "websockets", target_arch = "wasm32"))]
pub type WebSocketError = wasm_websocket_worker::WebSocketError;

/// Client for connecting to a `PliantDb` server.
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
    PendingRequest<<A as CustomApi>::Request, <A as CustomApi>::Response>;

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

impl<A: CustomApi> Client<A> {
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
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn new_with_certificate(
        url: Url,
        certificate: Option<fabruic::Certificate>,
    ) -> Result<Self, Error> {
        match url.scheme() {
            "pliantdb" => Ok(Self::new_pliant_client(url, certificate)),
            #[cfg(feature = "websockets")]
            "wss" | "ws" => Self::new_websocket_client(url).await,
            other => {
                return Err(Error::InvalidUrl(format!("unsupported scheme {}", other)));
            }
        }
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
    pub async fn new(url: Url) -> Result<Self, Error> {
        match url.scheme() {
            #[cfg(not(target_arch = "wasm32"))]
            "pliantdb" => Ok(Self::new_pliant_client(url, None)),
            #[cfg(feature = "websockets")]
            "wss" | "ws" => Self::new_websocket_client(url).await,
            other => {
                return Err(Error::InvalidUrl(format!("unsupported scheme {}", other)));
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn new_pliant_client(url: Url, certificate: Option<fabruic::Certificate>) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();
        let worker = tokio::task::spawn(quic_worker::reconnecting_client_loop(
            url,
            certificate,
            request_receiver,
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
    async fn new_websocket_client(url: Url) -> Result<Self, Error> {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();

        let worker = tokio::task::spawn(tungstenite_worker::reconnecting_client_loop(
            url,
            request_receiver,
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
    async fn new_websocket_client(url: Url) -> Result<Self, Error> {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();

        wasm_websocket_worker::spawn_client(
            Arc::new(url),
            request_receiver,
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
    /// and database name when a [`Connection`](pliantdb_core::connection::Connection) function is called.
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
    ) -> Result<LoginResponse, pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::LoginWithPassword {
                username: username.to_string(),
                login_request,
            }))
            .await?
        {
            Response::Server(ServerResponse::PasswordLoginResponse { response }) => Ok(*response),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    /// Finishes setting a user's password by finishing the `OPAQUE-PAKE`
    /// login.
    pub async fn finish_login_with_password(
        &self,
        login_finalization: LoginFinalization,
    ) -> Result<(), pliantdb_core::Error> {
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
            other => Err(pliantdb_core::Error::Networking(
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
    ) -> Result<ClientFile, pliantdb_core::Error> {
        let (login, request) = ClientLogin::login(
            &ClientConfig::new(PASSWORD_CONFIG, None)?,
            previous_file,
            password,
        )?;
        let response = self.login_with_password(username, request).await?;
        let (new_file, login_finalization, _export_key) = login.finish(response)?;
        self.finish_login_with_password(login_finalization).await?;
        Ok(new_file)
    }

    async fn send_request(
        &self,
        request: Request<<A as CustomApi>::Request>,
    ) -> Result<Response<<A as CustomApi>::Response>, Error> {
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
    ) -> Result<<A as CustomApi>::Response, Error> {
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
    ) -> Result<(), pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::CreateDatabase(Database {
                name: name.to_string(),
                schema,
            })))
            .await?
        {
            Response::Server(ServerResponse::DatabaseCreated { .. }) => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn delete_database(&self, name: &str) -> Result<(), pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::DeleteDatabase {
                name: name.to_string(),
            }))
            .await?
        {
            Response::Server(ServerResponse::DatabaseDeleted { .. }) => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn list_databases(&self) -> Result<Vec<Database>, pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::ListDatabases))
            .await?
        {
            Response::Server(ServerResponse::Databases(databases)) => Ok(databases),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::ListAvailableSchemas))
            .await?
        {
            Response::Server(ServerResponse::AvailableSchemas(schemas)) => Ok(schemas),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn create_user(&self, username: &str) -> Result<u64, pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::CreateUser {
                username: username.to_string(),
            }))
            .await?
        {
            Response::Server(ServerResponse::UserCreated { id }) => Ok(id),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn set_user_password(
        &self,
        username: &str,
        password_request: pliantdb_core::custodian_password::RegistrationRequest,
    ) -> Result<pliantdb_core::custodian_password::RegistrationResponse, pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::SetPassword {
                username: username.to_string(),
                password_request,
            }))
            .await?
        {
            Response::Server(ServerResponse::FinishSetPassword { password_reponse }) => {
                Ok(*password_reponse)
            }
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
                networking::Error::UnexpectedResponse(format!("{:?}", other)),
            )),
        }
    }

    async fn finish_set_user_password(
        &self,
        username: &str,
        password_finalization: pliantdb_core::custodian_password::RegistrationFinalization,
    ) -> Result<(), pliantdb_core::Error> {
        match self
            .send_request(Request::Server(ServerRequest::FinishSetPassword {
                username: username.to_string(),
                password_finalization,
            }))
            .await?
        {
            Response::Ok => Ok(()),
            Response::Error(err) => Err(err),
            other => Err(pliantdb_core::Error::Networking(
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

async fn process_response_payload<R: Send + Sync + 'static, O: Send + Sync + 'static>(
    payload: Payload<Response<O>>,
    outstanding_requests: &OutstandingRequestMapHandle<R, O>,
    #[cfg(feature = "pubsub")] subscribers: &SubscriberMap,
) {
    if let Some(payload_id) = payload.id {
        let request = {
            let mut outstanding_requests = outstanding_requests.lock().await;
            outstanding_requests
                .remove(&payload_id)
                .expect("missing responder")
        };
        drop(request.responder.send(Ok(payload.wrapped)));
    } else {
        #[cfg(feature = "pubsub")]
        if let Response::Database(pliantdb_core::networking::DatabaseResponse::MessageReceived {
            subscriber_id,
            topic,
            payload,
        }) = payload.wrapped
        {
            let mut subscribers = subscribers.lock().await;
            if let Some(sender) = subscribers.get(&subscriber_id) {
                if sender
                    .send(std::sync::Arc::new(pliantdb_core::circulate::Message {
                        topic,
                        payload,
                    }))
                    .is_err()
                {
                    subscribers.remove(&subscriber_id);
                }
            }
        } else {
            unreachable!("only MessageReceived is allowed to not have an id")
        }
    }
}
