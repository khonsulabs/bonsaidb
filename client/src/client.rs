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

use async_trait::async_trait;
use flume::Sender;
use pliantdb_core::{
    fabruic::Certificate,
    networking::{
        self, Database, Payload, Request, Response, ServerConnection, ServerRequest, ServerResponse,
    },
    schema::{Schema, SchemaName, Schematic},
};
use tokio::{sync::Mutex, task::JoinHandle};
use url::Url;

pub use self::remote_database::RemoteDatabase;
use crate::error::Error;

mod remote_database;
#[cfg(feature = "websockets")]
mod websocket_worker;
mod worker;

#[cfg(feature = "pubsub")]
type SubscriberMap = Arc<Mutex<HashMap<u64, flume::Sender<Arc<Message>>>>>;

#[cfg(feature = "pubsub")]
use pliantdb_core::{circulate::Message, networking::DatabaseRequest};

/// Client for connecting to a `PliantDB` server.
#[derive(Clone, Debug)]
pub struct Client {
    pub(crate) data: Arc<Data>,
}

#[derive(Debug)]
pub struct Data {
    request_sender: Sender<PendingRequest>,
    worker: CancellableHandle<Result<(), Error>>,
    schemas: Mutex<HashMap<TypeId, Arc<Schematic>>>,
    request_id: AtomicU32,
    #[cfg(feature = "pubsub")]
    subscribers: SubscriberMap,
    #[cfg(feature = "test-util")]
    background_task_running: Arc<AtomicBool>,
}

impl Client {
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
    pub async fn new(url: Url, certificate: Option<Certificate>) -> Result<Self, Error> {
        match url.scheme() {
            "pliantdb" => {
                let certificate = certificate.ok_or_else(|| {
                    Error::InvalidUrl(String::from(
                        "certificate must be provided with pliantdb:// urls",
                    ))
                })?;

                Ok(Self::new_pliant_client(url, certificate))
            }
            #[cfg(feature = "websockets")]
            "wss" | "ws" => Self::new_websocket_client(url).await,
            other => {
                return Err(Error::InvalidUrl(format!("unsupported scheme {}", other)));
            }
        }
    }

    fn new_pliant_client(url: Url, certificate: Certificate) -> Self {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();
        let worker = tokio::task::spawn(worker::reconnecting_client_loop(
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
                #[cfg(feature = "pubsub")]
                subscribers,
                #[cfg(feature = "test-util")]
                background_task_running,
            }),
        }
    }

    #[cfg(feature = "websockets")]
    async fn new_websocket_client(url: Url) -> Result<Self, Error> {
        let (request_sender, request_receiver) = flume::unbounded();

        #[cfg(feature = "pubsub")]
        let subscribers = SubscriberMap::default();
        let worker = tokio::task::spawn(websocket_worker::reconnecting_client_loop(
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
                worker: CancellableHandle {
                    worker,
                    #[cfg(feature = "test-util")]
                    background_task_running: background_task_running.clone(),
                },
                schemas: Mutex::default(),
                request_id: AtomicU32::default(),
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
    pub async fn database<DB: Schema>(&self, name: &str) -> Result<RemoteDatabase<DB>, Error> {
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

    async fn send_request(&self, request: Request) -> Result<Response, Error> {
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
        let _ = self
            .send_request(Request::Database {
                database,
                request: DatabaseRequest::UnregisterSubscriber { subscriber_id: id },
            })
            .await;
        let mut subscribers = self.data.subscribers.lock().await;
        subscribers.remove(&id);
    }
}

#[async_trait]
impl ServerConnection for Client {
    async fn create_database(
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
}

type OutstandingRequestMap = HashMap<u32, Sender<Result<Response, Error>>>;
type OutstandingRequestMapHandle = Arc<Mutex<OutstandingRequestMap>>;

#[derive(Debug)]
pub struct PendingRequest {
    request: Payload<Request>,
    responder: Sender<Result<Response, Error>>,
}

#[derive(Debug)]
struct CancellableHandle<T> {
    worker: JoinHandle<T>,
    #[cfg(feature = "test-util")]
    background_task_running: Arc<AtomicBool>,
}

impl<T> Drop for CancellableHandle<T> {
    fn drop(&mut self) {
        self.worker.abort();
        #[cfg(feature = "test-util")]
        self.background_task_running.store(false, Ordering::Release);
    }
}
