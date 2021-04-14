#[cfg(test)]
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
    schema::{Schema, Schematic},
};
use tokio::{sync::Mutex, task::JoinHandle};
use url::Url;

pub use self::remote_database::RemoteDatabase;
use crate::error::Error;

mod remote_database;
#[cfg(feature = "websockets")]
mod websocket_worker;
mod worker;

/// Client for connecting to a `PliantDB` server.
#[derive(Clone, Debug)]
pub struct Client {
    /// todo switch to a single arc wrapping data.
    request_sender: Sender<PendingRequest>,
    worker: Arc<CancellableHandle<Result<(), Error>>>,
    schemas: Arc<Mutex<HashMap<TypeId, Arc<Schematic>>>>,
    request_id: Arc<AtomicU32>,
    #[cfg(test)]
    pub(crate) background_task_running: Arc<AtomicBool>,
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
    pub async fn new(url: &Url, certificate: Option<Certificate>) -> Result<Self, Error> {
        match url.scheme() {
            "pliantdb" => {
                let certificate = certificate.ok_or_else(|| {
                    Error::InvalidUrl(String::from(
                        "certificate must be provided with pliantdb:// urls",
                    ))
                })?;

                Self::new_pliant_client(url, certificate)
            }
            #[cfg(feature = "websockets")]
            "wss" | "ws" => Self::new_websocket_client(url).await,
            other => {
                return Err(Error::InvalidUrl(format!("unsupported scheme {}", other)));
            }
        }
    }

    fn new_pliant_client(url: &Url, certificate: Certificate) -> Result<Self, Error> {
        let host = url
            .host_str()
            .ok_or_else(|| Error::InvalidUrl(String::from("url must specify a host")))?;
        let mut server_name = host.to_owned();
        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "server" => {
                    server_name = value.to_string();
                }
                _ => {
                    return Err(Error::InvalidUrl(format!(
                        "invalid query string parameter '{}'",
                        name
                    )))
                }
            }
        }

        let (request_sender, request_receiver) = flume::unbounded();

        let worker = tokio::task::spawn(worker::reconnecting_client_loop(
            host.to_string(),
            url.port().unwrap_or(5645),
            server_name,
            certificate,
            request_receiver,
        ));

        #[cfg(test)]
        let background_task_running = Arc::new(AtomicBool::new(true));

        let client = Self {
            request_sender,
            worker: Arc::new(CancellableHandle {
                worker,
                #[cfg(test)]
                background_task_running: background_task_running.clone(),
            }),
            schemas: Arc::default(),
            request_id: Arc::default(),
            #[cfg(test)]
            background_task_running,
        };

        Ok(client)
    }

    #[cfg(feature = "websockets")]
    async fn new_websocket_client(url: &Url) -> Result<Self, Error> {
        let (request_sender, request_receiver) = flume::unbounded();

        let worker = tokio::task::spawn(websocket_worker::reconnecting_client_loop(
            url.clone(),
            request_receiver,
        ));

        #[cfg(test)]
        let background_task_running = Arc::new(AtomicBool::new(true));

        let client = Self {
            request_sender,
            worker: Arc::new(CancellableHandle {
                worker,
                #[cfg(test)]
                background_task_running: background_task_running.clone(),
            }),
            schemas: Arc::default(),
            request_id: Arc::default(),
            #[cfg(test)]
            background_task_running,
        };

        Ok(client)
    }

    /// Returns a structure representing a remote database. No validations are
    /// done when this method is executed. The server will validate the schema
    /// and database name when a [`Connection`](pliantdb_core::connection::Connection) function is called.
    pub async fn database<DB: Schema>(&self, name: &str) -> RemoteDatabase<DB> {
        let mut schemas = self.schemas.lock().await;
        let schema = schemas
            .entry(TypeId::of::<DB>())
            .or_insert_with(|| Arc::new(DB::schematic()))
            .clone();
        RemoteDatabase::new(self.clone(), name.to_string(), schema)
    }

    async fn send_request(&self, request: Request) -> Result<Response, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        let id = self.request_id.fetch_add(1, Ordering::SeqCst);
        self.request_sender.send(PendingRequest {
            request: Payload {
                id,
                wrapped: request,
            },
            responder: result_sender.clone(),
        })?;

        result_receiver.recv_async().await?
    }
}

#[async_trait]
impl ServerConnection for Client {
    async fn create_database(
        &self,
        name: &str,
        schema: pliantdb_core::schema::Id,
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

    async fn list_available_schemas(
        &self,
    ) -> Result<Vec<pliantdb_core::schema::Id>, pliantdb_core::Error> {
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
    #[cfg(test)]
    background_task_running: Arc<AtomicBool>,
}

impl<T> Drop for CancellableHandle<T> {
    fn drop(&mut self) {
        self.worker.abort();
        #[cfg(test)]
        self.background_task_running.store(false, Ordering::Release);
    }
}
