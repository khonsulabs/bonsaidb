#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures::StreamExt;
use pliantdb_networking::{
    fabruic::{self, Certificate, Endpoint},
    Api, Database, Payload, Request, Response, ServerConnection, ServerRequest, ServerResponse,
};
use tokio::{sync::Mutex, task::JoinHandle};
pub use url;
use url::Url;

use crate::error::Error;

/// Client for connecting to a `PliantDB` server.
#[derive(Clone, Debug)]
pub struct Client {
    request_sender: Sender<ClientRequest>,
    worker: Arc<CancellableHandle<Result<(), Error>>>,
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
    pub fn new(url: &Url, certificate: Certificate) -> Result<Self, Error> {
        if url.scheme() != "pliantdb" {
            return Err(Error::InvalidUrl(String::from(
                "url should begin with pliantdb://",
            )));
        }

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

        #[cfg(test)]
        let background_task_running = Arc::new(AtomicBool::new(true));
        #[cfg(test)]
        let task_shutdown_signal = background_task_running.clone();

        let connect_to = format!("{}:{}", host.to_string(), url.port().unwrap_or(5000));
        let worker = tokio::task::spawn(async move {
            let result =
                reconnecting_client_loop(connect_to, server_name, certificate, request_receiver)
                    .await;

            #[cfg(test)]
            task_shutdown_signal.store(false, Ordering::Release);

            result
        });

        let client = Self {
            request_sender,
            worker: Arc::new(CancellableHandle { worker }),
            #[cfg(test)]
            background_task_running,
        };

        Ok(client)
    }

    async fn send_request(&self, request: Request<'static>) -> Result<Response<'static>, Error> {
        let (result_sender, result_receiver) = flume::bounded(1);
        println!("Sending request");
        self.request_sender.send(ClientRequest {
            request,
            responder: result_sender,
        })?;
        println!("Sent request");

        dbg!(result_receiver.recv_async().await)?
    }
}

#[async_trait]
impl ServerConnection for Client {
    async fn create_database(
        &self,
        name: &str,
        schema: pliantdb_core::schema::Id,
    ) -> Result<(), pliantdb_networking::Error> {
        match self
            .send_request(Request::Server(ServerRequest::CreateDatabase(Database {
                name: Cow::Owned(name.to_string()),
                schema,
            })))
            .await?
        {
            Response::Server(ServerResponse::DatabaseCreated { .. }) => Ok(()),
            other => Err(pliantdb_networking::Error::UnexpectedResponse(format!(
                "{:?}",
                other
            ))),
        }
    }

    async fn delete_database(&self, name: &str) -> Result<(), pliantdb_networking::Error> {
        match self
            .send_request(Request::Server(ServerRequest::DeleteDatabase {
                name: Cow::Owned(name.to_string()),
            }))
            .await?
        {
            Response::Server(ServerResponse::DatabaseDeleted { .. }) => Ok(()),
            other => Err(pliantdb_networking::Error::UnexpectedResponse(format!(
                "{:?}",
                other
            ))),
        }
    }

    async fn list_databases(&self) -> Result<Vec<Database<'static>>, pliantdb_networking::Error> {
        match self
            .send_request(Request::Server(ServerRequest::ListDatabases))
            .await?
        {
            Response::Server(ServerResponse::Databases(databases)) => Ok(databases),
            other => Err(pliantdb_networking::Error::UnexpectedResponse(format!(
                "{:?}",
                other
            ))),
        }
    }

    async fn list_available_schemas(
        &self,
    ) -> Result<Vec<pliantdb_core::schema::Id>, pliantdb_networking::Error> {
        match self
            .send_request(Request::Server(ServerRequest::ListAvailableSchemas))
            .await?
        {
            Response::Server(ServerResponse::AvailableSchemas(schemas)) => Ok(schemas),
            other => Err(pliantdb_networking::Error::UnexpectedResponse(format!(
                "{:?}",
                other
            ))),
        }
    }
}

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
async fn reconnecting_client_loop(
    host: String,
    server_name: String,
    certificate: Certificate,
    request_receiver: Receiver<ClientRequest>,
) -> Result<(), Error> {
    while let Ok(request) = request_receiver.recv_async().await {
        if let Err((responder, err)) = dbg!(
            connect_and_process(
                &host,
                &server_name,
                &certificate,
                request,
                &request_receiver,
            )
            .await
        ) {
            if let Some(responder) = responder {
                let _ = responder.try_send(Err(err));
            }
            // TODO implement logic to slow reconnects if the connection is
            // dropped. Right now it only sleeps until the next request.
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
    }

    Ok(())
}

async fn connect_and_process(
    host: &str,
    server_name: &str,
    certificate: &Certificate,
    initial_request: ClientRequest,
    request_receiver: &Receiver<ClientRequest>,
) -> Result<(), (Option<Sender<Result<Response<'static>, Error>>>, Error)> {
    let (_connection, payload_sender, payload_receiver) = connect(host, server_name, certificate)
        .await
        .map_err(|err| (Some(initial_request.responder.clone()), err))?;

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(outstanding_requests.clone(), payload_receiver));

    let mut request_id = 0;
    let ClientRequest { request, responder } = initial_request;
    payload_sender
        .send(&Payload {
            id: request_id,
            api: Api::Request(request),
        })
        .map_err(|err| (Some(responder.clone()), Error::from(err)))?;
    {
        let mut outstanding_requests = outstanding_requests.lock().await;
        outstanding_requests.insert(request_id, responder);
        request_id += 1;
    }
    println!("Sent payload");

    // TODO switch to select
    futures::try_join!(
        process_requests(
            outstanding_requests,
            request_id,
            request_receiver,
            payload_sender
        ),
        async {
            match request_processor.await {
                Ok(result) => result,
                Err(_) => Err(Error::Disconnected),
            }
        }
    )
    .map_err(|err| (None, err))?;

    Ok(())
}

async fn process_requests(
    outstanding_requests: OutstandingRequestMapHandle,
    mut request_id: u64,
    request_receiver: &Receiver<ClientRequest>,
    payload_sender: fabruic::Sender<Payload<'static>>,
) -> Result<(), Error> {
    while let Ok(client_request) = request_receiver.recv_async().await {
        payload_sender.send(&Payload {
            id: request_id,
            api: Api::Request(client_request.request),
        })?;
        let mut outstanding_requests = outstanding_requests.lock().await;
        outstanding_requests.insert(request_id, client_request.responder);
        request_id += 1;
    }

    // Return an error to make sure try_join returns.
    Err(Error::Disconnected)
}

type OutstandingRequestMap = HashMap<u64, Sender<Result<Response<'static>, Error>>>;
type OutstandingRequestMapHandle = Arc<Mutex<OutstandingRequestMap>>;

pub async fn process(
    outstanding_requests: OutstandingRequestMapHandle,
    mut payload_receiver: fabruic::Receiver<Payload<'static>>,
) -> Result<(), Error> {
    while let Some(payload) = dbg!(payload_receiver.next().await) {
        let payload = payload?;
        let mut outstanding_requests = outstanding_requests.lock().await;
        let responder = outstanding_requests
            .remove(&payload.id)
            .expect("missing responder");
        let response = match payload.api {
            Api::Request(_) => unreachable!("server should never send a requset"),
            Api::Response(response) => response,
        };
        let _ = responder.send(Ok(response));
    }

    Err(Error::Disconnected)
}

async fn connect(
    host: &str,
    server_name: &str,
    certificate: &Certificate,
) -> Result<
    (
        fabruic::Connection,
        fabruic::Sender<Payload<'static>>,
        fabruic::Receiver<Payload<'static>>,
    ),
    Error,
> {
    println!("Binding");
    let endpoint = Endpoint::new_client("[::]:0", certificate)?;
    println!("Connecting to: {}", host);
    let connection = endpoint.connect(&host, server_name).await?;
    println!("Opening stream");
    let (sender, receiver) = connection.open_stream().await?;

    println!("Opened stream");
    Ok((connection, sender, receiver))
}

struct ClientRequest {
    request: Request<'static>,
    responder: Sender<Result<Response<'static>, Error>>,
}

#[derive(Debug)]
struct CancellableHandle<T> {
    worker: JoinHandle<T>,
}

impl<T> Drop for CancellableHandle<T> {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
