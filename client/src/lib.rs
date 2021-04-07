use std::{collections::HashMap, sync::Arc, time::Duration};

use flume::{Receiver, Sender};
use futures::StreamExt;
use pliantdb_networking::{
    fabruic::{self, Certificate, Endpoint},
    Api, Payload, Request, Response,
};
use tokio::{sync::Mutex, task::JoinHandle};
pub use url;
use url::Url;

#[derive(Clone, Debug)]
pub struct Client {
    request_sender: Sender<ClientRequest>,
    worker: Arc<CancellableHandle<Result<(), Error>>>,
}

impl Client {
    pub fn connect(url: &Url, certificate: Certificate) -> Result<Self, Error> {
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

        let worker = tokio::task::spawn(reconnecting_client_loop(
            format!("{}:{}", host.to_string(), url.port().unwrap_or(5000)),
            server_name,
            certificate,
            request_receiver,
        ));

        let client = Self {
            request_sender,
            worker: Arc::new(CancellableHandle { worker }),
        };

        Ok(client)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred from networking
    #[error("a networking error occurred: '{0}'")]
    Networking(#[from] fabruic::Error),

    /// An invalid Url was provided.
    #[error("invalid url: '{0}'")]
    InvalidUrl(String),
}

struct ClientRequest {
    request: Request<'static>,
    responder: Sender<Result<Response<'static>, Error>>,
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

    // todo create a binning system for handling requests and responses. Insert the first request and then loop for the rest.
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
                Err(_) => Err(Error::Networking(fabruic::Error::AlreadyClosed)),
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
    Err(Error::Networking(fabruic::Error::AlreadyClosed))
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

    Err(Error::Networking(fabruic::Error::AlreadyClosed))
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
    let endpoint = Endpoint::new_client("[::]:0", &certificate)?;
    println!("Connecting to: {}", host);
    let connection = endpoint.connect(&host, &server_name).await?;
    println!("Opening stream");
    let (sender, receiver) = connection.open_stream().await?;

    println!("Opened stream");
    Ok((connection, sender, receiver))
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use pliantdb_core::{
        schema::Collection,
        test_util::{Basic, TestDirectory},
    };
    use pliantdb_networking::DatabaseRequest;
    use pliantdb_server::test_util::{initialize_basic_server, BASIC_SERVER_NAME};

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test() -> anyhow::Result<()> {
        let directory = TestDirectory::new("client-test");
        let server = initialize_basic_server(directory.as_ref()).await?;
        let task_server = server.clone();
        let server_task = tokio::spawn(async move { task_server.listen_on("[::1]:5000").await });
        // Give the server time to start listening
        tokio::time::sleep(Duration::from_millis(100)).await;
        let url = Url::parse(&format!(
            "pliantdb://[::1]:5000?server={}",
            BASIC_SERVER_NAME
        ))?;

        let client = Client::connect(&url, server.certificate().await?)?;
        let (result_sender, result_receiver) = flume::bounded(1);
        println!("Sending request");
        client.request_sender.send(ClientRequest {
            request: Request::Database {
                database: Cow::from("tests"),
                request: DatabaseRequest::Get {
                    collection: Basic::collection_id(),
                    id: 0,
                },
            },
            responder: result_sender,
        })?;
        println!("Sent request");

        let result = dbg!(result_receiver.recv_async().await)?;
        println!("Result: {:?}", result);

        drop(client);
        println!("Calling shutdown");
        server.shutdown(None).await?;
        server_task.await??;

        Ok(())
    }
}
