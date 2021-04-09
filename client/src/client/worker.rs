use std::time::Duration;

use flume::{Receiver, Sender};
use futures::StreamExt;
use pliantdb_core::networking::{
    fabruic::{self, Certificate, Endpoint},
    Api, Payload, Response,
};

use crate::{
    client::{OutstandingRequestMapHandle, PendingRequest},
    Error,
};

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
pub async fn reconnecting_client_loop(
    host: String,
    server_name: String,
    certificate: Certificate,
    request_receiver: Receiver<PendingRequest>,
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
    initial_request: PendingRequest,
    request_receiver: &Receiver<PendingRequest>,
) -> Result<(), (Option<Sender<Result<Response<'static>, Error>>>, Error)> {
    let (_connection, payload_sender, payload_receiver) = connect(host, server_name, certificate)
        .await
        .map_err(|err| (Some(initial_request.responder.clone()), err))?;

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(outstanding_requests.clone(), payload_receiver));

    let mut request_id = 0;
    let PendingRequest { request, responder } = initial_request;
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
    request_receiver: &Receiver<PendingRequest>,
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
    let endpoint = Endpoint::new_client(certificate)?;
    let addr = tokio::net::lookup_host(host)
        .await
        .map_err(|err| Error::InvalidUrl(err.to_string()))?
        .next()
        .ok_or_else(|| Error::InvalidUrl(String::from("No IP found for host.")))?;
    let connection = endpoint.connect(addr, server_name).await?;
    let (sender, receiver) = connection.open_stream().await?;

    Ok((connection, sender, receiver))
}
