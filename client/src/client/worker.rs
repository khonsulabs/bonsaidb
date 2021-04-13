use std::time::Duration;

use flume::{Receiver, Sender};
use futures::StreamExt;
use pliantdb_core::networking::{
    fabruic::{self, Certificate, Endpoint},
    Payload, Request, Response,
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
        if let Err((responder, err)) = connect_and_process(
            &host,
            &server_name,
            &certificate,
            request,
            &request_receiver,
        )
        .await
        {
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
) -> Result<(), (Option<Sender<Result<Response, Error>>>, Error)> {
    let (_connection, payload_sender, payload_receiver) = connect(host, server_name, certificate)
        .await
        .map_err(|err| (Some(initial_request.responder.clone()), err))?;

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(outstanding_requests.clone(), payload_receiver));

    let PendingRequest { request, responder } = initial_request;
    payload_sender
        .send(&request)
        .map_err(|err| (Some(responder.clone()), Error::from(err)))?;
    {
        let mut outstanding_requests = outstanding_requests.lock().await;
        outstanding_requests.insert(request.id, responder);
    }

    // TODO switch to select
    futures::try_join!(
        process_requests(outstanding_requests, request_receiver, payload_sender),
        async { request_processor.await.map_err(|_| Error::Disconnected)? }
    )
    .map_err(|err| (None, err))?;

    Ok(())
}

async fn process_requests(
    outstanding_requests: OutstandingRequestMapHandle,
    request_receiver: &Receiver<PendingRequest>,
    payload_sender: fabruic::Sender<Payload<Request>>,
) -> Result<(), Error> {
    while let Ok(client_request) = request_receiver.recv_async().await {
        let mut outstanding_requests = outstanding_requests.lock().await;
        outstanding_requests.insert(client_request.request.id, client_request.responder);
        payload_sender.send(&client_request.request)?;
    }

    // Return an error to make sure try_join returns.
    Err(Error::Disconnected)
}

pub async fn process(
    outstanding_requests: OutstandingRequestMapHandle,
    mut payload_receiver: fabruic::Receiver<Payload<Response>>,
) -> Result<(), Error> {
    while let Some(payload) = payload_receiver.next().await {
        let responder = {
            let mut outstanding_requests = outstanding_requests.lock().await;
            outstanding_requests
                .remove(&payload.id)
                .expect("missing responder")
        };
        let _ = responder.send(Ok(payload.wrapped));
    }

    Err(Error::Disconnected)
}

async fn connect(
    host: &str,
    server_name: &str,
    certificate: &Certificate,
) -> Result<
    (
        fabruic::Connection<()>,
        fabruic::Sender<Payload<Request>>,
        fabruic::Receiver<Payload<Response>>,
    ),
    Error,
> {
    let endpoint = Endpoint::new_client(certificate)?;
    // TODO This could be handled by fabruic, or we could bring in trust-dns for
    // an alternative option here.
    let addr = tokio::net::lookup_host(host)
        .await
        .map_err(|err| Error::InvalidUrl(err.to_string()))?
        .next()
        .ok_or_else(|| Error::InvalidUrl(String::from("No IP found for host.")))?;
    let connecting = endpoint.connect(addr, server_name)?;
    let connection = connecting.accept::<()>().await?;
    let (sender, receiver) = connection.open_stream(&()).await?;

    Ok((connection, sender, receiver))
}
