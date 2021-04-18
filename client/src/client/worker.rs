use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use flume::{Receiver, Sender};
use futures::StreamExt;
use pliantdb_core::networking::{
    fabruic::{self, Certificate, Endpoint},
    DatabaseResponse, Payload, Request, Response,
};

use crate::{
    client::{OutstandingRequestMapHandle, PendingRequest, SubscriberMap},
    Error,
};

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
pub async fn reconnecting_client_loop(
    host: String,
    port: u16,
    server_name: String,
    certificate: Certificate,
    request_receiver: Receiver<PendingRequest>,
    subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Ok(request) = request_receiver.recv_async().await {
        if let Err((responder, err)) = connect_and_process(
            &host,
            port,
            &server_name,
            &certificate,
            request,
            &request_receiver,
            &subscribers,
        )
        .await
        {
            if let Some(responder) = responder {
                let _ = responder.try_send(Err(err));
            }
            continue;
        }
    }

    Ok(())
}

async fn connect_and_process(
    host: &str,
    port: u16,
    server_name: &str,
    certificate: &Certificate,
    initial_request: PendingRequest,
    request_receiver: &Receiver<PendingRequest>,
    subscribers: &SubscriberMap,
) -> Result<(), (Option<Sender<Result<Response, Error>>>, Error)> {
    let (_connection, payload_sender, payload_receiver) =
        connect(host, port, server_name, certificate)
            .await
            .map_err(|err| (Some(initial_request.responder.clone()), err))?;

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(
        outstanding_requests.clone(),
        payload_receiver,
        subscribers.clone(),
    ));

    let PendingRequest { request, responder } = initial_request;
    payload_sender
        .send(&request)
        .map_err(|err| (Some(responder.clone()), Error::from(err)))?;
    {
        let mut outstanding_requests = outstanding_requests.lock().await;
        outstanding_requests.insert(request.id.expect("all requests require ids"), responder);
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
        outstanding_requests.insert(
            client_request.request.id.expect("all requests require ids"),
            client_request.responder,
        );
        payload_sender.send(&client_request.request)?;
    }

    // Return an error to make sure try_join returns.
    Err(Error::Disconnected)
}

pub async fn process(
    outstanding_requests: OutstandingRequestMapHandle,
    mut payload_receiver: fabruic::Receiver<Payload<Response>>,
    subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Some(payload) = payload_receiver.next().await {
        if let Some(payload_id) = payload.id {
            let responder = {
                let mut outstanding_requests = outstanding_requests.lock().await;
                outstanding_requests
                    .remove(&payload_id)
                    .expect("missing responder")
            };
            let _ = responder.send(Ok(payload.wrapped));
        } else if let Response::Database(DatabaseResponse::MessageReceived {
            subscriber_id,
            message,
        }) = payload.wrapped
        {
            let mut subscribers = subscribers.lock().await;
            if let Some(sender) = subscribers.get(&subscriber_id) {
                if sender.send(Arc::new(message)).is_err() {
                    subscribers.remove(&subscriber_id);
                }
            }
        } else {
            unreachable!("only MessageReceived is allowed to not have an id")
        }
    }

    Err(Error::Disconnected)
}

async fn connect(
    host: &str,
    port: u16,
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
    // When parsing an ipv6 URL such as pliantdb://[::1]:5645, the host will
    // contain the brackets which we must strip before parsing the IpAddr.
    let possible_ip = if host.starts_with('[') && host.ends_with(']') {
        &host[1..host.len() - 1]
    } else {
        host
    };

    // Attempt to parse the ip address string
    let possible_ip = if let Ok(ip) = possible_ip.parse::<IpAddr>() {
        Some(ip)
    } else if possible_ip == "localhost" {
        // If feature trusted-dns is enabled, a DNSSEC-supporting resolver will
        // be used, which won't support localhost.
        // TODO auto-detect ipv6 on loopback? Does it matter? Need to test an ipv6 only machine.
        Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
    } else {
        None
    };
    let connecting = if let Some(ip) = possible_ip {
        endpoint.connect(SocketAddr::new(ip, port), server_name)?
    } else {
        #[cfg(feature = "trusted-dns")]
        // If using trusted-dns, use fabruic to do the hostname resolution. In
        // this mode, server_name is not used to validate, just the host name.
        let connector = async move { endpoint.connect_with(port, host).await };

        #[cfg(not(feature = "trusted-dns"))]
        // When not using trusted-dns, resolve to an IP address using the system
        // resolver, and then connect using the server_name to validate the
        // certificate.
        let connector = async move {
            let addr = tokio::net::lookup_host(&format!("{}:{}", host, port))
                .await
                .map_err(|err| Error::InvalidUrl(err.to_string()))?
                .next()
                .ok_or_else(|| Error::InvalidUrl(String::from("No IP found for host.")))?;
            endpoint.connect(addr, server_name).map_err(Error::from)
        };

        connector.await?
    };

    let connection = connecting.accept::<()>().await?;
    let (sender, receiver) = connection.open_stream(&()).await?;

    Ok((connection, sender, receiver))
}
