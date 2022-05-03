use std::{collections::HashMap, sync::Arc};

use bonsaidb_core::{api::ApiName, networking::Payload};
use bonsaidb_utils::fast_async_lock;
use fabruic::{self, Certificate, Endpoint};
use flume::Receiver;
use futures::StreamExt;
use url::Url;

use super::PendingRequest;
use crate::{
    client::{AnyApiCallback, OutstandingRequestMapHandle, SubscriberMap},
    Error,
};

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
pub async fn reconnecting_client_loop(
    mut url: Url,
    protocol_version: &'static str,
    certificate: Option<Certificate>,
    request_receiver: Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
    subscribers: SubscriberMap,
) -> Result<(), Error> {
    if url.port().is_none() && url.scheme() == "bonsaidb" {
        let _ = url.set_port(Some(5645));
    }

    subscribers.clear();
    while let Ok(request) = request_receiver.recv_async().await {
        if let Err((failed_request, err)) = connect_and_process(
            &url,
            protocol_version,
            certificate.as_ref(),
            request,
            &request_receiver,
            custom_apis.clone(),
        )
        .await
        {
            if let Some(failed_request) = failed_request {
                drop(failed_request.responder.send(Err(err)));
            }
            continue;
        }
    }

    Ok(())
}

async fn connect_and_process(
    url: &Url,
    protocol_version: &str,
    certificate: Option<&Certificate>,
    initial_request: PendingRequest,
    request_receiver: &Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
) -> Result<(), (Option<PendingRequest>, Error)> {
    let (_connection, payload_sender, payload_receiver) =
        match connect(url, certificate, protocol_version).await {
            Ok(result) => result,
            Err(err) => return Err((Some(initial_request), err)),
        };

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(
        outstanding_requests.clone(),
        payload_receiver,
        custom_apis,
    ));

    if let Err(err) = payload_sender.send(&initial_request.request) {
        return Err((Some(initial_request), Error::from(err)));
    }

    {
        let mut outstanding_requests = fast_async_lock!(outstanding_requests);
        outstanding_requests.insert(
            initial_request
                .request
                .id
                .expect("all requests require ids"),
            initial_request,
        );
    }

    if let Err(err) = futures::try_join!(
        process_requests(
            outstanding_requests.clone(),
            request_receiver,
            payload_sender
        ),
        async { request_processor.await.map_err(|_| Error::Disconnected)? }
    ) {
        // Our socket was disconnected, clear the outstanding requests before returning.
        let mut outstanding_requests = fast_async_lock!(outstanding_requests);
        for (_, pending) in outstanding_requests.drain() {
            drop(pending.responder.send(Err(Error::Disconnected)));
        }
        return Err((None, err));
    }

    Ok(())
}

async fn process_requests(
    outstanding_requests: OutstandingRequestMapHandle,
    request_receiver: &Receiver<PendingRequest>,
    payload_sender: fabruic::Sender<Payload>,
) -> Result<(), Error> {
    while let Ok(client_request) = request_receiver.recv_async().await {
        let mut outstanding_requests = fast_async_lock!(outstanding_requests);
        payload_sender.send(&client_request.request)?;
        outstanding_requests.insert(
            client_request.request.id.expect("all requests require ids"),
            client_request,
        );
    }

    // Return an error to make sure try_join returns.
    Err(Error::Disconnected)
}

pub async fn process(
    outstanding_requests: OutstandingRequestMapHandle,
    mut payload_receiver: fabruic::Receiver<Payload>,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
) -> Result<(), Error> {
    while let Some(payload) = payload_receiver.next().await {
        let payload = payload?;
        super::process_response_payload(payload, &outstanding_requests, &custom_apis).await;
    }

    Err(Error::Disconnected)
}

async fn connect(
    url: &Url,
    certificate: Option<&Certificate>,
    protocol_version: &str,
) -> Result<
    (
        fabruic::Connection<()>,
        fabruic::Sender<Payload>,
        fabruic::Receiver<Payload>,
    ),
    Error,
> {
    let mut endpoint = Endpoint::builder();
    endpoint
        .set_max_idle_timeout(None)
        .map_err(|err| Error::Core(bonsaidb_core::Error::other("quic", err)))?;
    endpoint.set_protocols([protocol_version.as_bytes().to_vec()]);
    let endpoint = endpoint
        .build()
        .map_err(|err| Error::Core(bonsaidb_core::Error::other("quic", err)))?;
    let connecting = if let Some(certificate) = certificate {
        endpoint.connect_pinned(url, certificate, None).await?
    } else {
        endpoint.connect(url).await?
    };

    let connection = connecting.accept::<()>().await.map_err(|err| {
        if matches!(err, fabruic::error::Connecting::ProtocolMismatch) {
            Error::ProtocolVersionMismatch
        } else {
            Error::from(err)
        }
    })?;
    let (sender, receiver) = connection.open_stream(&()).await?;

    Ok((connection, sender, receiver))
}
