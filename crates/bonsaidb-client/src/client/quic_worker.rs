use std::sync::Arc;

use bonsaidb_core::{
    custom_api::{CustomApi, CustomApiResult},
    networking::{Payload, Request, Response},
};
use fabruic::{self, Certificate, Endpoint};
use flume::Receiver;
use futures::StreamExt;
use url::Url;

use super::{CustomApiCallback, PendingRequest};
use crate::{
    client::{OutstandingRequestMapHandle, SubscriberMap},
    Error,
};

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
pub async fn reconnecting_client_loop<A: CustomApi>(
    mut url: Url,
    protocol_version: &'static [u8],
    certificate: Option<Certificate>,
    request_receiver: Receiver<PendingRequest<A>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    subscribers: SubscriberMap,
) -> Result<(), Error<A::Error>> {
    if url.port().is_none() && url.scheme() == "bonsaidb" {
        let _ = url.set_port(Some(5645));
    }

    subscribers.clear().await;
    while let Ok(request) = request_receiver.recv_async().await {
        if let Err((failed_request, err)) = connect_and_process(
            &url,
            protocol_version,
            certificate.as_ref(),
            request,
            &request_receiver,
            custom_api_callback.clone(),
            &subscribers,
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

async fn connect_and_process<A: CustomApi>(
    url: &Url,
    protocol_version: &[u8],
    certificate: Option<&Certificate>,
    initial_request: PendingRequest<A>,
    request_receiver: &Receiver<PendingRequest<A>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    subscribers: &SubscriberMap,
) -> Result<(), (Option<PendingRequest<A>>, Error<A::Error>)> {
    let (_connection, payload_sender, payload_receiver) =
        match connect::<A>(url, certificate, protocol_version).await {
            Ok(result) => result,
            Err(err) => return Err((Some(initial_request), err)),
        };

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(
        outstanding_requests.clone(),
        payload_receiver,
        custom_api_callback,
        subscribers.clone(),
    ));

    if let Err(err) = payload_sender.send(&initial_request.request) {
        return Err((Some(initial_request), Error::from(err)));
    }

    {
        let mut outstanding_requests = outstanding_requests.lock().await;
        outstanding_requests.insert(
            initial_request
                .request
                .id
                .expect("all requests require ids"),
            initial_request,
        );
    }

    if let Err(err) = futures::try_join!(
        process_requests::<A>(
            outstanding_requests.clone(),
            request_receiver,
            payload_sender
        ),
        async { request_processor.await.map_err(|_| Error::Disconnected)? }
    ) {
        // Our socket was disconnected, clear the outstanding requests before returning.
        let mut outstanding_requests = outstanding_requests.lock().await;
        for (_, pending) in outstanding_requests.drain() {
            drop(pending.responder.send(Err(Error::Disconnected)));
        }
        return Err((None, err));
    }

    Ok(())
}

async fn process_requests<A: CustomApi>(
    outstanding_requests: OutstandingRequestMapHandle<A>,
    request_receiver: &Receiver<PendingRequest<A>>,
    payload_sender: fabruic::Sender<Payload<Request<A::Request>>>,
) -> Result<(), Error<A::Error>> {
    while let Ok(client_request) = request_receiver.recv_async().await {
        let mut outstanding_requests = outstanding_requests.lock().await;
        payload_sender.send(&client_request.request)?;
        outstanding_requests.insert(
            client_request.request.id.expect("all requests require ids"),
            client_request,
        );
    }

    // Return an error to make sure try_join returns.
    Err(Error::Disconnected)
}

pub async fn process<A: CustomApi>(
    outstanding_requests: OutstandingRequestMapHandle<A>,
    mut payload_receiver: fabruic::Receiver<Payload<Response<CustomApiResult<A>>>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    subscribers: SubscriberMap,
) -> Result<(), Error<A::Error>> {
    while let Some(payload) = payload_receiver.next().await {
        let payload = payload?;
        super::process_response_payload(
            payload,
            &outstanding_requests,
            custom_api_callback.as_deref(),
            &subscribers,
        )
        .await;
    }

    Err(Error::Disconnected)
}

async fn connect<A: CustomApi>(
    url: &Url,
    certificate: Option<&Certificate>,
    protocol_version: &[u8],
) -> Result<
    (
        fabruic::Connection<()>,
        fabruic::Sender<Payload<Request<A::Request>>>,
        fabruic::Receiver<Payload<Response<CustomApiResult<A>>>>,
    ),
    Error<A::Error>,
> {
    let mut endpoint = Endpoint::builder();
    endpoint
        .set_max_idle_timeout(None)
        .map_err(|err| Error::Core(bonsaidb_core::Error::Transport(err.to_string())))?;
    endpoint.set_protocols([protocol_version.to_vec()]);
    let endpoint = endpoint
        .build()
        .map_err(|err| Error::Core(bonsaidb_core::Error::Transport(err.to_string())))?;
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
