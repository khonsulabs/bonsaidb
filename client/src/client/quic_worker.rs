use std::sync::Arc;

use bonsaidb_core::networking::{Payload, Request, Response};
use fabruic::{self, Certificate, Endpoint};
use flume::Receiver;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use url::Url;

use super::{CustomApiCallback, PendingRequest};
#[cfg(feature = "pubsub")]
use crate::client::SubscriberMap;
use crate::{client::OutstandingRequestMapHandle, Error};

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
pub async fn reconnecting_client_loop<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
>(
    mut url: Url,
    certificate: Option<Certificate>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    if url.port().is_none() && url.scheme() == "bonsaidb" {
        let _ = url.set_port(Some(5645));
    }

    while let Ok(request) = request_receiver.recv_async().await {
        if let Err((failed_request, err)) = connect_and_process(
            &url,
            certificate.as_ref(),
            request,
            &request_receiver,
            custom_api_callback.clone(),
            #[cfg(feature = "pubsub")]
            &subscribers,
        )
        .await
        {
            println!(
                "Received an error: {:?}. Had request: {:?}",
                err,
                failed_request.is_some()
            );
            if let Some(failed_request) = failed_request {
                drop(failed_request.responder.send(Err(err)));
            }
            continue;
        }
    }

    Ok(())
}

async fn connect_and_process<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
>(
    url: &Url,
    certificate: Option<&Certificate>,
    initial_request: PendingRequest<R, O>,
    request_receiver: &Receiver<PendingRequest<R, O>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    #[cfg(feature = "pubsub")] subscribers: &SubscriberMap,
) -> Result<(), (Option<PendingRequest<R, O>>, Error)> {
    let (_connection, payload_sender, payload_receiver) = match connect(url, certificate).await {
        Ok(result) => result,
        Err(err) => return Err((Some(initial_request), err)),
    };

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(
        outstanding_requests.clone(),
        payload_receiver,
        custom_api_callback,
        #[cfg(feature = "pubsub")]
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

    futures::try_join!(
        process_requests(outstanding_requests, request_receiver, payload_sender),
        async { request_processor.await.map_err(|_| Error::Disconnected)? }
    )
    .map_err(|err| (None, err))?;

    Ok(())
}

async fn process_requests<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
>(
    outstanding_requests: OutstandingRequestMapHandle<R, O>,
    request_receiver: &Receiver<PendingRequest<R, O>>,
    payload_sender: fabruic::Sender<Payload<Request<R>>>,
) -> Result<(), Error> {
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

pub async fn process<R: Send + Sync + 'static, O: Send + Sync + 'static>(
    outstanding_requests: OutstandingRequestMapHandle<R, O>,
    mut payload_receiver: fabruic::Receiver<Payload<Response<O>>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Some(payload) = payload_receiver.next().await {
        let payload = payload?;
        super::process_response_payload(
            payload,
            &outstanding_requests,
            custom_api_callback.as_ref(),
            #[cfg(feature = "pubsub")]
            &subscribers,
        )
        .await;
    }

    Err(Error::Disconnected)
}

async fn connect<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
>(
    url: &Url,
    certificate: Option<&Certificate>,
) -> Result<
    (
        fabruic::Connection<()>,
        fabruic::Sender<Payload<Request<R>>>,
        fabruic::Receiver<Payload<Response<O>>>,
    ),
    Error,
> {
    let endpoint = Endpoint::new_client()
        .map_err(|err| Error::Core(bonsaidb_core::Error::Transport(err.to_string())))?;
    let connecting = if let Some(certificate) = certificate {
        endpoint.connect_pinned(url, certificate, None).await?
    } else {
        endpoint.connect(url).await?
    };

    let connection = connecting.accept::<()>().await?;
    let (sender, receiver) = connection.open_stream(&()).await?;

    Ok((connection, sender, receiver))
}
