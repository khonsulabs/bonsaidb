use flume::{Receiver, Sender};
use futures::StreamExt;
use pliantdb_core::networking::{
    fabruic::{self, Certificate, Endpoint},
    Payload, Request, Response,
};
use url::Url;

#[cfg(feature = "pubsub")]
use crate::client::SubscriberMap;
use crate::{
    client::{OutstandingRequestMapHandle, PendingRequest},
    Error,
};

/// This function will establish a connection and try to keep it active. If an
/// error occurs, any queries that come in while reconnecting will have the
/// error replayed to them.
pub async fn reconnecting_client_loop(
    url: Url,
    certificate: Certificate,
    request_receiver: Receiver<PendingRequest>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Ok(request) = request_receiver.recv_async().await {
        if let Err((responder, err)) = connect_and_process(
            &url,
            &certificate,
            request,
            &request_receiver,
            #[cfg(feature = "pubsub")]
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
    url: &Url,
    certificate: &Certificate,
    initial_request: PendingRequest,
    request_receiver: &Receiver<PendingRequest>,
    #[cfg(feature = "pubsub")] subscribers: &SubscriberMap,
) -> Result<(), (Option<Sender<Result<Response, Error>>>, Error)> {
    let (_connection, payload_sender, payload_receiver) = connect(url, certificate)
        .await
        .map_err(|err| (Some(initial_request.responder.clone()), err))?;

    let outstanding_requests = OutstandingRequestMapHandle::default();
    let request_processor = tokio::spawn(process(
        outstanding_requests.clone(),
        payload_receiver,
        #[cfg(feature = "pubsub")]
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
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Some(payload) = payload_receiver.next().await {
        let payload = payload?;
        if let Some(payload_id) = payload.id {
            let responder = {
                let mut outstanding_requests = outstanding_requests.lock().await;
                outstanding_requests
                    .remove(&payload_id)
                    .expect("missing responder")
            };
            let _ = responder.send(Ok(payload.wrapped));
        } else {
            #[cfg(feature = "pubsub")]
            {
                use std::sync::Arc;

                use pliantdb_core::{circulate::Message, networking::DatabaseResponse};

                if let Response::Database(DatabaseResponse::MessageReceived {
                    subscriber_id,
                    topic,
                    payload,
                }) = payload.wrapped
                {
                    let mut subscribers = subscribers.lock().await;
                    if let Some(sender) = subscribers.get(&subscriber_id) {
                        if sender.send(Arc::new(Message { topic, payload })).is_err() {
                            subscribers.remove(&subscriber_id);
                        }
                    }

                    continue;
                }
            }
            unreachable!("only MessageReceived is allowed to not have an id")
        }
    }

    Err(Error::Disconnected)
}

async fn connect(
    url: &Url,
    certificate: &Certificate,
) -> Result<
    (
        fabruic::Connection<()>,
        fabruic::Sender<Payload<Request>>,
        fabruic::Receiver<Payload<Response>>,
    ),
    Error,
> {
    let endpoint = Endpoint::new_client()
        .map_err(|err| Error::Core(pliantdb_core::Error::Transport(err.to_string())))?;
    let connecting = endpoint.connect_pinned(url, certificate, None).await?;

    let connection = connecting.accept::<()>().await?;
    let (sender, receiver) = connection.open_stream(&()).await?;

    Ok((connection, sender, receiver))
}
