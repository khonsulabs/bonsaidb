use std::sync::Arc;

use bonsaidb_core::{
    custom_api::{CustomApi, CustomApiResult},
    networking::{Payload, Response},
};
use flume::Receiver;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::{CustomApiCallback, PendingRequest};
use crate::{
    client::{OutstandingRequestMapHandle, SubscriberMap},
    Error,
};

pub async fn reconnecting_client_loop<A: CustomApi>(
    url: Url,
    request_receiver: Receiver<PendingRequest<A>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    subscribers: SubscriberMap,
) -> Result<(), Error<A::Error>> {
    while let Ok(request) = request_receiver.recv_async().await {
        let (stream, _) = match tokio_tungstenite::connect_async(&url).await {
            Ok(result) => result,
            Err(err) => {
                drop(request.responder.send(Err(Error::WebSocket(err))));
                continue;
            }
        };

        let (mut sender, receiver) = stream.split();

        let outstanding_requests = OutstandingRequestMapHandle::default();
        {
            let mut outstanding_requests = outstanding_requests.lock().await;
            if let Err(err) = sender
                .send(Message::Binary(bincode::serialize(&request.request)?))
                .await
            {
                drop(request.responder.send(Err(Error::WebSocket(err))));
                continue;
            }
            outstanding_requests.insert(
                request.request.id.expect("all requests must have ids"),
                request,
            );
        }

        if let Err(err) = tokio::try_join!(
            request_sender(&request_receiver, sender, outstanding_requests.clone()),
            response_processor(
                receiver,
                outstanding_requests.clone(),
                custom_api_callback.as_deref(),
                subscribers.clone()
            )
        ) {
            // Our socket was disconnected, clear the outstanding requests before returning.
            let mut outstanding_requests = outstanding_requests.lock().await;
            for (_, pending) in outstanding_requests.drain() {
                drop(pending.responder.send(Err(Error::Disconnected)));
            }
            eprintln!("Error on socket {:?}", err);
        }
    }

    Ok(())
}

async fn request_sender<Api: CustomApi>(
    request_receiver: &Receiver<PendingRequest<Api>>,
    mut sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    outstanding_requests: OutstandingRequestMapHandle<Api>,
) -> Result<(), Error<Api::Error>> {
    while let Ok(pending) = request_receiver.recv_async().await {
        let mut outstanding_requests = outstanding_requests.lock().await;
        sender
            .send(Message::Binary(bincode::serialize(&pending.request)?))
            .await?;

        outstanding_requests.insert(
            pending.request.id.expect("all requests must have ids"),
            pending,
        );
    }

    Err(Error::Disconnected)
}

#[allow(clippy::collapsible_else_if)] // not possible due to cfg statement
async fn response_processor<A: CustomApi>(
    mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outstanding_requests: OutstandingRequestMapHandle<A>,
    custom_api_callback: Option<&dyn CustomApiCallback<A>>,
    subscribers: SubscriberMap,
) -> Result<(), Error<A::Error>> {
    while let Some(message) = receiver.next().await {
        let message = message?;
        match message {
            Message::Binary(response) => {
                let payload =
                    bincode::deserialize::<Payload<Response<CustomApiResult<A>>>>(&response)?;

                super::process_response_payload(
                    payload,
                    &outstanding_requests,
                    custom_api_callback,
                    &subscribers,
                )
                .await;
            }
            other => {
                eprintln!("Unexpected websocket message: {:?}", other);
            }
        }
    }

    Ok(())
}
