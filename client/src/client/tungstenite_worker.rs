use bonsaidb_core::networking::{Payload, Response};
use flume::Receiver;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

use super::PendingRequest;
#[cfg(feature = "pubsub")]
use crate::client::SubscriberMap;
use crate::{client::OutstandingRequestMapHandle, Error};

pub async fn reconnecting_client_loop<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Url,
    request_receiver: Receiver<PendingRequest<R, O>>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
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
                outstanding_requests,
                #[cfg(feature = "pubsub")]
                subscribers.clone()
            )
        ) {
            println!("Error on socket {:?}", err);
        }
    }

    Ok(())
}

async fn request_sender<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    request_receiver: &Receiver<PendingRequest<R, O>>,
    mut sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    outstanding_requests: OutstandingRequestMapHandle<R, O>,
) -> Result<(), Error> {
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
async fn response_processor<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Send + Sync + for<'de> Deserialize<'de> + 'static,
>(
    mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outstanding_requests: OutstandingRequestMapHandle<R, O>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Some(message) = receiver.next().await {
        let message = message?;
        match message {
            Message::Binary(response) => {
                let payload = bincode::deserialize::<Payload<Response<O>>>(&response)?;

                super::process_response_payload(
                    payload,
                    &outstanding_requests,
                    #[cfg(feature = "pubsub")]
                    &subscribers,
                )
                .await;
            }
            other => {
                println!("Unexpected websocket message: {:?}", other);
            }
        }
    }

    Ok(())
}
