use flume::Receiver;
use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use pliantdb_core::{
    networking::{Payload, Response},
    permissions::Action,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_tungstenite::{tungstenite::Message, MaybeTlsStream, WebSocketStream};
use url::Url;

#[cfg(feature = "pubsub")]
use crate::client::SubscriberMap;
use crate::{
    client::{OutstandingRequestMapHandle, PendingRequest},
    Error,
};

pub async fn reconnecting_client_loop<
    R: Action + Serialize + for<'de> Deserialize<'de>,
    O: Serialize + for<'de> Deserialize<'de> + Send,
>(
    url: Url,
    mut request_receiver: Receiver<PendingRequest<R, O>>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Ok(request) = request_receiver.recv_async().await {
        let (stream, _) = match tokio_tungstenite::connect_async(&url).await {
            Ok(result) => result,
            Err(err) => {
                let _ = request.responder.send(Err(Error::WebSocket(err)));
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
                let _ = request.responder.send(Err(Error::WebSocket(err)));
                continue;
            }
            outstanding_requests.insert(
                request.request.id.expect("all requests must have ids"),
                request.responder,
            );
        }

        if let Err(err) = tokio::try_join!(
            request_sender(&mut request_receiver, sender, outstanding_requests.clone()),
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
    R: Action + Serialize + for<'de> Deserialize<'de>,
    O: Serialize + for<'de> Deserialize<'de> + Send,
>(
    request_receiver: &mut Receiver<PendingRequest<R, O>>,
    mut sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    outstanding_requests: OutstandingRequestMapHandle<O>,
) -> Result<(), Error> {
    while let Ok(pending) = request_receiver.recv_async().await {
        {
            let mut outstanding_requests = outstanding_requests.lock().await;
            outstanding_requests.insert(
                pending.request.id.expect("all requests must have ids"),
                pending.responder,
            );
        }
        sender
            .send(Message::Binary(bincode::serialize(&pending.request)?))
            .await?;
    }

    Err(Error::Disconnected)
}

#[allow(clippy::collapsible_else_if)] // not possible due to cfg statement
async fn response_processor<O: for<'de> Deserialize<'de> + Send>(
    mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outstanding_requests: OutstandingRequestMapHandle<O>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    while let Some(message) = receiver.next().await {
        let message = message?;
        match message {
            Message::Binary(response) => {
                let payload = bincode::deserialize::<Payload<Response<O>>>(&response)?;

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
                    if let Response::Database(
                        pliantdb_core::networking::DatabaseResponse::MessageReceived {
                            subscriber_id,
                            topic,
                            payload,
                        },
                    ) = payload.wrapped
                    {
                        let mut subscribers = subscribers.lock().await;
                        if let Some(sender) = subscribers.get(&subscriber_id) {
                            if sender
                                .send(std::sync::Arc::new(pliantdb_core::circulate::Message {
                                    topic,
                                    payload,
                                }))
                                .is_err()
                            {
                                subscribers.remove(&subscriber_id);
                            }
                        }
                    } else {
                        unreachable!("only MessageReceived is allowed to not have an id")
                    }
                }
            }
            other => {
                println!("Unexpected websocket message: {:?}", other);
            }
        }
    }

    Ok(())
}
