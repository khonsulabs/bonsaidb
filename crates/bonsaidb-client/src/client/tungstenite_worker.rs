use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bonsaidb_core::api::ApiName;
use bonsaidb_core::networking::Payload;
use bonsaidb_utils::fast_async_lock;
use flume::Receiver;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;

use super::PendingRequest;
use crate::client::{AnyApiCallback, OutstandingRequestMapHandle, SubscriberMap};
use crate::Error;

pub async fn reconnecting_client_loop(
    url: Url,
    protocol_version: &str,
    request_receiver: Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
    subscribers: SubscriberMap,
    connection_counter: Arc<AtomicU32>,
) -> Result<(), Error> {
    while let Ok(request) = {
        subscribers.clear();
        request_receiver.recv_async().await
    } {
        connection_counter.fetch_add(1, Ordering::SeqCst);
        let (stream, _) = match tokio_tungstenite::connect_async(
            tokio_tungstenite::tungstenite::handshake::client::Request::get(url.as_str())
                .header("Sec-WebSocket-Protocol", protocol_version)
                .header("Sec-WebSocket-Version", "13")
                .header("Sec-WebSocket-Key", generate_key())
                .header("Host", url.host_str().expect("no host"))
                .header("Connection", "Upgrade")
                .header("Upgrade", "websocket")
                .body(())
                .unwrap(),
        )
        .await
        {
            Ok(result) => result,
            Err(err) => {
                drop(request.responder.send(Err(Error::from(err))));
                continue;
            }
        };

        let (mut sender, receiver) = stream.split();

        let outstanding_requests = OutstandingRequestMapHandle::default();
        {
            let mut outstanding_requests = fast_async_lock!(outstanding_requests);
            if let Err(err) = sender
                .send(Message::Binary(bincode::serialize(&request.request)?))
                .await
            {
                drop(request.responder.send(Err(Error::from(err))));
                continue;
            }
            outstanding_requests.insert(
                request.request.id.expect("all requests must have ids"),
                request,
            );
        }

        if let Err(err) = tokio::try_join!(
            request_sender(&request_receiver, sender, outstanding_requests.clone()),
            response_processor(receiver, outstanding_requests.clone(), &custom_apis,)
        ) {
            // Our socket was disconnected, clear the outstanding requests before returning.
            let mut outstanding_requests = fast_async_lock!(outstanding_requests);
            for (_, pending) in outstanding_requests.drain() {
                drop(pending.responder.send(Err(Error::Disconnected)));
            }
            log::error!("Error on socket {:?}", err);
        }
    }

    Ok(())
}

async fn request_sender(
    request_receiver: &Receiver<PendingRequest>,
    mut sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    outstanding_requests: OutstandingRequestMapHandle,
) -> Result<(), Error> {
    while let Ok(pending) = request_receiver.recv_async().await {
        let mut outstanding_requests = fast_async_lock!(outstanding_requests);
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
async fn response_processor(
    mut receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    outstanding_requests: OutstandingRequestMapHandle,
    custom_apis: &HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
) -> Result<(), Error> {
    while let Some(message) = receiver.next().await {
        let message = message?;
        match message {
            Message::Binary(response) => {
                let payload = bincode::deserialize::<Payload>(&response)?;

                super::process_response_payload(payload, &outstanding_requests, custom_apis).await;
            }
            other => {
                log::error!("Unexpected websocket message: {:?}", other);
            }
        }
    }

    Ok(())
}
