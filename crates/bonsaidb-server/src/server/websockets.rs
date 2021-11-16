use hyper::{
    header::{HeaderValue, CONNECTION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, UPGRADE},
    StatusCode,
};
use sha1::Digest;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{tungstenite::protocol::Role, WebSocketStream};

use crate::{Backend, CustomServer, Error};

impl<B: Backend> CustomServer<B> {
    pub(crate) async fn handle_raw_websocket_connection<
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        connection: S,
        peer_address: std::net::SocketAddr,
    ) -> Result<(), Error> {
        let stream = tokio_tungstenite::accept_async(connection).await?;
        self.handle_websocket(stream, peer_address).await;
        Ok(())
    }

    /// Handles upgrading an HTTP connection to the `WebSocket` protocol based
    /// on the upgrade `request`.
    pub async fn upgrade_websocket(
        &self,
        peer_address: std::net::SocketAddr,
        mut request: hyper::Request<hyper::Body>,
    ) -> hyper::Response<hyper::Body> {
        let mut response = hyper::Response::new(hyper::Body::empty());
        // Send a 400 to any request that doesn't have
        // an `Upgrade` header.
        if !request.headers().contains_key(UPGRADE) {
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return response;
        }

        let sec_websocket_key = if let Some(key) = request.headers_mut().remove(SEC_WEBSOCKET_KEY) {
            key
        } else {
            *response.status_mut() = StatusCode::BAD_REQUEST;
            return response;
        };

        let task_self = self.clone();
        tokio::spawn(async move {
            match hyper::upgrade::on(&mut request).await {
                Ok(upgraded) => {
                    let ws = WebSocketStream::from_raw_socket(upgraded, Role::Server, None).await;
                    task_self.handle_websocket(ws, peer_address).await;
                }
                Err(err) => {
                    log::error!("Error upgrading websocket: {:?}", err);
                }
            }
        });

        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        response
            .headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("websocket"));
        response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("upgrade"));
        response.headers_mut().insert(
            SEC_WEBSOCKET_ACCEPT,
            compute_websocket_accept_header(sec_websocket_key.as_bytes()),
        );

        response
    }

    /// Handles an established `tokio-tungstenite` `WebSocket` stream.
    pub async fn handle_websocket<
        S: futures::Stream<Item = Result<tokio_tungstenite::tungstenite::Message, E>>
            + futures::Sink<tokio_tungstenite::tungstenite::Message>
            + Send
            + 'static,
        E: std::fmt::Debug + Send,
    >(
        &self,
        connection: S,
        peer_address: std::net::SocketAddr,
    ) {
        use bonsaidb_core::{
            custom_api::CustomApi,
            networking::{Payload, Request, Response},
        };
        use futures::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        use crate::Transport;

        let (mut sender, mut receiver) = connection.split();
        let (response_sender, response_receiver) = flume::unbounded();
        let (message_sender, message_receiver) = flume::unbounded();

        let (api_response_sender, api_response_receiver) = flume::unbounded();
        let client = if let Some(client) = self
            .initialize_client(Transport::WebSocket, peer_address, api_response_sender)
            .await
        {
            client
        } else {
            return;
        };
        let task_sender = response_sender.clone();
        tokio::spawn(async move {
            while let Ok(response) = api_response_receiver.recv_async().await {
                if task_sender
                    .send(Payload {
                        id: None,
                        wrapped: Response::Api(response),
                    })
                    .is_err()
                {
                    break;
                }
            }
        });

        tokio::spawn(async move {
            while let Ok(response) = message_receiver.recv_async().await {
                if sender.send(response).await.is_err() {
                    break;
                }
            }

            Result::<(), Error>::Ok(())
        });

        let task_sender = message_sender.clone();
        tokio::spawn(async move {
            while let Ok(response) = response_receiver.recv_async().await {
                if task_sender
                    .send(Message::Binary(bincode::serialize(&response)?))
                    .is_err()
                {
                    break;
                }
            }

            Result::<(), Error>::Ok(())
        });

        let (request_sender, request_receiver) =
            flume::bounded::<Payload<Request<<B::CustomApi as CustomApi>::Request>>>(
                self.data.client_simultaneous_request_limit,
            );
        let task_self = self.clone();
        tokio::spawn(async move {
            task_self
                .handle_client_requests(client.clone(), request_receiver, response_sender)
                .await;
        });

        while let Some(payload) = receiver.next().await {
            match payload {
                Ok(Message::Binary(binary)) => {
                    match bincode::deserialize::<
                        Payload<Request<<B::CustomApi as CustomApi>::Request>>,
                    >(&binary)
                    {
                        Ok(payload) => drop(request_sender.send_async(payload).await),
                        Err(err) => {
                            log::error!("[server] error decoding message: {:?}", err);
                            break;
                        }
                    }
                }
                Ok(Message::Close(_)) => break,
                Ok(Message::Ping(payload)) => {
                    drop(message_sender.send(Message::Pong(payload)));
                }
                other => {
                    log::error!("[server] unexpected message: {:?}", other);
                }
            }
        }
    }
}

fn compute_websocket_accept_header(key: &[u8]) -> hyper::header::HeaderValue {
    let mut digest = sha1::Sha1::default();
    digest.update(key);
    digest.update(&b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"[..]);
    let encoded = base64::encode(&digest.finalize());
    hyper::header::HeaderValue::from_str(&encoded).expect("base64 is a valid value")
}
