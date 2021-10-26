use std::sync::{Arc, Mutex};

use bonsaidb_core::networking::{Payload, Response};
use flume::Receiver;
use serde::{Deserialize, Serialize};
use url::Url;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::{
    client::{CustomApiCallback, OutstandingRequestMapHandle, PendingRequest, SubscriberMap},
    Error,
};

pub fn spawn_client<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    subscribers: SubscriberMap,
) {
    wasm_bindgen_futures::spawn_local(create_websocket(
        url,
        request_receiver,
        custom_api_callback,
        subscribers,
    ));
}

async fn create_websocket<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    subscribers: SubscriberMap,
) {
    // Receive the next/initial request when we are reconnecting.
    let initial_request = match request_receiver.recv_async().await {
        Ok(r) => r,
        Err(_) => return,
    };
    // In wasm we're not going to have a real loop. We're going create a
    // websocket and store it in JS. This will allow us to get around Send/Sync
    // issues since each access of the websocket can pull it from js.
    let ws = match WebSocket::new(&url.to_string()) {
        Ok(ws) => ws,
        Err(err) => {
            drop(
                initial_request
                    .responder
                    .send(Err(Error::from(WebSocketError::from(err)))),
            );
            spawn_client(
                url,
                request_receiver,
                custom_api_callback.clone(),
                subscribers,
            );
            return;
        }
    };

    let (connection_request_sender, connection_request_receiver) = flume::unbounded();
    let (shutdown_sender, shutdown_receiver) = flume::unbounded();
    forward_request_with_shutdown(
        request_receiver.clone(),
        shutdown_receiver,
        connection_request_sender,
    );

    let initial_request = Arc::new(Mutex::new(Some(initial_request)));
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    let outstanding_requests = OutstandingRequestMapHandle::default();

    let onopen_callback = on_open_callback(
        connection_request_receiver,
        initial_request.clone(),
        outstanding_requests.clone(),
        ws.clone(),
    );
    ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));

    let onmessage_callback = on_message_callback(
        outstanding_requests.clone(),
        custom_api_callback.clone(),
        subscribers.clone(),
    );
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));

    let onerror_callback =
        on_error_callback(ws.clone(), initial_request.clone(), shutdown_sender.clone());
    ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));

    let onclose_callback = on_close_callback(
        url.clone(),
        request_receiver.clone(),
        shutdown_sender.clone(),
        ws.clone(),
        initial_request.clone(),
        custom_api_callback.clone(),
        subscribers.clone(),
    );
    ws.set_onclose(Some(onclose_callback.as_ref().unchecked_ref()));
}

fn forward_request_with_shutdown<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    request_receiver: flume::Receiver<PendingRequest<R, O>>,
    shutdown_receiver: flume::Receiver<()>,
    request_sender: flume::Sender<PendingRequest<R, O>>,
) {
    wasm_bindgen_futures::spawn_local(async move {
        let mut receive_request = Box::pin(request_receiver.recv_async());
        let mut receive_shutdown = Box::pin(shutdown_receiver.recv_async());
        loop {
            let res = futures::select! {
                request = receive_request => request,
                _ = receive_shutdown => Err(flume::RecvError::Disconnected)
            };
            if let Ok(request) = res {
                if request_sender.send(request).is_err() {
                    break;
                }
            } else {
                break;
            }
        }
    });
}

fn on_open_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    request_receiver: Receiver<PendingRequest<R, O>>,
    initial_request: Arc<Mutex<Option<PendingRequest<R, O>>>>,
    requests: OutstandingRequestMapHandle<R, O>,
    ws: WebSocket,
) -> JsValue {
    Closure::once_into_js(move || {
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(initial_request) = take_initial_request(initial_request) {
                if send_request(&ws, initial_request, &requests).await {
                    while let Ok(pending) = request_receiver.recv_async().await {
                        if !send_request(&ws, pending, &requests).await {
                            break;
                        }
                    }
                }
            }

            drop(ws.close());
            drop(ws);
        });
    })
}

#[must_use]
async fn send_request<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    ws: &WebSocket,
    pending: PendingRequest<R, O>,
    requests: &OutstandingRequestMapHandle<R, O>,
) -> bool {
    let mut outstanding_requests = requests.lock().await;
    let bytes = match bincode::serialize(&pending.request) {
        Ok(bytes) => bytes,
        Err(err) => {
            drop(pending.responder.send(Err(Error::from(err))));
            // Despite not sending, this error was handled, so we report
            // success.
            return true;
        }
    };
    match ws.send_with_u8_array(&bytes) {
        Ok(_) => {
            outstanding_requests.insert(
                pending.request.id.expect("all requests must have ids"),
                pending,
            );
            true
        }
        Err(err) => {
            drop(
                pending
                    .responder
                    .send(Err(Error::from(WebSocketError::from(err)))),
            );
            false
        }
    }
}

fn on_message_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    outstanding_requests: OutstandingRequestMapHandle<R, O>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    subscribers: SubscriberMap,
) -> JsValue {
    Closure::wrap(Box::new(move |e: MessageEvent| {
        // Handle difference Text/Binary,...
        if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let array = js_sys::Uint8Array::new(&abuf);
            let payload = match bincode::deserialize::<Payload<Response<O>>>(&array.to_vec()) {
                Ok(payload) => payload,
                Err(err) => {
                    log::error!("error deserializing response: {:?}", err);
                    return;
                }
            };

            let outstanding_requests = outstanding_requests.clone();
            let subscribers = subscribers.clone();
            let custom_api_callback = custom_api_callback.clone();
            wasm_bindgen_futures::spawn_local(async move {
                super::process_response_payload::<R, O>(
                    payload,
                    &outstanding_requests,
                    custom_api_callback.as_ref(),
                    &subscribers,
                )
                .await;
            });
        } else {
            log::warn!("Unexpected WebSocket message received: {:?}", e.data());
        }
    }) as Box<dyn FnMut(MessageEvent)>)
    .into_js_value()
}

fn on_error_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    ws: WebSocket,
    initial_request: Arc<Mutex<Option<PendingRequest<R, O>>>>,
    shutdown: flume::Sender<()>,
) -> JsValue {
    Closure::once_into_js(move |e: ErrorEvent| {
        ws.set_onerror(None);
        let _ = shutdown.send(());
        if let Some(initial_request) = take_initial_request(initial_request) {
            drop(
                initial_request
                    .responder
                    .send(Err(Error::from(WebSocketError(
                        e.error().as_string().unwrap_or_default(),
                    )))),
            );
        } else {
            log::error!(
                "websocket error '{}'",
                e.error().as_string().unwrap_or_default()
            );
        }

        ws.close().unwrap();
    })
}

fn take_initial_request<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    initial_request: Arc<Mutex<Option<PendingRequest<R, O>>>>,
) -> Option<PendingRequest<R, O>> {
    let mut initial_request = initial_request.lock().unwrap();
    initial_request.take()
}

fn on_close_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    shutdown: flume::Sender<()>,
    ws: WebSocket,
    initial_request: Arc<Mutex<Option<PendingRequest<R, O>>>>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<O>>>,
    subscribers: SubscriberMap,
) -> JsValue {
    Closure::once_into_js(move |c: CloseEvent| {
        let _ = shutdown.send(());
        ws.set_onclose(None);

        if let Some(initial_request) = take_initial_request(initial_request) {
            drop(
                initial_request
                    .responder
                    .send(Err(Error::from(WebSocketError(format!(
                        "connection closed ({}). Reason: {:?}",
                        c.code(),
                        c.reason()
                    ))))),
            );
        } else {
            log::error!("websocket closed ({}): {:?}", c.code(), c.reason());
        }

        spawn_client(
            url,
            request_receiver,
            custom_api_callback.clone(),
            subscribers,
        );
    })
}

#[derive(thiserror::Error, Debug)]
#[error("WebSocket error: {0}")]
pub struct WebSocketError(String);

impl From<JsValue> for WebSocketError {
    fn from(value: JsValue) -> Self {
        Self(if let Some(value) = value.as_string() {
            value
        } else if let Some(value) = value.as_f64() {
            value.to_string()
        } else if let Some(value) = value.as_bool() {
            value.to_string()
        } else if value.is_null() {
            String::from("(null)")
        } else {
            String::from("(undefined)")
        })
    }
}
