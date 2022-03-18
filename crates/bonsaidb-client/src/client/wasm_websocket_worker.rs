use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bonsaidb_core::{
    networking::{Payload, Response},
    schema::Name,
};
use bonsaidb_utils::fast_async_lock;
use flume::Receiver;
use url::Url;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::{
    client::{AnyCustomApiCallback, OutstandingRequestMapHandle, PendingRequest, SubscriberMap},
    Error,
};

pub fn spawn_client(
    url: Arc<Url>,
    protocol_version: &'static str,
    request_receiver: Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>>,
    subscribers: SubscriberMap,
) {
    wasm_bindgen_futures::spawn_local(create_websocket(
        url,
        protocol_version,
        request_receiver,
        custom_apis,
        subscribers,
    ));
}

async fn create_websocket(
    url: Arc<Url>,
    protocol_version: &'static str,
    request_receiver: Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>>,
    subscribers: SubscriberMap,
) {
    subscribers.clear().await;

    // Receive the next/initial request when we are reconnecting.
    let initial_request = match request_receiver.recv_async().await {
        Ok(r) => r,
        Err(_) => return,
    };

    // In wasm we're not going to have a real loop. We're going create a
    // websocket and store it in JS. This will allow us to get around Send/Sync
    // issues since each access of the websocket can pull it from js.
    let ws = match WebSocket::new_with_str(&url.to_string(), protocol_version) {
        Ok(ws) => ws,
        Err(err) => {
            drop(
                initial_request
                    .responder
                    .send(Err(Error::from(WebSocketError::from(err)))),
            );
            spawn_client(
                url,
                protocol_version,
                request_receiver,
                custom_apis.clone(),
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
        outstanding_requests,
        custom_apis.clone(),
        subscribers.clone(),
    );
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));

    let onerror_callback =
        on_error_callback(ws.clone(), initial_request.clone(), shutdown_sender.clone());
    ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));

    let onclose_callback = on_close_callback(
        url.clone(),
        protocol_version,
        request_receiver.clone(),
        shutdown_sender,
        ws.clone(),
        initial_request,
        custom_apis.clone(),
        subscribers.clone(),
    );
    ws.set_onclose(Some(onclose_callback.as_ref().unchecked_ref()));
}

#[allow(clippy::mut_mut)] // futures::select!
fn forward_request_with_shutdown(
    request_receiver: flume::Receiver<PendingRequest>,
    shutdown_receiver: flume::Receiver<()>,
    request_sender: flume::Sender<PendingRequest>,
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

fn on_open_callback(
    request_receiver: Receiver<PendingRequest>,
    initial_request: Arc<Mutex<Option<PendingRequest>>>,
    requests: OutstandingRequestMapHandle,
    ws: WebSocket,
) -> JsValue {
    Closure::once_into_js(move || {
        wasm_bindgen_futures::spawn_local(async move {
            if let Some(initial_request) = take_initial_request(&initial_request) {
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

#[allow(clippy::future_not_send)]
async fn send_request(
    ws: &WebSocket,
    pending: PendingRequest,
    requests: &OutstandingRequestMapHandle,
) -> bool {
    let mut outstanding_requests = fast_async_lock!(requests);
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

fn on_message_callback(
    outstanding_requests: OutstandingRequestMapHandle,
    custom_apis: Arc<HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>>,
    subscribers: SubscriberMap,
) -> JsValue {
    Closure::wrap(Box::new(move |e: MessageEvent| {
        // Handle difference Text/Binary,...
        if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let array = js_sys::Uint8Array::new(&abuf);
            let payload = match bincode::deserialize::<Payload<Response>>(&array.to_vec()) {
                Ok(payload) => payload,
                Err(err) => {
                    log::error!("error deserializing response: {:?}", err);
                    return;
                }
            };

            let outstanding_requests = outstanding_requests.clone();
            let subscribers = subscribers.clone();
            let custom_apis = custom_apis.clone();
            wasm_bindgen_futures::spawn_local(async move {
                super::process_response_payload(
                    payload,
                    &outstanding_requests,
                    &custom_apis,
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

fn on_error_callback(
    ws: WebSocket,
    initial_request: Arc<Mutex<Option<PendingRequest>>>,
    shutdown: flume::Sender<()>,
) -> JsValue {
    Closure::once_into_js(move |e: ErrorEvent| {
        ws.set_onerror(None);
        let _ = shutdown.send(());
        if let Some(initial_request) = take_initial_request(&initial_request) {
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

fn take_initial_request(initial_request: &Mutex<Option<PendingRequest>>) -> Option<PendingRequest> {
    let mut initial_request = initial_request.lock().unwrap();
    initial_request.take()
}

#[allow(clippy::too_many_arguments)]
fn on_close_callback(
    url: Arc<Url>,
    protocol_version: &'static str,
    request_receiver: Receiver<PendingRequest>,
    shutdown: flume::Sender<()>,
    ws: WebSocket,
    initial_request: Arc<Mutex<Option<PendingRequest>>>,
    custom_apis: Arc<HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>>,
    subscribers: SubscriberMap,
) -> JsValue {
    Closure::once_into_js(move |c: CloseEvent| {
        let _ = shutdown.send(());
        ws.set_onclose(None);

        if let Some(initial_request) = take_initial_request(&initial_request) {
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
            protocol_version,
            request_receiver,
            custom_apis.clone(),
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
