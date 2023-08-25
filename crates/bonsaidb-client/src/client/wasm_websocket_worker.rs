use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bonsaidb_core::api::ApiName;
use bonsaidb_core::networking::Payload;
use bonsaidb_utils::fast_async_lock;
use flume::Receiver;
use url::Url;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::client::{
    disconnect_pending_requests, AnyApiCallback, OutstandingRequestMapHandle, PendingRequest,
    SubscriberMap,
};
use crate::Error;

#[allow(clippy::too_many_arguments)]
pub fn spawn_client(
    url: Arc<Url>,
    protocol_version: &'static str,
    request_receiver: Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
    subscribers: SubscriberMap,
    connection_counter: Arc<AtomicU32>,
    pending_error: Option<Error>,
    connect_timeout: Duration,
) {
    wasm_bindgen_futures::spawn_local(create_websocket(
        url,
        protocol_version,
        request_receiver,
        custom_apis,
        subscribers,
        connection_counter,
        pending_error,
        connect_timeout,
    ));
}

#[allow(clippy::too_many_arguments)]
async fn create_websocket(
    url: Arc<Url>,
    protocol_version: &'static str,
    request_receiver: Receiver<PendingRequest>,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
    subscribers: SubscriberMap,
    connection_counter: Arc<AtomicU32>,
    pending_error: Option<Error>,
    connect_timeout: Duration,
) {
    subscribers.clear();

    // Receive the next/initial request when we are reconnecting.
    let Ok(mut initial_request) = request_receiver.recv_async().await else {
        return;
    };
    if let Some(error) = pending_error {
        drop(initial_request.responder.send(Err(error)));
        let Ok(next_request) = request_receiver.recv_async().await else {
            return;
        };
        initial_request = next_request;
    }

    connection_counter.fetch_add(1, Ordering::SeqCst);
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
                connection_counter,
                None,
                connect_timeout,
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

    let onmessage_callback = on_message_callback(outstanding_requests.clone(), custom_apis.clone());
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));

    let onerror_callback =
        on_error_callback(ws.clone(), initial_request.clone(), shutdown_sender.clone());
    ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));

    if let Some(window) = web_sys::window() {
        let _: Result<_, _> = window.set_timeout_with_callback_and_timeout_and_arguments_0(
            connect_timeout_callback(ws.clone(), initial_request.clone(), shutdown_sender.clone())
                .as_ref()
                .unchecked_ref(),
            connect_timeout.as_millis().try_into().unwrap_or(i32::MAX),
        );
    }

    let onclose_callback = on_close_callback(
        url.clone(),
        protocol_version,
        request_receiver.clone(),
        shutdown_sender,
        ws.clone(),
        initial_request,
        outstanding_requests,
        custom_apis.clone(),
        subscribers.clone(),
        connection_counter.clone(),
        connect_timeout,
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
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
) -> JsValue {
    Closure::wrap(Box::new(move |e: MessageEvent| {
        // Handle difference Text/Binary,...
        if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let array = js_sys::Uint8Array::new(&abuf);
            let payload = match bincode::deserialize::<Payload>(&array.to_vec()) {
                Ok(payload) => payload,
                Err(err) => {
                    log::error!("error deserializing response: {:?}", err);
                    return;
                }
            };

            let outstanding_requests = outstanding_requests.clone();
            let custom_apis = custom_apis.clone();
            wasm_bindgen_futures::spawn_local(async move {
                super::process_response_payload(payload, &outstanding_requests, &custom_apis).await;
            });
        } else {
            log::warn!("Unexpected WebSocket message received: {:?}", e.data());
        }
    }) as Box<dyn FnMut(MessageEvent)>)
    .into_js_value()
}

fn connect_timeout_callback(
    ws: WebSocket,
    initial_request: Arc<Mutex<Option<PendingRequest>>>,
    shutdown: flume::Sender<()>,
) -> JsValue {
    Closure::once_into_js(move || {
        // We only want to treat this as a timeout if the initial request hasn't
        // been processed yet.
        if let Some(initial_request) = take_initial_request(&initial_request) {
            ws.set_onerror(None);
            let _: Result<_, _> = shutdown.send(());
            drop(
                initial_request
                    .responder
                    .send(Err(Error::connect_timeout())),
            );
            ws.close().unwrap();
        }
    })
}

fn on_error_callback(
    ws: WebSocket,
    initial_request: Arc<Mutex<Option<PendingRequest>>>,
    shutdown: flume::Sender<()>,
) -> JsValue {
    Closure::once_into_js(move |e: ErrorEvent| {
        ws.set_onerror(None);
        let _: Result<_, _> = shutdown.send(());
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
    outstanding_requests: OutstandingRequestMapHandle,
    custom_apis: Arc<HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>>,
    subscribers: SubscriberMap,
    connection_counter: Arc<AtomicU32>,
    connect_timeout: Duration,
) -> JsValue {
    Closure::once_into_js(move |c: CloseEvent| {
        let _: Result<_, _> = shutdown.send(());
        ws.set_onclose(None);

        let mut pending_error = Some(Error::from(WebSocketError(format!(
            "connection closed ({}). Reason: {:?}",
            c.code(),
            c.reason()
        ))));

        if let Some(initial_request) = take_initial_request(&initial_request) {
            drop(
                initial_request
                    .responder
                    .send(Err(pending_error.take().expect("this is the first check"))),
            );
        }

        wasm_bindgen_futures::spawn_local(async move {
            disconnect_pending_requests(&outstanding_requests, &mut pending_error).await;

            spawn_client(
                url,
                protocol_version,
                request_receiver,
                custom_apis.clone(),
                subscribers,
                connection_counter,
                pending_error,
                connect_timeout,
            );
        });
    })
}

#[derive(thiserror::Error, Debug)]
#[error("WebSocket error: {0}")]
pub struct WebSocketError(String);

impl From<JsValue> for WebSocketError {
    fn from(value: JsValue) -> Self {
        Self(if value.is_object() {
            let obj: js_sys::Object = value.dyn_into().expect("just checked");
            let description = obj.to_string();
            js_sys::JSON::stringify(&description).map_or_else(
                |_| String::from("(object)"),
                |str| ToString::to_string(&str),
            )
        } else if let Some(value) = value.as_string() {
            value
        } else if let Some(value) = value.as_f64() {
            value.to_string()
        } else if let Some(value) = value.as_bool() {
            value.to_string()
        } else if value.is_null() {
            String::from("(null)")
        } else {
            String::new()
        })
    }
}
