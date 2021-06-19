use std::sync::{
    atomic::{AtomicI32, Ordering},
    Arc,
};

use flume::Receiver;
use pliantdb_core::networking::{Payload, Response};
use serde::{Deserialize, Serialize};
use url::Url;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{CloseEvent, ErrorEvent, MessageEvent, WebSocket};

#[cfg(feature = "pubsub")]
use crate::client::SubscriberMap;
use crate::{
    client::{OutstandingRequestMapHandle, PendingRequest},
    Error,
};

pub fn reconnecting_client_loop<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Url,
    request_receiver: Receiver<PendingRequest<R, O>>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    spawn_reconnecting_websocket(
        Arc::new(url),
        request_receiver,
        Arc::default(),
        #[cfg(feature = "pubsub")]
        subscribers,
    )
}

fn respawn_reconnecting_websocket<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    reconnect_delay: Arc<AtomicI32>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) {
    let delay_in_ms = reconnect_delay
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |existing_delay| {
            Some(existing_delay.clamp(50, 30000) * 2)
        })
        .unwrap();
    log::info!("Respawning with {} delay", delay_in_ms);
    let window = web_sys::window().expect("no window");
    window
        .set_timeout_with_callback_and_timeout_and_arguments_0(
            Closure::once_into_js(move || {
                spawn_reconnecting_websocket(url, request_receiver, reconnect_delay, subscribers)
                    .unwrap()
            })
            .as_ref()
            .unchecked_ref(),
            delay_in_ms,
        )
        .unwrap();
}

fn spawn_reconnecting_websocket<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    reconnect_delay: Arc<AtomicI32>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> Result<(), Error> {
    // In wasm we're not going to have a real loop. We're going create a websocket and store it in JS. This will allow us to get around Send/Sync issues since each access of the websocket can pull it from js.
    log::info!("spawning");
    let ws = match WebSocket::new(&url.to_string()) {
        Ok(ws) => ws,
        Err(err) => {
            log::info!("Error connecting");
            respawn_reconnecting_websocket(
                url,
                request_receiver,
                reconnect_delay,
                #[cfg(feature = "pubsub")]
                subscribers,
            );
            return Err(Error::from(WebSocketError::from(err)));
        }
    };
    let onerror_callback = on_error_callback(ws.clone());
    ws.set_onerror(Some(onerror_callback.as_ref().unchecked_ref()));
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    let outstanding_requests = OutstandingRequestMapHandle::default();
    let onmessage_callback = on_message_callback(
        outstanding_requests.clone(),
        #[cfg(feature = "pubsub")]
        subscribers.clone(),
    );

    let onopen_callback = on_open_callback(
        url.clone(),
        request_receiver.clone(),
        reconnect_delay.clone(),
        outstanding_requests.clone(),
        ws.clone(),
        #[cfg(feature = "pubsub")]
        subscribers.clone(),
    );

    let onclose_callback = on_close_callback(
        url.clone(),
        request_receiver.clone(),
        reconnect_delay.clone(),
        ws.clone(),
        #[cfg(feature = "pubsub")]
        subscribers.clone(),
    );

    ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));
    ws.set_onclose(Some(onclose_callback.as_ref().unchecked_ref()));

    let window = web_sys::window().unwrap();
    js_sys::Reflect::set(&window, &JsValue::symbol(Some("pliantdb_websocket")), &ws).unwrap();

    Ok(())
}

fn on_open_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    reconnect_delay: Arc<AtomicI32>,
    requests: OutstandingRequestMapHandle<R, O>,
    ws: WebSocket,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> JsValue {
    Closure::wrap(Box::new(move |_| {
        log::info!("Opened!");
        // Upon success connecting, reset the reconnect delay.
        reconnect_delay.store(0, Ordering::SeqCst);

        let closure_ws = ws.clone();
        let request_receiver = request_receiver.clone();
        let requests = requests.clone();
        let reconnect_delay = reconnect_delay.clone();
        let url = url.clone();
        #[cfg(feature = "pubsub")]
        let subscribers = subscribers.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Ok(pending) = request_receiver.recv_async().await {
                let mut outstanding_requests = requests.lock().await;
                let bytes = match bincode::serialize(&pending.request) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        drop(pending.responder.send(Err(Error::from(err))));
                        continue;
                    }
                };
                match closure_ws.send_with_u8_array(&bytes) {
                    Ok(_) => {
                        outstanding_requests.insert(
                            pending.request.id.expect("all requests must have ids"),
                            pending,
                        );
                    }
                    Err(err) => {
                        drop(
                            pending
                                .responder
                                .send(Err(Error::from(WebSocketError::from(err)))),
                        );
                        break;
                    }
                }
            }

            drop(closure_ws.close());
            respawn_reconnecting_websocket(
                url,
                request_receiver,
                reconnect_delay,
                #[cfg(feature = "pubsub")]
                subscribers,
            );
        });
    }) as Box<dyn FnMut(JsValue)>)
    .into_js_value()
}

fn on_message_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    outstanding_requests: OutstandingRequestMapHandle<R, O>,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
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
            #[cfg(feature = "pubsub")]
            let subscribers = subscribers.clone();
            wasm_bindgen_futures::spawn_local(async move {
                super::process_response_payload::<R, O>(
                    payload,
                    &outstanding_requests,
                    #[cfg(feature = "pubsub")]
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

fn on_error_callback(ws: WebSocket) -> JsValue {
    Closure::once_into_js(move |e: ErrorEvent| {
        log::error!(
            "websocket error '{}'",
            e.error().as_string().unwrap_or_default()
        );
        ws.set_onerror(None);

        ws.close().unwrap();
    })
}

fn on_close_callback<
    R: Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
    O: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
>(
    url: Arc<Url>,
    request_receiver: Receiver<PendingRequest<R, O>>,
    reconnect_delay: Arc<AtomicI32>,
    ws: WebSocket,
    #[cfg(feature = "pubsub")] subscribers: SubscriberMap,
) -> JsValue {
    Closure::once_into_js(move |c: CloseEvent| {
        log::error!("websocket closed ({}): {:?}", c.code(), c.reason());
        ws.set_onclose(None);

        respawn_reconnecting_websocket(
            url,
            request_receiver,
            reconnect_delay,
            #[cfg(feature = "pubsub")]
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
