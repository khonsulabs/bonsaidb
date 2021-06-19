use flume::Receiver;
use pliantdb_core::networking::{Payload, Response};
use serde::{Deserialize, Serialize};
use url::Url;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use web_sys::{MessageEvent, WebSocket};

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
    // In wasm we're not going to have a real loop. We're going create a websocket and store it in JS. This will allow us to get around Send/Sync issues since each access of the websocket can pull it from js.
    let ws = WebSocket::new(&url.to_string()).map_err(WebSocketError::from)?;
    ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
    let outstanding_requests = OutstandingRequestMapHandle::default();
    #[cfg(feature = "pubsub")]
    let closure_subscribers = subscribers.clone();
    let closure_requests = outstanding_requests.clone();
    let onmessage_callback = Closure::wrap(Box::new(move |e: MessageEvent| {
        log::info!("received response");
        // Handle difference Text/Binary,...
        if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let array = js_sys::Uint8Array::new(&abuf);
            let payload = bincode::deserialize::<Payload<Response<O>>>(&array.to_vec())
                .expect("error parsing payload"); // TODO remove expect
            log::info!("received payload: {:?}", payload.id);

            let outstanding_requests = closure_requests.clone();
            #[cfg(feature = "pubsub")]
            let subscribers = closure_subscribers.clone();
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
            // console_log!("Unexpected websocket message: {:?}", e.data());
        }
    }) as Box<dyn FnMut(MessageEvent)>)
    .into_js_value();

    let closure_ws = ws.clone();
    let closure_requests = outstanding_requests.clone();
    let onopen_callback = Closure::wrap(Box::new(move |_| {
        log::info!("connected");
        let closure_ws = closure_ws.clone();
        let request_receiver = request_receiver.clone();
        let closure_requests = closure_requests.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Ok(pending) = request_receiver.recv_async().await {
                log::info!("received request");
                let mut outstanding_requests = closure_requests.lock().await;
                closure_ws
                    .send_with_u8_array(&bincode::serialize(&pending.request).unwrap())
                    .unwrap(); // TODO remove unwrap

                outstanding_requests.insert(
                    pending.request.id.expect("all requests must have ids"),
                    pending,
                );
            }
        });
    }) as Box<dyn FnMut(JsValue)>)
    .into_js_value();

    ws.set_onopen(Some(onopen_callback.as_ref().unchecked_ref()));
    ws.set_onmessage(Some(onmessage_callback.as_ref().unchecked_ref()));

    let window = web_sys::window().unwrap();
    js_sys::Reflect::set(&window, &JsValue::symbol(Some("pliantdb_websocket")), &ws).unwrap();

    Ok(())
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
