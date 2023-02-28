use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use bonsaidb_core::api;
use bonsaidb_core::api::ApiName;
use bonsaidb_core::networking::CURRENT_PROTOCOL_VERSION;
#[cfg(not(target_arch = "wasm32"))]
use fabruic::Certificate;
#[cfg(not(target_arch = "wasm32"))]
use tokio::runtime::Handle;
use url::Url;

use crate::client::{AnyApiCallback, ApiCallback};
#[cfg(not(target_arch = "wasm32"))]
use crate::BlockingClient;
use crate::{AsyncClient, Error};

pub struct Async;
#[cfg(not(target_arch = "wasm32"))]
pub struct Blocking;

/// Builds a new [`Client`] with custom settings.
#[must_use]
pub struct Builder<AsyncMode> {
    url: Url,
    protocol_version: &'static str,
    custom_apis: HashMap<ApiName, Option<Arc<dyn AnyApiCallback>>>,
    #[cfg(not(target_arch = "wasm32"))]
    certificate: Option<fabruic::Certificate>,
    #[cfg(not(target_arch = "wasm32"))]
    tokio: Option<Handle>,
    mode: PhantomData<AsyncMode>,
}

impl<AsyncMode> Builder<AsyncMode> {
    /// Creates a new builder for a client connecting to `url`.
    pub(crate) fn new(url: Url) -> Self {
        Self {
            url,
            protocol_version: CURRENT_PROTOCOL_VERSION,
            custom_apis: HashMap::new(),
            #[cfg(not(target_arch = "wasm32"))]
            certificate: None,
            #[cfg(not(target_arch = "wasm32"))]
            tokio: None,
            mode: PhantomData,
        }
    }

    /// Specifies the tokio runtime this client should use for its async tasks.
    /// If not specified, `Client` will try to acquire a handle via
    /// `tokio::runtime::Handle::try_current()`.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_runtime(mut self, handle: Handle) -> Self {
        self.tokio = Some(handle);
        self
    }

    /// Enables using a [`Api`](api::Api) with this client. If you want to
    /// receive out-of-band API requests, set a callback using
    /// `with_custom_api_callback` instead.
    pub fn with_api<Api: api::Api>(mut self) -> Self {
        self.custom_apis.insert(Api::name(), None);
        self
    }

    /// Enables using a [`Api`](api::Api) with this client. `callback` will be
    /// invoked when custom API responses are received from the server.
    pub fn with_api_callback<Api: api::Api>(mut self, callback: ApiCallback<Api>) -> Self {
        self.custom_apis
            .insert(Api::name(), Some(Arc::new(callback)));
        self
    }

    /// Connects to a server using a pinned `certificate`. Only supported with BonsaiDb protocol-based connections.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_certificate(mut self, certificate: Certificate) -> Self {
        self.certificate = Some(certificate);
        self
    }

    /// Overrides the protocol version. Only for testing purposes.
    #[cfg(feature = "test-util")]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_protocol_version(mut self, version: &'static str) -> Self {
        self.protocol_version = version;
        self
    }

    fn finish_internal(self) -> Result<AsyncClient, Error> {
        AsyncClient::new_from_parts(
            self.url,
            self.protocol_version,
            self.custom_apis,
            #[cfg(not(target_arch = "wasm32"))]
            self.certificate,
            #[cfg(not(target_arch = "wasm32"))]
            self.tokio.or_else(|| Handle::try_current().ok()),
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Builder<Blocking> {
    /// Finishes building the client for use in a blocking (not async) context.
    pub fn build(self) -> Result<BlockingClient, Error> {
        self.finish_internal().map(BlockingClient)
    }
}

impl Builder<Async> {
    /// Finishes building the client for use in a tokio async context.
    pub fn build(self) -> Result<AsyncClient, Error> {
        self.finish_internal()
    }
}
