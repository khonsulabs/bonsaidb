use std::{collections::HashMap, sync::Arc};

use bonsaidb_core::{custom_api::CustomApi, networking::CURRENT_PROTOCOL_VERSION, schema::Name};
#[cfg(not(target_arch = "wasm32"))]
use fabruic::Certificate;
#[cfg(not(target_arch = "wasm32"))]
use tokio::runtime::Handle;
use url::Url;

use crate::{
    client::{AnyCustomApiCallback, CustomApiCallback},
    Client, Error,
};

/// Builds a new [`Client`] with custom settings.
#[must_use]
pub struct Builder {
    url: Url,
    protocol_version: &'static str,
    custom_apis: HashMap<Name, Option<Arc<dyn AnyCustomApiCallback>>>,
    #[cfg(not(target_arch = "wasm32"))]
    certificate: Option<fabruic::Certificate>,
    #[cfg(not(target_arch = "wasm32"))]
    tokio: Option<Handle>,
}

impl Builder {
    /// Creates a new builder for a client connecting to `url`.
    pub(crate) fn new(url: Url) -> Self {
        Self {
            url,
            protocol_version: CURRENT_PROTOCOL_VERSION,
            custom_apis: HashMap::new(),
            #[cfg(not(target_arch = "wasm32"))]
            certificate: None,
            #[cfg(not(target_arch = "wasm32"))]
            tokio: Handle::try_current().ok(),
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

    /// Enables using a [`CustomApi`] with this client. If you want to receive
    /// out-of-band API requests, set a callback using
    /// `with_custom_api_callback` instead.
    pub fn with_api<Api: CustomApi>(mut self) -> Self {
        self.custom_apis.insert(Api::name(), None);
        self
    }

    /// Enables using a [`CustomApi`] with this client. `callback` will be
    /// invoked when custom API responses are received from the server.
    pub fn with_api_callback<Api: CustomApi>(mut self, callback: CustomApiCallback<Api>) -> Self {
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

    /// Finishes building the client.
    pub fn finish(self) -> Result<Client, Error> {
        Client::new_from_parts(
            self.url,
            self.protocol_version,
            self.custom_apis,
            #[cfg(not(target_arch = "wasm32"))]
            self.certificate,
            #[cfg(not(target_arch = "wasm32"))]
            self.tokio,
        )
    }
}
