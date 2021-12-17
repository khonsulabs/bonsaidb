use std::{marker::PhantomData, sync::Arc};

use bonsaidb_core::{custom_api::CustomApi, networking::CURRENT_PROTOCOL_VERSION};
#[cfg(not(target_arch = "wasm32"))]
use fabruic::Certificate;
use url::Url;

use crate::{client::CustomApiCallback, Client, Error};

/// Builds a new [`Client`] with custom settings.
#[must_use]
pub struct Builder<A: CustomApi = ()> {
    url: Url,
    protocol_version: &'static [u8],
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A>>>,
    #[cfg(not(target_arch = "wasm32"))]
    certificate: Option<fabruic::Certificate>,
    _api: PhantomData<A>,
}

impl Builder<()> {
    /// Creates a new builder for a client connecting to `url`.
    pub(crate) fn new(url: Url) -> Self {
        Self {
            url,
            protocol_version: CURRENT_PROTOCOL_VERSION,
            custom_api_callback: None,
            #[cfg(not(target_arch = "wasm32"))]
            certificate: None,
            _api: PhantomData::default(),
        }
    }

    /// Enables using a [`CustomApi`] with this client. If you want to receive
    /// out-of-band API requests, set a callback using
    /// `with_custom_api_callback` instead.
    pub fn with_custom_api<A: CustomApi>(self) -> Builder<A> {
        Builder {
            url: self.url,
            protocol_version: self.protocol_version,
            custom_api_callback: None,
            #[cfg(not(target_arch = "wasm32"))]
            certificate: self.certificate,
            _api: PhantomData::default(),
        }
    }

    /// Enables using a [`CustomApi`] with this client. `callback` will be
    /// invoked when custom API responses are received from the server.
    pub fn with_custom_api_callback<A: CustomApi, C: CustomApiCallback<A>>(
        self,
        callback: C,
    ) -> Builder<A> {
        Builder {
            url: self.url,
            protocol_version: self.protocol_version,
            custom_api_callback: Some(Arc::new(callback)),
            #[cfg(not(target_arch = "wasm32"))]
            certificate: self.certificate,
            _api: PhantomData::default(),
        }
    }
}

impl<A: CustomApi> Builder<A> {
    /// Connects to a server using a pinned `certificate`. Only supported with `BonsaiDb` protocol-based connections.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_certificate(mut self, certificate: Certificate) -> Self {
        self.certificate = Some(certificate);
        self
    }

    /// Overrides the protocol version. Only for testing purposes.
    #[cfg(feature = "test-util")]
    pub fn with_protocol_version(mut self, version: &'static [u8]) -> Self {
        self.protocol_version = version;
        self
    }

    /// Finishes building the client.
    pub async fn finish(self) -> Result<Client<A>, Error<A::Error>> {
        Client::<A>::new_from_parts(
            self.url,
            self.protocol_version,
            self.custom_api_callback,
            #[cfg(not(target_arch = "wasm32"))]
            self.certificate,
        )
        .await
    }
}
