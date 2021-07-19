use std::{marker::PhantomData, sync::Arc};

use bonsaidb_core::custom_api::CustomApi;
#[cfg(not(target_arch = "wasm32"))]
use fabruic::Certificate;
use url::Url;

use crate::{client::CustomApiCallback, Client, Error};

/// Builds a new [`Client`] with custom settings.
#[must_use]
pub struct Builder<A: CustomApi = ()> {
    url: Url,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A::Response>>>,
    #[cfg(not(target_arch = "wasm32"))]
    certificate: Option<fabruic::Certificate>,
    _api: PhantomData<A>,
}

impl Builder<()> {
    /// Creates a new builder for a client connecting to `url`.
    pub(crate) fn new(url: Url) -> Self {
        Self {
            url,
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
            custom_api_callback: None,
            #[cfg(not(target_arch = "wasm32"))]
            certificate: self.certificate,
            _api: PhantomData::default(),
        }
    }

    /// Enables using a [`CustomApi`] with this client. `callback` will be
    /// invoked when custom API responses are received from the server.
    pub fn with_custom_api_callback<A: CustomApi, C: CustomApiCallback<A::Response>>(
        self,
        callback: C,
    ) -> Builder<A> {
        Builder {
            url: self.url,
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

    /// Finishes building the client.
    pub async fn finish(self) -> Result<Client<A>, Error> {
        Client::<A>::new_from_parts(
            self.url,
            self.custom_api_callback,
            #[cfg(not(target_arch = "wasm32"))]
            self.certificate,
        )
        .await
    }
}
