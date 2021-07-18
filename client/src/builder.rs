use std::{marker::PhantomData, sync::Arc};

use bonsaidb_core::custom_api::CustomApi;
use fabruic::Certificate;
use url::Url;

use crate::{client::CustomApiCallback, Client, Error};

/// Builds a new [`Client`] with custom settings.
#[must_use]
pub struct Builder<A: CustomApi = ()> {
    url: Url,
    certificate: Option<fabruic::Certificate>,
    custom_api_callback: Option<Arc<dyn CustomApiCallback<A::Response>>>,
    _api: PhantomData<A>,
}

impl Builder<()> {
    /// Creates a new builder for a client connecting to `url`.
    pub(crate) fn new(url: Url) -> Self {
        Self {
            url,
            certificate: None,
            custom_api_callback: None,
            _api: PhantomData::default(),
        }
    }

    /// Enables using a [`CustomApi`] with this client. If you want to receive
    /// out-of-band API requests, set a callback using
    /// `with_custom_api_callback` instead.
    pub fn with_custom_api<A: CustomApi>(self) -> Builder<A> {
        Builder {
            url: self.url,
            certificate: self.certificate,
            custom_api_callback: None,
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
            certificate: self.certificate,
            custom_api_callback: Some(Arc::new(callback)),
            _api: PhantomData::default(),
        }
    }
}

impl<A: CustomApi> Builder<A> {
    /// Connects to a server using a pinned `certificate`. Only supported with `BonsaiDb` protocol-based connections.
    pub fn with_certificate(mut self, certificate: Certificate) -> Self {
        self.certificate = Some(certificate);
        self
    }

    /// Finishes building the client.
    pub async fn finish(self) -> Result<Client<A>, Error> {
        Client::<A>::new_with_certificate(self.url, self.certificate, self.custom_api_callback)
            .await
    }
}
