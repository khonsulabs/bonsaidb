//! Tests invoking an API defined in a custom backend.

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        api::{Api, Infallible},
        async_trait::async_trait,
        test_util::{Basic, TestDirectory},
    },
    local::config::Builder,
    server::{
        api::CustomApiHandler, Backend, ConnectedClient, CustomServer, DefaultPermissions,
        ServerConfiguration,
    },
};
use bonsaidb_core::schema::{ApiName, Qualified};
use bonsaidb_server::api::DispatcherResult;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct CustomBackend;

impl Backend for CustomBackend {
    type ClientData = u64;
}

#[tokio::test]
async fn custom_api() -> anyhow::Result<()> {
    let dir = TestDirectory::new("custom_api.bonsaidb");
    let server = CustomServer::<CustomBackend>::open(
        ServerConfiguration::new(&dir)
            .default_permissions(DefaultPermissions::AllowAll)
            .with_api::<SetValueHandler, _>()?
            .with_schema::<Basic>()?,
    )
    .await?;
    server.install_self_signed_certificate(false).await?;
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();
    tokio::spawn(async move { server.listen_on(12346).await });

    let client = Client::build(Url::parse("bonsaidb://localhost:12346")?)
        .with_api::<SetValue>()
        .with_certificate(certificate)
        .finish()?;

    let old_value = client
        .send_api_request_async(&SetValue { new_value: 1 })
        .await?;
    assert_eq!(old_value, None);
    let old_value = client
        .send_api_request_async(&SetValue { new_value: 2 })
        .await?;
    assert_eq!(old_value, Some(1));

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct SetValue {
    new_value: u64,
}

impl Api for SetValue {
    type Response = Option<u64>;
    type Error = Infallible;

    fn name() -> ApiName {
        ApiName::private("set-value")
    }
}

#[derive(Debug)]
struct SetValueHandler;

#[async_trait]
impl CustomApiHandler<CustomBackend, SetValue> for SetValueHandler {
    async fn handle(
        _server: &CustomServer<CustomBackend>,
        client: &ConnectedClient<CustomBackend>,
        request: SetValue,
    ) -> DispatcherResult<SetValue> {
        let mut data = client.client_data().await;
        let existing_value = data.replace(request.new_value);
        Ok(existing_value)
    }
}
