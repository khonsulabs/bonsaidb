//! Tests invoking an API defined in a custom backend.

use bonsaidb::client::url::Url;
use bonsaidb::client::AsyncClient;
use bonsaidb::core::api::{Api, Infallible};
use bonsaidb::core::async_trait::async_trait;
use bonsaidb::core::test_util::{Basic, TestDirectory};
use bonsaidb::local::config::Builder;
use bonsaidb::server::api::Handler;
use bonsaidb::server::{Backend, CustomServer, DefaultPermissions, ServerConfiguration};
use bonsaidb_core::api::ApiName;
use bonsaidb_core::schema::Qualified;
use bonsaidb_server::api::{HandlerResult, HandlerSession};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default)]
struct CustomBackend;

impl Backend for CustomBackend {
    type ClientData = u64;
    type Error = Infallible;
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

    let client = AsyncClient::build(Url::parse("bonsaidb://localhost:12346")?)
        .with_api::<SetValue>()
        .with_certificate(certificate)
        .build()?;

    let old_value = client.send_api_request(&SetValue { new_value: 1 }).await?;
    assert_eq!(old_value, None);
    let old_value = client.send_api_request(&SetValue { new_value: 2 }).await?;
    assert_eq!(old_value, Some(1));

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
struct SetValue {
    new_value: u64,
}

impl Api for SetValue {
    type Error = Infallible;
    type Response = Option<u64>;

    fn name() -> ApiName {
        ApiName::private("set-value")
    }
}

#[derive(Debug)]
struct SetValueHandler;

#[async_trait]
impl Handler<SetValue, CustomBackend> for SetValueHandler {
    async fn handle(
        session: HandlerSession<'_, CustomBackend>,
        request: SetValue,
    ) -> HandlerResult<SetValue> {
        let mut data = session.client.client_data().await;
        let existing_value = data.replace(request.new_value);
        Ok(existing_value)
    }
}
