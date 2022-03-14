//! Tests invoking an API defined in a custom backend.

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        async_trait::async_trait,
        custom_api::{CustomApi, Infallible},
        permissions::{Actionable, Dispatcher, Permissions},
        test_util::{Basic, TestDirectory},
    },
    local::config::Builder,
    server::{
        Backend, BackendError, ConnectedClient, CustomApiDispatcher, CustomServer,
        DefaultPermissions, ServerConfiguration,
    },
};
use bonsaidb_local::custom_api::CustomApiDispatcher;
use serde::{Deserialize, Serialize};

#[derive(Debug, Dispatcher)]
#[dispatcher(input = CustomRequest, actionable = bonsaidb::core::actionable)]
struct CustomBackend {
    client: ConnectedClient<Self>,
}

impl Backend for CustomBackend {
    type ClientData = u64;
}

impl CustomApiDispatcher for CustomBackend {
    type Api = Api;

    fn new(_server: &CustomServer<Self>, client: &ConnectedClient<Self>) -> Self {
        Self {
            client: client.clone(),
        }
    }
}

#[derive(Debug)]
pub struct Api;

impl CustomApi for Api {
    type Request = CustomRequest;
    type Response = CustomResponse;
    type Error = Infallible;
}

#[derive(Serialize, Deserialize, Debug, Actionable)]
#[actionable(actionable = bonsaidb_core::actionable)]
enum CustomRequest {
    #[actionable(protection = "none")]
    SetValue(u64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum CustomResponse {
    ExistingValue(Option<u64>),
}

#[tokio::test]
async fn custom_api() -> anyhow::Result<()> {
    let dir = TestDirectory::new("custom_api.bonsaidb");
    let server = CustomServer::<CustomBackend>::open(
        ServerConfiguration::new(&dir)
            .default_permissions(DefaultPermissions::AllowAll)
            .with_custom_api(Api)
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
        .with_custom_api::<CustomBackend>()
        .with_certificate(certificate)
        .finish()
        .await?;

    let CustomResponse::ExistingValue(old_data) =
        client.send_api_request(CustomRequest::SetValue(1)).await?;
    assert_eq!(old_data, None);
    let CustomResponse::ExistingValue(old_data) =
        client.send_api_request(CustomRequest::SetValue(2)).await?;
    assert_eq!(old_data, Some(1));

    Ok(())
}

impl CustomRequestDispatcher for CustomBackend {
    type Output = CustomResponse;
    type Error = BackendError<Infallible>;
}

#[async_trait]
impl SetValueHandler for CustomBackend {
    async fn handle(
        &self,
        _permissions: &Permissions,
        value: u64,
    ) -> Result<CustomResponse, BackendError<Infallible>> {
        let mut data = self.client.client_data().await;
        let existing_value = data.replace(value);
        Ok(CustomResponse::ExistingValue(existing_value))
    }
}
