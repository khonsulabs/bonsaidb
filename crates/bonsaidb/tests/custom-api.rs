//! Tests invoking an API defined in a custom backend.

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        custom_api::CustomApi,
        permissions::{Actionable, Dispatcher, Permissions},
        test_util::{Basic, TestDirectory},
    },
    server::{
        Backend, BackendError, Configuration, ConnectedClient, CustomServer, DefaultPermissions,
    },
};
use bonsaidb_core::custom_api::Infallible;
use serde::{Deserialize, Serialize};

#[derive(Debug, Dispatcher)]
#[dispatcher(input = CustomRequest)]
struct CustomBackend {
    client: ConnectedClient<Self>,
}

impl Backend for CustomBackend {
    type CustomApi = Self;
    type CustomApiDispatcher = Self;
    type ClientData = u64;

    fn dispatcher_for(
        _server: &CustomServer<Self>,
        client: &ConnectedClient<Self>,
    ) -> Self::CustomApiDispatcher {
        CustomBackend {
            client: client.clone(),
        }
    }
}

impl CustomApi for CustomBackend {
    type Request = CustomRequest;
    type Response = CustomResponse;
    type Error = Infallible;
}

#[derive(Serialize, Deserialize, Debug, Actionable)]
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
        dir.as_ref(),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            ..Configuration::default()
        },
    )
    .await?;
    server.install_self_signed_certificate(false).await?;
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();
    server.register_schema::<Basic>().await?;
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

#[actionable::async_trait]
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
