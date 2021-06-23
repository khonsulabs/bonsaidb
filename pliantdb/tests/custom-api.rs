//! Tests invoking an API defined in a custom backend.

use pliantdb::{
    client::{url::Url, Client},
    core::{
        custom_api::CustomApi,
        permissions::{Actionable, Dispatcher, Permissions},
        test_util::{Basic, TestDirectory},
    },
    server::{Backend, Configuration, CustomServer},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Dispatcher)]
#[dispatcher(input = CustomRequest)]
struct CustomBackend;

impl Backend for CustomBackend {
    type CustomApi = Self;

    type CustomApiDispatcher = Self;
}

impl CustomApi for CustomBackend {
    type Request = CustomRequest;

    type Response = CustomResponse;
}

#[derive(Serialize, Deserialize, Debug, Actionable)]
enum CustomRequest {
    #[actionable(protection = "none")]
    Ping,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum CustomResponse {
    Pong,
}

#[tokio::test]
async fn custom_api() -> anyhow::Result<()> {
    let dir = TestDirectory::new("custom_api.pliantdb");
    let server = CustomServer::<CustomBackend>::open(
        dir.as_ref(),
        Configuration {
            default_permissions: Permissions::allow_all(),
            ..Configuration::default()
        },
    )
    .await?;
    server.set_custom_api_dispatcher(CustomBackend).await;
    server
        .install_self_signed_certificate("test", false)
        .await?;
    let certificate = server.certificate().await?;
    server.register_schema::<Basic>().await?;
    tokio::spawn(async move { server.listen_on(12346).await });

    let client = Client::<CustomBackend>::new_with_certificate(
        Url::parse("pliantdb://localhost:12346")?,
        Some(certificate),
    )
    .await?;

    let CustomResponse::Pong = client.send_api_request(CustomRequest::Ping).await?;

    Ok(())
}

impl CustomRequestDispatcher for CustomBackend {
    type Output = CustomResponse;
    type Error = anyhow::Error;
}

#[actionable::async_trait]
impl PingHandler for CustomBackend {
    async fn handle(&self, _permissions: &Permissions) -> Result<CustomResponse, anyhow::Error> {
        Ok(CustomResponse::Pong)
    }
}
