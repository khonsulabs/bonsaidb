//! Shows basic setup of a custom api server.

use std::{path::Path, time::Duration};

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        actionable::{Actionable, Dispatcher, Permissions},
        async_trait::async_trait,
        custom_api::{CustomApi, Infallible},
    },
    server::{
        Backend, BackendError, Configuration, ConnectedClient, CustomApiDispatcher, CustomServer,
        DefaultPermissions,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct ExampleBackend;

#[derive(Debug)]
pub enum ExampleApi {}

// ANCHOR: server-traits
impl Backend for ExampleBackend {
    type CustomApi = ExampleApi;
    type CustomApiDispatcher = ExampleDispatcher;
    type ClientData = ();
}

#[derive(Debug, Dispatcher)]
#[dispatcher(input = Request, actionable = bonsaidb::core::actionable)]
pub struct ExampleDispatcher {
    server: CustomServer<ExampleBackend>,
}

impl CustomApiDispatcher<ExampleBackend> for ExampleDispatcher {
    fn new(
        server: &CustomServer<ExampleBackend>,
        _client: &ConnectedClient<ExampleBackend>,
    ) -> Self {
        Self {
            server: server.clone(),
        }
    }
}

#[async_trait]
impl RequestDispatcher for ExampleDispatcher {
    type Output = Response;
    type Error = BackendError<Infallible>;
}

#[async_trait]
impl PingHandler for ExampleDispatcher {
    async fn handle(
        &self,
        _permissions: &Permissions,
    ) -> Result<Response, BackendError<Infallible>> {
        Ok(Response::Pong)
    }
}
// ANCHOR_END: server-traits

// ANCHOR: api-types
#[derive(Serialize, Deserialize, Actionable, Debug)]
#[actionable(actionable = bonsaidb::core::actionable)]
pub enum Request {
    #[actionable(protection = "none")]
    Ping,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    Pong,
}

impl CustomApi for ExampleApi {
    type Request = Request;
    type Response = Response;
    type Error = Infallible;
}
// ANCHOR_END: api-types

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let server = CustomServer::<ExampleBackend>::open(
        Path::new("server-data.bonsaidb"),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            ..Default::default()
        },
    )
    .await?;
    if server.certificate_chain().await.is_err() {
        server.install_self_signed_certificate(true).await?;
    }
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();

    // If websockets are enabled, we'll also listen for websocket traffic. The
    // QUIC-based connection should be overall better to use than WebSockets,
    // but it's much easier to route WebSocket traffic across the internet.
    #[cfg(feature = "websockets")]
    {
        let server = server.clone();
        tokio::spawn(async move {
            server
                .listen_for_websockets_on("localhost:8080", false)
                .await
        });
    }

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // To allow this example to run both websockets and QUIC, we're going to gather the clients
    // into a collection and use join_all to wait until they finish.
    let mut tasks = Vec::new();
    #[cfg(feature = "websockets")]
    {
        // To connect over websockets, use the websocket scheme.
        tasks.push(ping_the_server(
            Client::build(Url::parse("ws://localhost:8080")?)
                .with_custom_api()
                .finish()
                .await?,
            "websockets",
        ));
    }

    // To connect over QUIC, use the bonsaidb scheme.
    tasks.push(ping_the_server(
        Client::build(Url::parse("bonsaidb://localhost")?)
            .with_custom_api()
            .with_certificate(certificate)
            .finish()
            .await?,
        "bonsaidb",
    ));

    // Wait for the clients to finish
    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}

// ANCHOR: api-call
async fn ping_the_server(
    client: Client<ExampleApi>,
    client_name: &str,
) -> Result<(), bonsaidb::core::Error> {
    match client.send_api_request(Request::Ping).await {
        Ok(Response::Pong) => {
            println!("Received Pong from server on {}", client_name);
        }
        other => println!(
            "Unexpected response from API call on {}: {:?}",
            client_name, other
        ),
    }

    Ok(())
}
// ANCHOR_END: api-call
