//! Shows basic setup of a custom api server.
//!
//! This example has a section in the User Guide: https://dev.bonsaidb.io/main/guide/about/access-models/custom-api-server.html

use std::time::Duration;

use bonsaidb::{
    client::{url::Url, ApiError, Client},
    core::{
        actionable::Permissions,
        api::{Api, ApiName, Infallible},
        async_trait::async_trait,
        connection::{
            AsyncStorageConnection, Authentication, AuthenticationMethod, SensitiveString,
        },
        keyvalue::AsyncKeyValue,
        permissions::{
            bonsai::{BonsaiAction, ServerAction},
            Action, Identifier, Statement,
        },
        schema::Qualified,
    },
    local::config::Builder,
    server::{
        api::{Handler, HandlerResult, HandlerSession},
        Backend, CustomServer, ServerConfiguration,
    },
};
use serde::{Deserialize, Serialize};

/// The `Backend` for the BonsaiDb server.
#[derive(Debug)]
pub struct ExampleBackend;

// ANCHOR: api-types
#[derive(Serialize, Deserialize, Debug)]
pub struct Ping;

impl Api for Ping {
    type Response = Pong;
    type Error = Infallible;

    fn name() -> ApiName {
        ApiName::private("ping")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Pong;

#[derive(Serialize, Deserialize, Debug)]
pub struct IncrementCounter {
    amount: u64,
}

impl Api for IncrementCounter {
    type Response = Counter;
    type Error = Infallible;

    fn name() -> ApiName {
        ApiName::private("increment")
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Counter(pub u64);
// ANCHOR_END: api-types

// ANCHOR: server-traits
impl Backend for ExampleBackend {
    type Error = Infallible;
    type ClientData = ();
}

/// Dispatches Requests and returns Responses.
#[derive(Debug)]
pub struct ExampleHandler;

/// The Request::Ping variant has `#[actionable(protection = "none")]`, which
/// causes `PingHandler` to be generated with a single method and no implicit
/// permission handling.
#[async_trait]
impl Handler<ExampleBackend, Ping> for ExampleHandler {
    async fn handle(
        _session: HandlerSession<'_, ExampleBackend>,
        _request: Ping,
    ) -> HandlerResult<Ping> {
        Ok(Pong)
    }
}
// ANCHOR_END: server-traits

// ANCHOR: permission-handles
/// The permissible actions that can be granted for this example api.
#[derive(Debug, Action)]
#[action(actionable = bonsaidb::core::actionable)]
pub enum ExampleActions {
    Increment,
    DoSomethingCustom,
}

pub async fn increment_counter<S: AsyncStorageConnection<Database = C>, C: AsyncKeyValue>(
    storage: &S,
    as_client: &S,
    amount: u64,
) -> Result<u64, bonsaidb::core::Error> {
    as_client.check_permission(&[Identifier::from("increment")], &ExampleActions::Increment)?;
    let database = storage.database::<()>("counter").await?;
    database.increment_key_by("counter", amount).await
}

#[async_trait]
impl Handler<ExampleBackend, IncrementCounter> for ExampleHandler {
    async fn handle(
        session: HandlerSession<'_, ExampleBackend>,
        request: IncrementCounter,
    ) -> HandlerResult<IncrementCounter> {
        Ok(Counter(
            increment_counter(session.server, &session.as_client, request.amount).await?,
        ))
    }
}
// ANCHOR_END: permission-handles

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    // ANCHOR: server-init
    let server = CustomServer::<ExampleBackend>::open(
        ServerConfiguration::new("custom-api.bonsaidb")
            .default_permissions(Permissions::from(
                Statement::for_any()
                    .allowing(&BonsaiAction::Server(ServerAction::Connect))
                    .allowing(&BonsaiAction::Server(ServerAction::Authenticate(
                        AuthenticationMethod::PasswordHash,
                    ))),
            ))
            .authenticated_permissions(Permissions::from(vec![
                Statement::for_any().allowing(&ExampleActions::Increment)
            ]))
            .with_api::<ExampleHandler, Ping>()?
            .with_api::<ExampleHandler, IncrementCounter>()?
            .with_schema::<()>()?,
    )
    .await?;
    // ANCHOR_END: server-init

    server.create_database::<()>("counter", true).await?;

    // Create a user to allow testing authenticated permissions
    match server.create_user("test-user").await {
        Ok(_) | Err(bonsaidb::core::Error::UniqueKeyViolation { .. }) => {}
        Err(other) => anyhow::bail!(other),
    }

    server
        .set_user_password("test-user", SensitiveString::from("hunter2"))
        .await?;

    if server.certificate_chain().await.is_err() {
        server.install_self_signed_certificate(true).await?;
    }
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();

    // If websockets are enabled, we'll also listen for websocket traffic.
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
        tasks.push(invoke_apis(
            Client::build(Url::parse("ws://localhost:8080")?).finish()?,
            "websockets",
        ));
    }

    // To connect over QUIC, use the bonsaidb scheme.
    tasks.push(invoke_apis(
        Client::build(Url::parse("bonsaidb://localhost")?)
            .with_certificate(certificate)
            .finish()?,
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
async fn invoke_apis(client: Client, client_name: &str) -> Result<(), bonsaidb::core::Error> {
    ping_the_server(&client, client_name).await?;

    // Calling DoSomethingSimple and DoSomethingCustom will check permissions, which our client currently doesn't have access to.
    assert!(matches!(
        client
            .send_api_request_async(&IncrementCounter { amount: 1 })
            .await,
        Err(ApiError::Client(bonsaidb::client::Error::Core(
            bonsaidb::core::Error::PermissionDenied(_)
        )))
    ));
    assert!(matches!(
        client
            .send_api_request_async(&IncrementCounter { amount: 1 })
            .await,
        Err(ApiError::Client(bonsaidb::client::Error::Core(
            bonsaidb::core::Error::PermissionDenied(_)
        )))
    ));

    // Now, let's authenticate and try calling the APIs that previously were denied permissions
    let authenticated_client = client
        .authenticate(Authentication::password(
            "test-user",
            SensitiveString(String::from("hunter2")),
        )?)
        .await
        .unwrap();
    assert!(matches!(
        authenticated_client
            .send_api_request_async(&IncrementCounter { amount: 1 })
            .await,
        Ok(Counter(_))
    ));
    assert!(matches!(
        authenticated_client
            .send_api_request_async(&IncrementCounter { amount: 1 })
            .await,
        Ok(Counter(_))
    ));

    Ok(())
}

// ANCHOR: api-call
async fn ping_the_server(client: &Client, client_name: &str) -> Result<(), bonsaidb::core::Error> {
    match client.send_api_request_async(&Ping).await {
        Ok(Pong) => {
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

#[test]
fn runs() {
    main().unwrap()
}
