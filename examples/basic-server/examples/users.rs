//! Shows basic usage of users and permissions.

use std::time::Duration;

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        admin::PermissionGroup,
        connection::{AsyncStorageConnection, Authentication, SensitiveString},
        permissions::{
            bonsai::{AuthenticationMethod, BonsaiAction, ServerAction},
            Permissions, Statement,
        },
        schema::{InsertError, SerializedCollection},
    },
    local::config::Builder,
    server::{Server, ServerConfiguration},
};

mod support;
use support::schema::Shape;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    drop(env_logger::try_init());
    let server = setup_server().await?;

    // Create a database user, or get its ID if it already existed.
    let user_id = match server.create_user("ecton").await {
        Ok(id) => {
            // Set the user's password.
            server
                .set_user_password("ecton", SensitiveString("hunter2".to_string()))
                .await?;

            id
        }
        Err(bonsaidb::core::Error::UniqueKeyViolation {
            existing_document, ..
        }) => existing_document.id.deserialize()?,
        Err(other) => anyhow::bail!(other),
    };

    // Create an administrators permission group, or get its ID if it already existed.
    let admin = server.admin().await;
    let administrator_group_id = match (PermissionGroup {
        name: String::from("administrators"),
        statements: vec![Statement::allow_all_for_any_resource()],
    }
    .push_into_async(&admin)
    .await)
    {
        Ok(doc) => doc.header.id,
        Err(InsertError {
            error:
                bonsaidb::core::Error::UniqueKeyViolation {
                    existing_document, ..
                },
            ..
        }) => existing_document.id.deserialize()?,
        Err(other) => anyhow::bail!(other),
    };

    // Make our user a member of the administrators group.
    server
        .add_permission_group_to_user(user_id, administrator_group_id)
        .await?;

    // ANCHOR_END: setup

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let client = Client::build(Url::parse("bonsaidb://localhost")?)
        .with_certificate(
            server
                .certificate_chain()
                .await?
                .into_end_entity_certificate(),
        )
        .finish()?;
    let db = client.database::<Shape>("my-database").await?;

    // Before authenticating, inserting a shape shouldn't work.
    match Shape::new(3).push_into_async(&db).await {
        Err(InsertError {
            error: bonsaidb::core::Error::PermissionDenied(denied),
            ..
        }) => {
            log::info!(
                "Permission was correctly denied before logging in: {:?}",
                denied
            );
        }
        _ => unreachable!("permission shouldn't be allowed"),
    }

    // Now, log in and try again.
    let authenticated_client = client
        .authenticate(
            "ecton",
            Authentication::Password(SensitiveString(String::from("hunter2"))),
        )
        .await
        .unwrap();
    let db = authenticated_client
        .database::<Shape>("my-database")
        .await?;
    let shape_doc = Shape::new(3).push_into_async(&db).await?;
    log::info!("Successully inserted document {:?}", shape_doc);

    drop(db);
    drop(client);

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}

async fn setup_server() -> anyhow::Result<Server> {
    let server = Server::open(
        ServerConfiguration::new("users-server-data.bonsaidb")
            .default_permissions(Permissions::from(
                Statement::for_any()
                    .allowing(&BonsaiAction::Server(ServerAction::Connect))
                    .allowing(&BonsaiAction::Server(ServerAction::Authenticate(
                        AuthenticationMethod::PasswordHash,
                    ))),
            ))
            .with_schema::<Shape>()?,
    )
    .await?;
    if server.certificate_chain().await.is_err() {
        server.install_self_signed_certificate(true).await?;
    }
    server.create_database::<Shape>("my-database", true).await?;

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    Ok(server)
}

#[test]
fn runs() {
    main().unwrap();
    main().unwrap();
}
