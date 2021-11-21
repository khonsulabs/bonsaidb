//! Shows basic usage of a ser resources: (), actions: () ver.

use std::{path::Path, time::Duration};

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        admin::PermissionGroup,
        connection::StorageConnection,
        document::KeyId,
        permissions::{
            bonsai::{BonsaiAction, ServerAction},
            Action, ActionNameList, Permissions, ResourceName, Statement,
        },
        schema::{Collection, InsertError},
    },
    server::{Configuration, DefaultPermissions, Server, StorageConfiguration},
};

mod support;
use support::schema::Shape;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let server = setup_server().await?;

    // Create a database user, or get its ID if it already existed.
    let user_id = match server.create_user("ecton").await {
        Ok(id) => {
            // Set the user's password. This uses OPAQUE to ensure the password
            // never leaves the machine that executes `set_user_password_str`.
            server.set_user_password_str("ecton", "hunter2").await?;

            id
        }
        Err(bonsaidb::core::Error::UniqueKeyViolation {
            existing_document, ..
        }) => existing_document.id,
        Err(other) => anyhow::bail!(other),
    };

    // Create an administrators permission group, or get its ID if it already existed.
    let admin = server.admin().await;
    let administrator_group_id = match (PermissionGroup {
        name: String::from("administrators"),
        statements: vec![Statement::allow_all()],
    }
    .insert_into(&admin)
    .await)
    {
        Ok(doc) => doc.header.id,
        Err(InsertError {
            error:
                bonsaidb::core::Error::UniqueKeyViolation {
                    existing_document, ..
                },
            ..
        }) => existing_document.id,
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
        .finish()
        .await?;
    let db = client.database::<Shape>("my-database").await?;

    // Before authenticating, inserting a shape shouldn't work.
    match Shape::new(3).insert_into(&db).await {
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
    client
        .login_with_password_str("ecton", "hunter2", None)
        .await?;
    let shape_doc = Shape::new(3).insert_into(&db).await?;
    log::info!("Successully inserted document {:?}", shape_doc);

    drop(db);
    drop(client);

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}

async fn setup_server() -> anyhow::Result<Server> {
    let server = Server::open(
        Path::new("users-server-data.bonsaidb"),
        Configuration {
            default_permissions: DefaultPermissions::Permissions(Permissions::from(vec![
                Statement {
                    resources: vec![ResourceName::any()],
                    actions: ActionNameList::List(vec![
                        BonsaiAction::Server(ServerAction::Connect).name(),
                        BonsaiAction::Server(ServerAction::LoginWithPassword).name(),
                    ]),
                },
            ])),
            storage: StorageConfiguration {
                default_encryption_key: Some(KeyId::Master),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?;
    if server.certificate_chain().await.is_err() {
        server.install_self_signed_certificate(true).await?;
    }
    server.register_schema::<Shape>().await?;
    server.create_database::<Shape>("my-database", true).await?;

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    Ok(server)
}
