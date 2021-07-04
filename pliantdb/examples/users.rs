//! Shows basic usage of a ser resources: (), actions: () ver.

use std::{path::Path, time::Duration};

use pliantdb::{
    client::{url::Url, Client},
    core::{
        connection::ServerConnection,
        document::KeyId,
        permissions::{
            pliant::{PliantAction, ServerAction},
            Action, ActionNameList, Permissions, ResourceName, Statement,
        },
        schema::Collection,
        Error,
    },
    server::{
        admin::{PermissionGroup, User},
        Configuration, Server, StorageConfiguration,
    },
};

mod support;
use support::schema::Shape;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = setup_server().await?;

    // Create a database user, or get its ID if it already existed.
    let user_id = match server.create_user("ecton").await {
        Ok(id) => {
            // Set the user's password. This uses OPAQUE to ensure the password never leaves the machine that executes `set_user_password_str`.
            server.set_user_password_str("ecton", "hunter2").await?;

            id
        }
        Err(pliantdb_core::Error::UniqueKeyViolation {
            existing_document_id,
            ..
        }) => existing_document_id,
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
        Err(pliantdb_core::Error::UniqueKeyViolation {
            existing_document_id,
            ..
        }) => existing_document_id,
        Err(other) => anyhow::bail!(other),
    };

    // Make our user a member of the administrators group.
    let mut ecton_doc = User::get(user_id, &admin).await?.unwrap();
    if !ecton_doc.contents.groups.contains(&administrator_group_id) {
        ecton_doc.contents.groups.push(administrator_group_id);
        ecton_doc.update(&admin).await?;
    }
    // ANCHOR_END: setup

    // Spawn our QUIC-based protocol listener.
    let task_server = server.clone();
    tokio::spawn(async move { task_server.listen_on(5645).await });

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let client = Client::<()>::new_with_certificate(
        Url::parse("pliantdb://localhost")?,
        Some(server.certificate().await?),
    )
    .await?;
    let db = client.database::<Shape>("my-database").await?;

    // Before authenticating, inserting a shape shouldn't work.
    match Shape::new(3).insert_into(&db).await {
        Err(pliantdb_core::Error::PermissionDenied(denied)) => {
            println!(
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
    println!("Successully inserted document {:?}", shape_doc);

    drop(db);
    drop(client);

    // Shut the server down gracefully (or forcefully after 5 seconds).
    server.shutdown(Some(Duration::from_secs(5))).await?;

    Ok(())
}

async fn setup_server() -> anyhow::Result<Server> {
    let server = Server::open(
        Path::new("users-server-data.pliantdb"),
        Configuration {
            default_permissions: Permissions::from(vec![Statement {
                resources: vec![ResourceName::any()],
                actions: ActionNameList::List(vec![PliantAction::Server(
                    ServerAction::LoginWithPassword,
                )
                .name()]),
            }]),
            storage: StorageConfiguration {
                default_encryption_key: Some(KeyId::Master),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?;
    if server.certificate().await.is_err() {
        server
            .install_self_signed_certificate("example-server", true)
            .await?;
    }
    server.register_schema::<Shape>().await?;
    match server.create_database::<Shape>("my-database").await {
        Ok(()) => {}
        Err(Error::DatabaseNameAlreadyTaken(_)) => {}
        Err(err) => panic!(
            "Unexpected error from server during create_database: {:?}",
            err
        ),
    }
    Ok(server)
}