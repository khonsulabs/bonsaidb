//! Shows basic usage of users and permissions.

use std::time::Duration;

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        admin::{PermissionGroup, Role},
        connection::{
            AsyncStorageConnection, Authentication, AuthenticationMethod, SensitiveString,
        },
        permissions::{
            bonsai::{BonsaiAction, DatabaseAction, DocumentAction, ServerAction},
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

    // This example shows off basic user authentication as well as the ability
    // to assume roles. The server will be configured to only allow connected
    // users the ability to authenticate. All other usage will result in
    // PermissionDenied errors.
    //
    // We will create a user that belongs to a PermissionGroup that gives it the
    // ability to insert and read documents, but not delete them. This example
    // will demonstrate how to authenticate as the user, and shows how
    // permission is denied before authentication but is allowed on the
    // authenticated client.
    //
    // The other bit of setup we are going to do is create an administrators
    // group that the user belongs to. This permission group will allow the user
    // to assume any identity (user or role) in BonsaiDb. We are going to show
    // how to use this to escalate privileges by creating a "superuser" Role,
    // which belongs to a "superusers" group that grants all privileges.
    //
    // This example will finish by using the authenticated client to assume the
    // Superuser role and delete the record we inserted. While this is a complex
    // setup, it is a powerful pattern in Role Based Access Control which can
    // help protect users from accidentally performing a dangerous operation.

    // Create a database user, or get its ID if it already existed.
    let user_id = match server.create_user("ecton").await {
        Ok(id) => {
            // Set the user's password.
            server
                .set_user_password("ecton", SensitiveString::from("hunter2"))
                .await?;

            id
        }
        Err(bonsaidb::core::Error::UniqueKeyViolation {
            existing_document, ..
        }) => existing_document.id.deserialize()?,
        Err(other) => anyhow::bail!(other),
    };

    // Create an basic-users permission group, or get its ID if it already existed.
    let admin = server.admin().await;
    let users_group_id = match (PermissionGroup {
        name: String::from("basic-users"),
        statements: vec![Statement::for_any()
            .allowing(&BonsaiAction::Database(DatabaseAction::Document(
                DocumentAction::Insert,
            )))
            .allowing(&BonsaiAction::Database(DatabaseAction::Document(
                DocumentAction::Get,
            )))],
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

    // Make our user a member of the basic-users group.
    server
        .add_permission_group_to_user(user_id, users_group_id)
        .await?;

    // Create an superusers group, which has all permissions
    let superusers_group_id = match (PermissionGroup {
        name: String::from("superusers"),
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

    let superuser_role_id = match (Role {
        name: String::from("superuser"),
        groups: vec![superusers_group_id],
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

    let administrators_group_id = match (PermissionGroup {
        name: String::from("administrators"),
        statements: vec![
            Statement::for_any().allowing(&BonsaiAction::Server(ServerAction::AssumeIdentity))
        ],
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
        .add_permission_group_to_user(user_id, administrators_group_id)
        .await?;

    // Give a moment for the listeners to start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    {
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
            .authenticate(Authentication::password(
                "ecton",
                SensitiveString(String::from("hunter2")),
            )?)
            .await?;

        let db = authenticated_client
            .database::<Shape>("my-database")
            .await?;
        let shape_doc = Shape::new(3).push_into_async(&db).await?;
        println!("Successully inserted document {:?}", shape_doc);

        // The "basic-users" group and "administrators" groups do not give
        // permission to delete documents:
        assert!(matches!(
            shape_doc.delete_async(&db).await.unwrap_err(),
            bonsaidb::core::Error::PermissionDenied { .. }
        ));

        // But we can assume the Superuser role to delete the document:
        let as_superuser =
            Role::assume_identity_async(superuser_role_id, &authenticated_client).await?;
        shape_doc
            .delete_async(&as_superuser.database::<Shape>("my-database").await?)
            .await?;
    }

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
