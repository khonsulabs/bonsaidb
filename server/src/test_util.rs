#![allow(missing_docs)]

use std::path::Path;

use pliantdb_core::{
    networking::ServerConnection,
    schema::{self, Schema},
    test_util::Basic,
};

use crate::Server;

pub const BASIC_SERVER_NAME: &str = "basic-server";

pub async fn initialize_basic_server(path: &Path) -> anyhow::Result<Server> {
    let server = Server::open(path).await?;
    server.register_schema::<Basic>().await?;
    server
        .install_self_signed_certificate(BASIC_SERVER_NAME, false)
        .await?;

    server.create_database("tests", Basic::schema_id()).await?;

    Ok(server)
}

pub async fn basic_server_connection_tests<C: ServerConnection>(server: C) -> anyhow::Result<()> {
    let schemas = server.list_available_schemas().await?;
    assert_eq!(schemas, vec![Basic::schema_id()]);

    let databases = server.list_databases().await?;
    assert_eq!(
        databases,
        vec![pliantdb_core::networking::Database {
            name: String::from("tests"),
            schema: Basic::schema_id()
        }]
    );

    server
        .create_database("another-db", Basic::schema_id())
        .await?;
    server.delete_database("another-db").await?;

    assert!(matches!(
        server.delete_database("another-db").await,
        Err(pliantdb_core::Error::Networking(
            pliantdb_core::networking::Error::DatabaseNotFound(_)
        ))
    ));

    assert!(matches!(
        server.create_database("tests", Basic::schema_id()).await,
        Err(pliantdb_core::Error::Networking(
            pliantdb_core::networking::Error::DatabaseNameAlreadyTaken(_)
        ))
    ));

    assert!(matches!(
        server
            .create_database("|invalidname", Basic::schema_id())
            .await,
        Err(pliantdb_core::Error::Networking(
            pliantdb_core::networking::Error::InvalidDatabaseName(_)
        ))
    ));

    assert!(matches!(
        server
            .create_database("another-db", schema::Id::from("unknown schema"))
            .await,
        Err(pliantdb_core::Error::Networking(
            pliantdb_core::networking::Error::SchemaNotRegistered(_)
        ))
    ));

    Ok(())
}
