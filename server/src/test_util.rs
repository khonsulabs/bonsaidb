#![allow(missing_docs)]

use std::path::Path;

use pliantdb_core::{networking::ServerConnection, schema::Schema, test_util::Basic};

use crate::{Configuration, Server};

pub const BASIC_SERVER_NAME: &str = "basic-server";

pub async fn initialize_basic_server(path: &Path) -> anyhow::Result<Server> {
    let server = Server::open(path, Configuration::default()).await?;
    server.register_schema::<Basic>().await?;
    server
        .install_self_signed_certificate(BASIC_SERVER_NAME, false)
        .await?;

    server
        .create_database("tests", Basic::schema_name()?)
        .await?;

    Ok(server)
}
