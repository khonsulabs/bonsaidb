#![allow(missing_docs)]

use std::path::Path;

use pliantdb_core::{schema::Schema, test_util::Basic};
use pliantdb_networking::ServerConnection;

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
