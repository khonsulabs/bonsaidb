#![allow(missing_docs)]

use std::path::Path;

use bonsaidb_core::{connection::StorageConnection, test_util::BasicSchema};
use bonsaidb_local::config::Builder;

use crate::{config::DefaultPermissions, Error, Server, ServerConfiguration};

pub const BASIC_SERVER_NAME: &str = "basic-server";

pub async fn initialize_basic_server(path: &Path) -> Result<Server, Error> {
    let server = Server::open(
        ServerConfiguration::new(path)
            .server_name(BASIC_SERVER_NAME)
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<BasicSchema>()?,
    )
    .await?;
    server.install_self_signed_certificate(false).await?;

    server
        .create_database::<BasicSchema>("tests", false)
        .await?;

    Ok(server)
}
