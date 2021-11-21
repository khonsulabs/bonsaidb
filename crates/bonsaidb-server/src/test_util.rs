#![allow(missing_docs)]

use std::path::Path;

use bonsaidb_core::{connection::StorageConnection, test_util::BasicSchema};

use crate::{config::DefaultPermissions, Configuration, Error, Server};

pub const BASIC_SERVER_NAME: &str = "basic-server";

pub async fn initialize_basic_server(path: &Path) -> Result<Server, Error> {
    let server = Server::open(
        path,
        Configuration {
            server_name: BASIC_SERVER_NAME.to_string(),
            default_permissions: DefaultPermissions::AllowAll,
            ..Configuration::default()
        },
    )
    .await?;
    server.register_schema::<BasicSchema>().await?;
    server.install_self_signed_certificate(false).await?;

    server
        .create_database::<BasicSchema>("tests", false)
        .await?;

    Ok(server)
}
