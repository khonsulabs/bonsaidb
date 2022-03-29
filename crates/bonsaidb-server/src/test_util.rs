#![allow(missing_docs)]

use std::path::Path;

use bonsaidb_core::{connection::AsyncStorageConnection, test_util::BasicSchema};
use bonsaidb_local::config::Builder;

use crate::{config::DefaultPermissions, BackendError, Server, ServerConfiguration};

pub const BASIC_SERVER_NAME: &str = "basic-server";

#[cfg_attr(not(feature = "compression"), allow(unused_mut))]
pub async fn initialize_basic_server(path: &Path) -> Result<Server, BackendError> {
    let mut config = ServerConfiguration::new(path)
        .server_name(BASIC_SERVER_NAME)
        .default_permissions(DefaultPermissions::AllowAll)
        .with_schema::<BasicSchema>()?;
    #[cfg(feature = "compression")]
    {
        config = config.default_compression(bonsaidb_local::config::Compression::Lz4);
    }
    let server = Server::open(config).await?;
    assert_eq!(server.primary_domain(), BASIC_SERVER_NAME);

    server.install_self_signed_certificate(false).await?;

    server
        .create_database::<BasicSchema>("tests", false)
        .await?;

    Ok(server)
}
