use std::{borrow::Cow, path::Path};

use pliantdb_core::{
    self as core,
    schema::{Collection, Schema},
    test_util::{self, Basic, TestDirectory},
};
use pliantdb_networking::{DatabaseRequest, Request};

use crate::{hosted::Database, Error, Server};

async fn initialize_basic_server(path: &Path) -> Result<Server, Error> {
    let server = Server::open(path).await?;
    server.register_schema::<Basic>().await?;

    server.create_database("tests", Basic::schema_id()).await?;

    Ok(server)
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_test() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("simple-test");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    let db = server.database::<Basic>("tests").await?;
    test_util::store_retrieve_update_delete_tests(&db).await
}

struct TestHarness {
    _directory: TestDirectory,
    server: Server,
}

impl TestHarness {
    pub async fn new(name: &str) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(name);
        let server = initialize_basic_server(directory.as_ref()).await?;
        Ok(Self {
            _directory: directory,
            server,
        })
    }

    pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<Database<'a, 'b, Basic>> {
        let db = self.server.database::<Basic>("tests").await?;
        Ok(db)
    }
}

pliantdb_core::define_connection_test_suite!(TestHarness);

// TODO test creating/deleting databases/listing
// TODO test available schemas

#[tokio::test(flavor = "multi_thread")]
async fn handle_request_tests() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("handle-requests");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    assert!(matches!(
        server
            .handle_request(Request::Database {
                database: Cow::from("tests"),
                request: DatabaseRequest::Get {
                    collection: Basic::collection_id(),
                    id: 0
                }
            })
            .await,
        Err(Error::Core(core::Error::DocumentNotFound { .. }))
    ));

    Ok(())
}
