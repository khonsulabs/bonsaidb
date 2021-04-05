use std::{collections::HashMap, path::Path};

use pliantdb_core::{
    schema::Schema,
    test_util::{self, Basic, TestDirectory},
};

use crate::{hosted::Database, Error, Server};

async fn initialize_basic_server(path: &Path) -> Result<Server, Error> {
    let mut schemas = HashMap::new();
    schemas.insert(Basic::schema_id(), Basic::schematic());
    let server = Server::open(path, schemas).await?;

    server.create_database("tests", Basic::schema_id()).await?;

    Ok(server)
}

#[tokio::test(flavor = "multi_thread")]
async fn simple_test() -> Result<(), anyhow::Error> {
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
