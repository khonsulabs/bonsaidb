use pliantdb_core::test_util::{self, Basic, HarnessTest, TestDirectory};

use crate::{
    hosted::Database,
    test_util::{basic_server_connection_tests, initialize_basic_server},
    Server,
};

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
    pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(test.to_string());
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

#[tokio::test(flavor = "multi_thread")]
async fn basic_server_tests() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("handle-requests");
    let server = initialize_basic_server(test_dir.as_ref()).await?;

    basic_server_connection_tests(server).await
}
