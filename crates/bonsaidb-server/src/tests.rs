use actionable::{Permissions, Statement};
use bonsaidb_core::test_util::{self, BasicSchema, HarnessTest, TestDirectory};

use crate::{server::ServerDatabase, test_util::initialize_basic_server, Server};

#[tokio::test]
async fn simple_test() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("simple-test");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    let db = server.database::<BasicSchema>("tests").await?;
    test_util::store_retrieve_update_delete_tests(&db).await
}

struct TestHarness {
    _directory: TestDirectory,
    server: Server,
}

impl TestHarness {
    pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(format!("server-{}", test));
        let server = initialize_basic_server(directory.as_ref()).await?;
        Ok(Self {
            _directory: directory,
            server,
        })
    }

    pub const fn server_name() -> &'static str {
        "server"
    }

    pub const fn server(&self) -> &'_ Server {
        &self.server
    }

    pub async fn connect(&self) -> anyhow::Result<ServerDatabase<'_, (), BasicSchema>> {
        let db = self.server.database::<BasicSchema>("tests").await?;
        Ok(db)
    }

    #[allow(dead_code)]
    async fn connect_with_permissions(
        &self,
        permissions: Vec<Statement>,
        _label: &str,
    ) -> anyhow::Result<ServerDatabase<'_, (), BasicSchema>> {
        let mut db = self.connect().await?;
        db.db = db
            .db
            .with_effective_permissions(Permissions::from(permissions));
        Ok(db)
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.server.shutdown(None).await?;
        Ok(())
    }
}

bonsaidb_core::define_connection_test_suite!(TestHarness);
bonsaidb_core::define_pubsub_test_suite!(TestHarness);
bonsaidb_core::define_kv_test_suite!(TestHarness);
