use bonsaidb_core::actionable::{Permissions, Statement};
use bonsaidb_core::connection::AsyncStorageConnection;
use bonsaidb_core::test_util::{self, BasicSchema, HarnessTest, TestDirectory};

use crate::server::ServerDatabase;
use crate::test_util::initialize_basic_server;
use crate::Server;

#[tokio::test]
async fn simple_test() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("simple-test");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    let db = server.database::<BasicSchema>("tests").await?;
    test_util::store_retrieve_update_delete_tests(&db).await
}

#[tokio::test]
async fn install_self_signed_certificate_tests() -> anyhow::Result<()> {
    let test_dir = TestDirectory::new("cert-install-test");
    let server = initialize_basic_server(test_dir.as_ref()).await?;
    // initialize_basic_server already installs a cert, so this should fail.
    assert!(server.install_self_signed_certificate(false).await.is_err());
    let old_certificate = server
        .certificate_chain()
        .await
        .unwrap()
        .into_end_entity_certificate();
    server.install_self_signed_certificate(true).await.unwrap();
    let new_certificate = server
        .certificate_chain()
        .await
        .unwrap()
        .into_end_entity_certificate();
    assert_ne!(new_certificate, old_certificate);
    Ok(())
}

struct TestHarness {
    _directory: TestDirectory,
    server: Server,
}

impl TestHarness {
    pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(format!("server-{test}"));
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

    pub async fn connect(&self) -> anyhow::Result<ServerDatabase> {
        let db = self.server.database::<BasicSchema>("tests").await?;
        Ok(db)
    }

    #[allow(dead_code)]
    async fn connect_with_permissions(
        &self,
        permissions: Vec<Statement>,
        _label: &str,
    ) -> anyhow::Result<ServerDatabase> {
        let mut db = self.connect().await?;
        db.db = db
            .db
            .with_effective_permissions(Permissions::from(permissions))
            .unwrap();
        Ok(db)
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.server.shutdown(None).await?;
        Ok(())
    }
}

bonsaidb_core::define_async_connection_test_suite!(TestHarness);
bonsaidb_core::define_async_pubsub_test_suite!(TestHarness);
bonsaidb_core::define_async_kv_test_suite!(TestHarness);
