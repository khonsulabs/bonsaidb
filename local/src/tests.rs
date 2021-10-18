use std::time::Duration;

use bonsaidb_core::{
    connection::{AccessPolicy, Connection, ServerConnection},
    permissions::{Permissions, Statement},
    test_util::{
        Basic, BasicByBrokenParentId, BasicByParentId, BasicCollectionWithNoViews,
        BasicCollectionWithOnlyBrokenParentId, BasicSchema, HarnessTest, TestDirectory,
    },
};
use config::Configuration;

use super::*;
use crate::Database;

struct TestHarness {
    _directory: TestDirectory,
    db: Database<BasicSchema>,
}

impl TestHarness {
    async fn new(test: HarnessTest) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(format!("local-{}", test));
        let storage = Storage::open_local(&directory, Configuration::default()).await?;
        storage.register_schema::<BasicSchema>().await?;
        storage.create_database::<BasicSchema>("tests").await?;
        let db = storage.database("tests").await?;

        Ok(Self {
            _directory: directory,
            db,
        })
    }

    const fn server_name() -> &'static str {
        "local"
    }

    fn server(&self) -> &'_ Storage {
        self.db.storage()
    }

    async fn connect_with_permissions(
        &self,
        permissions: Vec<Statement>,
        _label: &str,
    ) -> anyhow::Result<Database<BasicSchema>> {
        Ok(self
            .db
            .with_effective_permissions(Permissions::from(permissions)))
    }

    async fn connect(&self) -> anyhow::Result<Database<BasicSchema>> {
        Ok(self.db.clone())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

bonsaidb_core::define_connection_test_suite!(TestHarness);

#[cfg(feature = "pubsub")]
bonsaidb_core::define_pubsub_test_suite!(TestHarness);

#[cfg(feature = "keyvalue")]
bonsaidb_core::define_kv_test_suite!(TestHarness);

#[test]
fn integrity_checks() -> anyhow::Result<()> {
    let path = TestDirectory::new("integrity-checks");
    // To ensure full cleanup between each block, each runs in its own runtime;

    // Add a doc with no views installed
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            {
                let db = Database::<BasicCollectionWithNoViews>::open_local(
                    &path,
                    Configuration::default(),
                )
                .await?;
                let collection = db.collection::<BasicCollectionWithNoViews>();
                collection.push(&Basic::default().with_parent_id(1)).await?;
            }
            tokio::time::sleep(Duration::from_millis(100)).await; // TODO need to be able to shut down a local database, including background jobs.
            Result::<(), anyhow::Error>::Ok(())
        })
        .unwrap();
    }
    // Connect with a new view and see the automatic update with a query
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let db = Database::<BasicCollectionWithOnlyBrokenParentId>::open_local(
                &path,
                Configuration::default(),
            )
            .await?;
            // Give the integrity scanner time to run if it were to run (it shouldn't in this configuration).
            tokio::time::sleep(Duration::from_millis(100)).await;

            // NoUpdate should return data without the validation checker having run.
            assert_eq!(
                db.view::<BasicByBrokenParentId>()
                    .with_access_policy(AccessPolicy::NoUpdate)
                    .query()
                    .await?
                    .len(),
                0
            );

            // Regular query should show the correct data
            assert_eq!(db.view::<BasicByBrokenParentId>().query().await?.len(), 1);
            tokio::time::sleep(Duration::from_millis(100)).await; // TODO need to be able to shut down a local database, including background jobs.
            Result::<(), anyhow::Error>::Ok(())
        })
        .unwrap();
    }
    // Connect with a fixed view, and wait for the integrity scanner to work
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            let db = Database::<Basic>::open_local(
                &path,
                Configuration {
                    views: config::Views {
                        check_integrity_on_open: true,
                    },
                    ..Configuration::default()
                },
            )
            .await?;
            for _ in 0_u8..10 {
                tokio::time::sleep(Duration::from_millis(1000)).await;
                if db
                    .view::<BasicByParentId>()
                    .with_access_policy(AccessPolicy::NoUpdate)
                    .with_key(Some(1))
                    .query()
                    .await?
                    .len()
                    == 1
                {
                    return Result::<(), anyhow::Error>::Ok(());
                }
            }

            panic!("Integrity checker didn't run in the allocated time")
        })
        .unwrap();
    }

    Ok(())
}

#[test]
fn encryption() -> anyhow::Result<()> {
    let path = TestDirectory::new("encryption");
    let document_header = {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let db = Database::<Basic>::open_local(&path, Configuration::default()).await?;

            let document_header = db.collection::<Basic>().push(&Basic::new("hello")).await?;

            // Retrieve the document, showing that it was stored successfully.
            let doc = db
                .collection::<Basic>()
                .get(document_header.id)
                .await?
                .expect("doc not found");
            assert_eq!(&doc.contents::<Basic>()?.value, "hello");

            Result::<_, anyhow::Error>::Ok(document_header)
        })?
    };

    // By resetting the encryption key, we should be able to force an error in
    // decryption, which proves that the document was encrypted. To ensure the
    // server starts up and generates a new key, we must delete the sealing key.
    std::fs::remove_file(path.join("master-keys"))?;

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let db = Database::<Basic>::open_local(&path, Configuration::default()).await?;

        // Try retrieving the document, but expect an error decrypting.
        if let Err(bonsaidb_core::Error::Database(err)) =
            db.collection::<Basic>().get(document_header.id).await
        {
            assert!(err.contains("vault"));
        } else {
            panic!("successfully retrieved encrypted document without keys");
        }

        Result::<_, anyhow::Error>::Ok(())
    })?;

    Ok(())
}

#[test]
#[cfg(feature = "keyvalue")]
fn expiration_after_close() -> anyhow::Result<()> {
    use bonsaidb_core::{kv::Kv, test_util::TimingTest};
    loop {
        let path = TestDirectory::new("expiration-after-close");
        // To ensure full cleanup between each block, each runs in its own runtime;
        let timing = TimingTest::new(Duration::from_millis(500));
        // Set a key with an expiration, then close it. Then try to validate it
        // exists after opening, and then expires at the correct time.
        {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let db = Database::<()>::open_local(&path, Configuration::default()).await?;

                db.set_key("a", &0_u32)
                    .expire_in(Duration::from_secs(3))
                    .await?;

                Result::<(), anyhow::Error>::Ok(())
            })?;
        }

        {
            let rt = tokio::runtime::Runtime::new()?;
            let retry = rt.block_on(async {
                let db = Database::<()>::open_local(&path, Configuration::default()).await?;

                if timing.elapsed() > Duration::from_secs(1) {
                    return Ok(true);
                }

                assert_eq!(db.get_key("a").into().await?, Some(0_u32));

                timing.wait_until(Duration::from_secs(4)).await;

                assert!(db.get_key("a").await?.is_none());

                Result::<bool, anyhow::Error>::Ok(false)
            })?;

            if retry {
                println!("Retrying  expiration_after_close because it was too slow");
                continue;
            }
        }

        break;
    }
    Ok(())
}
