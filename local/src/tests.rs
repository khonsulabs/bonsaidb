use std::time::Duration;

use pliantdb_core::{
    connection::{AccessPolicy, Connection},
    kv::Kv,
    test_util::{
        Basic, BasicByBrokenParentId, BasicByParentId, BasicCollectionWithNoViews,
        BasicCollectionWithOnlyBrokenParentId, HarnessTest, TestDirectory, TimingTest,
    },
};

use super::*;
use crate::Storage;

struct TestHarness {
    _directory: TestDirectory,
    db: Storage<Basic>,
}

impl TestHarness {
    async fn new(test: HarnessTest) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(test.to_string());
        let db = Storage::<Basic>::open_local(&directory, &Configuration::default()).await?;
        Ok(Self {
            _directory: directory,
            db,
        })
    }

    async fn connect(&self) -> anyhow::Result<Storage<Basic>> {
        Ok(self.db.clone())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

pliantdb_core::define_connection_test_suite!(TestHarness);

pliantdb_core::define_pubsub_test_suite!(TestHarness);

pliantdb_core::define_kv_test_suite!(TestHarness);

#[test]
fn integrity_checks() -> anyhow::Result<()> {
    let path = TestDirectory::new("integrity-checks");
    // To ensure full cleanup between each block, each runs in its own runtime;

    // Add a doc with no views installed
    {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let db =
                Storage::<BasicCollectionWithNoViews>::open_local(&path, &Configuration::default())
                    .await?;
            let collection = db.collection::<BasicCollectionWithNoViews>();
            collection.push(&Basic::default().with_parent_id(1)).await?;
            Result::<(), anyhow::Error>::Ok(())
        })?;
    }
    // Connect with a new view
    {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let db = Storage::<BasicCollectionWithOnlyBrokenParentId>::open_local(
                &path,
                &Configuration::default(),
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
            Result::<(), anyhow::Error>::Ok(())
        })?;
    }
    // Connect with a fixed view, and wait for the integrity scanner to work
    {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let db = Storage::<Basic>::open_local(
                &path,
                &Configuration {
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
    }
}

#[test]
fn expiration_after_close() -> anyhow::Result<()> {
    loop {
        let path = TestDirectory::new("expiration-after-close");
        // To ensure full cleanup between each block, each runs in its own runtime;
        let timing = TimingTest::new(Duration::from_millis(500));
        // Set a key with an expiration, then close it. Then try to validate it
        // exists after opening, and then expires at the correct time.
        {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let db = Storage::<()>::open_local(&path, &Configuration::default()).await?;

                db.set_key("a", &0_u32)
                    .expire_in(Duration::from_secs(3))
                    .await?;

                Result::<(), anyhow::Error>::Ok(())
            })?;
        }

        {
            let rt = tokio::runtime::Runtime::new()?;
            let retry = rt.block_on(async {
                let db = Storage::<()>::open_local(&path, &Configuration::default()).await?;

                if timing.elapsed() > Duration::from_secs(1) {
                    return Ok(true);
                }

                assert_eq!(db.get_key("a").await?, Some(0_u32));

                timing.wait_until(Duration::from_secs(4)).await;

                assert!(db.get_key::<u32, _>("a").await?.is_none());

                Result::<bool, anyhow::Error>::Ok(false)
            })?;

            if retry {
                continue;
            }
        }

        break;
    }
    Ok(())
}
