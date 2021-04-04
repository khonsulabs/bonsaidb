use std::time::Duration;

use pliantdb_core::{
    connection::{AccessPolicy, Connection},
    test_util::{
        self, Basic, BasicByBrokenParentId, BasicByParentId, BasicCollectionWithNoViews,
        BasicCollectionWithOnlyBrokenParentId, TestDirectory,
    },
};

use super::*;
use crate::Storage;

struct TestHarness {
    _directory: TestDirectory,
    db: Storage<Basic>,
}

impl TestHarness {
    pub async fn new(name: &str) -> anyhow::Result<Self> {
        let directory = TestDirectory::new(name);
        let db = Storage::<Basic>::open_local(&directory, &Configuration::default()).await?;
        Ok(Self {
            _directory: directory,
            db,
        })
    }

    pub async fn connect(&self) -> anyhow::Result<Storage<Basic>> {
        Ok(self.db.clone())
    }
}

pliantdb_core::define_connection_test_suite!(TestHarness);

#[tokio::test(flavor = "multi_thread")]
async fn integrity_checks() -> anyhow::Result<()> {
    let path = TestDirectory::new("integrity-checks");
    // Add a doc with no views installed
    {
        let db =
            Storage::<BasicCollectionWithNoViews>::open_local(&path, &Configuration::default())
                .await?;
        let collection = db.collection::<BasicCollectionWithNoViews>();
        collection.push(&Basic::default().with_parent_id(1)).await?;
    }
    // Connect with a new view
    {
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
    }
    // Connect with a fixed view, and wait for the integrity scanner to work
    {
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
            tokio::time::sleep(Duration::from_millis(20)).await;
            if db
                .view::<BasicByParentId>()
                .with_access_policy(AccessPolicy::NoUpdate)
                .with_key(Some(1))
                .query()
                .await?
                .len()
                == 1
            {
                return Ok(());
            }
        }

        panic!("Integrity checker didn't run in the allocated time")
    }
}
