mod compatibility;

use std::time::Duration;

#[cfg(feature = "encryption")]
use bonsaidb_core::test_util::EncryptedBasic;
use bonsaidb_core::{
    connection::{AccessPolicy, Connection},
    document::DocumentId,
    permissions::{Permissions, Statement},
    test_util::{
        Basic, BasicByBrokenParentId, BasicByParentId, BasicCollectionWithNoViews,
        BasicCollectionWithOnlyBrokenParentId, BasicSchema, HarnessTest, TestDirectory,
    },
};

use crate::{
    config::{Builder, StorageConfiguration},
    Database, Storage,
};

macro_rules! define_local_suite {
    ($name:ident) => {
        mod $name {
            use super::*;
            #[cfg(feature = "async")]
            mod r#async {
                use bonsaidb_core::connection::AsyncStorageConnection;

                use super::*;
                use crate::{AsyncDatabase, AsyncStorage};
                struct AsyncTestHarness {
                    _directory: TestDirectory,
                    db: AsyncDatabase,
                    storage: AsyncStorage,
                }

                impl AsyncTestHarness {
                    async fn new(test: HarnessTest) -> anyhow::Result<Self> {
                        let directory =
                            TestDirectory::new(format!("async-{}-{}", stringify!($name), test));
                        let mut config =
                            StorageConfiguration::new(&directory).with_schema::<BasicSchema>()?;
                        if stringify!($name) == "memory" {
                            config = config.memory_only()
                        }

                        #[cfg(feature = "compression")]
                        {
                            config = config.default_compression(crate::config::Compression::Lz4);
                        }

                        let storage = AsyncStorage::open(config).await?;
                        let db = storage
                            .create_database::<BasicSchema>("tests", false)
                            .await?;

                        Ok(Self {
                            _directory: directory,
                            storage,
                            db,
                        })
                    }

                    const fn server_name() -> &'static str {
                        stringify!($name)
                    }

                    fn server(&self) -> &'_ AsyncStorage {
                        &self.storage
                    }

                    #[allow(dead_code)]
                    async fn connect_with_permissions(
                        &self,
                        permissions: Vec<Statement>,
                        _label: &str,
                    ) -> anyhow::Result<AsyncDatabase> {
                        Ok(self
                            .db
                            .with_effective_permissions(Permissions::from(permissions))
                            .unwrap())
                    }

                    async fn connect(&self) -> anyhow::Result<AsyncDatabase> {
                        Ok(self.db.clone())
                    }

                    pub async fn shutdown(&self) -> anyhow::Result<()> {
                        Ok(())
                    }
                }

                bonsaidb_core::define_async_connection_test_suite!(AsyncTestHarness);

                bonsaidb_core::define_async_pubsub_test_suite!(AsyncTestHarness);

                bonsaidb_core::define_async_kv_test_suite!(AsyncTestHarness);
            }
            mod blocking {
                use bonsaidb_core::connection::StorageConnection;

                use super::*;
                struct BlockingTestHarness {
                    _directory: TestDirectory,
                    db: Database,
                    storage: Storage,
                }

                impl BlockingTestHarness {
                    fn new(test: HarnessTest) -> anyhow::Result<Self> {
                        let directory =
                            TestDirectory::new(format!("blocking-{}-{}", stringify!($name), test));
                        let mut config =
                            StorageConfiguration::new(&directory).with_schema::<BasicSchema>()?;
                        if stringify!($name) == "memory" {
                            config = config.memory_only()
                        }

                        #[cfg(feature = "compression")]
                        {
                            config = config.default_compression(crate::config::Compression::Lz4);
                        }

                        let storage = Storage::open(config)?;
                        let db = storage.create_database::<BasicSchema>("tests", false)?;

                        Ok(Self {
                            _directory: directory,
                            storage,
                            db,
                        })
                    }

                    const fn server_name() -> &'static str {
                        stringify!($name)
                    }

                    fn server(&self) -> &'_ Storage {
                        &self.storage
                    }

                    #[allow(dead_code)]
                    fn connect_with_permissions(
                        &self,
                        permissions: Vec<Statement>,
                        _label: &str,
                    ) -> anyhow::Result<Database> {
                        Ok(self
                            .db
                            .with_effective_permissions(Permissions::from(permissions))
                            .unwrap())
                    }

                    fn connect(&self) -> anyhow::Result<Database> {
                        Ok(self.db.clone())
                    }

                    pub fn shutdown(&self) -> anyhow::Result<()> {
                        Ok(())
                    }
                }

                bonsaidb_core::define_blocking_connection_test_suite!(BlockingTestHarness);

                bonsaidb_core::define_blocking_pubsub_test_suite!(BlockingTestHarness);

                bonsaidb_core::define_blocking_kv_test_suite!(BlockingTestHarness);
            }
        }
    };
}

define_local_suite!(persisted);
define_local_suite!(memory);

#[test]
#[cfg_attr(not(feature = "compression"), allow(unused_mut))]
fn integrity_checks() -> anyhow::Result<()> {
    let path = TestDirectory::new("integrity-checks");
    let mut config = StorageConfiguration::new(&path);
    #[cfg(feature = "compression")]
    {
        config = config.default_compression(crate::config::Compression::Lz4);
    }
    // To ensure full cleanup between each block, each runs in its own runtime;

    // Add a doc with no views installed
    {
        let db = Database::open::<BasicCollectionWithNoViews>(config.clone())?;
        let collection = db.collection::<BasicCollectionWithNoViews>();
        collection.push(&Basic::default().with_parent_id(DocumentId::from_u64(1)))?;
    }
    // Connect with a new view and see the automatic update with a query
    {
        let db = Database::open::<BasicCollectionWithOnlyBrokenParentId>(config.clone())?;
        // Give the integrity scanner time to run if it were to run (it shouldn't in this configuration).
        std::thread::sleep(Duration::from_millis(100));

        // NoUpdate should return data without the validation checker having run.
        assert_eq!(
            db.view::<BasicByBrokenParentId>()
                .with_access_policy(AccessPolicy::NoUpdate)
                .query()?
                .len(),
            0
        );

        // Regular query should show the correct data
        assert_eq!(db.view::<BasicByBrokenParentId>().query()?.len(), 1);
    }

    // Connect with a fixed view, and wait for the integrity scanner to work
    let db = Database::open::<Basic>(config.check_view_integrity_on_open(true))?;
    for _ in 0_u8..100 {
        std::thread::sleep(Duration::from_millis(1000));
        if db
            .view::<BasicByParentId>()
            .with_access_policy(AccessPolicy::NoUpdate)
            .with_key(Some(1))
            .query()?
            .len()
            == 1
        {
            return Ok(());
        }
    }

    unreachable!("Integrity checker didn't run in the allocated time")
}

#[test]
#[cfg(feature = "encryption")]
fn encryption() -> anyhow::Result<()> {
    use bonsaidb_core::schema::SerializedCollection;
    let path = TestDirectory::new("encryption");
    let document_header = {
        let db = Database::open::<BasicSchema>(StorageConfiguration::new(&path))?;

        let document_header = db
            .collection::<EncryptedBasic>()
            .push(&EncryptedBasic::new("hello"))?;

        // Retrieve the document, showing that it was stored successfully.
        let doc = db
            .collection::<EncryptedBasic>()
            .get(document_header.id)?
            .expect("doc not found");
        assert_eq!(&EncryptedBasic::document_contents(&doc)?.value, "hello");

        document_header
    };

    // By resetting the encryption key, we should be able to force an error in
    // decryption, which proves that the document was encrypted. To ensure the
    // server starts up and generates a new key, we must delete the sealing key.

    std::fs::remove_file(path.join("master-keys"))?;

    let db = Database::open::<BasicSchema>(StorageConfiguration::new(&path))?;

    // Try retrieving the document, but expect an error decrypting.
    if let Err(bonsaidb_core::Error::Database(err)) =
        db.collection::<EncryptedBasic>().get(document_header.id)
    {
        assert!(err.contains("vault"));
    } else {
        panic!("successfully retrieved encrypted document without keys");
    }

    Ok(())
}

#[test]
fn expiration_after_close() -> anyhow::Result<()> {
    use bonsaidb_core::{keyvalue::KeyValue, test_util::TimingTest};
    loop {
        let path = TestDirectory::new("expiration-after-close");
        // To ensure full cleanup between each block, each runs in its own runtime;
        let timing = TimingTest::new(Duration::from_millis(100));
        // Set a key with an expiration, then close it. Then try to validate it
        // exists after opening, and then expires at the correct time.
        {
            let db = Database::open::<()>(StorageConfiguration::new(&path))?;

            // TODO This is a workaroun for the key-value expiration task
            // taking ownership of an instance of Database. If this async
            // task runs too quickly, sometimes things don't get cleaned up
            // if that task hasn't completed. This pause ensures the startup
            // tasks complete before we continue with the test. This should
            // be replaced with a proper shutdown call for the local
            // storage/database.
            std::thread::sleep(Duration::from_millis(100));

            db.set_key("a", &0_u32)
                .expire_in(Duration::from_secs(3))
                .execute()?;
        }

        {
            let db = Database::open::<()>(StorageConfiguration::new(&path))?;

            let key = db.get_key("a").query()?;
            // Due to not having a reliable way to shut down the database,
            // we can't make many guarantees about what happened after
            // setting the key in the above block. If we get None back,
            // we'll consider the test needing to retry. Once we have a
            // shutdown operation that guarantees that the key-value store
            // persists, the key.is_none() check shoud be removed, instead
            // asserting `key.is_some()`.
            if timing.elapsed() > Duration::from_secs(1) || key.is_none() {
                println!("Retrying  expiration_after_close because it was too slow");
                continue;
            }

            timing.wait_until(Duration::from_secs(4));

            assert!(db.get_key("a").query()?.is_none());
        }

        break;
    }
    Ok(())
}
