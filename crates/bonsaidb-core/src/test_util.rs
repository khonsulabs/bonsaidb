#![allow(clippy::missing_panics_doc)]

use std::borrow::Cow;
use std::fmt::{Debug, Display};
use std::io::ErrorKind;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use transmog_pot::Pot;

use crate::admin::{PermissionGroup, Role, User};
use crate::connection::{
    AccessPolicy, AsyncConnection, AsyncStorageConnection, Connection, StorageConnection,
};
use crate::document::{
    BorrowedDocument, CollectionDocument, CollectionHeader, DocumentId, Emit, Header, KeyId,
};
use crate::keyvalue::{AsyncKeyValue, KeyValue};
use crate::limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS};
use crate::schema::view::map::{Mappings, ViewMappedValue};
use crate::schema::view::{MapReduce, ReduceResult, SerializedView, ViewUpdatePolicy};
use crate::schema::{
    Collection, CollectionName, MappedValue, NamedCollection, Qualified, Schema, SchemaName,
    Schematic, SerializedCollection, View, ViewMapResult, ViewSchema,
};
use crate::transaction::{Operation, OperationResult, Transaction};
use crate::Error;
#[cfg(feature = "token-authentication")]
use crate::{
    admin::AuthenticationToken,
    connection::{HasSession, Identity, IdentityReference, Session},
};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Clone, Collection)]
// This collection purposely uses names with characters that need
// escaping, since it's used in backup/restore.
#[collection(name = "_basic", authority = "khonsulabs_", views = [BasicCount, BasicByParentId, BasicByParentIdEager, BasicByTag, BasicByCategory, BasicByCategoryCow], core = crate)]
#[must_use]
pub struct Basic {
    pub value: String,
    pub category: Option<String>,
    pub parent_id: Option<u64>,
    pub tags: Vec<String>,
}

impl Basic {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            tags: Vec::default(),
            category: None,
            parent_id: None,
        }
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub const fn with_parent_id(mut self, parent_id: u64) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Basic, key = (), value = usize, name = "count", core = crate)]
#[view_schema(core = crate)]
pub struct BasicCount;

impl MapReduce for BasicCount {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        document.header.emit_key_and_value((), 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View)]
#[view(collection = Basic, key = Option<u64>, value = usize, name = "by-parent-id", core = crate)]
pub struct BasicByParentId;

impl ViewSchema for BasicByParentId {
    type MappedKey<'doc> = <Self::View as View>::Key;
    type View = Self;

    fn version(&self) -> u64 {
        1
    }
}

impl MapReduce for BasicByParentId {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let contents = Basic::document_contents(document)?;
        document.header.emit_key_and_value(contents.parent_id, 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Basic, key = Option<u64>, value = usize, name = "by-parent-id-eager", core = crate)]
#[view_schema(core = crate, version = 1, policy = Eager)]
pub struct BasicByParentIdEager;

impl MapReduce for BasicByParentIdEager {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let contents = Basic::document_contents(document)?;
        document.header.emit_key_and_value(contents.parent_id, 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Basic, key = String, value = usize, name = "by-category", core = crate)]
#[view_schema(core = crate)]
pub struct BasicByCategory;

impl MapReduce for BasicByCategory {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let contents = Basic::document_contents(document)?;
        if let Some(category) = &contents.category {
            document
                .header
                .emit_key_and_value(category.to_lowercase(), 1)
        } else {
            Ok(Mappings::none())
        }
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Basic, key = String, value = usize, name = "by-category-cow", core = crate)]
#[view_schema(core = crate, mapped_key = Cow<'doc, str>)]
pub struct BasicByCategoryCow;

impl MapReduce for BasicByCategoryCow {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        #[derive(Deserialize, Debug)]
        struct BorrowedBasic<'a> {
            category: Option<&'a str>,
        }
        let contents: BorrowedBasic<'_> = pot::from_slice(&document.contents)?;
        if let Some(category) = &contents.category {
            document
                .header
                .emit_key_and_value(Cow::Borrowed(category), 1)
        } else {
            Ok(Mappings::none())
        }
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings
            .iter()
            .map(|map| {
                assert!(matches!(map.key, Cow::Borrowed(_)));
                map.value
            })
            .sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Basic, key = String, value = usize, name = "by-tag", core = crate)]
#[view_schema(core = crate)]
pub struct BasicByTag;

impl MapReduce for BasicByTag {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let contents = Basic::document_contents(document)?;
        contents
            .tags
            .iter()
            .map(|tag| document.header.emit_key_and_value(tag.clone(), 1))
            .collect()
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Basic, key = (), value = (), name = "by-parent-id", core = crate)]
#[view_schema(core = crate)]
pub struct BasicByBrokenParentId;

impl MapReduce for BasicByBrokenParentId {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        document.header.emit()
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Default, Clone, Collection)]
#[collection(name = "encrypted-basic", authority = "khonsulabs", views = [EncryptedBasicCount, EncryptedBasicByParentId, EncryptedBasicByCategory])]
#[collection(encryption_key = Some(KeyId::Master), encryption_optional, core = crate)]
#[must_use]
pub struct EncryptedBasic {
    pub value: String,
    pub category: Option<String>,
    pub parent_id: Option<u64>,
}

impl EncryptedBasic {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            category: None,
            parent_id: None,
        }
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    pub const fn with_parent_id(mut self, parent_id: u64) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = EncryptedBasic, key = (), value = usize, name = "count", core = crate)]
#[view_schema(core = crate)]
pub struct EncryptedBasicCount;

impl MapReduce for EncryptedBasicCount {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        document.header.emit_key_and_value((), 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = EncryptedBasic, key = Option<u64>, value = usize, name = "by-parent-id", core = crate)]
#[view_schema(core = crate)]
pub struct EncryptedBasicByParentId;

impl MapReduce for EncryptedBasicByParentId {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let contents = EncryptedBasic::document_contents(document)?;
        document.header.emit_key_and_value(contents.parent_id, 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = EncryptedBasic, key = String, value = usize, name = "by-category", core = crate)]
#[view_schema(core = crate)]
pub struct EncryptedBasicByCategory;

impl MapReduce for EncryptedBasicByCategory {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let contents = EncryptedBasic::document_contents(document)?;
        if let Some(category) = &contents.category {
            document
                .header
                .emit_key_and_value(category.to_lowercase(), 1)
        } else {
            Ok(Mappings::none())
        }
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<'_, Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug, Schema)]
#[schema(name = "basic", collections = [Basic, EncryptedBasic, Unique], core = crate)]
pub struct BasicSchema;

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Default, Collection)]
#[collection(name = "unique", authority = "khonsulabs", views = [UniqueValue], core = crate)]
pub struct Unique {
    pub value: String,
}

impl Unique {
    pub fn new(value: impl Display) -> Self {
        Self {
            value: value.to_string(),
        }
    }
}

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Unique, key = String, value = (), name = "unique-value", core = crate)]
#[view_schema(core = crate, policy = Unique)]
pub struct UniqueValue;

impl MapReduce for UniqueValue {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
        let entry = Unique::document_contents(document)?;
        document.header.emit_key(entry.value)
    }
}

impl NamedCollection for Unique {
    type ByNameView = UniqueValue;
}

#[derive(Debug)]
pub struct TestDirectory(pub PathBuf);

impl TestDirectory {
    pub fn absolute<S: AsRef<Path>>(path: S) -> Self {
        let path = path.as_ref().to_owned();
        if path.exists() {
            std::fs::remove_dir_all(&path).expect("error clearing temporary directory");
        }
        Self(path)
    }

    pub fn new<S: AsRef<Path>>(name: S) -> Self {
        let path = std::env::temp_dir().join(name);
        if path.exists() {
            std::fs::remove_dir_all(&path).expect("error clearing temporary directory");
        }
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_dir_all(&self.0) {
            if err.kind() != ErrorKind::NotFound {
                eprintln!("Failed to clean up temporary folder: {err:?}");
            }
        }
    }
}

impl AsRef<Path> for TestDirectory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Deref for TestDirectory {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct BasicCollectionWithNoViews;

impl Collection for BasicCollectionWithNoViews {
    type PrimaryKey = u64;

    fn collection_name() -> CollectionName {
        Basic::collection_name()
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl SerializedCollection for BasicCollectionWithNoViews {
    type Contents = Basic;
    type Format = Pot;

    fn format() -> Self::Format {
        Pot::default()
    }
}

#[derive(Debug)]
pub struct BasicCollectionWithOnlyBrokenParentId;

impl Collection for BasicCollectionWithOnlyBrokenParentId {
    type PrimaryKey = u64;

    fn collection_name() -> CollectionName {
        Basic::collection_name()
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(BasicByBrokenParentId)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Collection)]
#[collection(name = "unassociated", authority = "khonsulabs", core = crate)]
pub struct UnassociatedCollection;

#[derive(Copy, Clone, Debug)]
pub enum HarnessTest {
    ServerConnectionTests = 1,
    StoreRetrieveUpdate,
    Overwrite,
    NotFound,
    Conflict,
    BadUpdate,
    NoUpdate,
    GetMultiple,
    List,
    ListTransactions,
    Transactions,
    TransactionCheck,
    ViewQuery,
    UnassociatedCollection,
    Compact,
    ViewUpdate,
    ViewMultiEmit,
    ViewUnimplementedReduce,
    ViewAccessPolicies,
    ViewCow,
    Encryption,
    UniqueViews,
    NamedCollection,
    PubSubSimple,
    UserManagement,
    TokenAuthentication,
    PubSubMultipleSubscribers,
    PubSubDropAndSend,
    PubSubUnsubscribe,
    PubSubDropCleanup,
    PubSubPublishAll,
    KvBasic,
    KvConcurrency,
    KvSet,
    KvIncrementDecrement,
    KvExpiration,
    KvDeleteExpire,
    KvTransactions,
}

impl HarnessTest {
    #[must_use]
    pub const fn port(self, base: u16) -> u16 {
        base + self as u16
    }
}

impl Display for HarnessTest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

/// Compares two f64's accounting for the epsilon.
#[macro_export]
macro_rules! assert_f64_eq {
    ($a:expr, $b:expr) => {{
        let a: f64 = $a;
        let b: f64 = $b;
        assert!((a - b).abs() <= f64::EPSILON, "{:?} <> {:?}", a, b);
    }};
}

/// Creates a test suite that tests methods available on [`AsyncConnection`]
#[macro_export]
macro_rules! define_async_connection_test_suite {
    ($harness:ident) => {
        #[cfg(test)]
        mod r#async_connection {
            use super::$harness;
            #[tokio::test]
            async fn server_connection_tests() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::ServerConnectionTests).await?;
                let db = harness.server();
                $crate::test_util::basic_server_connection_tests(
                    db.clone(),
                    &format!("server-connection-tests-{}", $harness::server_name()),
                )
                .await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn store_retrieve_update_delete() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::StoreRetrieveUpdate).await?;
                let db = harness.connect().await?;
                $crate::test_util::store_retrieve_update_delete_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn overwrite() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Overwrite).await?;
                let db = harness.connect().await?;
                $crate::test_util::overwrite_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn not_found() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::NotFound).await?;
                let db = harness.connect().await?;

                $crate::test_util::not_found_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn conflict() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Conflict).await?;
                let db = harness.connect().await?;

                $crate::test_util::conflict_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn bad_update() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::BadUpdate).await?;
                let db = harness.connect().await?;

                $crate::test_util::bad_update_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn no_update() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::NoUpdate).await?;
                let db = harness.connect().await?;

                $crate::test_util::no_update_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn get_multiple() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::GetMultiple).await?;
                let db = harness.connect().await?;

                $crate::test_util::get_multiple_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn list() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::List).await?;
                let db = harness.connect().await?;

                $crate::test_util::list_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn list_transactions() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::ListTransactions).await?;
                let db = harness.connect().await?;

                $crate::test_util::list_transactions_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn transactions() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Transactions).await?;
                let db = harness.connect().await?;

                $crate::test_util::transaction_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn transaction_check() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::TransactionCheck).await?;
                let db = harness.connect().await?;

                $crate::test_util::transaction_check_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn view_query() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewQuery).await?;
                let db = harness.connect().await?;

                $crate::test_util::view_query_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn unassociated_collection() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::UnassociatedCollection).await?;
                let db = harness.connect().await?;

                $crate::test_util::unassociated_collection_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn unimplemented_reduce() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::ViewUnimplementedReduce).await?;
                let db = harness.connect().await?;

                $crate::test_util::unimplemented_reduce(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn view_update() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewUpdate).await?;
                let db = harness.connect().await?;

                $crate::test_util::view_update_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn view_multi_emit() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewMultiEmit).await?;
                let db = harness.connect().await?;

                $crate::test_util::view_multi_emit_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn view_access_policies() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::ViewAccessPolicies).await?;
                let db = harness.connect().await?;

                $crate::test_util::view_access_policy_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn unique_views() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::UniqueViews).await?;
                let db = harness.connect().await?;

                $crate::test_util::unique_view_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn named_collection() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::NamedCollection).await?;
                let db = harness.connect().await?;

                $crate::test_util::named_collection_tests(&db).await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn user_management() -> anyhow::Result<()> {
                use $crate::connection::AsyncStorageConnection;
                let harness = $harness::new($crate::test_util::HarnessTest::UserManagement).await?;
                let _db = harness.connect().await?;
                let server = harness.server();
                let admin = server
                    .database::<$crate::admin::Admin>($crate::admin::ADMIN_DATABASE_NAME)
                    .await?;

                $crate::test_util::user_management_tests(
                    &admin,
                    server.clone(),
                    $harness::server_name(),
                )
                .await?;
                harness.shutdown().await
            }

            #[tokio::test]
            #[cfg(feature = "token-authentication")]
            async fn token_authentication() -> anyhow::Result<()> {
                use $crate::connection::AsyncStorageConnection;
                let harness =
                    $harness::new($crate::test_util::HarnessTest::TokenAuthentication).await?;
                let _db = harness.connect().await?;
                let server = harness.server();
                let admin = server
                    .database::<$crate::admin::Admin>($crate::admin::ADMIN_DATABASE_NAME)
                    .await?;

                $crate::test_util::token_authentication_tests(
                    &admin,
                    server,
                    $harness::server_name(),
                )
                .await?;
                harness.shutdown().await
            }

            #[tokio::test]
            async fn compaction() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Compact).await?;
                let db = harness.connect().await?;

                $crate::test_util::compaction_tests(&db).await?;
                harness.shutdown().await
            }
        }
    };
}

/// Creates a test suite that tests methods available on [`AsyncConnection`]
#[macro_export]
macro_rules! define_blocking_connection_test_suite {
    ($harness:ident) => {
        #[cfg(test)]
        mod blocking_connection {
            use super::$harness;
            #[test]
            fn server_connection_tests() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ServerConnectionTests)?;
                let db = harness.server();
                $crate::test_util::blocking_basic_server_connection_tests(
                    db,
                    &format!("server-connection-tests-{}", $harness::server_name()),
                )?;
                harness.shutdown()
            }

            #[test]
            fn store_retrieve_update_delete() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::StoreRetrieveUpdate)?;
                let db = harness.connect()?;
                $crate::test_util::blocking_store_retrieve_update_delete_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn overwrite() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Overwrite)?;
                let db = harness.connect()?;
                $crate::test_util::blocking_overwrite_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn not_found() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::NotFound)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_not_found_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn conflict() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Conflict)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_conflict_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn bad_update() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::BadUpdate)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_bad_update_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn no_update() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::NoUpdate)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_no_update_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn get_multiple() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::GetMultiple)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_get_multiple_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn list() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::List)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_list_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn list_transactions() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ListTransactions)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_list_transactions_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn transaction_check() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::TransactionCheck)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_transaction_check_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn transactions() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Transactions)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_transaction_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn view_query() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewQuery)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_view_query_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn unassociated_collection() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::UnassociatedCollection)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_unassociated_collection_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn unimplemented_reduce() -> anyhow::Result<()> {
                let harness =
                    $harness::new($crate::test_util::HarnessTest::ViewUnimplementedReduce)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_unimplemented_reduce(&db)?;
                harness.shutdown()
            }

            #[test]
            fn view_update() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewUpdate)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_view_update_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn view_multi_emit() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewMultiEmit)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_view_multi_emit_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn view_access_policies() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewAccessPolicies)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_view_access_policy_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn unique_views() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::UniqueViews)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_unique_view_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn cow_views() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::ViewCow)?;
                let db = harness.connect()?;
                $crate::test_util::blocking_cow_views(&db)?;
                harness.shutdown()
            }

            #[test]
            fn named_collection() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::NamedCollection)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_named_collection_tests(&db)?;
                harness.shutdown()
            }

            #[test]
            fn user_management() -> anyhow::Result<()> {
                use $crate::connection::StorageConnection;
                let harness = $harness::new($crate::test_util::HarnessTest::UserManagement)?;
                let _db = harness.connect()?;
                let server = harness.server();
                let admin =
                    server.database::<$crate::admin::Admin>($crate::admin::ADMIN_DATABASE_NAME)?;

                $crate::test_util::blocking_user_management_tests(
                    &admin,
                    server,
                    $harness::server_name(),
                )?;
                harness.shutdown()
            }

            #[test]
            #[cfg(feature = "token-authentication")]
            fn token_authentication() -> anyhow::Result<()> {
                use $crate::connection::StorageConnection;
                let harness = $harness::new($crate::test_util::HarnessTest::TokenAuthentication)?;
                let _db = harness.connect()?;
                let server = harness.server();
                let admin =
                    server.database::<$crate::admin::Admin>($crate::admin::ADMIN_DATABASE_NAME)?;

                $crate::test_util::blocking_token_authentication_tests(
                    &admin,
                    server,
                    $harness::server_name(),
                )?;
                harness.shutdown()
            }

            #[test]
            fn compaction() -> anyhow::Result<()> {
                let harness = $harness::new($crate::test_util::HarnessTest::Compact)?;
                let db = harness.connect()?;

                $crate::test_util::blocking_compaction_tests(&db)?;
                harness.shutdown()
            }
        }
    };
}

pub async fn store_retrieve_update_delete_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(&header.id)
        .await?
        .expect("couldn't retrieve stored item");
    let mut value = Basic::document_contents(&doc)?;
    assert_eq!(original_value, value);
    let old_revision = doc.header.revision;

    // Update the value
    value.value = String::from("updated_value");
    Basic::set_document_contents(&mut doc, value.clone())?;
    db.update::<Basic, _>(&mut doc).await?;

    // update should cause the revision to be changed
    assert_ne!(doc.header.revision, old_revision);

    // Check the value in the database to ensure it has the new document
    let doc = collection
        .get(&header.id)
        .await?
        .expect("couldn't retrieve stored item");
    assert_eq!(Basic::document_contents(&doc)?, value);

    // These operations should have created two transactions with one change each
    let transactions = db.list_executed_transactions(None, None).await?;
    assert_eq!(transactions.len(), 2);
    assert!(transactions[0].id < transactions[1].id);
    for transaction in &transactions {
        let changes = transaction
            .changes
            .documents()
            .expect("incorrect transaction type");
        assert_eq!(changes.documents.len(), 1);
        assert_eq!(changes.collections.len(), 1);
        assert_eq!(changes.collections[0], Basic::collection_name());
        assert_eq!(changes.documents[0].collection, 0);
        assert_eq!(header.id, changes.documents[0].id.deserialize()?);
        assert!(!changes.documents[0].deleted);
    }

    db.collection::<Basic>().delete(&doc).await?;
    assert!(collection.get(&header.id).await?.is_none());
    let transactions = db
        .list_executed_transactions(Some(transactions.last().as_ref().unwrap().id + 1), None)
        .await?;
    assert_eq!(transactions.len(), 1);
    let transaction = transactions.first().unwrap();
    let changes = transaction
        .changes
        .documents()
        .expect("incorrect transaction type");
    assert_eq!(changes.documents.len(), 1);
    assert_eq!(changes.collections[0], Basic::collection_name());
    assert_eq!(header.id, changes.documents[0].id.deserialize()?);
    assert!(changes.documents[0].deleted);

    // Use the Collection interface
    let mut doc = original_value.clone().push_into_async(db).await?;
    doc.contents.category = Some(String::from("updated"));
    doc.update_async(db).await?;
    let reloaded = Basic::get_async(&doc.header.id, db).await?.unwrap();
    assert_eq!(doc.contents, reloaded.contents);

    // Test Connection::insert with a specified id
    let doc = BorrowedDocument::with_contents::<Basic, _>(&42, &Basic::new("42"))?;
    let document_42 = db
        .insert::<Basic, _, _>(Some(&doc.header.id), doc.contents.into_vec())
        .await?;
    assert_eq!(document_42.id, 42);
    let document_43 = Basic::new("43").insert_into_async(&43, db).await?;
    assert_eq!(document_43.header.id, 43);

    // Test that inserting a document with the same ID results in a conflict:
    let conflict_err = Basic::new("43")
        .with_parent_id(document_42.id)
        .insert_into_async(&doc.header.id, db)
        .await
        .unwrap_err();
    assert!(matches!(conflict_err.error, Error::DocumentConflict(..)));

    // Test bulk insert
    let docs = Basic::push_all_async([Basic::new("44"), Basic::new("45")], db)
        .await
        .unwrap();
    assert_eq!(docs[0].contents.value, "44");
    assert_eq!(docs[1].contents.value, "45");

    Ok(())
}

#[allow(clippy::cognitive_complexity)] // TODO should be split into multiple tests
pub fn blocking_store_retrieve_update_delete_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value)?;

    let mut doc = collection
        .get(&header.id)?
        .expect("couldn't retrieve stored item");
    let mut value = Basic::document_contents(&doc)?;
    assert_eq!(original_value, value);
    let old_revision = doc.header.revision;

    // Update the value
    value.value = String::from("updated_value");
    Basic::set_document_contents(&mut doc, value.clone())?;
    db.update::<Basic, _>(&mut doc)?;

    // update should cause the revision to be changed
    assert_ne!(doc.header.revision, old_revision);

    // Check the value in the database to ensure it has the new document
    let doc = collection
        .get(&header.id)?
        .expect("couldn't retrieve stored item");
    assert_eq!(Basic::document_contents(&doc)?, value);

    // These operations should have created two transactions with one change each
    let transactions = db.list_executed_transactions(None, None)?;
    assert_eq!(transactions.len(), 2);
    assert!(transactions[0].id < transactions[1].id);
    for transaction in &transactions {
        let changes = transaction
            .changes
            .documents()
            .expect("incorrect transaction type");
        assert_eq!(changes.documents.len(), 1);
        assert_eq!(changes.collections.len(), 1);
        assert_eq!(changes.collections[0], Basic::collection_name());
        assert_eq!(changes.documents[0].collection, 0);
        assert_eq!(header.id, changes.documents[0].id.deserialize()?);
        assert!(!changes.documents[0].deleted);
    }

    db.collection::<Basic>().delete(&doc)?;
    assert!(collection.get(&header.id)?.is_none());
    let transactions =
        db.list_executed_transactions(Some(transactions.last().as_ref().unwrap().id + 1), None)?;
    assert_eq!(transactions.len(), 1);
    let transaction = transactions.first().unwrap();
    let changes = transaction
        .changes
        .documents()
        .expect("incorrect transaction type");
    assert_eq!(changes.documents.len(), 1);
    assert_eq!(changes.collections[0], Basic::collection_name());
    assert_eq!(header.id, changes.documents[0].id.deserialize()?);
    assert!(changes.documents[0].deleted);

    // Use the Collection interface
    let mut doc = original_value.push_into(db)?;
    doc.contents.category = Some(String::from("updated"));
    doc.update(db)?;
    let reloaded = Basic::get(&doc.header.id, db)?.unwrap();
    assert_eq!(doc.contents, reloaded.contents);

    // Test Connection::insert with a specified id
    let doc = BorrowedDocument::with_contents::<Basic, _>(&42, &Basic::new("42"))?;
    let document_42 = db.insert::<Basic, _, _>(Some(&doc.header.id), doc.contents.into_vec())?;
    assert_eq!(document_42.id, 42);
    let document_43 = Basic::new("43").insert_into(&43, db)?;
    assert_eq!(document_43.header.id, 43);

    // Test that inserting a document with the same ID results in a conflict:
    let conflict_err = Basic::new("43")
        .insert_into(&doc.header.id, db)
        .unwrap_err();
    assert!(matches!(conflict_err.error, Error::DocumentConflict(..)));

    // Test bulk insert
    let docs = Basic::push_all([Basic::new("44"), Basic::new("45")], db).unwrap();
    assert_eq!(docs[0].contents.value, "44");
    assert_eq!(docs[1].contents.value, "45");

    Ok(())
}

pub async fn overwrite_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    // Test Connection::insert with a specified id
    let doc = BorrowedDocument::with_contents::<Basic, _>(&42, &Basic::new("42"))?;
    let document_42 = db
        .insert::<Basic, _, _>(Some(&doc.header.id), doc.contents.into_vec())
        .await?;
    assert_eq!(document_42.id, 42);
    let document_43 = Basic::new("43").insert_into_async(&43, db).await?;
    assert_eq!(document_43.header.id, 43);

    // Test that overwriting works
    let overwritten = Basic::new("43")
        .with_parent_id(document_42.id)
        .overwrite_into_async(&doc.header.id, db)
        .await
        .unwrap();
    assert!(overwritten.header.revision.id > doc.header.revision.id);
    let children = db
        .view::<BasicByParentIdEager>()
        .with_access_policy(AccessPolicy::NoUpdate)
        .with_key(&Some(document_42.id))
        .query()
        .await?;
    assert_eq!(children[0].source.revision, overwritten.header.revision);

    // Verify that unique/eager views are updated when overwriting into a non-existant key
    let insert_via_overwrite = Basic::new("1000")
        .with_parent_id(document_43.header.id)
        .overwrite_into_async(&1_000, db)
        .await
        .unwrap();
    let children = db
        .view::<BasicByParentIdEager>()
        .with_access_policy(AccessPolicy::NoUpdate)
        .with_key(&Some(document_43.header.id))
        .query()
        .await?;
    assert_eq!(
        children[0].source.revision,
        insert_via_overwrite.header.revision
    );

    Ok(())
}

pub fn blocking_overwrite_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    // Test Connection::insert with a specified id
    let doc = BorrowedDocument::with_contents::<Basic, _>(&42, &Basic::new("42"))?;
    let document_42 = db.insert::<Basic, _, _>(Some(&doc.header.id), doc.contents.into_vec())?;
    assert_eq!(document_42.id, 42);
    let document_43 = Basic::new("43").insert_into(&43, db)?;
    assert_eq!(document_43.header.id, 43);

    // Test that overwriting works
    let overwritten = Basic::new("43")
        .with_parent_id(document_42.id)
        .overwrite_into(&doc.header.id, db)
        .unwrap();
    assert!(overwritten.header.revision.id > doc.header.revision.id);
    let children = db
        .view::<BasicByParentIdEager>()
        .with_access_policy(AccessPolicy::NoUpdate)
        .with_key(&Some(document_42.id))
        .query()?;
    assert_eq!(children[0].source.revision, overwritten.header.revision);

    // Verify that unique/eager views are updated when overwriting into a non-existant key
    let insert_via_overwrite = Basic::new("1000")
        .with_parent_id(document_43.header.id)
        .overwrite_into(&1_000, db)
        .unwrap();
    let children = db
        .view::<BasicByParentIdEager>()
        .with_access_policy(AccessPolicy::NoUpdate)
        .with_key(&Some(document_43.header.id))
        .query()?;
    assert_eq!(
        children[0].source.revision,
        insert_via_overwrite.header.revision
    );

    Ok(())
}

pub async fn not_found_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    assert!(db.collection::<Basic>().get(&1).await?.is_none());

    assert!(db.last_transaction_id().await?.is_none());

    Ok(())
}

pub fn blocking_not_found_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    assert!(db.collection::<Basic>().get(&1)?.is_none());

    assert!(db.last_transaction_id()?.is_none());

    Ok(())
}

pub async fn conflict_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(&header.id)
        .await?
        .expect("couldn't retrieve stored item");
    let mut value = Basic::document_contents(&doc)?;
    value.value = String::from("updated_value");
    Basic::set_document_contents(&mut doc, value.clone())?;
    db.update::<Basic, _>(&mut doc).await?;

    // To generate a conflict, let's try to do the same update again by
    // reverting the header
    doc.header = Header::try_from(header).unwrap();
    let conflicting_header = db
        .update::<Basic, _>(&mut doc)
        .await
        .expect_err("conflict should have generated an error")
        .conflicting_document::<Basic>()
        .expect("conflict not detected");
    assert_eq!(conflicting_header.id, doc.header.id);

    // Let's force an update through overwrite. After this succeeds, the header
    // is updated to the new revision.
    db.collection::<Basic>().overwrite(&mut doc).await.unwrap();

    // Now, let's use the CollectionDocument API to modify the document through a refetch.
    let mut doc = CollectionDocument::<Basic>::try_from(&doc)?;
    doc.modify_async(db, |doc| {
        doc.contents.value = String::from("modify worked");
    })
    .await?;
    assert_eq!(doc.contents.value, "modify worked");
    let doc = Basic::get_async(&doc.header.id, db).await?.unwrap();
    assert_eq!(doc.contents.value, "modify worked");

    Ok(())
}

pub fn blocking_conflict_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value)?;

    let mut doc = collection
        .get(&header.id)?
        .expect("couldn't retrieve stored item");
    let mut value = Basic::document_contents(&doc)?;
    value.value = String::from("updated_value");
    Basic::set_document_contents(&mut doc, value)?;
    db.update::<Basic, _>(&mut doc)?;

    // To generate a conflict, let's try to do the same update again by
    // reverting the header
    doc.header = Header::try_from(header).unwrap();
    let conflicting_header = db
        .update::<Basic, _>(&mut doc)
        .expect_err("conflict should have generated an error")
        .conflicting_document::<Basic>()
        .expect("conflict not detected");
    assert_eq!(conflicting_header.id, doc.header.id);

    // Let's force an update through overwrite. After this succeeds, the header
    // is updated to the new revision.
    db.collection::<Basic>().overwrite(&mut doc).unwrap();

    // Now, let's use the CollectionDocument API to modify the document through a refetch.
    let mut doc = CollectionDocument::<Basic>::try_from(&doc)?;
    doc.modify(db, |doc| {
        doc.contents.value = String::from("modify worked");
    })?;
    assert_eq!(doc.contents.value, "modify worked");
    let doc = Basic::get(&doc.header.id, db)?.unwrap();
    assert_eq!(doc.contents.value, "modify worked");

    Ok(())
}

pub async fn bad_update_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let mut doc = BorrowedDocument::with_contents::<Basic, _>(&1, &Basic::default())?;
    match db.update::<Basic, _>(&mut doc).await {
        Err(Error::DocumentNotFound(collection, id)) => {
            assert_eq!(collection, Basic::collection_name());
            assert_eq!(id.as_ref(), &DocumentId::from_u64(1));
            Ok(())
        }
        other => panic!("expected DocumentNotFound from update but got: {other:?}"),
    }
}

pub fn blocking_bad_update_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let mut doc = BorrowedDocument::with_contents::<Basic, _>(&1, &Basic::default())?;
    match db.update::<Basic, _>(&mut doc) {
        Err(Error::DocumentNotFound(collection, id)) => {
            assert_eq!(collection, Basic::collection_name());
            assert_eq!(id.as_ref(), &DocumentId::from_u64(1));
            Ok(())
        }
        other => panic!("expected DocumentNotFound from update but got: {other:?}"),
    }
}

pub async fn no_update_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(&header.id)
        .await?
        .expect("couldn't retrieve stored item");
    db.update::<Basic, _>(&mut doc).await?;

    assert_eq!(CollectionHeader::try_from(doc.header)?, header);

    Ok(())
}

pub fn blocking_no_update_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value)?;

    let mut doc = collection
        .get(&header.id)?
        .expect("couldn't retrieve stored item");
    db.update::<Basic, _>(&mut doc)?;

    assert_eq!(CollectionHeader::try_from(doc.header)?, header);

    Ok(())
}

pub async fn get_multiple_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let doc1_value = Basic::new("initial_value");
    let doc1 = collection.push(&doc1_value).await?;

    let doc2_value = Basic::new("second_value");
    let doc2 = collection.push(&doc2_value).await?;

    let both_docs = Basic::get_multiple_async([&doc1.id, &doc2.id], db).await?;
    assert_eq!(both_docs.len(), 2);

    let out_of_order = Basic::get_multiple_async([&doc2.id, &doc1.id], db).await?;
    assert_eq!(out_of_order.len(), 2);

    // The order of get_multiple isn't guaranteed, so these two checks are done
    // with iterators instead of direct indexing
    let doc1 = both_docs
        .iter()
        .find(|doc| doc.header.id == doc1.id)
        .expect("Couldn't find doc1");
    assert_eq!(doc1.contents.value, doc1_value.value);
    let doc2 = both_docs
        .iter()
        .find(|doc| doc.header.id == doc2.id)
        .expect("Couldn't find doc2");
    assert_eq!(doc2.contents.value, doc2_value.value);

    Ok(())
}

pub fn blocking_get_multiple_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let doc1_value = Basic::new("initial_value");
    let doc1 = collection.push(&doc1_value)?;

    let doc2_value = Basic::new("second_value");
    let doc2 = collection.push(&doc2_value)?;

    let both_docs = Basic::get_multiple([&doc1.id, &doc2.id], db)?;
    assert_eq!(both_docs.len(), 2);

    let out_of_order = Basic::get_multiple([&doc2.id, &doc1.id], db)?;
    assert_eq!(out_of_order.len(), 2);

    // The order of get_multiple isn't guaranteed, so these two checks are done
    // with iterators instead of direct indexing
    let doc1 = both_docs
        .iter()
        .find(|doc| doc.header.id == doc1.id)
        .expect("Couldn't find doc1");
    assert_eq!(doc1.contents.value, doc1_value.value);
    let doc2 = both_docs
        .iter()
        .find(|doc| doc.header.id == doc2.id)
        .expect("Couldn't find doc2");
    assert_eq!(doc2.contents.value, doc2_value.value);

    Ok(())
}

pub async fn list_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let doc1_value = Basic::new("initial_value");
    let doc1 = collection.push(&doc1_value).await?;

    let doc2_value = Basic::new("second_value");
    let doc2 = collection.push(&doc2_value).await?;

    let all_docs = Basic::all_async(db).await?;
    assert_eq!(all_docs.len(), 2);
    assert_eq!(Basic::all_async(db).count().await?, 2);

    let both_docs = Basic::list_async(doc1.id..=doc2.id, db).await?;
    assert_eq!(both_docs.len(), 2);
    assert_eq!(Basic::list_async(doc1.id..=doc2.id, db).count().await?, 2);

    assert_eq!(both_docs[0].contents.value, doc1_value.value);
    assert_eq!(both_docs[1].contents.value, doc2_value.value);

    let both_headers = Basic::list_async(doc1.id..=doc2.id, db).headers().await?;

    assert_eq!(both_headers.len(), 2);

    let one_doc = Basic::list_async(doc1.id..doc2.id, db).await?;
    assert_eq!(one_doc.len(), 1);

    let limited = Basic::list_async(doc1.id..=doc2.id, db)
        .limit(1)
        .descending()
        .await?;
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].contents.value, doc2_value.value);

    Ok(())
}

pub fn blocking_list_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let doc1_value = Basic::new("initial_value");
    let doc1 = collection.push(&doc1_value)?;

    let doc2_value = Basic::new("second_value");
    let doc2 = collection.push(&doc2_value)?;

    let all_docs = Basic::all(db).query()?;
    assert_eq!(all_docs.len(), 2);
    assert_eq!(Basic::all(db).count()?, 2);

    let both_docs = Basic::list(doc1.id..=doc2.id, db).query()?;
    assert_eq!(both_docs.len(), 2);
    assert_eq!(Basic::list(doc1.id..=doc2.id, db).count()?, 2);

    assert_eq!(both_docs[0].contents.value, doc1_value.value);
    assert_eq!(both_docs[1].contents.value, doc2_value.value);

    let both_headers = Basic::list(doc1.id..=doc2.id, db).headers()?;

    assert_eq!(both_headers.len(), 2);

    let one_doc = Basic::list(doc1.id..doc2.id, db).query()?;
    assert_eq!(one_doc.len(), 1);

    let limited = Basic::list(doc1.id..=doc2.id, db)
        .limit(1)
        .descending()
        .query()?;
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].contents.value, doc2_value.value);

    Ok(())
}

pub async fn list_transactions_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();

    // create LIST_TRANSACTIONS_MAX_RESULTS + 1 items, giving us just enough
    // transactions to test the edge cases of `list_transactions`
    futures::future::join_all(
        (0..=(LIST_TRANSACTIONS_MAX_RESULTS))
            .map(|_| async { collection.push(&Basic::default()).await.unwrap() }),
    )
    .await;

    // Test defaults
    let transactions = db.list_executed_transactions(None, None).await?;
    assert_eq!(
        u32::try_from(transactions.len()).unwrap(),
        LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT
    );

    // Test max results limit
    let transactions = db
        .list_executed_transactions(None, Some(LIST_TRANSACTIONS_MAX_RESULTS + 1))
        .await?;
    assert_eq!(
        u32::try_from(transactions.len()).unwrap(),
        LIST_TRANSACTIONS_MAX_RESULTS
    );

    // Test requesting 0 items
    let transactions = db.list_executed_transactions(None, Some(0)).await?;
    assert!(transactions.is_empty());

    // Test doing a loop fetching until we get no more results
    let mut transactions = Vec::new();
    let mut starting_id = None;
    loop {
        let chunk = db
            .list_executed_transactions(starting_id, Some(100))
            .await?;
        if chunk.is_empty() {
            break;
        }

        let max_id = chunk.last().map(|tx| tx.id).unwrap();
        starting_id = Some(max_id + 1);
        transactions.extend(chunk);
    }

    assert_eq!(
        u32::try_from(transactions.len()).unwrap(),
        LIST_TRANSACTIONS_MAX_RESULTS + 1
    );

    Ok(())
}

pub fn blocking_list_transactions_tests<C: Connection + Clone + 'static>(
    db: &C,
) -> anyhow::Result<()> {
    // create LIST_TRANSACTIONS_MAX_RESULTS + 1 items, giving us just enough
    // transactions to test the edge cases of `list_transactions`
    let mut threads = Vec::with_capacity(num_cpus::get());
    let transaction_counter = Arc::new(AtomicU32::new(0));
    for _ in 0..threads.capacity() {
        let db = db.clone();
        let transaction_counter = transaction_counter.clone();
        threads.push(std::thread::spawn(move || {
            let collection = db.collection::<Basic>();
            while transaction_counter.fetch_add(1, Ordering::SeqCst)
                <= LIST_TRANSACTIONS_MAX_RESULTS
            {
                collection.push(&Basic::default()).unwrap();
            }
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }

    // Test defaults
    let transactions = db.list_executed_transactions(None, None)?;
    assert_eq!(
        u32::try_from(transactions.len()).unwrap(),
        LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT
    );

    // Test max results limit
    let transactions =
        db.list_executed_transactions(None, Some(LIST_TRANSACTIONS_MAX_RESULTS + 1))?;
    assert_eq!(
        u32::try_from(transactions.len()).unwrap(),
        LIST_TRANSACTIONS_MAX_RESULTS
    );

    // Test requesting 0 items
    let transactions = db.list_executed_transactions(None, Some(0))?;
    assert!(transactions.is_empty());

    // Test doing a loop fetching until we get no more results
    let mut transactions = Vec::new();
    let mut starting_id = None;
    loop {
        let chunk = db.list_executed_transactions(starting_id, Some(100))?;
        if chunk.is_empty() {
            break;
        }

        let max_id = chunk.last().map(|tx| tx.id).unwrap();
        starting_id = Some(max_id + 1);
        transactions.extend(chunk);
    }

    assert_eq!(
        u32::try_from(transactions.len()).unwrap(),
        LIST_TRANSACTIONS_MAX_RESULTS + 1
    );

    Ok(())
}

pub async fn transaction_tests<C: AsyncConnection + 'static>(db: &C) -> anyhow::Result<()> {
    let mut tx = Transaction::new();
    Basic::new("test").push_in_transaction(&mut tx)?;
    let results = tx.apply_async(db).await?;
    let OperationResult::DocumentUpdated { header, .. } = &results[0] else {
        unreachable!("unexpected tx result")
    };
    let id: u64 = header.id.deserialize()?;

    // Update the doc
    let mut tx = Transaction::new();
    let mut doc = Basic::get_async(&id, db).await?.expect("doc not found");
    doc.contents.category = Some(String::from("cat"));
    doc.update_in_transaction(&mut tx)?;
    tx.apply_async(db).await?;
    let doc = Basic::get_async(&id, db).await?.expect("doc not found");
    assert_eq!(doc.contents.category.as_deref(), Some("cat"));

    // Overwrite the document.
    let mut tx = Transaction::new();
    Basic::new("test").overwrite_in_transaction(&id, &mut tx)?;
    tx.apply_async(db).await?;
    let doc = Basic::get_async(&id, db).await?.expect("doc not found");
    assert_eq!(doc.contents.category, None);

    // Delete the document
    let mut tx = Transaction::new();
    doc.delete_in_transaction(&mut tx)?;
    tx.apply_async(db).await?;

    Ok(())
}

pub fn blocking_transaction_tests<C: Connection + 'static>(db: &C) -> anyhow::Result<()> {
    let mut tx = Transaction::new();
    Basic::new("test").push_in_transaction(&mut tx)?;
    let results = tx.apply(db)?;
    let OperationResult::DocumentUpdated { header, .. } = &results[0] else {
        unreachable!("unexpected tx result")
    };
    let id: u64 = header.id.deserialize()?;

    // Update the doc
    let mut tx = Transaction::new();
    let mut doc = Basic::get(&id, db)?.expect("doc not found");
    doc.contents.category = Some(String::from("cat"));
    doc.update_in_transaction(&mut tx)?;
    tx.apply(db)?;
    let doc = Basic::get(&id, db)?.expect("doc not found");
    assert_eq!(doc.contents.category.as_deref(), Some("cat"));

    // Overwrite the document.
    let mut tx = Transaction::new();
    Basic::new("test").overwrite_in_transaction(&id, &mut tx)?;
    tx.apply(db)?;
    let doc = Basic::get(&id, db)?.expect("doc not found");
    assert_eq!(doc.contents.category, None);

    // Delete the document
    let mut tx = Transaction::new();
    doc.delete_in_transaction(&mut tx)?;
    tx.apply(db)?;

    Ok(())
}

pub async fn transaction_check_tests<C: AsyncConnection + 'static>(db: &C) -> anyhow::Result<()> {
    let mut doc = Basic::new("test").push_into_async(db).await?;
    let mut refreshable = doc.clone();
    let initial_header = doc.header;
    doc.contents.value = String::from("updated");
    doc.update_async(db).await?;

    refreshable.refresh_async(db).await?;
    assert_eq!(refreshable.contents.value, "updated");

    // Positive flow, check id, as well as id + header.
    let mut tx = Transaction::new();
    tx.push(Operation::check_document_exists::<Basic>(&doc.header.id)?);
    tx.push(Operation::check_document_is_current::<Basic, _>(&doc)?);
    tx.push(Operation::insert_serialized::<Basic>(
        None,
        &Basic::new("new doc"),
    )?);
    tx.apply_async(db).await?;

    // Error flows. Ensure the first violation is the error returned.
    let mut tx = Transaction::new();
    tx.push(Operation::check_document_is_current::<Basic, _>(
        &initial_header,
    )?);
    tx.push(Operation::check_document_exists::<Basic>(&42)?);
    let result = tx.apply_async(db).await.unwrap_err();
    assert!(matches!(result, Error::DocumentConflict(_, _)));

    let mut tx = Transaction::new();
    tx.push(Operation::check_document_exists::<Basic>(&42)?);
    tx.push(Operation::check_document_is_current::<Basic, _>(
        &initial_header,
    )?);
    let result = tx.apply_async(db).await.unwrap_err();
    assert!(matches!(result, Error::DocumentNotFound(_, _)));

    Ok(())
}

pub fn blocking_transaction_check_tests<C: Connection + 'static>(db: &C) -> anyhow::Result<()> {
    let mut doc = Basic::new("test").push_into(db)?;
    let mut refreshable = doc.clone();
    let initial_header = doc.header;
    doc.contents.value = String::from("updated");
    doc.update(db)?;

    refreshable.refresh(db)?;
    assert_eq!(refreshable.contents.value, "updated");

    // Positive flow, check id, as well as id + header.
    let mut tx = Transaction::new();
    tx.push(Operation::check_document_exists::<Basic>(&doc.header.id)?);
    tx.push(Operation::check_document_is_current::<Basic, _>(&doc)?);
    tx.push(Operation::insert_serialized::<Basic>(
        None,
        &Basic::new("new doc"),
    )?);
    tx.apply(db)?;

    // Error flows. Ensure the first violation is the error returned.
    let mut tx = Transaction::new();
    tx.push(Operation::check_document_is_current::<Basic, _>(
        &initial_header,
    )?);
    tx.push(Operation::check_document_exists::<Basic>(&42)?);
    let result = tx.apply(db).unwrap_err();
    assert!(matches!(result, Error::DocumentConflict(_, _)));

    let mut tx = Transaction::new();
    tx.push(Operation::check_document_exists::<Basic>(&42)?);
    tx.push(Operation::check_document_is_current::<Basic, _>(
        &initial_header,
    )?);
    let result = tx.apply(db).unwrap_err();
    assert!(matches!(result, Error::DocumentNotFound(_, _)));

    Ok(())
}

pub async fn view_query_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let a = collection.push(&Basic::new("A")).await?;
    let b = collection.push(&Basic::new("B")).await?;
    let a_child = collection
        .push(
            &Basic::new("A.1")
                .with_parent_id(a.id)
                .with_category("Alpha"),
        )
        .await?;
    collection
        .push(&Basic::new("B.1").with_parent_id(b.id).with_category("Beta"))
        .await?;
    collection
        .push(&Basic::new("B.2").with_parent_id(b.id).with_category("beta"))
        .await?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 1);

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .query_with_collection_docs()
        .await?;
    assert_eq!(a_children.len(), 1);
    assert_eq!(a_children.get(0).unwrap().document.header, a_child);

    let b_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(b.id))
        .query()
        .await?;
    assert_eq!(b_children.len(), 2);

    let a_and_b_children = db
        .view::<BasicByParentId>()
        .with_keys([&Some(a.id), &Some(b.id)])
        .query()
        .await?;
    assert_eq!(a_and_b_children.len(), 3);

    // Test out of order keys
    let a_and_b_children = db
        .view::<BasicByParentId>()
        .with_keys([&Some(b.id), &Some(a.id)])
        .query()
        .await?;
    assert_eq!(a_and_b_children.len(), 3);

    let has_parent = db
        .view::<BasicByParentId>()
        .with_key_range(Some(0)..=Some(u64::MAX))
        .query()
        .await?;
    assert_eq!(has_parent.len(), 3);
    // Verify the result is sorted ascending
    assert!(has_parent
        .windows(2)
        .all(|window| window[0].key <= window[1].key));

    // Test limiting and descending order
    let last_with_parent = db
        .view::<BasicByParentId>()
        .with_key_range(Some(0)..=Some(u64::MAX))
        .descending()
        .limit(1)
        .query()
        .await?;
    assert_eq!(last_with_parent.iter().map(|m| m.key).unique().count(), 1);
    assert_eq!(last_with_parent[0].key, has_parent[2].key);

    let items_with_categories = db.view::<BasicByCategory>().query().await?;
    assert_eq!(items_with_categories.len(), 3);

    // Test deleting
    let deleted_count = db
        .view::<BasicByParentId>()
        .with_key(&Some(b.id))
        .delete_docs()
        .await?;
    assert_eq!(b_children.len() as u64, deleted_count);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(b.id))
            .query()
            .await?
            .len(),
        0
    );

    Ok(())
}

pub fn blocking_view_query_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let a = collection.push(&Basic::new("A"))?;
    let b = collection.push(&Basic::new("B"))?;
    let a_child = collection.push(
        &Basic::new("A.1")
            .with_parent_id(a.id)
            .with_category("Alpha"),
    )?;
    collection.push(&Basic::new("B.1").with_parent_id(b.id).with_category("Beta"))?;
    collection.push(&Basic::new("B.2").with_parent_id(b.id).with_category("beta"))?;

    let a_children = db.view::<BasicByParentId>().with_key(&Some(a.id)).query()?;
    assert_eq!(a_children.len(), 1);

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .query_with_collection_docs()?;
    assert_eq!(a_children.len(), 1);
    assert_eq!(a_children.get(0).unwrap().document.header, a_child);

    let b_children = db.view::<BasicByParentId>().with_key(&Some(b.id)).query()?;
    assert_eq!(b_children.len(), 2);

    let a_and_b_children = db
        .view::<BasicByParentId>()
        .with_keys(&[Some(a.id), Some(b.id)])
        .query()?;
    assert_eq!(a_and_b_children.len(), 3);

    // Test out of order keys
    let a_and_b_children = db
        .view::<BasicByParentId>()
        .with_keys(&[Some(b.id), Some(a.id)])
        .query()?;
    assert_eq!(a_and_b_children.len(), 3);

    let has_parent = db
        .view::<BasicByParentId>()
        .with_key_range(Some(0)..=Some(u64::MAX))
        .query()?;
    assert_eq!(has_parent.len(), 3);
    // Verify the result is sorted ascending
    assert!(has_parent
        .windows(2)
        .all(|window| window[0].key <= window[1].key));

    // Test limiting and descending order
    let last_with_parent = db
        .view::<BasicByParentId>()
        .with_key_range(Some(0)..=Some(u64::MAX))
        .descending()
        .limit(1)
        .query()?;
    assert_eq!(last_with_parent.iter().map(|m| m.key).unique().count(), 1);
    assert_eq!(last_with_parent[0].key, has_parent[2].key);

    let items_with_categories = db.view::<BasicByCategory>().query()?;
    assert_eq!(items_with_categories.len(), 3);

    // Test deleting
    let deleted_count = db
        .view::<BasicByParentId>()
        .with_key(&Some(b.id))
        .delete_docs()?;
    assert_eq!(b_children.len() as u64, deleted_count);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(b.id))
            .query()?
            .len(),
        0
    );

    Ok(())
}

pub async fn unassociated_collection_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let result = db
        .collection::<UnassociatedCollection>()
        .push(&UnassociatedCollection)
        .await;
    match result {
        Err(Error::CollectionNotFound) => {}
        other => unreachable!("unexpected result: {:?}", other),
    }

    Ok(())
}

pub fn blocking_unassociated_collection_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let result = db
        .collection::<UnassociatedCollection>()
        .push(&UnassociatedCollection);
    match result {
        Err(Error::CollectionNotFound) => {}
        other => unreachable!("unexpected result: {:?}", other),
    }

    Ok(())
}

pub async fn unimplemented_reduce<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    assert!(matches!(
        db.view::<UniqueValue>().reduce().await,
        Err(Error::ReduceUnimplemented)
    ));
    Ok(())
}

pub fn blocking_unimplemented_reduce<C: Connection>(db: &C) -> anyhow::Result<()> {
    assert!(matches!(
        db.view::<UniqueValue>().reduce(),
        Err(Error::ReduceUnimplemented)
    ));
    Ok(())
}

pub async fn view_update_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let a = collection.push(&Basic::new("A")).await?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);
    // The reduce function of `BasicByParentId` acts as a "count" of records.
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .reduce()
            .await?,
        0
    );

    // Verify the eager view is available without any updates
    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .with_access_policy(AccessPolicy::NoUpdate)
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .with_access_policy(AccessPolicy::NoUpdate)
            .reduce()
            .await?,
        0
    );

    // Test inserting a new record and the view being made available
    let a_child = collection
        .push(
            &Basic::new("A.1")
                .with_parent_id(a.id)
                .with_category("Alpha"),
        )
        .await?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 1);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .reduce()
            .await?,
        1
    );

    // Verify reduce_grouped matches our expectations.
    assert_eq!(
        db.view::<BasicByParentId>().reduce_grouped().await?,
        vec![MappedValue::new(None, 1,), MappedValue::new(Some(a.id), 1,),]
    );

    // Test updating the record and the view being updated appropriately
    let b = collection.push(&Basic::new("B")).await?;
    let mut doc = db.collection::<Basic>().get(&a_child.id).await?.unwrap();
    let mut basic = Basic::document_contents(&doc)?;
    basic.parent_id = Some(b.id);
    Basic::set_document_contents(&mut doc, basic)?;
    db.update::<Basic, _>(&mut doc).await?;

    let b_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(b.id))
        .query()
        .await?;
    assert_eq!(b_children.len(), 1);
    assert_eq!(b_children[0].source, doc.header);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .reduce()
            .await?,
        0
    );
    assert_eq!(db.view::<BasicByParentId>().reduce().await?, 3);

    // Update the record, but don't change its mapping. Ensure the source's
    // header is updated.
    let mut basic = Basic::document_contents(&doc)?;
    basic.value = String::from("new value");
    Basic::set_document_contents(&mut doc, basic)?;
    db.update::<Basic, _>(&mut doc).await?;
    let b_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(b.id))
        .query()
        .await?;
    assert_eq!(b_children.len(), 1);
    assert_eq!(b_children[0].source, doc.header);

    // Test deleting a record and ensuring it goes away
    db.collection::<Basic>().delete(&doc).await?;

    let all_entries = db.view::<BasicByParentId>().query().await?;
    assert_eq!(all_entries.len(), 2);

    // Verify reduce_grouped matches our expectations.
    assert_eq!(
        db.view::<BasicByParentId>().reduce_grouped().await?,
        vec![MappedValue::new(None, 2,),]
    );

    // Remove the final document, which has a parent id of None. We'll add a new
    // document with None to verify that the mapper handles the edge case of a
    // delete/insert in the same mapping operation.
    db.view::<BasicByParentId>().delete_docs().await?;
    collection.push(&Basic::new("B")).await?;

    let all_entries = db.view::<BasicByParentId>().query().await?;
    assert_eq!(all_entries.len(), 1);

    Ok(())
}

pub fn blocking_view_update_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let a = collection.push(&Basic::new("A"))?;

    let a_children = db.view::<BasicByParentId>().with_key(&Some(a.id)).query()?;
    assert_eq!(a_children.len(), 0);
    // The reduce function of `BasicByParentId` acts as a "count" of records.
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .reduce()?,
        0
    );

    // Verify the eager view is available without any updates
    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .with_access_policy(AccessPolicy::NoUpdate)
        .query()?;
    assert_eq!(a_children.len(), 0);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .with_access_policy(AccessPolicy::NoUpdate)
            .reduce()?,
        0
    );

    // Test inserting a new record and the view being made available
    let a_child = collection.push(
        &Basic::new("A.1")
            .with_parent_id(a.id)
            .with_category("Alpha"),
    )?;

    let a_children = db.view::<BasicByParentId>().with_key(&Some(a.id)).query()?;
    assert_eq!(a_children.len(), 1);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .reduce()?,
        1
    );

    // Verify reduce_grouped matches our expectations.
    assert_eq!(
        db.view::<BasicByParentId>().reduce_grouped()?,
        vec![MappedValue::new(None, 1,), MappedValue::new(Some(a.id), 1,),]
    );

    // Test updating the record and the view being updated appropriately
    let b = collection.push(&Basic::new("B"))?;
    let mut doc = db.collection::<Basic>().get(&a_child.id)?.unwrap();
    let mut basic = Basic::document_contents(&doc)?;
    basic.parent_id = Some(b.id);
    Basic::set_document_contents(&mut doc, basic)?;
    db.update::<Basic, _>(&mut doc)?;

    let b_children = db.view::<BasicByParentId>().with_key(&Some(b.id)).query()?;
    assert_eq!(b_children.len(), 1);
    assert_eq!(b_children[0].source, doc.header);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .reduce()?,
        0
    );
    assert_eq!(db.view::<BasicByParentId>().reduce()?, 3);

    // Update the record, but don't change its mapping. Ensure the source's
    // header is updated.
    let mut basic = Basic::document_contents(&doc)?;
    basic.value = String::from("new value");
    Basic::set_document_contents(&mut doc, basic)?;
    db.update::<Basic, _>(&mut doc)?;
    let b_children = db.view::<BasicByParentId>().with_key(&Some(b.id)).query()?;
    assert_eq!(b_children.len(), 1);
    assert_eq!(b_children[0].source, doc.header);

    // Test deleting a record and ensuring it goes away
    db.collection::<Basic>().delete(&doc)?;

    let all_entries = db.view::<BasicByParentId>().query()?;
    assert_eq!(all_entries.len(), 2);

    // Verify reduce_grouped matches our expectations.
    assert_eq!(
        db.view::<BasicByParentId>().reduce_grouped()?,
        vec![MappedValue::new(None, 2,),]
    );

    // Remove the final document, which has a parent id of None. We'll add a new
    // document with None to verify that the mapper handles the edge case of a
    // delete/insert in the same mapping operation.
    db.view::<BasicByParentId>().delete_docs()?;
    collection.push(&Basic::new("B"))?;

    let all_entries = db.view::<BasicByParentId>().query()?;
    assert_eq!(all_entries.len(), 1);

    Ok(())
}

pub async fn view_multi_emit_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let mut a = Basic::new("A")
        .with_tag("red")
        .with_tag("green")
        .push_into_async(db)
        .await?;
    let mut b = Basic::new("B")
        .with_tag("blue")
        .with_tag("green")
        .push_into_async(db)
        .await?;

    assert_eq!(db.view::<BasicByTag>().query().await?.len(), 4);

    assert_eq!(
        db.view::<BasicByTag>()
            .with_key("green")
            .query()
            .await?
            .len(),
        2
    );

    assert_eq!(
        db.view::<BasicByTag>().with_key("red").query().await?.len(),
        1
    );

    assert_eq!(
        db.view::<BasicByTag>()
            .with_key("blue")
            .query()
            .await?
            .len(),
        1
    );

    // Change tags
    a.contents.tags = vec![String::from("red"), String::from("blue")];
    a.update_async(db).await?;

    assert_eq!(
        db.view::<BasicByTag>()
            .with_key("green")
            .query()
            .await?
            .len(),
        1
    );

    assert_eq!(
        db.view::<BasicByTag>().with_key("red").query().await?.len(),
        1
    );

    assert_eq!(
        db.view::<BasicByTag>()
            .with_key("blue")
            .query()
            .await?
            .len(),
        2
    );
    b.contents.tags.clear();
    b.update_async(db).await?;

    assert_eq!(
        db.view::<BasicByTag>()
            .with_key("green")
            .query()
            .await?
            .len(),
        0
    );

    assert_eq!(
        db.view::<BasicByTag>().with_key("red").query().await?.len(),
        1
    );

    assert_eq!(
        db.view::<BasicByTag>()
            .with_key("blue")
            .query()
            .await?
            .len(),
        1
    );

    Ok(())
}

pub fn blocking_view_multi_emit_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let mut a = Basic::new("A")
        .with_tag("red")
        .with_tag("green")
        .push_into(db)?;
    let mut b = Basic::new("B")
        .with_tag("blue")
        .with_tag("green")
        .push_into(db)?;

    assert_eq!(db.view::<BasicByTag>().query()?.len(), 4);

    assert_eq!(db.view::<BasicByTag>().with_key("green").query()?.len(), 2);

    assert_eq!(db.view::<BasicByTag>().with_key("red").query()?.len(), 1);

    assert_eq!(db.view::<BasicByTag>().with_key("blue").query()?.len(), 1);

    // Change tags
    a.contents.tags = vec![String::from("red"), String::from("blue")];
    a.update(db)?;

    assert_eq!(db.view::<BasicByTag>().with_key("green").query()?.len(), 1);

    assert_eq!(db.view::<BasicByTag>().with_key("red").query()?.len(), 1);

    assert_eq!(db.view::<BasicByTag>().with_key("blue").query()?.len(), 2);
    b.contents.tags.clear();
    b.update(db)?;

    assert_eq!(db.view::<BasicByTag>().with_key("green").query()?.len(), 0);

    assert_eq!(db.view::<BasicByTag>().with_key("red").query()?.len(), 1);

    assert_eq!(db.view::<BasicByTag>().with_key("blue").query()?.len(), 1);

    Ok(())
}

pub async fn view_access_policy_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();

    // Query the view to get it into a consistent, mapped state. If we don't
    // query it, the integrity checker can end up spawning a mapping job. This
    // job would run in the background and can include the next documents being
    // inserted.
    let all_entries = db.view::<BasicByParentId>().query().await?;
    assert_eq!(all_entries.len(), 0);

    let a = collection.push(&Basic::new("A")).await?;

    // Test inserting a record that should match the view, but ask for it to be
    // NoUpdate. Verify we get no matches.
    collection
        .push(
            &Basic::new("A.1")
                .with_parent_id(a.id)
                .with_category("Alpha"),
        )
        .await?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .with_access_policy(AccessPolicy::NoUpdate)
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Verify the view still have no value, but this time ask for it to be
    // updated after returning
    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .with_access_policy(AccessPolicy::UpdateAfter)
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);

    // Waiting on background jobs can be unreliable in a CI environment
    for _ in 0..10_u8 {
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Now, the view should contain the entry.
        let a_children = db
            .view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .with_access_policy(AccessPolicy::NoUpdate)
            .query()
            .await?;
        if a_children.len() == 1 {
            return Ok(());
        }
    }
    panic!("view never updated")
}

pub fn blocking_view_access_policy_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    // Query the view to get it into a consistent, mapped state. If we don't
    // query it, the integrity checker can end up spawning a mapping job. This
    // job would run in the background and can include the next documents being
    // inserted.
    let all_entries = db.view::<BasicByParentId>().query()?;
    assert_eq!(all_entries.len(), 0);

    let collection = db.collection::<Basic>();
    let a = collection.push(&Basic::new("A"))?;

    // Test inserting a record that should match the view, but ask for it to be
    // NoUpdate. Verify we get no matches.
    collection.push(
        &Basic::new("A.1")
            .with_parent_id(a.id)
            .with_category("Alpha"),
    )?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .with_access_policy(AccessPolicy::NoUpdate)
        .query()?;
    assert_eq!(a_children.len(), 0);

    std::thread::sleep(Duration::from_millis(20));

    // Verify the view still have no value, but this time ask for it to be
    // updated after returning
    let a_children = db
        .view::<BasicByParentId>()
        .with_key(&Some(a.id))
        .with_access_policy(AccessPolicy::UpdateAfter)
        .query()?;
    assert_eq!(a_children.len(), 0);

    // Waiting on background jobs can be unreliable in a CI environment
    for _ in 0..10_u8 {
        std::thread::sleep(Duration::from_millis(20));

        // Now, the view should contain the entry.
        let a_children = db
            .view::<BasicByParentId>()
            .with_key(&Some(a.id))
            .with_access_policy(AccessPolicy::NoUpdate)
            .query()?;
        if a_children.len() == 1 {
            return Ok(());
        }
    }
    panic!("view never updated")
}

pub async fn unique_view_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    let first_doc = db.collection::<Unique>().push(&Unique::new("1")).await?;

    if let Err(Error::UniqueKeyViolation {
        view,
        existing_document,
        conflicting_document,
    }) = db.collection::<Unique>().push(&Unique::new("1")).await
    {
        assert_eq!(view, UniqueValue.view_name());
        assert_eq!(first_doc.id, existing_document.id.deserialize()?);
        // We can't predict the conflicting document id since it's generated
        // inside of the transaction, but we can assert that it's different than
        // the document that was previously stored.
        assert_ne!(conflicting_document, existing_document);
    } else {
        unreachable!("unique key violation not triggered");
    }

    let second_doc = db.collection::<Unique>().push(&Unique::new("2")).await?;
    let mut second_doc = db
        .collection::<Unique>()
        .get(&second_doc.id)
        .await?
        .unwrap();
    let mut contents = Unique::document_contents(&second_doc)?;
    contents.value = String::from("1");
    Unique::set_document_contents(&mut second_doc, contents)?;
    if let Err(Error::UniqueKeyViolation {
        view,
        existing_document,
        conflicting_document,
    }) = db.update::<Unique, _>(&mut second_doc).await
    {
        assert_eq!(view, UniqueValue.view_name());
        assert_eq!(first_doc.id, existing_document.id.deserialize()?);
        assert_eq!(conflicting_document.id, second_doc.header.id);
    } else {
        unreachable!("unique key violation not triggered");
    }

    Ok(())
}

pub fn blocking_unique_view_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let first_doc = db.collection::<Unique>().push(&Unique::new("1"))?;

    if let Err(Error::UniqueKeyViolation {
        view,
        existing_document,
        conflicting_document,
    }) = db.collection::<Unique>().push(&Unique::new("1"))
    {
        assert_eq!(view, UniqueValue.view_name());
        assert_eq!(first_doc.id, existing_document.id.deserialize()?);
        // We can't predict the conflicting document id since it's generated
        // inside of the transaction, but we can assert that it's different than
        // the document that was previously stored.
        assert_ne!(conflicting_document, existing_document);
    } else {
        unreachable!("unique key violation not triggered");
    }

    let second_doc = db.collection::<Unique>().push(&Unique::new("2"))?;
    let mut second_doc = db.collection::<Unique>().get(&second_doc.id)?.unwrap();
    let mut contents = Unique::document_contents(&second_doc)?;
    contents.value = String::from("1");
    Unique::set_document_contents(&mut second_doc, contents)?;
    if let Err(Error::UniqueKeyViolation {
        view,
        existing_document,
        conflicting_document,
    }) = db.update::<Unique, _>(&mut second_doc)
    {
        assert_eq!(view, UniqueValue.view_name());
        assert_eq!(first_doc.id, existing_document.id.deserialize()?);
        assert_eq!(conflicting_document.id, second_doc.header.id);
    } else {
        unreachable!("unique key violation not triggered");
    }

    Ok(())
}

pub fn blocking_cow_views<C: Connection>(db: &C) -> anyhow::Result<()> {
    db.collection::<Basic>()
        .push(&Basic::new("test").with_category("category"))?;

    let mapping = BasicByCategoryCow::entries(db)
        .query()?
        .into_iter()
        .next()
        .expect("mapping not found");
    assert_eq!(mapping.key, "category");
    assert_eq!(BasicByCategoryCow::entries(db).reduce()?, 1);

    Ok(())
}

pub async fn named_collection_tests<C: AsyncConnection>(db: &C) -> anyhow::Result<()> {
    Unique::new("0").push_into_async(db).await?;
    let original_entry = Unique::entry_async("1", db)
        .update_with(|_existing: &mut Unique| unreachable!())
        .or_insert_with(|| Unique::new("1"))
        .await?
        .expect("Document not inserted");

    let updated = Unique::entry_async("1", db)
        .update_with(|existing: &mut Unique| {
            existing.value = String::from("2");
        })
        .or_insert_with(|| unreachable!())
        .await?
        .unwrap();
    assert_eq!(original_entry.header.id, updated.header.id);
    assert_ne!(original_entry.contents.value, updated.contents.value);

    let retrieved = Unique::entry_async("2", db).await?.unwrap();
    assert_eq!(retrieved.contents.value, updated.contents.value);

    let conflict = Unique::entry_async("2", db)
        .update_with(|existing: &mut Unique| {
            existing.value = String::from("0");
        })
        .await;
    assert!(matches!(conflict, Err(Error::UniqueKeyViolation { .. })));

    Ok(())
}

pub fn blocking_named_collection_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    Unique::new("0").push_into(db)?;
    let original_entry = Unique::entry("1", db)
        .update_with(|_existing: &mut Unique| unreachable!())
        .or_insert_with(|| Unique::new("1"))
        .execute()?
        .expect("Document not inserted");

    let updated = Unique::entry("1", db)
        .update_with(|existing: &mut Unique| {
            existing.value = String::from("2");
        })
        .or_insert_with(|| unreachable!())
        .execute()?
        .unwrap();
    assert_eq!(original_entry.header.id, updated.header.id);
    assert_ne!(original_entry.contents.value, updated.contents.value);

    let retrieved = Unique::entry("2", db).execute()?.unwrap();
    assert_eq!(retrieved.contents.value, updated.contents.value);

    let conflict = Unique::entry("2", db)
        .update_with(|existing: &mut Unique| {
            existing.value = String::from("0");
        })
        .execute();
    assert!(matches!(conflict, Err(Error::UniqueKeyViolation { .. })));

    Ok(())
}

pub async fn compaction_tests<C: AsyncConnection + AsyncKeyValue>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    collection.push(&original_value).await?;

    // Test a collection compaction
    db.compact_collection::<Basic>().await?;

    // Test the key value store compaction
    db.set_key("foo", &1_u32).await?;
    db.compact_key_value_store().await?;

    // Compact everything... again...
    db.compact().await?;

    Ok(())
}

pub fn blocking_compaction_tests<C: Connection + KeyValue>(db: &C) -> anyhow::Result<()> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    collection.push(&original_value)?;

    // Test a collection compaction
    db.compact_collection::<Basic>()?;

    // Test the key value store compaction
    db.set_key("foo", &1_u32).execute()?;
    db.compact_key_value_store()?;

    // Compact everything... again...
    db.compact()?;

    Ok(())
}

pub async fn user_management_tests<C: AsyncConnection, S: AsyncStorageConnection>(
    admin: &C,
    server: S,
    server_name: &str,
) -> anyhow::Result<()> {
    let username = format!("user-management-tests-{server_name}");
    let user_id = server.create_user(&username).await?;
    // Test the default created user state.
    {
        let user = User::get_async(&user_id, admin)
            .await
            .unwrap()
            .expect("user not found");
        assert_eq!(user.contents.username, username);
        assert!(user.contents.groups.is_empty());
        assert!(user.contents.roles.is_empty());
    }

    let role = Role::named(format!("role-{server_name}"))
        .push_into_async(admin)
        .await
        .unwrap();
    let group = PermissionGroup::named(format!("group-{server_name}"))
        .push_into_async(admin)
        .await
        .unwrap();

    // Add the role and group.
    server
        .add_permission_group_to_user(user_id, &group)
        .await
        .unwrap();
    server.add_role_to_user(user_id, &role).await.unwrap();

    // Test the results
    {
        let user = User::get_async(&user_id, admin)
            .await
            .unwrap()
            .expect("user not found");
        assert_eq!(user.contents.groups, vec![group.header.id]);
        assert_eq!(user.contents.roles, vec![role.header.id]);
    }

    // Add the same things again (should not do anything). With names this time.
    server
        .add_permission_group_to_user(&username, &group)
        .await
        .unwrap();
    server.add_role_to_user(&username, &role).await.unwrap();
    {
        let user = User::load_async(&username, admin)
            .await
            .unwrap()
            .expect("user not found");
        assert_eq!(user.contents.groups, vec![group.header.id]);
        assert_eq!(user.contents.roles, vec![role.header.id]);
    }

    // Remove the group.
    server
        .remove_permission_group_from_user(user_id, &group)
        .await
        .unwrap();
    server.remove_role_from_user(user_id, &role).await.unwrap();
    {
        let user = User::get_async(&user_id, admin)
            .await
            .unwrap()
            .expect("user not found");
        assert!(user.contents.groups.is_empty());
        assert!(user.contents.roles.is_empty());
    }

    // Removing again shouldn't cause an error.
    server
        .remove_permission_group_from_user(user_id, &group)
        .await?;
    server.remove_role_from_user(user_id, &role).await?;

    // Remove the user
    server.delete_user(user_id).await?;
    // Test if user is removed.

    assert!(User::get_async(&user_id, admin).await.unwrap().is_none());

    Ok(())
}

pub fn blocking_user_management_tests<C: Connection, S: StorageConnection>(
    admin: &C,
    server: &S,
    server_name: &str,
) -> anyhow::Result<()> {
    let username = format!("user-management-tests-{server_name}");
    let user_id = server.create_user(&username)?;
    // Test the default created user state.
    {
        let user = User::get(&user_id, admin).unwrap().expect("user not found");
        assert_eq!(user.contents.username, username);
        assert!(user.contents.groups.is_empty());
        assert!(user.contents.roles.is_empty());
    }

    let role = Role::named(format!("role-{server_name}"))
        .push_into(admin)
        .unwrap();
    let group = PermissionGroup::named(format!("group-{server_name}"))
        .push_into(admin)
        .unwrap();

    // Add the role and group.
    server
        .add_permission_group_to_user(user_id, &group)
        .unwrap();
    server.add_role_to_user(user_id, &role).unwrap();

    // Test the results
    {
        let user = User::get(&user_id, admin).unwrap().expect("user not found");
        assert_eq!(user.contents.groups, vec![group.header.id]);
        assert_eq!(user.contents.roles, vec![role.header.id]);
    }

    // Add the same things again (should not do anything). With names this time.
    server
        .add_permission_group_to_user(&username, &group)
        .unwrap();
    server.add_role_to_user(&username, &role).unwrap();
    {
        let user = User::load(&username, admin)
            .unwrap()
            .expect("user not found");
        assert_eq!(user.contents.groups, vec![group.header.id]);
        assert_eq!(user.contents.roles, vec![role.header.id]);
    }

    // Remove the group.
    server
        .remove_permission_group_from_user(user_id, &group)
        .unwrap();
    server.remove_role_from_user(user_id, &role).unwrap();
    {
        let user = User::get(&user_id, admin).unwrap().expect("user not found");
        assert!(user.contents.groups.is_empty());
        assert!(user.contents.roles.is_empty());
    }

    // Removing again shouldn't cause an error.
    server.remove_permission_group_from_user(user_id, &group)?;
    server.remove_role_from_user(user_id, &role)?;

    // Remove the user
    server.delete_user(user_id)?;
    // Test if user is removed.

    assert!(User::get(&user_id, admin).unwrap().is_none());

    Ok(())
}

#[cfg(feature = "token-authentication")]
pub async fn token_authentication_tests<C: AsyncConnection, S: AsyncStorageConnection>(
    admin: &C,
    server: &S,
    server_name: &str,
) -> anyhow::Result<()> {
    let username = format!("token-authentication-tests-{server_name}");
    let user_id = server.create_user(&username).await?;
    let user_token =
        AuthenticationToken::create_async(IdentityReference::user(&username)?, admin).await?;

    let as_user = server
        .authenticate_with_token(user_token.header.id, &user_token.contents.token)
        .await?;
    let identity = as_user.session().and_then(Session::identity);
    if let Some(Identity::User { id, .. }) = identity {
        assert_eq!(*id, user_id);
    }

    let role = Role::named(format!("token-role-{server_name}"))
        .push_into_async(admin)
        .await
        .unwrap();
    let role_token =
        AuthenticationToken::create_async(IdentityReference::role(role.header.id)?, admin).await?;

    let as_role = server
        .authenticate_with_token(role_token.header.id, &role_token.contents.token)
        .await?;
    let identity = as_role.session().and_then(Session::identity);
    if let Some(Identity::Role { id, .. }) = identity {
        assert_eq!(*id, role.header.id);
    }

    Ok(())
}

#[cfg(feature = "token-authentication")]
pub fn blocking_token_authentication_tests<C: Connection, S: StorageConnection>(
    admin: &C,
    server: &S,
    server_name: &str,
) -> anyhow::Result<()> {
    let username = format!("blocking-token-authentication-tests-{server_name}");
    let user_id = server.create_user(&username)?;
    let user_token = AuthenticationToken::create(&IdentityReference::user(&username)?, admin)?;

    let as_user =
        server.authenticate_with_token(user_token.header.id, &user_token.contents.token)?;
    let identity = as_user.session().and_then(Session::identity);
    if let Some(Identity::User { id, .. }) = identity {
        assert_eq!(*id, user_id);
    }

    let role = Role::named(format!("token-role-{server_name}"))
        .push_into(admin)
        .unwrap();
    let role_token = AuthenticationToken::create(&IdentityReference::role(role.header.id)?, admin)?;

    let as_role =
        server.authenticate_with_token(role_token.header.id, &role_token.contents.token)?;
    let identity = as_role.session().and_then(Session::identity);
    if let Some(Identity::Role { id, .. }) = identity {
        assert_eq!(*id, role.header.id);
    }

    Ok(())
}

/// Defines the `KeyValue` test suite
#[macro_export]
macro_rules! define_async_kv_test_suite {
    ($harness:ident) => {
        #[cfg(test)]
        mod r#async_kv {
            use super::$harness;
            #[tokio::test]
            async fn basic_kv_test() -> anyhow::Result<()> {
                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};
                let harness = $harness::new($crate::test_util::HarnessTest::KvBasic).await?;
                let db = harness.connect().await?;
                assert_eq!(
                    db.set_key("akey", &String::from("avalue")).await?,
                    KeyStatus::Inserted
                );
                assert_eq!(
                    db.get_key("akey").into().await?,
                    Some(String::from("avalue"))
                );
                assert_eq!(
                    db.set_key("akey", &String::from("new_value"))
                        .returning_previous_as()
                        .await?,
                    Some(String::from("avalue"))
                );
                assert_eq!(
                    db.get_key("akey").into().await?,
                    Some(String::from("new_value"))
                );
                assert_eq!(
                    db.get_key("akey").and_delete().into().await?,
                    Some(String::from("new_value"))
                );
                assert_eq!(db.get_key("akey").await?, None);
                assert_eq!(
                    db.set_key("akey", &String::from("new_value"))
                        .returning_previous()
                        .await?,
                    None
                );
                assert_eq!(db.delete_key("akey").await?, KeyStatus::Deleted);
                assert_eq!(db.delete_key("akey").await?, KeyStatus::NotChanged);

                harness.shutdown().await?;

                Ok(())
            }

            #[tokio::test]
            async fn kv_concurrency() -> anyhow::Result<()> {
                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};
                const WRITERS: usize = 100;
                const INCREMENTS: usize = 100;
                let harness = $harness::new($crate::test_util::HarnessTest::KvConcurrency).await?;
                let db = harness.connect().await?;

                let handles = (0..WRITERS).map(|_| {
                    let db = db.clone();
                    tokio::task::spawn(async move {
                        for _ in 0..INCREMENTS {
                            db.increment_key_by("concurrency", 1_u64).await.unwrap();
                        }
                    })
                });
                futures::future::join_all(handles).await;

                assert_eq!(
                    db.get_key("concurrency").into_u64().await.unwrap().unwrap(),
                    (WRITERS * INCREMENTS) as u64
                );

                harness.shutdown().await?;

                Ok(())
            }

            #[tokio::test]
            async fn kv_set_tests() -> anyhow::Result<()> {
                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};
                let harness = $harness::new($crate::test_util::HarnessTest::KvSet).await?;
                let db = harness.connect().await?;
                let kv = db.with_key_namespace("set");

                assert_eq!(
                    kv.set_key("a", &0_u32).only_if_exists().await?,
                    KeyStatus::NotChanged
                );
                assert_eq!(
                    kv.set_key("a", &0_u32).only_if_vacant().await?,
                    KeyStatus::Inserted
                );
                assert_eq!(
                    kv.set_key("a", &1_u32).only_if_vacant().await?,
                    KeyStatus::NotChanged
                );
                assert_eq!(
                    kv.set_key("a", &2_u32).only_if_exists().await?,
                    KeyStatus::Updated,
                );
                assert_eq!(
                    kv.set_key("a", &3_u32).returning_previous_as().await?,
                    Some(2_u32),
                );

                harness.shutdown().await?;

                Ok(())
            }

            #[tokio::test]
            async fn kv_increment_decrement_tests() -> anyhow::Result<()> {
                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};
                let harness =
                    $harness::new($crate::test_util::HarnessTest::KvIncrementDecrement).await?;
                let db = harness.connect().await?;
                let kv = db.with_key_namespace("increment_decrement");

                // Empty keys should be equal to 0
                assert_eq!(kv.increment_key_by("i64", 1_i64).await?, 1_i64);
                assert_eq!(kv.get_key("i64").into_i64().await?, Some(1_i64));
                assert_eq!(kv.increment_key_by("u64", 1_u64).await?, 1_u64);
                $crate::assert_f64_eq!(kv.increment_key_by("f64", 1_f64).await?, 1_f64);

                // Test float incrementing/decrementing an existing value
                $crate::assert_f64_eq!(kv.increment_key_by("f64", 1_f64).await?, 2_f64);
                $crate::assert_f64_eq!(kv.decrement_key_by("f64", 2_f64).await?, 0_f64);

                // Empty keys should be equal to 0
                assert_eq!(kv.decrement_key_by("i64_2", 1_i64).await?, -1_i64);
                assert_eq!(
                    kv.decrement_key_by("u64_2", 42_u64)
                        .allow_overflow()
                        .await?,
                    u64::MAX - 41
                );
                assert_eq!(kv.decrement_key_by("u64_3", 42_u64).await?, u64::MIN);
                $crate::assert_f64_eq!(kv.decrement_key_by("f64_2", 1_f64).await?, -1_f64);

                // Test decrement wrapping with overflow
                kv.set_numeric_key("i64", i64::MIN).await?;
                assert_eq!(
                    kv.decrement_key_by("i64", 1_i64).allow_overflow().await?,
                    i64::MAX
                );
                assert_eq!(
                    kv.decrement_key_by("u64", 2_u64).allow_overflow().await?,
                    u64::MAX
                );

                // Test increment wrapping with overflow
                assert_eq!(
                    kv.increment_key_by("i64", 1_i64).allow_overflow().await?,
                    i64::MIN
                );
                assert_eq!(
                    kv.increment_key_by("u64", 1_u64).allow_overflow().await?,
                    u64::MIN
                );

                // Test saturating increments.
                kv.set_numeric_key("i64", i64::MAX - 1).await?;
                kv.set_numeric_key("u64", u64::MAX - 1).await?;
                assert_eq!(kv.increment_key_by("i64", 2_i64).await?, i64::MAX);
                assert_eq!(kv.increment_key_by("u64", 2_u64).await?, u64::MAX);

                // Test saturating decrements.
                kv.set_numeric_key("i64", i64::MIN + 1).await?;
                kv.set_numeric_key("u64", u64::MIN + 1).await?;
                assert_eq!(kv.decrement_key_by("i64", 2_i64).await?, i64::MIN);
                assert_eq!(kv.decrement_key_by("u64", 2_u64).await?, u64::MIN);

                // Test numerical conversion safety using get
                {
                    // For i64 -> f64, the limit is 2^52 + 1 in either posive or
                    // negative directions.
                    kv.set_numeric_key("i64", (2_i64.pow(f64::MANTISSA_DIGITS)))
                        .await?;
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64().await?.unwrap(),
                        9_007_199_254_740_992_f64
                    );
                    kv.set_numeric_key("i64", -(2_i64.pow(f64::MANTISSA_DIGITS)))
                        .await?;
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64().await?.unwrap(),
                        -9_007_199_254_740_992_f64
                    );

                    kv.set_numeric_key("i64", (2_i64.pow(f64::MANTISSA_DIGITS) + 1))
                        .await?;
                    assert!(kv.get_key("i64").into_f64().await.is_err());
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64_lossy().await?.unwrap(),
                        9_007_199_254_740_993_f64
                    );
                    kv.set_numeric_key("i64", -(2_i64.pow(f64::MANTISSA_DIGITS) + 1))
                        .await?;
                    assert!(kv.get_key("i64").into_f64().await.is_err());
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64_lossy().await?.unwrap(),
                        -9_007_199_254_740_993_f64
                    );

                    // For i64 -> u64, the only limit is sign.
                    kv.set_numeric_key("i64", -1_i64).await?;
                    assert!(kv.get_key("i64").into_u64().await.is_err());
                    assert_eq!(
                        kv.get_key("i64").into_u64_lossy(true).await?.unwrap(),
                        0_u64
                    );
                    assert_eq!(
                        kv.get_key("i64").into_u64_lossy(false).await?.unwrap(),
                        u64::MAX
                    );

                    // For f64 -> i64, the limit is fractional numbers. Saturating isn't tested in this conversion path.
                    kv.set_numeric_key("f64", 1.1_f64).await?;
                    assert!(kv.get_key("f64").into_i64().await.is_err());
                    assert_eq!(
                        kv.get_key("f64").into_i64_lossy(false).await?.unwrap(),
                        1_i64
                    );
                    kv.set_numeric_key("f64", -1.1_f64).await?;
                    assert!(kv.get_key("f64").into_i64().await.is_err());
                    assert_eq!(
                        kv.get_key("f64").into_i64_lossy(false).await?.unwrap(),
                        -1_i64
                    );

                    // For f64 -> u64, the limit is fractional numbers or negative numbers. Saturating isn't tested in this conversion path.
                    kv.set_numeric_key("f64", 1.1_f64).await?;
                    assert!(kv.get_key("f64").into_u64().await.is_err());
                    assert_eq!(
                        kv.get_key("f64").into_u64_lossy(false).await?.unwrap(),
                        1_u64
                    );
                    kv.set_numeric_key("f64", -1.1_f64).await?;
                    assert!(kv.get_key("f64").into_u64().await.is_err());
                    assert_eq!(
                        kv.get_key("f64").into_u64_lossy(false).await?.unwrap(),
                        0_u64
                    );

                    // For u64 -> i64, the limit is > i64::MAX
                    kv.set_numeric_key("u64", i64::MAX as u64 + 1).await?;
                    assert!(kv.get_key("u64").into_i64().await.is_err());
                    assert_eq!(
                        kv.get_key("u64").into_i64_lossy(true).await?.unwrap(),
                        i64::MAX
                    );
                    assert_eq!(
                        kv.get_key("u64").into_i64_lossy(false).await?.unwrap(),
                        i64::MIN
                    );
                }

                // Test that non-numeric keys won't be changed when attempting to incr/decr
                kv.set_key("non-numeric", &String::from("test")).await?;
                assert!(kv.increment_key_by("non-numeric", 1_i64).await.is_err());
                assert!(kv.decrement_key_by("non-numeric", 1_i64).await.is_err());
                assert_eq!(
                    kv.get_key("non-numeric").into::<String>().await?.unwrap(),
                    String::from("test")
                );

                // Test that NaN cannot be stored
                kv.set_numeric_key("f64", 0_f64).await?;
                assert!(matches!(
                    kv.set_numeric_key("f64", f64::NAN).await,
                    Err(bonsaidb_core::Error::NotANumber)
                ));
                // Verify the value was unchanged.
                $crate::assert_f64_eq!(kv.get_key("f64").into_f64().await?.unwrap(), 0.);
                // Try to increment by nan
                assert!(matches!(
                    kv.increment_key_by("f64", f64::NAN).await,
                    Err(bonsaidb_core::Error::NotANumber)
                ));
                $crate::assert_f64_eq!(kv.get_key("f64").into_f64().await?.unwrap(), 0.);

                harness.shutdown().await?;

                Ok(())
            }

            #[tokio::test]
            async fn kv_expiration_tests() -> anyhow::Result<()> {
                use std::time::Duration;

                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};

                let harness = $harness::new($crate::test_util::HarnessTest::KvExpiration).await?;
                let db = harness.connect().await?;

                loop {
                    let kv = db.with_key_namespace("expiration");

                    kv.delete_key("a").await?;
                    kv.delete_key("b").await?;

                    // Test that the expiration is updated for key a, but not for key b.
                    let timing = $crate::test_util::TimingTest::new(Duration::from_millis(500));
                    let (r1, r2) = tokio::join!(
                        kv.set_key("a", &0_u32).expire_in(Duration::from_secs(2)),
                        kv.set_key("b", &0_u32).expire_in(Duration::from_secs(2))
                    );
                    if timing.elapsed() > Duration::from_millis(500) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed(),
                        );
                        continue;
                    }
                    assert_eq!(r1?, KeyStatus::Inserted);
                    assert_eq!(r2?, KeyStatus::Inserted);
                    let (r1, r2) = tokio::join!(
                        kv.set_key("a", &1_u32).expire_in(Duration::from_secs(4)),
                        kv.set_key("b", &1_u32)
                            .expire_in(Duration::from_secs(100))
                            .keep_existing_expiration()
                    );
                    if timing.elapsed() > Duration::from_secs(1) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed(),
                        );
                        continue;
                    }

                    assert_eq!(r1?, KeyStatus::Updated, "a wasn't an update");
                    assert_eq!(r2?, KeyStatus::Updated, "b wasn't an update");

                    let a = kv.get_key("a").into().await?;
                    assert_eq!(a, Some(1u32), "a shouldn't have expired yet");

                    // Before checking the value, make sure we haven't elapsed too
                    // much time. If so, just restart the test.
                    if !timing.wait_until_async(Duration::from_secs_f32(3.)).await {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed()
                        );
                        continue;
                    }

                    assert_eq!(kv.get_key("b").await?, None, "b never expired");

                    timing.wait_until_async(Duration::from_secs_f32(5.)).await;
                    assert_eq!(kv.get_key("a").await?, None, "a never expired");
                    break;
                }
                harness.shutdown().await?;

                Ok(())
            }

            #[tokio::test]
            async fn delete_expire_tests() -> anyhow::Result<()> {
                use std::time::Duration;

                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};

                let harness = $harness::new($crate::test_util::HarnessTest::KvDeleteExpire).await?;
                let db = harness.connect().await?;

                loop {
                    let kv = db.with_key_namespace("delete_expire");

                    kv.delete_key("a").await?;

                    let timing = $crate::test_util::TimingTest::new(Duration::from_millis(100));

                    // Create a key with an expiration. Delete the key. Set a new
                    // value at that key with no expiration. Ensure it doesn't
                    // expire.
                    kv.set_key("a", &0_u32)
                        .expire_in(Duration::from_secs(2))
                        .await?;
                    kv.delete_key("a").await?;
                    kv.set_key("a", &1_u32).await?;
                    if timing.elapsed() > Duration::from_secs(1) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed(),
                        );
                        continue;
                    }
                    if !timing.wait_until_async(Duration::from_secs_f32(2.5)).await {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed()
                        );
                        continue;
                    }

                    assert_eq!(kv.get_key("a").into().await?, Some(1u32));

                    break;
                }
                harness.shutdown().await?;

                Ok(())
            }

            #[tokio::test]
            // This test can fail when the machine its running on is under high load or
            // constrained resources. We need a command that persists (and waits until
            // persist) to make this test fully deterministic.
            async fn kv_transaction_tests() -> anyhow::Result<()> {
                use std::time::Duration;

                use $crate::connection::AsyncConnection;
                use $crate::keyvalue::{AsyncKeyValue, KeyStatus};
                // loop {
                let harness = $harness::new($crate::test_util::HarnessTest::KvTransactions).await?;
                let db = harness.connect().await?;
                // Generate several transactions that we can validate. Persisting
                // happens in the background, so we delay between each step to give
                // it a moment.
                db.set_key("expires", &0_u32)
                    .expire_in(Duration::from_secs(1))
                    .await?;
                db.set_key("akey", &String::from("avalue")).await?;
                db.set_numeric_key("nkey", 0_u64).await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                db.increment_key_by("nkey", 1_u64).await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                db.delete_key("nkey").await?;
                db.get_key("akey").and_delete().await?;
                tokio::time::sleep(Duration::from_millis(500)).await;
                // Ensure this doesn't generate a transaction.
                db.delete_key("nkey").await?;

                tokio::time::sleep(Duration::from_secs(1)).await;

                let transactions =
                    AsyncConnection::list_executed_transactions(&db, None, None).await?;
                let deleted_keys = transactions
                    .iter()
                    .filter_map(|tx| tx.changes.keys())
                    .flatten()
                    .filter(|changed_key| dbg!(changed_key).deleted)
                    .count();
                assert_eq!(deleted_keys, 3, "deleted keys wasn't 3");
                let akey_changes = transactions
                    .iter()
                    .filter_map(|tx| tx.changes.keys())
                    .flatten()
                    .filter(|changed_key| changed_key.key == "akey")
                    .count();
                assert_eq!(akey_changes, 2, "akey changes wasn't 2");
                let nkey_changes = transactions
                    .iter()
                    .filter_map(|tx| tx.changes.keys())
                    .flatten()
                    .filter(|changed_key| changed_key.key == "nkey")
                    .count();
                assert_eq!(nkey_changes, 3, "nkey changes wasn't 3");

                harness.shutdown().await?;
                // }

                Ok(())
            }
        }
    };
}

/// Defines the `KeyValue` test suite
#[macro_export]
macro_rules! define_blocking_kv_test_suite {
    ($harness:ident) => {
        #[cfg(test)]
        mod blocking_kv {
            use super::$harness;

            #[test]
            fn basic_kv_test() -> anyhow::Result<()> {
                use $crate::keyvalue::{KeyStatus, KeyValue};
                let harness = $harness::new($crate::test_util::HarnessTest::KvBasic)?;
                let db = harness.connect()?;
                assert_eq!(
                    db.set_key("akey", &String::from("avalue")).execute()?,
                    KeyStatus::Inserted
                );
                assert_eq!(db.get_key("akey").into()?, Some(String::from("avalue")));
                assert_eq!(
                    db.set_key("akey", &String::from("new_value"))
                        .returning_previous_as()?,
                    Some(String::from("avalue"))
                );
                assert_eq!(db.get_key("akey").into()?, Some(String::from("new_value")));
                assert_eq!(
                    db.get_key("akey").and_delete().into()?,
                    Some(String::from("new_value"))
                );
                assert_eq!(db.get_key("akey").query()?, None);
                assert_eq!(
                    db.set_key("akey", &String::from("new_value"))
                        .returning_previous()?,
                    None
                );
                assert_eq!(db.delete_key("akey")?, KeyStatus::Deleted);
                assert_eq!(db.delete_key("akey")?, KeyStatus::NotChanged);

                harness.shutdown()?;

                Ok(())
            }

            #[test]
            fn kv_concurrency() -> anyhow::Result<()> {
                use $crate::keyvalue::{KeyStatus, KeyValue};
                const WRITERS: usize = 100;
                const INCREMENTS: usize = 100;
                let harness = $harness::new($crate::test_util::HarnessTest::KvConcurrency)?;
                let db = harness.connect()?;

                let threads = (0..WRITERS)
                    .map(|_| {
                        let db = db.clone();
                        std::thread::spawn(move || {
                            for _ in 0..INCREMENTS {
                                db.increment_key_by("concurrency", 1_u64).execute().unwrap();
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                for thread in threads {
                    thread.join().unwrap();
                }

                assert_eq!(
                    db.get_key("concurrency").into_u64().unwrap().unwrap(),
                    (WRITERS * INCREMENTS) as u64
                );

                harness.shutdown()?;

                Ok(())
            }

            #[test]
            fn kv_set_tests() -> anyhow::Result<()> {
                use $crate::keyvalue::{KeyStatus, KeyValue};
                let harness = $harness::new($crate::test_util::HarnessTest::KvSet)?;
                let db = harness.connect()?;
                let kv = db.with_key_namespace("set");

                assert_eq!(
                    kv.set_key("a", &0_u32).only_if_exists().execute()?,
                    KeyStatus::NotChanged
                );
                assert_eq!(
                    kv.set_key("a", &0_u32).only_if_vacant().execute()?,
                    KeyStatus::Inserted
                );
                assert_eq!(
                    kv.set_key("a", &1_u32).only_if_vacant().execute()?,
                    KeyStatus::NotChanged
                );
                assert_eq!(
                    kv.set_key("a", &2_u32).only_if_exists().execute()?,
                    KeyStatus::Updated,
                );
                assert_eq!(
                    kv.set_key("a", &3_u32).returning_previous_as()?,
                    Some(2_u32),
                );

                harness.shutdown()?;

                Ok(())
            }

            #[test]
            fn kv_increment_decrement_tests() -> anyhow::Result<()> {
                use $crate::keyvalue::{KeyStatus, KeyValue};
                let harness = $harness::new($crate::test_util::HarnessTest::KvIncrementDecrement)?;
                let db = harness.connect()?;
                let kv = db.with_key_namespace("increment_decrement");

                // Empty keys should be equal to 0
                assert_eq!(kv.increment_key_by("i64", 1_i64).execute()?, 1_i64);
                assert_eq!(kv.get_key("i64").into_i64()?, Some(1_i64));
                assert_eq!(kv.increment_key_by("u64", 1_u64).execute()?, 1_u64);
                $crate::assert_f64_eq!(kv.increment_key_by("f64", 1_f64).execute()?, 1_f64);

                // Test float incrementing/decrementing an existing value
                $crate::assert_f64_eq!(kv.increment_key_by("f64", 1_f64).execute()?, 2_f64);
                $crate::assert_f64_eq!(kv.decrement_key_by("f64", 2_f64).execute()?, 0_f64);

                // Empty keys should be equal to 0
                assert_eq!(kv.decrement_key_by("i64_2", 1_i64).execute()?, -1_i64);
                assert_eq!(
                    kv.decrement_key_by("u64_2", 42_u64)
                        .allow_overflow()
                        .execute()?,
                    u64::MAX - 41
                );
                assert_eq!(kv.decrement_key_by("u64_3", 42_u64).execute()?, u64::MIN);
                $crate::assert_f64_eq!(kv.decrement_key_by("f64_2", 1_f64).execute()?, -1_f64);

                // Test decrement wrapping with overflow
                kv.set_numeric_key("i64", i64::MIN).execute()?;
                assert_eq!(
                    kv.decrement_key_by("i64", 1_i64)
                        .allow_overflow()
                        .execute()?,
                    i64::MAX
                );
                assert_eq!(
                    kv.decrement_key_by("u64", 2_u64)
                        .allow_overflow()
                        .execute()?,
                    u64::MAX
                );

                // Test increment wrapping with overflow
                assert_eq!(
                    kv.increment_key_by("i64", 1_i64)
                        .allow_overflow()
                        .execute()?,
                    i64::MIN
                );
                assert_eq!(
                    kv.increment_key_by("u64", 1_u64)
                        .allow_overflow()
                        .execute()?,
                    u64::MIN
                );

                // Test saturating increments.
                kv.set_numeric_key("i64", i64::MAX - 1).execute()?;
                kv.set_numeric_key("u64", u64::MAX - 1).execute()?;
                assert_eq!(kv.increment_key_by("i64", 2_i64).execute()?, i64::MAX);
                assert_eq!(kv.increment_key_by("u64", 2_u64).execute()?, u64::MAX);

                // Test saturating decrements.
                kv.set_numeric_key("i64", i64::MIN + 1).execute()?;
                kv.set_numeric_key("u64", u64::MIN + 1).execute()?;
                assert_eq!(kv.decrement_key_by("i64", 2_i64).execute()?, i64::MIN);
                assert_eq!(kv.decrement_key_by("u64", 2_u64).execute()?, u64::MIN);

                // Test numerical conversion safety using get
                {
                    // For i64 -> f64, the limit is 2^52 + 1 in either posive or
                    // negative directions.
                    kv.set_numeric_key("i64", (2_i64.pow(f64::MANTISSA_DIGITS)))
                        .execute()?;
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64()?.unwrap(),
                        9_007_199_254_740_992_f64
                    );
                    kv.set_numeric_key("i64", -(2_i64.pow(f64::MANTISSA_DIGITS)))
                        .execute()?;
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64()?.unwrap(),
                        -9_007_199_254_740_992_f64
                    );

                    kv.set_numeric_key("i64", (2_i64.pow(f64::MANTISSA_DIGITS) + 1))
                        .execute()?;
                    assert!(kv.get_key("i64").into_f64().is_err());
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64_lossy()?.unwrap(),
                        9_007_199_254_740_993_f64
                    );
                    kv.set_numeric_key("i64", -(2_i64.pow(f64::MANTISSA_DIGITS) + 1))
                        .execute()?;
                    assert!(kv.get_key("i64").into_f64().is_err());
                    $crate::assert_f64_eq!(
                        kv.get_key("i64").into_f64_lossy()?.unwrap(),
                        -9_007_199_254_740_993_f64
                    );

                    // For i64 -> u64, the only limit is sign.
                    kv.set_numeric_key("i64", -1_i64).execute()?;
                    assert!(kv.get_key("i64").into_u64().is_err());
                    assert_eq!(kv.get_key("i64").into_u64_lossy(true)?.unwrap(), 0_u64);
                    assert_eq!(kv.get_key("i64").into_u64_lossy(false)?.unwrap(), u64::MAX);

                    // For f64 -> i64, the limit is fractional numbers. Saturating isn't tested in this conversion path.
                    kv.set_numeric_key("f64", 1.1_f64).execute()?;
                    assert!(kv.get_key("f64").into_i64().is_err());
                    assert_eq!(kv.get_key("f64").into_i64_lossy(false)?.unwrap(), 1_i64);
                    kv.set_numeric_key("f64", -1.1_f64).execute()?;
                    assert!(kv.get_key("f64").into_i64().is_err());
                    assert_eq!(kv.get_key("f64").into_i64_lossy(false)?.unwrap(), -1_i64);

                    // For f64 -> u64, the limit is fractional numbers or negative numbers. Saturating isn't tested in this conversion path.
                    kv.set_numeric_key("f64", 1.1_f64).execute()?;
                    assert!(kv.get_key("f64").into_u64().is_err());
                    assert_eq!(kv.get_key("f64").into_u64_lossy(false)?.unwrap(), 1_u64);
                    kv.set_numeric_key("f64", -1.1_f64).execute()?;
                    assert!(kv.get_key("f64").into_u64().is_err());
                    assert_eq!(kv.get_key("f64").into_u64_lossy(false)?.unwrap(), 0_u64);

                    // For u64 -> i64, the limit is > i64::MAX
                    kv.set_numeric_key("u64", i64::MAX as u64 + 1).execute()?;
                    assert!(kv.get_key("u64").into_i64().is_err());
                    assert_eq!(kv.get_key("u64").into_i64_lossy(true)?.unwrap(), i64::MAX);
                    assert_eq!(kv.get_key("u64").into_i64_lossy(false)?.unwrap(), i64::MIN);
                }

                // Test that non-numeric keys won't be changed when attempting to incr/decr
                kv.set_key("non-numeric", &String::from("test")).execute()?;
                assert!(kv.increment_key_by("non-numeric", 1_i64).execute().is_err());
                assert!(kv.decrement_key_by("non-numeric", 1_i64).execute().is_err());
                assert_eq!(
                    kv.get_key("non-numeric").into::<String>()?.unwrap(),
                    String::from("test")
                );

                // Test that NaN cannot be stored
                kv.set_numeric_key("f64", 0_f64).execute()?;
                assert!(matches!(
                    kv.set_numeric_key("f64", f64::NAN).execute(),
                    Err(bonsaidb_core::Error::NotANumber)
                ));
                // Verify the value was unchanged.
                $crate::assert_f64_eq!(kv.get_key("f64").into_f64()?.unwrap(), 0.);
                // Try to increment by nan
                assert!(matches!(
                    kv.increment_key_by("f64", f64::NAN).execute(),
                    Err(bonsaidb_core::Error::NotANumber)
                ));
                $crate::assert_f64_eq!(kv.get_key("f64").into_f64()?.unwrap(), 0.);

                harness.shutdown()?;

                Ok(())
            }

            #[test]
            fn kv_expiration_tests() -> anyhow::Result<()> {
                use std::time::Duration;

                use $crate::keyvalue::{KeyStatus, KeyValue};

                let harness = $harness::new($crate::test_util::HarnessTest::KvExpiration)?;
                let db = harness.connect()?;

                loop {
                    let kv = db.with_key_namespace("expiration");

                    kv.delete_key("a")?;
                    kv.delete_key("b")?;

                    // Test that the expiration is updated for key a, but not for key b.
                    let timing = $crate::test_util::TimingTest::new(Duration::from_millis(500));
                    let r1 = kv
                        .set_key("a", &0_u32)
                        .expire_in(Duration::from_secs(2))
                        .execute()
                        .unwrap();
                    let r2 = kv
                        .set_key("b", &0_u32)
                        .expire_in(Duration::from_secs(2))
                        .execute()
                        .unwrap();
                    assert_eq!(r1, KeyStatus::Inserted);
                    assert_eq!(r2, KeyStatus::Inserted);
                    let r1 = kv
                        .set_key("a", &1_u32)
                        .expire_in(Duration::from_secs(4))
                        .execute()
                        .unwrap();
                    let r2 = kv
                        .set_key("b", &1_u32)
                        .expire_in(Duration::from_secs(100))
                        .keep_existing_expiration()
                        .execute()
                        .unwrap();

                    if timing.elapsed() > Duration::from_secs(1) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed(),
                        );
                        continue;
                    }

                    assert_eq!(r1, KeyStatus::Updated, "a wasn't an update");
                    assert_eq!(r2, KeyStatus::Updated, "b wasn't an update");

                    let a = kv.get_key("a").into()?;
                    assert_eq!(a, Some(1u32), "a shouldn't have expired yet");

                    // Before checking the value, make sure we haven't elapsed too
                    // much time. If so, just restart the test.
                    if !timing.wait_until(Duration::from_secs_f32(3.)) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed()
                        );
                        continue;
                    }

                    assert_eq!(kv.get_key("b").query()?, None, "b never expired");

                    timing.wait_until(Duration::from_secs_f32(5.));
                    assert_eq!(kv.get_key("a").query()?, None, "a never expired");
                    break;
                }
                harness.shutdown()?;

                Ok(())
            }

            #[test]
            fn delete_expire_tests() -> anyhow::Result<()> {
                use std::time::Duration;

                use $crate::keyvalue::{KeyStatus, KeyValue};

                let harness = $harness::new($crate::test_util::HarnessTest::KvDeleteExpire)?;
                let db = harness.connect()?;

                loop {
                    let kv = db.with_key_namespace("delete_expire");

                    kv.delete_key("a")?;

                    let timing = $crate::test_util::TimingTest::new(Duration::from_millis(100));

                    // Create a key with an expiration. Delete the key. Set a new
                    // value at that key with no expiration. Ensure it doesn't
                    // expire.
                    kv.set_key("a", &0_u32)
                        .expire_in(Duration::from_secs(2))
                        .execute()?;
                    kv.delete_key("a")?;
                    kv.set_key("a", &1_u32).execute()?;
                    if timing.elapsed() > Duration::from_secs(1) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed(),
                        );
                        continue;
                    }
                    if !timing.wait_until(Duration::from_secs_f32(2.5)) {
                        println!(
                            "Restarting test {}. Took too long {:?}",
                            line!(),
                            timing.elapsed()
                        );
                        continue;
                    }

                    assert_eq!(kv.get_key("a").into()?, Some(1u32));

                    break;
                }
                harness.shutdown()?;

                Ok(())
            }

            #[test]
            // This test can fail when the machine its running on is under high load or
            // constrained resources. We need a command that persists (and waits until
            // persist) to make this test fully deterministic.
            fn kv_transaction_tests() -> anyhow::Result<()> {
                use std::time::Duration;

                use $crate::connection::Connection;
                use $crate::keyvalue::{KeyStatus, KeyValue};
                // loop {
                let harness = $harness::new($crate::test_util::HarnessTest::KvTransactions)?;
                let db = harness.connect()?;
                // Generate several transactions that we can validate. Persisting
                // happens in the background, so we delay between each step to give
                // it a moment.
                db.set_key("expires", &0_u32)
                    .expire_in(Duration::from_secs(1))
                    .execute()?;
                db.set_key("akey", &String::from("avalue")).execute()?;
                db.set_numeric_key("nkey", 0_u64).execute()?;
                std::thread::sleep(Duration::from_millis(500));
                db.increment_key_by("nkey", 1_u64).execute()?;
                std::thread::sleep(Duration::from_millis(500));
                db.delete_key("nkey")?;
                db.get_key("akey").and_delete().query()?;
                std::thread::sleep(Duration::from_millis(500));
                // Ensure this doesn't generate a transaction.
                db.delete_key("nkey")?;

                std::thread::sleep(Duration::from_secs(1));

                let transactions = Connection::list_executed_transactions(&db, None, None)?;
                let deleted_keys = transactions
                    .iter()
                    .filter_map(|tx| tx.changes.keys())
                    .flatten()
                    .filter(|changed_key| dbg!(changed_key).deleted)
                    .count();
                assert_eq!(deleted_keys, 3, "deleted keys wasn't 3");
                let akey_changes = transactions
                    .iter()
                    .filter_map(|tx| tx.changes.keys())
                    .flatten()
                    .filter(|changed_key| changed_key.key == "akey")
                    .count();
                assert_eq!(akey_changes, 2, "akey changes wasn't 2");
                let nkey_changes = transactions
                    .iter()
                    .filter_map(|tx| tx.changes.keys())
                    .flatten()
                    .filter(|changed_key| changed_key.key == "nkey")
                    .count();
                assert_eq!(nkey_changes, 3, "nkey changes wasn't 3");

                harness.shutdown()?;
                // }

                Ok(())
            }
        }
    };
}

pub struct TimingTest {
    tolerance: Duration,
    start: Instant,
}

impl TimingTest {
    #[must_use]
    pub fn new(tolerance: Duration) -> Self {
        Self {
            tolerance,
            start: Instant::now(),
        }
    }

    #[allow(clippy::must_use_candidate)]
    pub fn wait_until(&self, absolute_duration: Duration) -> bool {
        let target = self.start + absolute_duration;
        let mut now = Instant::now();
        if let Some(sleep_duration) = target.checked_duration_since(now) {
            std::thread::sleep(sleep_duration);
            now = Instant::now();
        }
        let amount_past = now.checked_duration_since(target);
        // Return false if we're beyond the tolerance given
        amount_past.unwrap_or_default() < self.tolerance
    }

    pub async fn wait_until_async(&self, absolute_duration: Duration) -> bool {
        let target = self.start + absolute_duration;
        let mut now = Instant::now();
        if now < target {
            tokio::time::sleep_until(target.into()).await;
            now = Instant::now();
        }
        let amount_past = now.checked_duration_since(target);

        // Return false if we're beyond the tolerance given
        amount_past.unwrap_or_default() < self.tolerance
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        Instant::now()
            .checked_duration_since(self.start)
            .unwrap_or_default()
    }
}

pub async fn basic_server_connection_tests<C: AsyncStorageConnection>(
    server: C,
    newdb_name: &str,
) -> anyhow::Result<()> {
    let schemas = server.list_available_schemas().await?;

    let basic_schema = schemas
        .iter()
        .find(|s| s.name == BasicSchema::schema_name())
        .unwrap();
    assert!(basic_schema
        .collections()
        .any(|c| c.name == Basic::collection_name()));
    let basic_collection = basic_schema.collection(&Basic::collection_name()).unwrap();
    assert!(basic_collection
        .views()
        .any(|v| v.name == BasicByParentId.view_name()));
    let by_parent_id = basic_collection.view(&BasicByParentId.view_name()).unwrap();
    assert_eq!(by_parent_id.policy, ViewUpdatePolicy::Lazy);

    assert!(schemas
        .iter()
        .any(|s| s.name == SchemaName::new("khonsulabs", "bonsaidb-admin")));

    let databases = server.list_databases().await?;
    assert!(databases.iter().any(|db| db.name == "tests"));

    server
        .create_database::<BasicSchema>(newdb_name, false)
        .await?;
    server.delete_database(newdb_name).await?;

    assert!(matches!(
        server.delete_database(newdb_name).await,
        Err(Error::DatabaseNotFound(_))
    ));

    assert!(matches!(
        server.create_database::<BasicSchema>("tests", false).await,
        Err(Error::DatabaseNameAlreadyTaken(_))
    ));

    assert!(server
        .create_database::<BasicSchema>("tests", true)
        .await
        .is_ok());

    assert!(matches!(
        server
            .create_database::<BasicSchema>("|invalidname", false)
            .await,
        Err(Error::InvalidDatabaseName(_))
    ));

    assert!(matches!(
        server
            .create_database::<UnassociatedCollection>(newdb_name, false)
            .await,
        Err(Error::SchemaNotRegistered(_))
    ));

    Ok(())
}

pub fn blocking_basic_server_connection_tests<C: StorageConnection>(
    server: &C,
    newdb_name: &str,
) -> anyhow::Result<()> {
    let schemas = server.list_available_schemas()?;
    let basic_schema = schemas
        .iter()
        .find(|s| s.name == BasicSchema::schema_name())
        .unwrap();
    assert!(basic_schema
        .collections()
        .any(|c| c.name == Basic::collection_name()));
    let basic_collection = basic_schema.collection(&Basic::collection_name()).unwrap();
    assert!(basic_collection
        .views()
        .any(|v| v.name == BasicByParentId.view_name()));
    let by_parent_id = basic_collection.view(&BasicByParentId.view_name()).unwrap();
    assert_eq!(by_parent_id.policy, ViewUpdatePolicy::Lazy);

    assert!(schemas
        .iter()
        .any(|s| s.name == SchemaName::new("khonsulabs", "bonsaidb-admin")));

    let databases = server.list_databases()?;
    assert!(databases.iter().any(|db| db.name == "tests"));

    server.create_database::<BasicSchema>(newdb_name, false)?;
    server.delete_database(newdb_name)?;

    assert!(matches!(
        server.delete_database(newdb_name),
        Err(Error::DatabaseNotFound(_))
    ));

    assert!(matches!(
        server.create_database::<BasicSchema>("tests", false),
        Err(Error::DatabaseNameAlreadyTaken(_))
    ));

    assert!(server.create_database::<BasicSchema>("tests", true).is_ok());

    assert!(matches!(
        server.create_database::<BasicSchema>("|invalidname", false),
        Err(Error::InvalidDatabaseName(_))
    ));

    assert!(matches!(
        server.create_database::<UnassociatedCollection>(newdb_name, false),
        Err(Error::SchemaNotRegistered(_))
    ));

    Ok(())
}
