#![allow(clippy::missing_panics_doc)]

use std::{
    borrow::Cow,
    fmt::{Debug, Display},
    io::ErrorKind,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

use crate::{
    connection::{AccessPolicy, Connection},
    document::Document,
    limits::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS},
    schema::{
        view, Collection, CollectionName, InvalidNameError, MapResult, MappedValue, Name, Schema,
        SchemaName, Schematic, View,
    },
    Error,
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct Basic {
    pub value: String,
    pub category: Option<String>,
    pub parent_id: Option<u64>,
}

impl Basic {
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

    #[must_use]
    pub const fn with_parent_id(mut self, parent_id: u64) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

impl Collection for Basic {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "basic")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(BasicCount)?;
        schema.define_view(BasicByParentId)?;
        schema.define_view(BasicByCategory)
    }
}

#[derive(Debug)]
pub struct BasicCount;

impl View for BasicCount {
    type Collection = Basic;
    type Key = ();
    type Value = usize;

    fn version(&self) -> u64 {
        0
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("count")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        Ok(Some(document.emit_key_and_value((), 1)))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug)]
pub struct BasicByParentId;

impl View for BasicByParentId {
    type Collection = Basic;
    type Key = Option<u64>;
    type Value = usize;

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-parent-id")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let contents = document.contents::<Basic>()?;
        Ok(Some(document.emit_key_and_value(contents.parent_id, 1)))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug)]
pub struct BasicByCategory;

impl View for BasicByCategory {
    type Collection = Basic;
    type Key = String;
    type Value = usize;

    fn version(&self) -> u64 {
        0
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-category")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let contents = document.contents::<Basic>()?;
        if let Some(category) = &contents.category {
            Ok(Some(
                document.emit_key_and_value(category.to_lowercase(), 1),
            ))
        } else {
            Ok(None)
        }
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug)]
pub struct BasicByBrokenParentId;

impl View for BasicByBrokenParentId {
    type Collection = Basic;
    type Key = ();
    type Value = ();

    fn version(&self) -> u64 {
        0
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-parent-id")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        Ok(Some(document.emit()))
    }
}

#[derive(Debug)]
pub struct BasicSchema;

impl Schema for BasicSchema {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("khonsulabs", "basic")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_collection::<Basic>()
    }
}

pub struct TestDirectory(pub PathBuf);

impl TestDirectory {
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
                eprintln!("Failed to clean up temporary folder: {:?}", err);
            }
        }
    }
}

impl AsRef<Path> for TestDirectory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug)]
pub struct BasicCollectionWithNoViews;

impl Collection for BasicCollectionWithNoViews {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        Basic::collection_name()
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct BasicCollectionWithOnlyBrokenParentId;

impl Collection for BasicCollectionWithOnlyBrokenParentId {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        Basic::collection_name()
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(BasicByBrokenParentId)
    }
}

#[derive(Debug)]
pub struct UnassociatedCollection;

impl Collection for UnassociatedCollection {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "unassociated")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Copy, Clone, Debug)]
pub enum HarnessTest {
    StoreRetrieveUpdate = 1,
    NotFound,
    Conflict,
    BadUpdate,
    NoUpdate,
    GetMultiple,
    ListTransactions,
    ViewQuery,
    UnassociatedCollection,
    ViewUpdate,
    ViewAccessPolicies,
    PubSubSimple,
    PubSubMultipleSubscribers,
    PubSubDropAndSend,
    PubSubUnsubscribe,
    PubSubDropCleanup,
    PubSubPublishAll,
    KvBasic,
    KvSet,
    KvExpiration,
    KvDeleteExpire,
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

/// Creates a test suite that tests methods available on [`Connection`]
#[macro_export]
macro_rules! define_connection_test_suite {
    ($harness:ident) => {
        #[tokio::test]
        async fn store_retrieve_update_delete() -> Result<(), anyhow::Error> {
            let harness =
                $harness::new($crate::test_util::HarnessTest::StoreRetrieveUpdate).await?;
            let db = harness.connect().await?;
            $crate::test_util::store_retrieve_update_delete_tests(&db).await?;
            harness.shutdown().await
        }

        #[tokio::test]
        async fn not_found() -> Result<(), anyhow::Error> {
            let harness = $harness::new($crate::test_util::HarnessTest::NotFound).await?;
            let db = harness.connect().await?;

            $crate::test_util::not_found_tests(&db).await?;
            harness.shutdown().await
        }

        #[tokio::test]
        async fn conflict() -> Result<(), anyhow::Error> {
            let harness = $harness::new($crate::test_util::HarnessTest::Conflict).await?;
            let db = harness.connect().await?;

            $crate::test_util::conflict_tests(&db).await?;
            harness.shutdown().await
        }

        #[tokio::test]
        async fn bad_update() -> Result<(), anyhow::Error> {
            let harness = $harness::new($crate::test_util::HarnessTest::BadUpdate).await?;
            let db = harness.connect().await?;

            $crate::test_util::bad_update_tests(&db).await?;
            harness.shutdown().await
        }

        #[tokio::test]
        async fn no_update() -> Result<(), anyhow::Error> {
            let harness = $harness::new($crate::test_util::HarnessTest::NoUpdate).await?;
            let db = harness.connect().await?;

            $crate::test_util::no_update_tests(&db).await?;
            harness.shutdown().await
        }

        #[tokio::test]
        async fn get_multiple() -> Result<(), anyhow::Error> {
            let harness = $harness::new($crate::test_util::HarnessTest::GetMultiple).await?;
            let db = harness.connect().await?;

            $crate::test_util::get_multiple_tests(&db).await?;
            harness.shutdown().await
        }

        #[tokio::test]
        async fn list_transactions() -> Result<(), anyhow::Error> {
            let harness = $harness::new($crate::test_util::HarnessTest::ListTransactions).await?;
            let db = harness.connect().await?;

            $crate::test_util::list_transactions_tests(&db).await?;
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
        async fn unassociated_collection() -> Result<(), anyhow::Error> {
            let harness =
                $harness::new($crate::test_util::HarnessTest::UnassociatedCollection).await?;
            let db = harness.connect().await?;

            $crate::test_util::unassociated_collection_tests(&db).await?;
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
        async fn view_access_policies() -> anyhow::Result<()> {
            let harness = $harness::new($crate::test_util::HarnessTest::ViewAccessPolicies).await?;
            let db = harness.connect().await?;

            $crate::test_util::view_access_policy_tests(&db).await?;
            harness.shutdown().await
        }
    };
}

pub async fn store_retrieve_update_delete_tests<C: Connection>(
    db: &C,
) -> Result<(), anyhow::Error> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(header.id)
        .await?
        .expect("couldn't retrieve stored item");
    let mut value = doc.contents::<Basic>()?;
    assert_eq!(original_value, value);
    let old_revision = doc.header.revision.clone();

    // Update the value
    value.value = String::from("updated_value");
    doc.set_contents(&value)?;
    db.update(&mut doc).await?;

    // update should cause the revision to be changed
    assert_ne!(doc.header.revision, old_revision);

    // Check the value in the database to ensure it has the new document
    let doc = collection
        .get(header.id)
        .await?
        .expect("couldn't retrieve stored item");
    assert_eq!(doc.contents::<Basic>()?, value);

    // These operations should have created two transactions with one change each
    let transactions = db.list_executed_transactions(None, None).await?;
    assert_eq!(transactions.len(), 2);
    assert!(transactions[0].id < transactions[1].id);
    for transaction in &transactions {
        assert_eq!(transaction.changed_documents.len(), 1);
        assert_eq!(
            transaction.changed_documents[0].collection,
            Basic::collection_name()?
        );
        assert_eq!(transaction.changed_documents[0].id, header.id);
        assert_eq!(transaction.changed_documents[0].deleted, false);
    }

    db.delete(&doc).await?;
    assert!(collection.get(header.id).await?.is_none());
    let transactions = db
        .list_executed_transactions(Some(transactions.last().as_ref().unwrap().id + 1), None)
        .await?;
    assert_eq!(transactions.len(), 1);
    let transaction = transactions.first().unwrap();
    assert_eq!(transaction.changed_documents.len(), 1);
    assert_eq!(
        transaction.changed_documents[0].collection,
        Basic::collection_name()?
    );
    assert_eq!(transaction.changed_documents[0].id, header.id);
    assert_eq!(transaction.changed_documents[0].deleted, true);

    Ok(())
}

pub async fn not_found_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
    assert!(db.collection::<Basic>().get(1).await?.is_none());

    assert!(db.last_transaction_id().await?.is_none());

    Ok(())
}

pub async fn conflict_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(header.id)
        .await?
        .expect("couldn't retrieve stored item");
    let mut value = doc.contents::<Basic>()?;
    value.value = String::from("updated_value");
    doc.set_contents(&value)?;
    db.update(&mut doc).await?;

    // To generate a conflict, let's try to do the same update again by
    // reverting the header
    doc.header = Cow::Owned(header);
    match db
        .update(&mut doc)
        .await
        .expect_err("conflict should have generated an error")
    {
        Error::DocumentConflict(collection, id) => {
            assert_eq!(collection, Basic::collection_name()?);
            assert_eq!(id, doc.header.id);
        }
        other => return Err(anyhow::Error::from(other)),
    }

    Ok(())
}

pub async fn bad_update_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
    let mut doc = Document::with_contents(1, &Basic::default(), Basic::collection_name()?)?;
    match db.update(&mut doc).await {
        Err(Error::DocumentNotFound(collection, id)) => {
            assert_eq!(collection, Basic::collection_name()?);
            assert_eq!(id, 1);
            Ok(())
        }
        other => panic!("expected DocumentNotFound from update but got: {:?}", other),
    }
}

pub async fn no_update_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>();
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(header.id)
        .await?
        .expect("couldn't retrieve stored item");
    db.update(&mut doc).await?;

    assert_eq!(doc.header.as_ref(), &header);

    Ok(())
}

pub async fn get_multiple_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
    let collection = db.collection::<Basic>();
    let doc1_value = Basic::new("initial_value");
    let doc1 = collection.push(&doc1_value).await?;

    let doc2_value = Basic::new("second_value");
    let doc2 = collection.push(&doc2_value).await?;

    let both_docs = db.get_multiple::<Basic>(&[doc1.id, doc2.id]).await?;
    assert_eq!(both_docs.len(), 2);

    // The order of get_multiple isn't guaranteed, so these two checks are done
    // with iterators instead of direct indexing
    let doc1 = both_docs
        .iter()
        .find(|doc| doc.header.id == doc1.id)
        .expect("Couldn't find doc1");
    let doc1 = doc1.contents::<Basic>()?;
    assert_eq!(doc1.value, doc1_value.value);
    let doc2 = both_docs
        .iter()
        .find(|doc| doc.header.id == doc2.id)
        .expect("Couldn't find doc2");
    let doc2 = doc2.contents::<Basic>()?;
    assert_eq!(doc2.value, doc2_value.value);

    Ok(())
}

pub async fn list_transactions_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
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
    assert_eq!(transactions.len(), LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT);

    // Test max results limit
    let transactions = db
        .list_executed_transactions(None, Some(LIST_TRANSACTIONS_MAX_RESULTS + 1))
        .await?;
    assert_eq!(transactions.len(), LIST_TRANSACTIONS_MAX_RESULTS);

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

    assert_eq!(transactions.len(), LIST_TRANSACTIONS_MAX_RESULTS + 1);

    Ok(())
}

pub async fn view_query_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
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
        .with_key(Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 1);

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(Some(a.id))
        .query_with_docs()
        .await?;
    assert_eq!(a_children.len(), 1);
    assert_eq!(a_children[0].document.header.id, a_child.id);

    let b_children = db
        .view::<BasicByParentId>()
        .with_key(Some(b.id))
        .query()
        .await?;
    assert_eq!(b_children.len(), 2);

    let a_and_b_children = db
        .view::<BasicByParentId>()
        .with_keys(vec![Some(a.id), Some(b.id)])
        .query()
        .await?;
    assert_eq!(a_and_b_children.len(), 3);

    let has_parent = db
        .view::<BasicByParentId>()
        // TODO range is tough because there's no single structure that works
        // here. RangeBounds is a trait. We'll need to use something else, but
        // my quick search doesn't find a serde-compatible library already
        // written. This should be an inclusive range
        .with_key_range(Some(0)..Some(u64::MAX))
        .query()
        .await?;
    assert_eq!(has_parent.len(), 3);

    let items_with_categories = db.view::<BasicByCategory>().query().await?;
    assert_eq!(items_with_categories.len(), 3);

    Ok(())
}

pub async fn unassociated_collection_tests<C: Connection>(db: &C) -> Result<(), anyhow::Error> {
    assert!(matches!(
        db.collection::<UnassociatedCollection>()
            .push(&Basic::default())
            .await,
        Err(Error::CollectionNotFound)
    ));

    Ok(())
}

pub async fn view_update_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
    let a = collection.push(&Basic::new("A")).await?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);
    // The reduce function of `BasicByParentId` acts as a "count" of records.
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(Some(a.id))
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
        .with_key(Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 1);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(Some(a.id))
            .reduce()
            .await?,
        1
    );

    // Verify reduce_grouped matches our expectations.
    assert_eq!(
        db.view::<BasicByParentId>().reduce_grouped().await?,
        vec![
            MappedValue {
                key: None,
                value: 1,
            },
            MappedValue {
                key: Some(a.id),
                value: 1,
            },
        ]
    );

    // Test updating the record and the view being updated appropriately
    let mut doc = db.collection::<Basic>().get(a_child.id).await?.unwrap();
    let mut basic = doc.contents::<Basic>()?;
    basic.parent_id = None;
    doc.set_contents(&basic)?;
    db.update(&mut doc).await?;

    let a_children = db
        .view::<BasicByParentId>()
        .with_key(Some(a.id))
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);
    assert_eq!(
        db.view::<BasicByParentId>()
            .with_key(Some(a.id))
            .reduce()
            .await?,
        0
    );
    assert_eq!(db.view::<BasicByParentId>().reduce().await?, 2);

    // Test deleting a record and ensuring it goes away
    db.delete(&doc).await?;

    let all_entries = db.view::<BasicByParentId>().query().await?;
    assert_eq!(all_entries.len(), 1);

    // Verify reduce_grouped matches our expectations.
    assert_eq!(
        db.view::<BasicByParentId>().reduce_grouped().await?,
        vec![MappedValue {
            key: None,
            value: 1,
        },]
    );

    Ok(())
}

pub async fn view_access_policy_tests<C: Connection>(db: &C) -> anyhow::Result<()> {
    let collection = db.collection::<Basic>();
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
        .with_key(Some(a.id))
        .with_access_policy(AccessPolicy::NoUpdate)
        .query()
        .await?;
    assert_eq!(a_children.len(), 0);

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Verify the view still have no value, but this time ask for it to be
    // updated after returning
    let a_children = db
        .view::<BasicByParentId>()
        .with_key(Some(a.id))
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
            .with_key(Some(a.id))
            .with_access_policy(AccessPolicy::NoUpdate)
            .query()
            .await?;
        if a_children.len() == 1 {
            return Ok(());
        }
    }
    panic!("view never updated")
}

/// Defines the Kv test suite
#[macro_export]
macro_rules! define_kv_test_suite {
    ($harness:ident) => {
        #[tokio::test]
        async fn basic_kv_test() -> anyhow::Result<()> {
            use $crate::kv::{KeyStatus, Kv};
            let harness = $harness::new($crate::test_util::HarnessTest::KvBasic).await?;
            let db = harness.connect().await?;
            assert_eq!(
                db.set_key("akey", &String::from("avalue")).await?,
                KeyStatus::Inserted
            );
            assert_eq!(db.get_key("akey").await?, Some(String::from("avalue")));
            assert_eq!(
                db.set_key("akey", &String::from("new_value"))
                    .returning_previous()
                    .await?,
                Some(String::from("avalue"))
            );
            assert_eq!(db.get_key("akey").await?, Some(String::from("new_value")));
            assert_eq!(
                db.get_key("akey").and_delete().await?,
                Some(String::from("new_value"))
            );
            assert_eq!(db.get_key::<String, _>("akey").await?, None);
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
        async fn kv_set_tests() -> anyhow::Result<()> {
            use $crate::kv::{KeyStatus, Kv};
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
            assert_eq!(kv.set_key("a", &3_u32).returning_previous().await?, Some(2),);

            harness.shutdown().await?;

            Ok(())
        }

        #[tokio::test]
        async fn kv_expiration_tests() -> anyhow::Result<()> {
            use std::time::Duration;

            use $crate::kv::{KeyStatus, Kv};

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
                if timing.elapsed() > Duration::from_secs(2) {
                    println!(
                        "Restarting test {}. Took too long {:?}",
                        line!(),
                        timing.elapsed(),
                    );
                    continue;
                }

                assert_eq!(r1?, KeyStatus::Updated, "a wasn't an update");
                assert_eq!(r2?, KeyStatus::Updated, "b wasn't an update");

                let a = kv.get_key::<u32, _>("a").await?;

                // Before checking the value, make sure we haven't elapsed too
                // much time. If so, just restart the test.
                if !timing.wait_until(Duration::from_secs_f32(2.5)).await {
                    println!(
                        "Restarting test {}. Took too long {:?}",
                        line!(),
                        timing.elapsed()
                    );
                    continue;
                }

                assert_eq!(a, Some(1));
                assert_eq!(kv.get_key::<u32, _>("b").await?, None);

                timing.wait_until(Duration::from_secs_f32(5.)).await;
                assert_eq!(kv.get_key::<u32, _>("a").await?, None);
                break;
            }
            harness.shutdown().await?;

            Ok(())
        }

        #[tokio::test]
        async fn delete_expire_tests() -> anyhow::Result<()> {
            use std::time::Duration;

            use $crate::kv::{KeyStatus, Kv};

            let harness = $harness::new($crate::test_util::HarnessTest::KvDeleteExpire).await?;
            let db = harness.connect().await?;

            loop {
                let kv = db.with_key_namespace("delete_expire");

                kv.delete_key("a").await?;

                let timing = $crate::test_util::TimingTest::new(Duration::from_millis(500));

                // Create a key with an expiration. Delete the key. Set a new
                // value at that key with no expiration. Ensure it doesn't
                // expire.
                kv.set_key("a", &0_u32)
                    .expire_in(Duration::from_secs(2))
                    .await?;
                kv.delete_key("a").await?;
                kv.set_key("a", &1_u32).await?;
                if timing.elapsed() > Duration::from_secs(2) {
                    println!(
                        "Restarting test {}. Took too long {:?}",
                        line!(),
                        timing.elapsed(),
                    );
                    continue;
                }
                if !timing.wait_until(Duration::from_secs_f32(2.5)).await {
                    println!(
                        "Restarting test {}. Took too long {:?}",
                        line!(),
                        timing.elapsed()
                    );
                    continue;
                }

                assert_eq!(kv.get_key::<u32, _>("a").await?, Some(1));

                break;
            }
            harness.shutdown().await?;

            Ok(())
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

    pub async fn wait_until(&self, absolute_duration: Duration) -> bool {
        let target = self.start + absolute_duration;
        let now = Instant::now();
        if now > target {
            let amount_past = now - target;

            // Return false if we're beyond the tolerance given
            amount_past < self.tolerance
        } else {
            tokio::time::sleep_until(target.into()).await;
            true
        }
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        Instant::now()
            .checked_duration_since(self.start)
            .unwrap_or_default()
    }
}
