use std::borrow::Cow;

use pliantdb_core::{
    connection::Connection,
    document::Document,
    schema::Collection,
    test_util::{Basic, BasicByCategory, BasicByParentId, TestDirectory, UnassociatedCollection},
    Error,
};
use storage::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS};

use super::*;
use crate::Storage;

#[tokio::test(flavor = "multi_thread")]
async fn store_retrieve_update() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("store-retrieve-update");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;

    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>()?;
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
    for transaction in transactions {
        assert_eq!(transaction.changed_documents.len(), 1);
        assert_eq!(transaction.changed_documents[0].collection, Basic::id());
        assert_eq!(transaction.changed_documents[0].id, header.id);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn not_found() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("not-found");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;

    assert!(db.collection::<Basic>()?.get(1).await?.is_none());

    assert!(db.last_transaction_id().await?.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("conflict");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;

    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>()?;
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
            assert_eq!(collection, Basic::id());
            assert_eq!(id, doc.header.id);
        }
        other => return Err(anyhow::Error::from(other)),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_update() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("bad_update");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;

    let mut doc = Document::with_contents(1, &Basic::default(), Basic::id())?;
    match db.update(&mut doc).await {
        Err(Error::DocumentNotFound(collection, id)) => {
            assert_eq!(collection, Basic::id());
            assert_eq!(id, 1);
            Ok(())
        }
        other => panic!("expected DocumentNotFound from update but got: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn no_update() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("no-update");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;

    let original_value = Basic::new("initial_value");
    let collection = db.collection::<Basic>()?;
    let header = collection.push(&original_value).await?;

    let mut doc = collection
        .get(header.id)
        .await?
        .expect("couldn't retrieve stored item");
    db.update(&mut doc).await?;

    assert_eq!(doc.header.as_ref(), &header);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn list_transactions() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("list-transactions");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;
    let collection = db.collection::<Basic>()?;

    // create LIST_TRANSACTIONS_MAX_RESULTS + 1 items, giving us just enough
    // transactions to test the edge cases of `list_transactions`
    futures::future::join_all(
        (0..=(LIST_TRANSACTIONS_MAX_RESULTS))
            .map(|_| async { collection.push(&Basic::default()).await }),
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

#[tokio::test(flavor = "multi_thread")]
async fn view_query() -> anyhow::Result<()> {
    let path = TestDirectory::new("view-query");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;
    let collection = db.collection::<Basic>()?;
    let a = collection.push(&Basic::new("A")).await?;
    let b = collection.push(&Basic::new("B")).await?;
    collection
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

#[tokio::test(flavor = "multi_thread")]
async fn unassociated_collection() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("unassociated-collection");
    let db = Storage::<Basic>::open_local(path, &Configuration::default()).await?;
    assert!(matches!(
        db.collection::<UnassociatedCollection>(),
        Err(pliantdb_core::Error::CollectionNotFound)
    ));

    Ok(())
}
