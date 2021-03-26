use pliantdb_core::{
    connection::Connection,
    document::Document,
    schema::Collection,
    test_util::{Basic, BasicByParentId, BasicCollection, TestDirectory},
    Error,
};
use std::borrow::Cow;
use storage::{LIST_TRANSACTIONS_DEFAULT_RESULT_COUNT, LIST_TRANSACTIONS_MAX_RESULTS};

use super::*;
use crate::Storage;

#[tokio::test(flavor = "multi_thread")]
async fn store_retrieve_update() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("store-retrieve-update");
    let db = Storage::<BasicCollection>::open_local(path).await?;

    let original_value = Basic {
        value: String::from("initial_value"),
        parent_id: None,
    };
    let collection = db.collection::<BasicCollection>()?;
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
        assert_eq!(
            transaction.changed_documents[0].collection,
            BasicCollection::id()
        );
        assert_eq!(transaction.changed_documents[0].id, header.id);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn not_found() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("not-found");
    let db = Storage::<BasicCollection>::open_local(path).await?;

    assert!(db.collection::<BasicCollection>()?.get(1).await?.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("conflict");
    let db = Storage::<BasicCollection>::open_local(path).await?;

    let original_value = Basic {
        value: String::from("initial_value"),
        parent_id: None,
    };
    let collection = db.collection::<BasicCollection>()?;
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
            assert_eq!(collection, BasicCollection::id());
            assert_eq!(id, doc.header.id);
        }
        other => return Err(anyhow::Error::from(other)),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_update() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("bad_update");
    let db = Storage::<BasicCollection>::open_local(path).await?;

    let mut doc = Document::with_contents(1, &Basic::default(), BasicCollection::id())?;
    match db.update(&mut doc).await {
        Err(Error::DocumentNotFound(collection, id)) => {
            assert_eq!(collection, BasicCollection::id());
            assert_eq!(id, 1);
            Ok(())
        }
        other => panic!("expected DocumentNotFound from update but got: {:?}", other),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn no_update() -> Result<(), anyhow::Error> {
    let path = TestDirectory::new("no-update");
    let db = Storage::<BasicCollection>::open_local(path).await?;

    let original_value = Basic {
        value: String::from("initial_value"),
        parent_id: None,
    };
    let collection = db.collection::<BasicCollection>()?;
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
    let db = Storage::<BasicCollection>::open_local(path).await?;
    let collection = db.collection::<BasicCollection>()?;

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
    let path = TestDirectory::new("list-transactions");
    let db = Storage::<BasicCollection>::open_local(path).await?;
    let collection = db.collection::<BasicCollection>()?;
    let a = collection
        .push(&Basic {
            value: String::from("A"),
            parent_id: None,
        })
        .await?;
    let b = collection
        .push(&Basic {
            value: String::from("B"),
            parent_id: None,
        })
        .await?;
    collection
        .push(&Basic {
            value: String::from("A.1"),
            parent_id: Some(a.id),
        })
        .await?;
    collection
        .push(&Basic {
            value: String::from("B.1"),
            parent_id: Some(b.id),
        })
        .await?;
    collection
        .push(&Basic {
            value: String::from("B.2"),
            parent_id: Some(b.id),
        })
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

    Ok(())
}
