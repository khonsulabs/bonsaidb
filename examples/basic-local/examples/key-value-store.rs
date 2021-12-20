use std::time::Duration;

use bonsaidb::{
    core::keyvalue::{KeyStatus, KeyValue},
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};

// BonsaiDb supports a lightweight, atomic key-value store in addition to its
// acid-compliant transactional storage. This interface is meant to replicate
// functionality that you might use something like Redis for -- lightweight
// caching, or fast atomic operations. As with all of BonsaiDb's core features,
// the Key-Value store is supported across all methods of accessing BonsaiDb.

#[tokio::main]
async fn main() -> Result<(), bonsaidb::core::Error> {
    let db = Database::open::<()>(StorageConfiguration::new("key-value-store.bonsaidb")).await?;

    // The set_key method can be awaited to insert/replace a key. Values can be
    // anything supported by serde.
    db.set_key("mykey", &1_u32).await?;

    // Or, you can customize it's behavior:
    let old_value = db
        .set_key("mykey", &2_u32)
        .only_if_exists()
        .returning_previous_as()
        .await?;
    assert_eq!(old_value, Some(1_u32));

    // Retrieving is simple too.
    let value = db.get_key("mykey").into().await?;
    assert_eq!(value, Some(2_u32));

    // Namespacing is built-in as well, so that you can easily separate storage.
    let value = db.with_key_namespace("anamespace").get_key("mykey").await?;
    assert!(value.is_none());

    // Because of the atomic nature of the key-value store, you can use set_key
    // as a synchronized lock:
    let result = db
        .set_key("lockname", &()) // A useful value would be the unique id of the worker that aquired the lock.
        .only_if_vacant()
        .expire_in(Duration::from_millis(100))
        .await?;
    assert_eq!(result, KeyStatus::Inserted);

    let second_try = db
        .set_key("lockname", &())
        .only_if_vacant()
        .expire_in(Duration::from_millis(100))
        .await?;
    assert_eq!(second_try, KeyStatus::NotChanged);

    Ok(())
}

#[test]
fn runs() {
    main().unwrap()
}
