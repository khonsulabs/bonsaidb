use std::time::Duration;

use pliantdb_core::kv::{KeyStatus, Kv};
use pliantdb_local::{Configuration, Storage};

// PliantDB supports a lightweight, atomic key-value store in addition to its
// acid-compliant transactional storage. This interface is meant to replicate
// functionality that you might use something like Redis for -- lightweight
// caching, or fast atomic operations. As with all of PliantDB's core features,
// the Key-Value store is supported across all methods of accessing PliantDB.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db =
        Storage::<()>::open_local("key-value-store.pliantdb", &Configuration::default()).await?;

    // The set_key method can be awaited to insert/replace a key. Values can be
    // anything supported by serde.
    db.set_key("mykey", &1_u32).await?;

    // Or, you can customize it's behavior:
    let old_value = db
        .set_key("mykey", &2_u32)
        .only_if_exists()
        .returning_previous()
        .await?;
    assert_eq!(old_value, Some(1));

    // Retrieving is simple too.
    let value = db.get_key("mykey").await?;
    assert_eq!(value, Some(2_u32));

    // Namespacing is built-in as well, so that you can easily separate storage.
    let value: Option<u32> = db.with_key_namespace("anamespace").get_key("mykey").await?;
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
