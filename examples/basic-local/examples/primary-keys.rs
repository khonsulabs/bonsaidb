use bonsaidb::{
    core::{
        key::NextValueError,
        schema::{Collection, Schema, SerializedCollection},
    },
    local::{
        config::{Builder, StorageConfiguration},
        AsyncDatabase,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Schema)]
#[schema(name = "primary-keys", collections = [UserProfile, MultiKey])]
struct ExampleSchema;

// ANCHOR: derive_with_natural_id
#[derive(Debug, Serialize, Deserialize, Collection, Eq, PartialEq)]
#[collection(name = "user-profiles", primary_key = u32, natural_id = |user: &UserProfile| Some(user.external_id))]
struct UserProfile {
    pub external_id: u32,
    pub name: String,
}
// ANCHOR_END: derive_with_natural_id

// ANCHOR: derive
#[derive(Debug, Serialize, Deserialize, Collection, Eq, PartialEq)]
#[collection(name = "multi-key", primary_key = (u32, u64))]
struct MultiKey {
    value: String,
}
// ANCHOR_END: derive

#[tokio::main]
async fn main() -> Result<(), bonsaidb::core::Error> {
    drop(tokio::fs::remove_dir_all("primary-keys.bonsaidb").await);
    let db =
        AsyncDatabase::open::<ExampleSchema>(StorageConfiguration::new("primary-keys.bonsaidb"))
            .await?;

    // It's not uncommon to need to store data in a database that has an
    // "external" identifier. Some examples could be externally authenticated
    // user profiles, social networking site posts, or for normalizing a single
    // type's fields across multiple Collections.
    //
    // The UserProfile type in this example has a `u32` primary key, and it
    // defines a closure that returns the field `external_id`. When pushing a
    // value into a collection with a natural id, it will automatically become
    // the primary key:
    // ANCHOR: natural_id_query
    let user = UserProfile {
        external_id: 42,
        name: String::from("ecton"),
    }
    .push_into_async(&db)
    .await?;
    let retrieved_from_database = UserProfile::get_async(42, &db)
        .await?
        .expect("document not found");
    assert_eq!(user, retrieved_from_database);
    // ANCHOR_END: natural_id_query

    // It's possible to use any type that implements the Key trait as a primary
    // key, including tuples:
    // ANCHOR: query
    let inserted = MultiKey {
        value: String::from("hello"),
    }
    .insert_into_async((42, 64), &db)
    .await?;
    let retrieved = MultiKey::get_async((42, 64), &db)
        .await?
        .expect("document not found");
    assert_eq!(inserted, retrieved);
    // ANCHOR_END: query

    // Not all primary key types support automatic key assignment -- the Key
    // trait implementation controls that behavior:
    assert!(matches!(
        MultiKey {
            value: String::from("error"),
        }
        .push_into_async(&db)
        .await
        .unwrap_err()
        .error,
        bonsaidb::core::Error::DocumentPush(_, NextValueError::Unsupported)
    ));
    Ok(())
}

#[test]
fn runs() {
    main().unwrap()
}
