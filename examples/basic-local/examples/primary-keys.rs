use bonsaidb::core::key::{Key, NextValueError};
use bonsaidb::core::schema::{Collection, Schema, SerializedCollection};
use bonsaidb::local::config::{Builder, StorageConfiguration};
use bonsaidb::local::Database;
use serde::{Deserialize, Serialize};

#[derive(Debug, Schema)]
#[schema(name = "primary-keys", collections = [UserProfile, AssociatedProfileData])]
struct ExampleSchema;

// ANCHOR: derive_with_natural_id
#[derive(Debug, Serialize, Deserialize, Collection, Eq, PartialEq)]
#[collection(name = "user-profiles", primary_key = u32)]
struct UserProfile {
    #[natural_id]
    pub external_id: u32,
    pub name: String,
}
// ANCHOR_END: derive_with_natural_id

// ANCHOR: derive
#[derive(Debug, Serialize, Deserialize, Collection, Eq, PartialEq)]
#[collection(name = "multi-key", primary_key = AssociatedProfileKey)]
struct AssociatedProfileData {
    value: String,
}

#[derive(Key, Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
struct AssociatedProfileKey {
    pub user_id: u32,
    pub data_id: u64,
}
// ANCHOR_END: derive

fn main() -> Result<(), bonsaidb::core::Error> {
    drop(std::fs::remove_dir_all("primary-keys.bonsaidb"));
    let db = Database::open::<ExampleSchema>(StorageConfiguration::new("primary-keys.bonsaidb"))?;

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
    .push_into(&db)?;
    let retrieved_from_database = UserProfile::get(&42, &db)?.expect("document not found");
    assert_eq!(user, retrieved_from_database);
    // ANCHOR_END: natural_id_query

    // It's possible to use any type that implements the Key trait as a primary
    // key, including tuples:
    // ANCHOR: query
    let key = AssociatedProfileKey {
        user_id: user.header.id,
        data_id: 64,
    };
    let inserted = AssociatedProfileData {
        value: String::from("hello"),
    }
    .insert_into(&key, &db)?;
    let retrieved = AssociatedProfileData::get(&key, &db)?.expect("document not found");
    assert_eq!(inserted, retrieved);
    // ANCHOR_END: query

    // Not all primary key types support automatic key assignment -- the Key
    // trait implementation controls that behavior:
    assert!(matches!(
        AssociatedProfileData {
            value: String::from("error"),
        }
        .push_into(&db)
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
