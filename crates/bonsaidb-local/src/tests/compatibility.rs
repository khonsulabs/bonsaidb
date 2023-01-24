use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use bonsaidb_core::connection::{Connection, StorageConnection};
use bonsaidb_core::document::{CollectionDocument, Emit};
use bonsaidb_core::keyvalue::KeyValue;
use bonsaidb_core::schema::{
    Collection, CollectionViewSchema, ReduceResult, Schema, SerializedCollection, View,
    ViewMappedValue,
};
use bonsaidb_core::test_util::TestDirectory;
use fs_extra::dir;
use serde::{Deserialize, Serialize};

use crate::config::{Builder, StorageConfiguration};
use crate::{Database, Storage};

#[derive(Schema, Debug)]
#[schema(name = "schema-a", collections = [Basic], core = bonsaidb_core)]
struct SchemaA;

#[derive(Schema, Debug)]
#[schema(name = "schema-b", collections = [Unique, Basic], core = bonsaidb_core)]
struct SchemaB;

#[derive(Collection, Debug, Serialize, Deserialize)]
#[collection(name = "unique", core = bonsaidb_core, views = [UniqueView])]
struct Unique {
    name: String,
}

#[derive(View, Debug, Clone)]
#[view(collection = Unique, key = String, core = bonsaidb_core)]
struct UniqueView;

impl CollectionViewSchema for UniqueView {
    type View = UniqueView;

    fn unique(&self) -> bool {
        true
    }

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> bonsaidb_core::schema::ViewMapResult<Self::View> {
        document.header.emit_key(document.contents.name)
    }
}

#[derive(Collection, Clone, Debug, Serialize, Deserialize)]
#[collection(name = "basic", views = [Scores], core = bonsaidb_core)]
struct Basic {
    key: String,
    value: u32,
}

#[derive(Clone, View, Debug)]
#[view(collection = Basic, key = String, value = u32, core = bonsaidb_core)]
struct Scores;

impl CollectionViewSchema for Scores {
    type View = Scores;

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> bonsaidb_core::schema::ViewMapResult<Self::View> {
        document
            .header
            .emit_key_and_value(document.contents.key, document.contents.value)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

fn create_databases(path: impl AsRef<Path>) {
    let storage = Storage::open(
        StorageConfiguration::new(path)
            .with_schema::<SchemaA>()
            .unwrap()
            .with_schema::<SchemaB>()
            .unwrap(),
    )
    .unwrap();

    write_basic(&storage.create_database::<SchemaA>("a-1", false).unwrap());

    write_basic(&storage.create_database::<SchemaA>("a-2", false).unwrap());

    write_unique(&storage.create_database::<SchemaB>("b-1", false).unwrap());
    write_basic(&storage.database::<SchemaB>("b-1").unwrap());

    write_unique(&storage.create_database::<SchemaB>("b-2", false).unwrap());
    write_basic(&storage.database::<SchemaB>("b-2").unwrap());
    drop(storage);
}

fn write_basic(db: &Database) {
    db.set_numeric_key("integer", 1_u64).execute().unwrap();
    db.set_key("string", &"test").execute().unwrap();
    // Give the kv-store time to persist
    std::thread::sleep(Duration::from_millis(100));

    Basic {
        key: String::from("a"),
        value: 1,
    }
    .push_into(db)
    .unwrap();
    Basic {
        key: String::from("a"),
        value: 2,
    }
    .push_into(db)
    .unwrap();
    Basic {
        key: String::from("b"),
        value: 3,
    }
    .push_into(db)
    .unwrap();
    Basic {
        key: String::from("b"),
        value: 4,
    }
    .push_into(db)
    .unwrap();
    Basic {
        key: String::from("c"),
        value: 5,
    }
    .push_into(db)
    .unwrap();
}

fn write_unique(db: &Database) {
    Unique {
        name: String::from("jon"),
    }
    .push_into(db)
    .unwrap();
    Unique {
        name: String::from("jane"),
    }
    .push_into(db)
    .unwrap();
}

fn test_databases(path: impl AsRef<Path>) {
    let storage = Storage::open(
        StorageConfiguration::new(path)
            .with_schema::<SchemaA>()
            .unwrap()
            .with_schema::<SchemaB>()
            .unwrap(),
    )
    .unwrap();

    test_basic(&storage.database::<SchemaA>("a-1").unwrap());
    test_basic(&storage.database::<SchemaA>("a-2").unwrap());
    test_unique(&storage.database::<SchemaB>("b-1").unwrap());
    test_basic(&storage.database::<SchemaB>("b-1").unwrap());
    test_unique(&storage.database::<SchemaB>("b-2").unwrap());
    test_basic(&storage.database::<SchemaB>("b-2").unwrap());
}

fn test_basic(db: &Database) {
    assert_eq!(db.get_key("integer").into_u64().unwrap(), Some(1));
    assert_eq!(
        db.get_key("string").into::<String>().unwrap().as_deref(),
        Some("test")
    );

    let all_docs = Basic::all(db).query().unwrap();
    assert_eq!(all_docs.len(), 5);

    let a_scores = db
        .view::<Scores>()
        .with_key("a")
        .query_with_collection_docs()
        .unwrap();
    assert_eq!(a_scores.mappings.len(), 2);
    assert_eq!(a_scores.documents.len(), 2);

    for mapping in db.view::<Scores>().reduce_grouped().unwrap() {
        let expected_value = match mapping.key.as_str() {
            "a" => 3,
            "b" => 7,
            "c" => 5,
            _ => unreachable!(),
        };
        assert_eq!(mapping.value, expected_value);
    }

    let transactions = db.list_executed_transactions(None, None).unwrap();
    let kv_transactions = transactions
        .iter()
        .filter_map(|t| t.changes.keys())
        .collect::<Vec<_>>();
    assert_eq!(kv_transactions.len(), 2);
    let keys = kv_transactions
        .iter()
        .flat_map(|changed_keys| {
            changed_keys
                .iter()
                .map(|changed_key| changed_key.key.as_str())
        })
        .collect::<HashSet<_>>();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains("string"));
    assert!(keys.contains("integer"));

    let basic_transactions = transactions.iter().filter_map(|t| {
        t.changes.documents().and_then(|changes| {
            changes
                .collections
                .contains(&Basic::collection_name())
                .then(|| &changes.documents)
        })
    });
    assert_eq!(basic_transactions.count(), 5);
}

fn test_unique(db: &Database) {
    // Attempt to violate a unique key violation before accessing the view. This
    // tests the upgrade fpath for view verisons, if things have changed.
    Unique {
        name: String::from("jon"),
    }
    .push_into(db)
    .unwrap_err();
    let mappings = db
        .view::<UniqueView>()
        .with_key("jane")
        .query_with_collection_docs()
        .unwrap();
    assert_eq!(mappings.len(), 1);
    let jane = mappings.into_iter().next().unwrap().document;
    assert_eq!(jane.contents.name, "jane");
}

fn test_compatibility(dir: &str) {
    let project_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .unwrap_or_else(|_| String::from("./crates/bonsaidb-local")),
    );

    let test_dir = TestDirectory::new(format!("v{dir}-compatibility.nebari"));
    dir::copy(
        project_dir
            .join("src")
            .join("tests")
            .join("compatibility")
            .join(dir),
        &test_dir,
        &dir::CopyOptions {
            content_only: true,
            copy_inside: true,
            ..dir::CopyOptions::default()
        },
    )
    .unwrap();

    test_databases(&test_dir);
}

#[test]
fn self_compatibility() {
    let dir = TestDirectory::new("self-compatibiltiy.bonsaidb");
    create_databases(&dir);
    test_databases(&dir);
    if std::env::var("UPDATE_COMPATIBILITY")
        .map(|v| !v.is_empty())
        .unwrap_or_default()
    {
        let version = format!(
            "{}.{}",
            std::env::var("CARGO_PKG_VERSION_MAJOR").unwrap(),
            std::env::var("CARGO_PKG_VERSION_MINOR").unwrap()
        );
        let path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
        let version_path = path
            .join("src")
            .join("tests")
            .join("compatibility")
            .join(version);
        if version_path.exists() {
            std::fs::remove_dir_all(&version_path).unwrap();
        }
        dir::copy(
            &dir,
            version_path,
            &dir::CopyOptions {
                content_only: true,
                copy_inside: true,
                ..dir::CopyOptions::default()
            },
        )
        .unwrap();
    }
}

#[test]
fn compatible_with_0_1_x() {
    test_compatibility("0.1");
}

#[test]
fn compatible_with_0_2_x() {
    test_compatibility("0.2");
}
