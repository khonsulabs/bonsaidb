use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use bonsaidb_core::{
    connection::{Connection, StorageConnection},
    document::CollectionDocument,
    keyvalue::KeyValue,
    schema::{
        Collection, CollectionViewSchema, ReduceResult, Schema, SerializedCollection, View,
        ViewMappedValue,
    },
    test_util::TestDirectory,
};
use serde::{Deserialize, Serialize};

use crate::{
    config::{Builder, StorageConfiguration},
    Database, Storage,
};

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
        Ok(document.header.emit_key(document.contents.name))
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
        Ok(document
            .header
            .emit_key_and_value(document.contents.key, document.contents.value))
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

async fn create_databases(path: impl AsRef<Path>) {
    let storage = Storage::open(
        StorageConfiguration::new(path)
            .with_schema::<SchemaA>()
            .unwrap()
            .with_schema::<SchemaB>()
            .unwrap(),
    )
    .await
    .unwrap();

    storage
        .create_database::<SchemaA>("a-1", false)
        .await
        .unwrap();
    write_basic(storage.database::<SchemaA>("a-1").await.unwrap()).await;

    storage
        .create_database::<SchemaA>("a-2", false)
        .await
        .unwrap();
    write_basic(storage.database::<SchemaA>("a-2").await.unwrap()).await;

    storage
        .create_database::<SchemaB>("b-1", false)
        .await
        .unwrap();
    write_unique(storage.database::<SchemaB>("b-1").await.unwrap()).await;
    write_basic(storage.database::<SchemaB>("b-1").await.unwrap()).await;

    storage
        .create_database::<SchemaB>("b-2", false)
        .await
        .unwrap();
    write_unique(storage.database::<SchemaB>("b-2").await.unwrap()).await;
    write_basic(storage.database::<SchemaB>("b-2").await.unwrap()).await;
    drop(storage);
}

async fn write_basic(db: Database) {
    db.set_numeric_key("integer", 1_u64).await.unwrap();
    db.set_key("string", &"test").await.unwrap();
    // Give the kv-store time to persist
    tokio::time::sleep(Duration::from_millis(10)).await;

    Basic {
        key: String::from("a"),
        value: 1,
    }
    .push_into(&db)
    .await
    .unwrap();
    Basic {
        key: String::from("a"),
        value: 2,
    }
    .push_into(&db)
    .await
    .unwrap();
    Basic {
        key: String::from("b"),
        value: 3,
    }
    .push_into(&db)
    .await
    .unwrap();
    Basic {
        key: String::from("b"),
        value: 4,
    }
    .push_into(&db)
    .await
    .unwrap();
    Basic {
        key: String::from("c"),
        value: 5,
    }
    .push_into(&db)
    .await
    .unwrap();
}

async fn write_unique(db: Database) {
    Unique {
        name: String::from("jon"),
    }
    .push_into(&db)
    .await
    .unwrap();
    Unique {
        name: String::from("jane"),
    }
    .push_into(&db)
    .await
    .unwrap();
}

async fn test_databases(path: impl AsRef<Path>) {
    let storage = Storage::open(
        StorageConfiguration::new(path)
            .with_schema::<SchemaA>()
            .unwrap()
            .with_schema::<SchemaB>()
            .unwrap(),
    )
    .await
    .unwrap();

    test_basic(storage.database::<SchemaA>("a-1").await.unwrap()).await;
    test_basic(storage.database::<SchemaA>("a-2").await.unwrap()).await;
    test_unique(storage.database::<SchemaB>("b-1").await.unwrap()).await;
    test_basic(storage.database::<SchemaB>("b-1").await.unwrap()).await;
    test_unique(storage.database::<SchemaB>("b-2").await.unwrap()).await;
    test_basic(storage.database::<SchemaB>("b-2").await.unwrap()).await;
}

async fn test_basic(db: Database) {
    assert_eq!(db.get_key("integer").into_u64().await.unwrap(), Some(1));
    assert_eq!(
        db.get_key("string")
            .into::<String>()
            .await
            .unwrap()
            .as_deref(),
        Some("test")
    );

    let all_docs = Basic::list(.., &db).await.unwrap();
    assert_eq!(all_docs.len(), 5);

    let a_scores = db
        .view::<Scores>()
        .with_key(String::from("a"))
        .query_with_collection_docs()
        .await
        .unwrap();
    assert_eq!(a_scores.mappings.len(), 2);
    assert_eq!(a_scores.documents.len(), 2);

    for mapping in db.view::<Scores>().reduce_grouped().await.unwrap() {
        let expected_value = match mapping.key.as_str() {
            "a" => 3,
            "b" => 7,
            "c" => 5,
            _ => unreachable!(),
        };
        assert_eq!(mapping.value, expected_value);
    }
}

async fn test_unique(db: Database) {
    // Attempt to violate a unique key violation before accessing the view. This
    // tests the upgrade fpath for view verisons, if things have changed.
    Unique {
        name: String::from("jon"),
    }
    .push_into(&db)
    .await
    .unwrap_err();
    let mappings = db
        .view::<UniqueView>()
        .with_key(String::from("jane"))
        .query_with_collection_docs()
        .await
        .unwrap();
    assert_eq!(mappings.len(), 1);
    let jane = mappings.into_iter().next().unwrap().document;
    assert_eq!(jane.contents.name, "jane");
}

#[tokio::test]
async fn self_compatibility() {
    let project_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let dir = TestDirectory::absolute(project_dir.join(".self-compatibiltiy.bonsaidb"));
    create_databases(&dir).await;
    test_databases(&dir).await;
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
        std::fs::rename(&dir, version_path).unwrap();
    }
}

#[tokio::test]
async fn compatible_with_0_1_x() {
    let project_dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .unwrap_or_else(|_| String::from("./crates/bonsaidb-local")),
    );

    test_databases(
        project_dir
            .join("src")
            .join("tests")
            .join("compatibility")
            .join("0.1"),
    )
    .await;
}
