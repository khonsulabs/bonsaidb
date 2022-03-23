use core::fmt::Debug;

use bonsaidb::core::{
    document::{CollectionDocument, Emit, KeyId},
    schema::{
        Collection, CollectionViewSchema, DefaultSerialization, DefaultViewSerialization, Name,
        Qualified, Schematic, SerializedCollection, View, ViewMapResult,
    },
};
use serde::{Deserialize, Serialize};

#[test]
fn name_only() {
    #[derive(Collection, Debug)]
    #[collection(name = "Name", core = ::bonsaidb::core)]
    struct Test<T: Sync + Send + Debug>(T);

    assert_eq!(
        Test::<String>::collection_name(),
        bonsaidb::core::schema::CollectionName::private("Name")
    );
}

#[test]
fn name_and_authority() {
    #[derive(Collection, Debug)]
    #[collection(name = "Name", authority = "Authority")]
    struct Test<T: Sync + Send + Debug>(T);

    assert_eq!(
        Test::<String>::collection_name(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name")
    );
}

#[test]
fn views() {
    #[derive(Clone, Collection, Debug, Serialize, Deserialize)]
    #[collection(name = "Name", authority = "Authority", views = [ShapesByNumberOfSides])]
    struct Shape {
        pub sides: u32,
    }

    let schematic = Schematic::from_schema::<Shape>().unwrap();
    schematic
        .view::<ShapesByNumberOfSides>()
        .expect("couldn't find view");

    #[derive(Debug, Clone)]
    struct ShapesByNumberOfSides;

    impl View for ShapesByNumberOfSides {
        type Collection = Shape;
        type Key = u32;
        type Value = usize;

        fn name(&self) -> Name {
            Name::new("by-number-of-sides")
        }
    }

    impl CollectionViewSchema for ShapesByNumberOfSides {
        type View = Self;

        fn map(&self, document: CollectionDocument<Shape>) -> ViewMapResult<Self::View> {
            document
                .header
                .emit_key_and_value(document.contents.sides, 1)
        }
    }

    impl DefaultViewSerialization for ShapesByNumberOfSides {}
}

#[test]
fn serialization() {
    #[derive(Collection, Clone, Debug, Deserialize, Serialize)]
    #[collection(
        name = "Name",
        authority = "Authority",
        serialization = transmog_bincode::Bincode
    )]
    struct Test;

    assert_eq!(
        Test::collection_name(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name")
    );

    let _: transmog_bincode::Bincode = Test::format();
}

#[test]
fn serialization_none() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name", authority = "Authority", serialization = None)]
    struct Test;

    impl DefaultSerialization for Test {}

    assert_eq!(
        Test::collection_name(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name")
    );
}

// This could be done through using the macro in the tests for encryption
#[test]
// Pretty pointless, maybe also error?
fn encryption_optional_no_key() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name", encryption_optional)]
    struct Test;
}

#[test]
fn encryption_optional_with_key() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name")]
    #[collection(encryption_optional, encryption_key = Some(KeyId::Master))]
    struct Test;
}

#[test]
fn encryption_required_with_key() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name")]
    #[collection(encryption_required, encryption_key = Some(KeyId::Master))]
    struct Test;
}

#[test]
fn encryption_key() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name")]
    #[collection(encryption_key = Some(KeyId::Master))]
    struct Test;
}

#[test]
fn primary_key() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name")]
    #[collection(primary_key = u32)]
    struct Test;
}

#[test]
fn primary_key_natural_id() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name")]
    #[collection(primary_key = u32, natural_id = |_:&Self| Some(1_u32))]
    struct Test;
}

#[test]
fn natural_id() {
    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name")]
    #[collection( natural_id = |_:&Self| Some(1_u64))]
    struct Test;
}
