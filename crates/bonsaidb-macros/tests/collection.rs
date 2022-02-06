use core::fmt::Debug;

use bonsaidb::core::{document::CollectionDocument, schema::Schematic};

#[test]
fn name_only() {
    use bonsaidb::core::schema::Collection;
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
    use bonsaidb::core::schema::Collection;
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
    use bonsaidb::core::schema::{
        Collection, CollectionViewSchema, DefaultViewSerialization, Name, View, ViewMapResult,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Serialize, Deserialize)]
    #[collection(name = "Name", authority = "Authority", views = [ShapesByNumberOfSides])]
    struct Shape {
        pub sides: u32,
    }

    let schematic = Schematic::from_schema::<Shape>().unwrap();
    assert!(schematic.view::<ShapesByNumberOfSides>().is_some());

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
            Ok(document.emit_key_and_value(document.contents.sides, 1))
        }
    }

    impl DefaultViewSerialization for ShapesByNumberOfSides {}
}

#[test]
fn serialization() {
    use bonsaidb::core::schema::Collection;
    use bonsaidb::core::schema::SerializedCollection;
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Deserialize, Serialize)]
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
    use bonsaidb::core::schema::{Collection, DefaultSerialization};
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name", authority = "Authority", serialization = None)]
    struct Test;

    impl DefaultSerialization for Test {}

    assert_eq!(
        Test::collection_name(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name")
    );
}
