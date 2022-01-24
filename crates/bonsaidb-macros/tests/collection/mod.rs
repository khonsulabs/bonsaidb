use core::fmt::Debug;

use bonsaidb::core::schema::SerializedCollection;

#[test]
fn name_only() {
    use bonsaidb::core::schema::Collection;
    #[derive(Collection, Debug)]
    #[collection(name = "Name", authority = "Authority")]
    struct Test<T: Sync + Send + Debug>(T);

    assert_eq!(
        Test::<String>::collection_name().unwrap(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name").unwrap()
    );
}
#[test]
fn views() {
    use bonsaidb::core::schema::{
        Collection, CollectionDocument, CollectionViewSchema, DefaultViewSerialization,
        InvalidNameError, Name, View, ViewMapResult,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Serialize, Deserialize)]
    #[collection(name = "Name", authority = "Authority", views(ShapesByNumberOfSides))]
    struct Shape {
        pub sides: u32,
    }

    // TODO somehow test views

    #[derive(Debug, Clone)]
    struct ShapesByNumberOfSides;

    impl View for ShapesByNumberOfSides {
        type Collection = Shape;
        type Key = u32;
        type Value = usize;

        fn name(&self) -> Result<Name, InvalidNameError> {
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
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(
        name = "Name",
        authority = "Authority",
        serialization(transmog_bincode::Bincode)
    )]
    struct Test;

    assert_eq!(
        Test::collection_name().unwrap(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name").unwrap()
    );

    let _: transmog_bincode::Bincode = Test::format();
}

#[test]
fn serialization_none() {
    use bonsaidb::core::schema::{Collection, DefaultSerialization};
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Deserialize, Serialize)]
    #[collection(name = "Name", authority = "Authority", serialization(None))]
    struct Test;

    impl DefaultSerialization for Test {}

    assert_eq!(
        Test::collection_name().unwrap(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name").unwrap()
    );
}
