use bonsaidb::core::schema::Collection;

#[test]
fn name_only() {
    #[derive(Collection, Debug)]
    #[collection(name = "Name", authority = "Authority")]
    struct Test;

    assert_eq!(
        Test::collection_name().unwrap(),
        bonsaidb::core::schema::CollectionName::new("Authority", "Name").unwrap()
    );
}
#[test]
fn views() {
    use bonsaidb::core::schema::{
        CollectionDocument, CollectionViewSchema, DefaultSerialization, DefaultViewSerialization,
        InvalidNameError, Name, View, ViewMapResult,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Collection, Debug, Serialize, Deserialize)]
    #[collection(name = "Name", authority = "Authority", views(ShapesByNumberOfSides))]
    struct Shape {
        pub sides: u32,
    }

    impl DefaultSerialization for Shape {}

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
