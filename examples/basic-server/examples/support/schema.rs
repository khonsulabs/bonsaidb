use bonsaidb::core::document::{CollectionDocument, Emit};
use bonsaidb::core::schema::view::CollectionViewSchema;
use bonsaidb::core::schema::{Collection, ReduceResult, View, ViewMapResult, ViewMappedValue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "shapes", views = [ShapesByNumberOfSides])]
pub struct Shape {
    pub sides: u32,
}

impl Shape {
    pub fn new(sides: u32) -> Self {
        Self { sides }
    }
}

#[derive(Debug, Clone, View)]
#[view(collection = Shape, key = u32, value = usize, name = "by-number-of-sides")]
pub struct ShapesByNumberOfSides;

impl CollectionViewSchema for ShapesByNumberOfSides {
    type View = Self;

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        document
            .header
            .emit_key_and_value(document.contents.sides, 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
