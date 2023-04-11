use bonsaidb::core::document::{CollectionDocument, Emit};
use bonsaidb::core::schema::{
    Collection, CollectionMapReduce, ReduceResult, View, ViewMapResult, ViewMappedValue, ViewSchema,
};
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

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = Shape, key = u32, value = usize, name = "by-number-of-sides")]
pub struct ShapesByNumberOfSides;

impl CollectionMapReduce for ShapesByNumberOfSides {
    fn map<'doc>(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<'doc, Self::View> {
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
