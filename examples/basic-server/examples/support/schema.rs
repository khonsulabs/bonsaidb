use bonsaidb::core::{
    document::CollectionDocument,
    schema::{
        view::CollectionViewSchema, Collection, DefaultViewSerialization, Name, ReduceResult, View,
        ViewMapResult, ViewMappedValue,
    },
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

#[derive(Debug, Clone)]
pub struct ShapesByNumberOfSides;

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

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        Ok(document.emit_key_and_value(document.contents.sides, 1))
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}

impl DefaultViewSerialization for ShapesByNumberOfSides {}
