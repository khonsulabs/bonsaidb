use bonsaidb::core::{
    schema::{
        view::CollectionViewSchema, Collection, CollectionDocument, CollectionName,
        DefaultSerialization, DefaultViewSerialization, InvalidNameError, Name, ReduceResult,
        Schematic, View, ViewMapResult, ViewMappedValue,
    },
    Error,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Shape {
    pub sides: u32,
}

impl Shape {
    pub fn new(sides: u32) -> Self {
        Self { sides }
    }
}

impl Collection for Shape {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "shapes")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ShapesByNumberOfSides)
    }
}

impl DefaultSerialization for Shape {}

#[derive(Debug, Clone)]
pub struct ShapesByNumberOfSides;

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
