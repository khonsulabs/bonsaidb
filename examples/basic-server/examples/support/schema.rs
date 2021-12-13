use bonsaidb::core::{
    schema::{
        view::{self, CollectionView},
        Collection, CollectionDocument, CollectionName, InvalidNameError, MapResult, MappedValue,
        Name, Schematic,
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

#[derive(Debug)]
pub struct ShapesByNumberOfSides;

impl CollectionView for ShapesByNumberOfSides {
    type Collection = Shape;

    type Key = u32;

    type Value = usize;

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-number-of-sides")
    }

    fn map(
        &self,
        document: CollectionDocument<Self::Collection>,
    ) -> MapResult<Self::Key, Self::Value> {
        Ok(document.emit_key_and_value(document.contents.sides, 1))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
