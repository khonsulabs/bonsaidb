use bonsaidb_core::{
    document::Document,
    schema::{
        view, Collection, CollectionName, InvalidNameError, MapResult,
        MappedValue, Name, Schematic, View,
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

impl View for ShapesByNumberOfSides {
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
        document: &Document<'_>,
    ) -> MapResult<Self::Key, Self::Value> {
        let shape = document.contents::<Shape>()?;
        Ok(vec![document.emit_key_and_value(shape.sides, 1)])
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
