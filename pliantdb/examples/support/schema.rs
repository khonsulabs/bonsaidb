use std::borrow::Cow;

use pliantdb_core::{
    document::Document,
    schema::{view, Collection, CollectionId, MapResult, MappedValue, Schematic, View},
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
    fn collection_id() -> CollectionId {
        CollectionId::from("shapes")
    }

    fn define_views(schema: &mut Schematic) {
        schema.define_view(ShapesByNumberOfSides);
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

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-number-of-sides")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let shape = document.contents::<Shape>()?;
        Ok(Some(document.emit_key_and_value(shape.sides, 1)))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
