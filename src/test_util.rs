use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use view::Map;

use crate::{
    document::Document,
    schema::{collection, view, Collection, Database, MapResult, Schema, View},
};

pub struct BasicCollection;

impl Collection for BasicCollection {
    fn id() -> collection::Id {
        collection::Id::from("tests.basic")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view::<BasicCount>();
    }
}

pub struct BasicCount;

impl<'k> View<'k> for BasicCount {
    type MapKey = ();
    type MapValue = usize;
    type Reduce = usize;

    fn version() -> usize {
        0
    }

    fn name() -> Cow<'static, str> {
        Cow::from("count")
    }

    fn map(document: &Document<'_>) -> MapResult<'k, Self::MapKey, Self::MapValue> {
        Ok(Some(document.emit_key_and_value((), 1)))
    }

    fn reduce(
        mappings: &[Map<'k, Self::MapKey, Self::MapValue>],
        _rereduce: bool,
    ) -> Result<Self::Reduce, view::Error> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Basic {
    pub value: String,
    pub parent_id: Option<Uuid>,
}

pub struct BasicDatabase;

impl Database for BasicDatabase {
    fn define_collections(schema: &mut Schema) {
        schema.define_collection::<BasicCollection>();
    }
}
