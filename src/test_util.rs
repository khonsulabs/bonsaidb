use std::borrow::Cow;

use uuid::Uuid;
use view::Map;

use crate::schema::{collection, view, Collection, MapResult, View};
use serde::{Deserialize, Serialize};

pub struct BasicCollection;

impl Collection for BasicCollection {
    fn id() -> collection::Id {
        collection::Id::from("tests.basic")
    }

    fn define_views(schema: &mut crate::schema::Schema) {
        schema.define_view::<BasicCount, Self>();
    }
}

pub struct BasicCount;

impl<'k> View<'k, BasicCollection> for BasicCount {
    type MapKey = ();
    type MapValue = usize;
    type Reduce = usize;

    fn version() -> usize {
        0
    }

    fn name() -> Cow<'static, str> {
        Cow::from("count")
    }

    fn map(
        document: &crate::schema::Document<'_, BasicCollection>,
    ) -> MapResult<'k, Self::MapKey, Self::MapValue> {
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
    pub parent_id: Option<Uuid>,
}
