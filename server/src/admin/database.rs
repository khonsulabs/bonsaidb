use std::borrow::Cow;

use pliantdb_local::core::{
    document::Document,
    schema::{self, collection, Collection, Schematic, View},
};

#[derive(Debug)]
pub struct Database;

impl Collection for Database {
    fn collection_id() -> collection::Id {
        collection::Id::from("pliantdb.databases")
    }

    fn define_views(schema: &mut Schematic) {
        schema.define_view(ByName);
    }
}

#[derive(Debug)]
pub struct ByName;

impl View for ByName {
    type Collection = Database;
    type Key = String;
    type Value = schema::Id;

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-name")
    }

    fn map(&self, document: &Document<'_>) -> schema::MapResult<Self::Key, Self::Value> {
        let database = document.contents::<pliantdb_core::networking::Database>()?;
        Ok(Some(document.emit_key_and_value(
            database.name.to_ascii_lowercase(),
            database.schema,
        )))
    }
}
