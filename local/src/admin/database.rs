use pliantdb_core::{
    document::Document,
    schema::{self, Collection, CollectionName, InvalidNameError, Name, Schematic, View},
    Error,
};

/// A database stored in `PliantDb`.
#[derive(Debug)]
pub struct Database;

impl Collection for Database {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("pliantdb", "databases")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByName)
    }
}

#[derive(Debug)]
pub struct ByName;

impl View for ByName {
    type Collection = Database;
    type Key = String;
    type Value = schema::SchemaName;

    fn unique(&self) -> bool {
        true
    }

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-name")
    }

    fn map(&self, document: &Document<'_>) -> schema::MapResult<Self::Key, Self::Value> {
        let database = document.contents::<pliantdb_core::connection::Database>()?;
        Ok(Some(document.emit_key_and_value(
            database.name.to_ascii_lowercase(),
            database.schema,
        )))
    }
}
