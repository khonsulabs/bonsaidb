use serde::{Deserialize, Serialize};

use crate::{
    document::Document,
    schema::{
        self, Collection, CollectionName, InvalidNameError, Name, SchemaName, Schematic, View,
    },
    Error,
};

/// A database stored in `BonsaiDb`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Database {
    /// The name of the database.
    pub name: String,
    /// The schema defining the database.
    pub schema: SchemaName,
}

impl Collection for Database {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("bonsaidb", "databases")
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
        let database = document.contents::<Database>()?;
        Ok(vec![document.emit_key_and_value(
            database.name.to_ascii_lowercase(),
            database.schema,
        )])
    }
}
