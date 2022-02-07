use serde::{Deserialize, Serialize};

use crate::{
    define_basic_unique_mapped_view,
    document::CollectionDocument,
    schema::{Collection, NamedCollection, SchemaName},
};

/// A database stored in `BonsaiDb`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, Collection)]
#[collection(authority = "bonsaidb", name = "databases", views = [ByName], core = crate)]
pub struct Database {
    /// The name of the database.
    pub name: String,
    /// The schema defining the database.
    pub schema: SchemaName,
}

define_basic_unique_mapped_view!(
    ByName,
    Database,
    1,
    "by-name",
    String,
    SchemaName,
    |document: CollectionDocument<Database>| {
        document.header.emit_key_and_value(
            document.contents.name.to_ascii_lowercase(),
            document.contents.schema,
        )
    },
);

impl NamedCollection for Database {
    type ByNameView = ByName;
}
