use pliantdb_core::{
    document::Document,
    permissions::Statement,
    schema::{Collection, CollectionName, InvalidNameError, MapResult, Name, Schematic, View},
    Error,
};
use serde::{Deserialize, Serialize};

/// A named group of permissions statements.
#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::module_name_repetitions)]
pub struct PermissionGroup {
    /// The name of the group. Must be unique.
    pub name: String,
    /// The permission statements.
    pub statements: Vec<Statement>,
}

impl Collection for PermissionGroup {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "permission-group")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByName)
    }
}

/// A unique view of permission groups by name.
#[derive(Debug)]
pub struct ByName;

impl View for ByName {
    type Collection = PermissionGroup;
    type Key = String;
    type Value = ();

    fn unique(&self) -> bool {
        true
    }

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-name")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let group = document.contents::<PermissionGroup>()?;
        Ok(Some(document.emit_key(group.name)))
    }
}
