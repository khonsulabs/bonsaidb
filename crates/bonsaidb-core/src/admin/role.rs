use serde::{Deserialize, Serialize};

use crate::{
    document::Document,
    schema::{
        Collection, CollectionName, InvalidNameError, MapResult, Name, NamedCollection, Schematic,
        View,
    },
    Error,
};

/// An assignable role, which grants permissions based on the associated [`PermissionGroup`](crate::admin::PermissionGroup)s.
#[derive(Debug, Serialize, Deserialize)]
pub struct Role {
    /// The name of the role. Must be unique.
    pub name: String,
    /// The IDs of the permission groups this role belongs to.
    pub groups: Vec<u64>,
}

impl Role {
    /// Returns a new role with no groups and the name provided.
    pub fn named<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            groups: Vec::new(),
        }
    }

    /// Builder-style method. Returns self after replacing the current groups with `ids`.
    pub fn with_group_ids<I: IntoIterator<Item = u64>>(mut self, ids: I) -> Self {
        self.groups = ids.into_iter().collect();
        self
    }
}

impl Collection for Role {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "role")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByName)
    }
}

impl NamedCollection for Role {
    type ByNameView = ByName;
}

/// A unique view of roles by name.
#[derive(Debug)]
pub struct ByName;

impl View for ByName {
    type Collection = Role;
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
        let role = document.contents::<Role>()?;
        Ok(vec![document.emit_key(role.name)])
    }
}
