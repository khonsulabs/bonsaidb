use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    document::Document,
    permissions::Statement,
    schema::{
        Collection, CollectionName, InvalidNameError, MapResult, Name, NamedCollection, Schematic,
        View,
    },
    Error,
};

/// A named group of permissions statements.
#[derive(Debug, Serialize, Deserialize)]
pub struct PermissionGroup {
    /// The name of the group. Must be unique.
    pub name: String,
    /// The permission statements.
    pub statements: Vec<Statement>,
}

impl PermissionGroup {
    /// Returns a new group with no statements and the name provided.
    pub fn named<S: Into<String>>(name: S) -> Self {
        Self {
            name: name.into(),
            statements: Vec::new(),
        }
    }

    /// Builder-style method. Returns self after replacing the current statements with `statements`.
    pub fn with_group_ids<I: IntoIterator<Item = Statement>>(mut self, statements: I) -> Self {
        self.statements = statements.into_iter().collect();
        self
    }
}

#[async_trait]
impl Collection for PermissionGroup {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "permission-group")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByName)
    }
}

impl NamedCollection for PermissionGroup {
    type ByNameView = ByName;
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
