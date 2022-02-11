use serde::{Deserialize, Serialize};

use crate::{
    define_basic_unique_mapped_view,
    document::CollectionDocument,
    permissions::Statement,
    schema::{Collection, NamedCollection},
};

/// A named group of permissions statements.
#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "permission-group", authority="khonsulabs", views = [ByName], core = crate)]

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

impl NamedCollection for PermissionGroup {
    type ByNameView = ByName;
}

define_basic_unique_mapped_view!(
    ByName,
    PermissionGroup,
    1,
    "by-name",
    String,
    |document: CollectionDocument<PermissionGroup>| {
        document.header.emit_key(document.contents.name)
    }
);
