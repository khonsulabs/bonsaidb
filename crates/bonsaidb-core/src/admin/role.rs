use serde::{Deserialize, Serialize};

use crate::{
    define_basic_unique_mapped_view,
    document::{CollectionDocument, DocumentId},
    schema::{Collection, NamedCollection},
};

/// An assignable role, which grants permissions based on the associated [`PermissionGroup`](crate::admin::PermissionGroup)s.
#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "role", authority="khonsulabs", views = [ByName], core = crate)]
pub struct Role {
    /// The name of the role. Must be unique.
    pub name: String,
    /// The IDs of the permission groups this role belongs to.
    pub groups: Vec<DocumentId>,
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
    pub fn with_group_ids<I: IntoIterator<Item = DocumentId>>(mut self, ids: I) -> Self {
        self.groups = ids.into_iter().collect();
        self
    }
}

impl NamedCollection for Role {
    type ByNameView = ByName;
}

define_basic_unique_mapped_view!(
    ByName,
    Role,
    1,
    "by-name",
    String,
    |document: CollectionDocument<Role>| { document.header.emit_key(document.contents.name) }
);
