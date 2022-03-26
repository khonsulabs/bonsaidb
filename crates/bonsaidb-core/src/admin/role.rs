use actionable::Permissions;
use serde::{Deserialize, Serialize};

use crate::{
    admin::group,
    connection::{AsyncStorageConnection, Connection, IdentityReference, StorageConnection},
    define_basic_unique_mapped_view,
    document::{CollectionDocument, Emit},
    schema::{Collection, Nameable, NamedCollection, SerializedCollection},
};

/// An assignable role, which grants permissions based on the associated [`PermissionGroup`](crate::admin::PermissionGroup)s.
#[derive(Clone, Debug, Serialize, Deserialize, Collection)]
#[collection(name = "role", authority="khonsulabs", views = [ByName], core = crate)]
#[must_use]
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

    pub fn assume_identity<'name, Storage: StorageConnection>(
        name_or_id: impl Nameable<'name, u64>,
        storage: &Storage,
    ) -> Result<Storage::Authenticated, crate::Error> {
        storage.assume_identity(IdentityReference::Role(name_or_id.name()?))
    }

    pub async fn assume_identity_async<'name, Storage: AsyncStorageConnection>(
        name_or_id: impl Nameable<'name, u64> + Send,
        storage: &Storage,
    ) -> Result<Storage::Authenticated, crate::Error> {
        storage
            .assume_identity(IdentityReference::Role(name_or_id.name()?))
            .await
    }

    /// Calculates the effective permissions based on the groups this role is assigned.
    pub fn effective_permissions<C: Connection>(
        &self,
        admin: &C,
        inherit_permissions: &Permissions,
    ) -> Result<Permissions, crate::Error> {
        let groups = group::PermissionGroup::get_multiple(&self.groups, admin)?;

        // Combine the permissions from all the groups into one.
        let merged_permissions = Permissions::merged(
            groups
                .into_iter()
                .map(|group| Permissions::from(group.contents.statements))
                .collect::<Vec<_>>()
                .iter()
                .chain(std::iter::once(inherit_permissions)),
        );

        Ok(merged_permissions)
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
