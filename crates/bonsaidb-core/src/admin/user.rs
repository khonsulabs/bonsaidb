use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    admin::{group, role},
    connection::{
        AsyncConnection, AsyncStorageConnection, Connection, IdentityReference, SensitiveString,
        StorageConnection,
    },
    define_basic_unique_mapped_view,
    document::{CollectionDocument, Emit, KeyId},
    permissions::Permissions,
    schema::{Collection, NamedCollection, NamedReference, SerializedCollection},
};

/// A user that can authenticate with BonsaiDb.
#[derive(Clone, Debug, Serialize, Deserialize, Default, Collection)]
#[collection(name = "user", authority = "khonsulabs", views = [ByName])]
#[collection(encryption_key = Some(KeyId::Master), encryption_optional, core = crate)]
pub struct User {
    /// The name of the role. Must be unique.
    pub username: String,
    /// The IDs of the user groups this user belongs to.
    pub groups: Vec<u64>,
    /// The IDs of the roles this user has been assigned.
    pub roles: Vec<u64>,

    /// The user's stored password hash.
    ///
    /// This field is not feature gated to prevent losing stored passwords if
    /// the `password-hashing` feature is disabled and then re-enabled and user
    /// records are updated in the meantime.
    #[serde(default)]
    pub argon_hash: Option<SensitiveString>,
}

impl User {
    pub fn assume_identity<'name, Storage: StorageConnection>(
        name_or_id: impl Into<NamedReference<'name, u64>>,
        storage: &Storage,
    ) -> Result<Storage::Authenticated, crate::Error> {
        storage.assume_identity(IdentityReference::User(name_or_id.into()))
    }

    pub async fn assume_identity_async<'name, Storage: AsyncStorageConnection>(
        name_or_id: impl Into<NamedReference<'name, u64>> + Send,
        storage: &Storage,
    ) -> Result<Storage::Authenticated, crate::Error> {
        storage
            .assume_identity(IdentityReference::User(name_or_id.into()))
            .await
    }

    /// Returns a default user with the given username.
    pub fn default_with_username(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            ..Self::default()
        }
    }

    /// Calculates the effective permissions based on the groups and roles this
    /// user is assigned.
    pub fn effective_permissions<C: Connection>(
        &self,
        admin: &C,
        inherit_permissions: &Permissions,
    ) -> Result<Permissions, crate::Error> {
        // List all of the groups that this user belongs to because of role associations.
        let role_groups = if self.roles.is_empty() {
            Vec::default()
        } else {
            let roles = role::Role::get_multiple(self.groups.iter(), admin)?;
            roles
                .into_iter()
                .flat_map(|doc| doc.contents.groups)
                .unique()
                .collect::<Vec<_>>()
        };
        // Retrieve all of the groups.
        let groups = if role_groups.is_empty() {
            group::PermissionGroup::get_multiple(self.groups.iter(), admin)?
        } else {
            let mut all_groups = role_groups;
            all_groups.extend(self.groups.iter().copied());
            all_groups.dedup();
            group::PermissionGroup::get_multiple(all_groups, admin)?
        };

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

    pub async fn effective_permissions_async<C: AsyncConnection>(
        &self,
        admin: &C,
    ) -> Result<Permissions, crate::Error> {
        // List all of the groups that this user belongs to because of role associations.
        let role_groups = if self.roles.is_empty() {
            Vec::default()
        } else {
            let roles = role::Role::get_multiple_async(self.groups.iter(), admin).await?;
            roles
                .into_iter()
                .flat_map(|doc| doc.contents.groups)
                .unique()
                .collect::<Vec<_>>()
        };
        // Retrieve all of the groups.
        let groups = if role_groups.is_empty() {
            group::PermissionGroup::get_multiple_async(self.groups.iter(), admin).await?
        } else {
            let mut all_groups = role_groups;
            all_groups.extend(self.groups.iter().copied());
            all_groups.dedup();
            group::PermissionGroup::get_multiple_async(all_groups, admin).await?
        };

        // Combine the permissions from all the groups into one.
        let merged_permissions = Permissions::merged(
            groups
                .into_iter()
                .map(|group| Permissions::from(group.contents.statements))
                .collect::<Vec<_>>()
                .iter(),
        );

        Ok(merged_permissions)
    }
}

impl NamedCollection for User {
    type ByNameView = ByName;
}

define_basic_unique_mapped_view!(
    ByName,
    User,
    1,
    "by-name",
    String,
    |document: CollectionDocument<User>| { document.header.emit_key(document.contents.username) }
);
