use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    admin::{group, role},
    connection::Connection,
    custodian_password::{ServerFile, ServerRegistration},
    define_basic_unique_mapped_view,
    document::KeyId,
    permissions::Permissions,
    schema::{
        Collection, CollectionDocument, CollectionName, DefaultSerialization, NamedCollection,
        Schematic,
    },
    Error, ENCRYPTION_ENABLED,
};

/// A user that can authenticate with `BonsaiDb`.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct User {
    /// The name of the role. Must be unique.
    pub username: String,
    /// The IDs of the user groups this user belongs to.
    pub groups: Vec<u64>,
    /// The IDs of the roles this user has been assigned.
    pub roles: Vec<u64>,

    /// An `OPAQUE PAKE` payload.
    pub password_hash: Option<ServerFile>,

    /// A temporary password state. Each call to SetPassword will overwrite the
    /// previous state.
    pub pending_password_change_state: Option<ServerRegistration>,
}

impl User {
    /// Returns a default user with the given username.
    pub fn default_with_username(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            ..Self::default()
        }
    }

    /// Calculates the effective permissions based on the groups and roles this
    /// user is assigned.
    pub async fn effective_permissions<C: Connection>(
        &self,
        admin: &C,
    ) -> Result<Permissions, crate::Error> {
        // List all of the groups that this user belongs to because of role associations.
        let role_groups = if self.roles.is_empty() {
            Vec::default()
        } else {
            let roles = admin.get_multiple::<role::Role>(&self.groups).await?;
            let role_groups = roles
                .into_iter()
                .map(|doc| doc.contents::<role::Role>().map(|role| role.groups))
                .collect::<Result<Vec<Vec<u64>>, _>>()?;
            role_groups
                .into_iter()
                .flat_map(Vec::into_iter)
                .collect::<Vec<u64>>()
        };
        // Retrieve all of the groups.
        let groups = if role_groups.is_empty() {
            admin
                .get_multiple::<group::PermissionGroup>(&self.groups)
                .await?
        } else {
            let mut all_groups = role_groups;
            all_groups.extend(self.groups.iter().copied());
            all_groups.dedup();
            admin
                .get_multiple::<group::PermissionGroup>(&all_groups)
                .await?
        };

        // Combine the permissions from all the groups into one.
        let merged_permissions = Permissions::merged(
            groups
                .into_iter()
                .map(|group| {
                    group
                        .contents::<group::PermissionGroup>()
                        .map(|group| Permissions::from(group.statements))
                })
                .collect::<Result<Vec<_>, _>>()?
                .iter(),
        );

        Ok(merged_permissions)
    }
}

#[async_trait]
impl Collection for User {
    fn encryption_key() -> Option<KeyId> {
        if ENCRYPTION_ENABLED {
            Some(KeyId::Master)
        } else {
            None
        }
    }

    fn collection_name() -> CollectionName {
        CollectionName::new("khonsulabs", "user")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByName)
    }
}

impl DefaultSerialization for User {}

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
