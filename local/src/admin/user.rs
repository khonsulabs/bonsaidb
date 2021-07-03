use pliantdb_core::{
    custodian_password::{ServerFile, ServerRegistration},
    document::Document,
    schema::{Collection, CollectionName, InvalidNameError, MapResult, Name, Schematic, View},
    Error,
};
use serde::{Deserialize, Serialize};

/// An assignable role, which grants permissions based on the associated
/// [`PermissionGroup`](crate::permissions::PermissionGroup)s.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct User {
    /// The name of the role. Must be unique.
    pub name: String,
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
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }
}

impl Collection for User {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "user")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ByName)
    }
}

/// A unique view of roles by name.
#[derive(Debug)]
pub struct ByName;

impl View for ByName {
    type Collection = User;
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
        let role = document.contents::<User>()?;
        Ok(Some(document.emit_key(role.name)))
    }
}