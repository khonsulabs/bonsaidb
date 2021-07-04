use pliantdb_core::{
    schema::{InvalidNameError, Schema, SchemaName, Schematic},
    Error,
};

pub(crate) mod database;
pub(crate) mod encryption_key;
pub(crate) mod group;
pub(crate) mod password_config;
pub(crate) mod role;
pub(crate) mod user;

pub use self::{database::Database, group::PermissionGroup, role::Role, user::User};

/// The `PliantDb` administration schema.
#[derive(Debug)]
pub struct Admin;

impl Schema for Admin {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("khonsulabs", "pliantdb-admin")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_collection::<database::Database>()?;
        schema.define_collection::<encryption_key::EncryptionKeyVersion>()?;
        schema.define_collection::<group::PermissionGroup>()?;
        schema.define_collection::<role::Role>()?;
        schema.define_collection::<user::User>()?;
        schema.define_collection::<password_config::PasswordConfig>()?;

        Ok(())
    }
}
