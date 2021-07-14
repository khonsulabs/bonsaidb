use crate::{
    schema::{InvalidNameError, Schema, SchemaName, Schematic},
    Error,
};

#[doc(hidden)]
pub mod database;
// pub(crate) mod encryption_key;
#[doc(hidden)]
pub mod group;
#[doc(hidden)]
pub mod password_config;
#[doc(hidden)]
pub mod role;
#[doc(hidden)]
pub mod user;

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
        // schema.define_collection::<encryption_key::EncryptionKeyVersion>()?;
        schema.define_collection::<group::PermissionGroup>()?;
        schema.define_collection::<role::Role>()?;
        schema.define_collection::<user::User>()?;
        schema.define_collection::<password_config::PasswordConfig>()?;

        Ok(())
    }
}
