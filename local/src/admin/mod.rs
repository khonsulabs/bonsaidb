use pliantdb_core::{
    schema::{InvalidNameError, Schema, SchemaName, Schematic},
    Error,
};

pub mod database;
pub mod encryption_key;
pub mod group;
pub mod password_config;
pub mod role;
pub mod user;

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
