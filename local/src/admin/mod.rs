use pliantdb_core::{
    permissions::{PermissionGroup, Role},
    schema::{InvalidNameError, Schema, SchemaName, Schematic},
    Error,
};

pub mod database;

#[derive(Debug)]
pub struct Admin;

impl Schema for Admin {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("khonsulabs", "pliantdb-admin")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_collection::<database::Database>()?;
        schema.define_collection::<PermissionGroup>()?;
        schema.define_collection::<Role>()?;

        Ok(())
    }
}
