use pliantdb_core::{
    schema::{InvalidNameError, Schema, SchemaName},
    Error,
};

pub mod database;

#[derive(Debug)]
pub struct Admin;

impl Schema for Admin {
    fn schema_name() -> Result<SchemaName, InvalidNameError> {
        SchemaName::new("khonsulabs", "pliantdb.admin")
    }

    fn define_collections(
        schema: &mut pliantdb_local::core::schema::Schematic,
    ) -> Result<(), Error> {
        schema.define_collection::<database::Database>()
    }
}
