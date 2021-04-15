use pliantdb_core::schema::{self, Schema};
use schema::SchemaId;

pub mod database;

#[derive(Debug)]
pub struct Admin;

impl Schema for Admin {
    fn schema_id() -> SchemaId {
        SchemaId::from("pliantdb.admin")
    }

    fn define_collections(schema: &mut pliantdb_local::core::schema::Schematic) {
        schema.define_collection::<database::Database>();
    }
}
