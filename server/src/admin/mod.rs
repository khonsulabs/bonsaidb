use pliantdb_local::core::schema::Schema;

pub mod database;

#[derive(Debug)]
pub struct Admin;

impl Schema for Admin {
    fn define_collections(schema: &mut pliantdb_local::core::schema::Schematic) {
        schema.define_collection::<database::Database>();
    }
}
