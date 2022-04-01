use bonsaidb_core::schema::{Qualified, Schema, SchemaName, Schematic};
use bonsaidb_jobs::queue::{Queue, QueueOwner};
use bonsaidb_local::{
    config::{Builder, StorageConfiguration},
    Database,
};

#[derive(Debug)]
pub struct TestSchema;

impl Schema for TestSchema {
    fn schema_name() -> SchemaName {
        SchemaName::private("jobs")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        bonsaidb_jobs::define_collections(schema)
    }
}

fn main() {
    let db = Database::open::<TestSchema>(StorageConfiguration::new("jobs-test.bonsaidb")).unwrap();
    let queue = Queue::create(QueueOwner::Backend, "hello-world", &db).unwrap();
    println!("Queue: {queue:?}");
}
