use bonsaidb::{
    core::{
        connection::Connection,
        schema::{Collection, CollectionName, DefaultSerialization, InvalidNameError, Schematic},
        test_util::TestDirectory,
        Error,
    },
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use criterion::{measurement::WallTime, BenchmarkGroup, BenchmarkId};
use ubyte::ToByteUnit;

use crate::collections::ResizableDocument;

impl Collection for ResizableDocument {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "resizable-docs")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

impl DefaultSerialization for ResizableDocument {}

async fn save_document(doc: &ResizableDocument, db: &Database) {
    db.collection::<ResizableDocument>()
        .push(doc)
        .await
        .unwrap();
}

pub(super) fn save_documents(group: &mut BenchmarkGroup<WallTime>, doc: &ResizableDocument) {
    group.bench_function(
        BenchmarkId::new("bonsaidb-local", doc.data.len().bytes()),
        |b| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let path = TestDirectory::new("benches-basics.bonsaidb");
            let db = runtime
                .block_on(Database::open::<ResizableDocument>(
                    StorageConfiguration::new(&path),
                ))
                .unwrap();
            b.to_async(&runtime).iter(|| save_document(doc, &db));
        },
    );

    // TODO bench read performance
    // TODO bench read + write performance (with different numbers of readers/writers)
    // TODO (once supported) bench batch saving
}
