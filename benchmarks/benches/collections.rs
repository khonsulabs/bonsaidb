//! This benchmark focuses on benchmarking collection-related functionality.

use std::sync::Arc;

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
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct ResizableDocument {
    data: Vec<u8>,
}

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

fn criterion_benchmark(c: &mut Criterion) {
    static KB: usize = 1024;

    // First set of benchmarks tests inserting documents
    let mut group = c.benchmark_group("save_documents");
    for size in [KB, 2 * KB, 8 * KB, 32 * KB, KB * KB].iter() {
        let mut data = Vec::with_capacity(*size);
        data.resize_with(*size, || 7u8);
        let doc = Arc::new(ResizableDocument { data });
        let doc = &doc;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(*size as u64), size, |b, _| {
            let path = TestDirectory::new(format!("benches-basics-{}.bonsaidb", size));
            let db = runtime
                .block_on(Database::open::<ResizableDocument>(
                    StorageConfiguration::new(&path),
                ))
                .unwrap();
            b.to_async(&runtime).iter(|| save_document(doc, &db));
        });
    }
    group.finish();

    // TODO bench read performance
    // TODO bench read + write performance (with different numbers of readers/writers)
    // TODO (once supported) bench batch saving
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
