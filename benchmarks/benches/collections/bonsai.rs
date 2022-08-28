#[cfg(feature = "compression")]
use bonsaidb::local::config::Compression;
use bonsaidb::{
    core::{connection::Connection, test_util::TestDirectory},
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use criterion::{measurement::WallTime, BenchmarkGroup, BenchmarkId};
use ubyte::ToByteUnit;

use crate::collections::ResizableDocument;

fn save_document(doc: &ResizableDocument, db: &Database) {
    db.collection::<ResizableDocument>().push(doc).unwrap();
}

#[cfg_attr(not(feature = "compression"), allow(unused_mut))]
pub(super) fn save_documents(group: &mut BenchmarkGroup<WallTime>, doc: &ResizableDocument) {
    let path = TestDirectory::absolute("benches-basics.bonsaidb");
    let mut configs = vec![("bonsaidb-local", StorageConfiguration::new(&path))];
    #[cfg(feature = "compression")]
    {
        configs.push((
            "bonsaidb-local+lz4",
            StorageConfiguration::new(&path).default_compression(Compression::Lz4),
        ))
    }
    for (label, config) in configs {
        group.bench_function(BenchmarkId::new(label, doc.data.len().bytes()), |b| {
            let db = Database::open::<ResizableDocument>(config.clone()).unwrap();
            b.iter(|| save_document(doc, &db));
        });
    }

    // TODO bench read performance
    // TODO bench read + write performance (with different numbers of readers/writers)
    // TODO (once supported) bench batch saving
}
