use bonsaidb::core::arc_bytes::serde::Bytes;
use bonsaidb::core::schema::Collection;
use criterion::{Criterion, Throughput};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

mod bonsai;
#[cfg(feature = "sqlite")]
mod rusqlite;

#[derive(Serialize, Deserialize, Debug, Collection)]
#[collection(name = "resizable-docs")]
struct ResizableDocument {
    data: Bytes,
}

pub fn save_documents(c: &mut Criterion) {
    static KB: usize = 1024;

    // First set of benchmarks tests inserting documents
    let mut group = c.benchmark_group("save_documents");
    for size in [KB, 2 * KB, 8 * KB, 32 * KB, KB * KB] {
        let mut rng = thread_rng();
        group.throughput(Throughput::Bytes(size as u64));
        let mut data = (0..size).map(|_| rng.gen()).collect::<Vec<_>>();
        data.resize_with(size, || 7u8);
        let doc = ResizableDocument {
            data: Bytes::from(data),
        };

        bonsai::save_documents(&mut group, &doc);
        #[cfg(feature = "sqlite")]
        rusqlite::save_documents(&mut group, &doc);
    }
    group.finish();
}
