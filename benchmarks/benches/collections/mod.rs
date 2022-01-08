use criterion::{Criterion, Throughput};
use serde::{Deserialize, Serialize};

mod bonsai;
#[cfg(feature = "sqlite")]
mod rusqlite;

#[derive(Serialize, Deserialize, Debug)]
struct ResizableDocument {
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

pub fn save_documents(c: &mut Criterion) {
    static KB: usize = 1024;

    // First set of benchmarks tests inserting documents
    let mut group = c.benchmark_group("save_documents");
    for size in [KB, 2 * KB, 8 * KB, 32 * KB, KB * KB] {
        group.throughput(Throughput::Bytes(size as u64));
        let mut data = Vec::with_capacity(size);
        data.resize_with(size, || 7u8);
        let doc = ResizableDocument { data };

        bonsai::save_documents(&mut group, &doc);
        #[cfg(feature = "sqlite")]
        rusqlite::save_documents(&mut group, &doc);
    }
    group.finish();
}
