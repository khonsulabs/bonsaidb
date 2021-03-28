// This is not a benchmark that is meant to be taken seriously at this time. It
// was written purely to help test an in-development async runtime that this
// database will benefit from.
//
// The problems with the current speed of this database hinge on how
// ACID-compliant you wnat your data writes to be. As of writing this, there are
// no configuration options to change this, but eventually you will have control
// over whether to flush after every write or to flush periodically. Flushing
// periodically will drastically improve speed, but it potentially will lead to
// lost transactions.
//
// When operating `PliantDB` in a local or single-server mode, we must recommend
// flushing on each write -- the default configuration. Comparatively speaking,
// this will hurt performance in many benchmarks, including this one below. The
// purpose of this benchmark is to help test the blocking nature of sled within
// an async interface when properly marking each interaction with sled as
// blocking to the async runtime.
//
// Once clustering is available, it will be recommended to have enough
// redundancy in your architecture to allow running the cluster with periodic
// flushing enabled. Because the quorum will self-correct when an individual
// node loses data, as long as you design with enough redundancy in your
// cluster, the risk of data loss goes down drastically.
//
// TODO Some of this explanation eventually should be moved somewhere more useful

use std::{borrow::Cow, sync::Arc};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pliantdb_core::{
    connection::Connection,
    schema::{collection, Collection, Schema},
    test_util::TestDirectory,
};
use pliantdb_local::{Configuration, Storage};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct ResizableDocument<'a> {
    #[serde(borrow)]
    data: Cow<'a, [u8]>,
}

impl<'a> Collection for ResizableDocument<'a> {
    fn id() -> collection::Id {
        collection::Id::from("resizable-docs")
    }

    fn define_views(_schema: &mut Schema) {}
}

async fn save_document(doc: &ResizableDocument<'_>, db: &Storage<ResizableDocument<'static>>) {
    db.collection::<ResizableDocument<'static>>()
        .unwrap()
        .push(doc)
        .await
        .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    static KB: usize = 1024;
    let runtime = tokio::runtime::Runtime::new().unwrap();

    // First set of benchmarks tests inserting documents
    let mut group = c.benchmark_group("save_documents");
    for size in [KB, 2 * KB, 8 * KB, 32 * KB, KB * KB].iter() {
        let path = TestDirectory::new(format!("benches-basics-{}.pliantdb", size));
        let db = runtime
            .block_on(Storage::open_local(&path, &Configuration::default()))
            .unwrap();
        let mut data = Vec::with_capacity(*size);
        data.resize_with(*size, || 7u8);
        let doc = Arc::new(ResizableDocument {
            data: Cow::Owned(data),
        });
        let doc = &doc;
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(*size as u64), size, |b, _| {
            b.to_async(&runtime).iter(|| save_document(&doc, &db));
        });
    }
    group.finish();

    // TODO bench read performance
    // TODO bench read + write performance (with different numbers of readers/writers)
    // TODO (once supported) bench batch saving
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
