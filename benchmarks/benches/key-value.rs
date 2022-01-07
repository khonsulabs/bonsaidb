//! This benchmark focuses on benchmarking collection-related functionality.

use criterion::{criterion_group, criterion_main, Criterion};
mod key_value;

fn all_benches(c: &mut Criterion) {
    env_logger::init();
    key_value::write_blobs(c);
}

//     static KB: usize = 1024;
//     let runtime = tokio::runtime::Runtime::new().unwrap();

//     // First set of benchmarks tests inserting documents
//     let mut group = c.benchmark_group("blob-keys");
//     for size in [KB, 2 * KB, 8 * KB, 32 * KB, KB * KB].iter() {
//         let mut data = Vec::with_capacity(*size);
//         data.resize_with(*size, || 7u8);
//         let doc = Arc::new(ResizableDocument { data });
//         let doc = &doc;
//         group.throughput(Throughput::Bytes(*size as u64));
//         group.bench_with_input(BenchmarkId::from_parameter(*size as u64), size, |b, _| {
//             let path = TestDirectory::new(format!("benches-basics-{}.bonsaidb", size));
//             let db = runtime
//                 .block_on(Database::open::<ResizableDocument>(
//                     StorageConfiguration::new(&path),
//                 ))
//                 .unwrap();
//             b.to_async(&runtime).iter(|| save_document(doc, &db));
//         });
//     }
//     group.finish();

//     // TODO bench read performance
//     // TODO bench read + write performance (with different numbers of readers/writers)
//     // TODO (once supported) bench batch saving
// }

criterion_group!(benches, all_benches);
criterion_main!(benches);
