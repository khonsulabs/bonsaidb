use criterion::{Criterion, Throughput};

mod bonsai;

#[cfg(feature = "redis")]
mod redis;

pub fn write_blobs(c: &mut Criterion) {
    let mut group = c.benchmark_group("set-bytes");

    for blob_size in [64, 1024, 1024 * 1024, 1024 * 1024 * 8] {
        group.throughput(Throughput::Bytes(blob_size));

        let blob = vec![42; blob_size as usize];

        bonsai::write_blobs(&mut group, &blob);

        #[cfg(feature = "redis")]
        redis::write_blobs(&mut group, &blob);
    }
}
