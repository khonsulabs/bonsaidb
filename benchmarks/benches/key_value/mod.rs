use criterion::{Criterion, Throughput};

mod bonsai;

#[cfg(feature = "redis")]
mod redis;

pub fn benches(c: &mut Criterion) {
    write_blobs(c);
    read_blobs(c);
    increment(c);
}

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

pub fn read_blobs(c: &mut Criterion) {
    let mut group = c.benchmark_group("get-bytes");

    for blob_size in [64, 1024, 1024 * 1024, 1024 * 1024 * 8] {
        group.throughput(Throughput::Bytes(blob_size));

        let blob = vec![42; blob_size as usize];

        bonsai::read_blobs(&mut group, &blob);

        #[cfg(feature = "redis")]
        redis::read_blobs(&mut group, &blob);
    }
}

pub fn increment(c: &mut Criterion) {
    let mut group = c.benchmark_group("increment");
    bonsai::increment(&mut group);

    #[cfg(feature = "redis")]
    redis::increment(&mut group);
}
