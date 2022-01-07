use criterion::{measurement::WallTime, BenchmarkGroup, BenchmarkId};
use ubyte::ToByteUnit;

pub fn write_blobs(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let client = redis::Client::open("redis://localhost").unwrap();
    let mut connection = client.get_connection().unwrap();
    // Disable saving
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("SAVE")
        .arg("")
        .execute(&mut connection);
    // The high level api would require allocations. However, by using the low
    // level API we can skip the clone.
    let data = std::str::from_utf8(data).unwrap();
    c.bench_function(BenchmarkId::new("redis", data.len().bytes()), |b| {
        b.iter(|| {
            redis::cmd("SET")
                .arg("blob")
                .arg(data)
                .execute(&mut connection);
        });
    });

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut connection = runtime.block_on(async { client.get_async_connection().await.unwrap() });
    c.bench_function(BenchmarkId::new("redis-async", data.len().bytes()), |b| {
        b.iter(|| {
            runtime.block_on(async {
                redis::cmd("SET")
                    .arg("blob")
                    .arg(data)
                    .query_async::<_, ()>(&mut connection)
                    .await
                    .unwrap();
            })
        });
    });
}
