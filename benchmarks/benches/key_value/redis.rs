use criterion::{measurement::WallTime, BenchmarkGroup, BenchmarkId};
use ubyte::ToByteUnit;

pub fn read_blobs(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let client = redis::Client::open("redis://localhost").unwrap();
    let mut connection = client.get_connection().unwrap();
    // The high level api would require allocations. However, by using the low
    // level API we can skip the clone.
    let data = std::str::from_utf8(data).unwrap();
    redis::cmd("SET")
        .arg("blob")
        .arg(data)
        .execute(&mut connection);
    c.bench_function(BenchmarkId::new("redis", data.len().bytes()), |b| {
        b.iter(|| {
            redis::cmd("GET")
                .arg("blob")
                .query::<Vec<u8>>(&mut connection)
                .unwrap();
        });
    });

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut connection = runtime.block_on(async { client.get_async_connection().await.unwrap() });
    c.bench_function(BenchmarkId::new("redis-async", data.len().bytes()), |b| {
        b.iter(|| {
            runtime.block_on(async {
                redis::cmd("GET")
                    .arg("blob")
                    .query_async::<_, Vec<u8>>(&mut connection)
                    .await
                    .unwrap();
            })
        });
    });
}

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

pub fn increment(c: &mut BenchmarkGroup<WallTime>) {
    let client = redis::Client::open("redis://localhost").unwrap();
    let mut connection = client.get_connection().unwrap();
    // Disable saving
    redis::cmd("CONFIG")
        .arg("SET")
        .arg("SAVE")
        .arg("")
        .execute(&mut connection);
    c.bench_function("redis", |b| {
        b.iter(|| {
            redis::cmd("INCR").arg("u64").execute(&mut connection);
        });
    });

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut connection = runtime.block_on(async { client.get_async_connection().await.unwrap() });
    c.bench_function("redis-async", |b| {
        b.iter(|| {
            runtime.block_on(async {
                redis::cmd("INCR")
                    .arg("u64")
                    .query_async::<_, ()>(&mut connection)
                    .await
                    .unwrap();
            })
        });
    });
}
