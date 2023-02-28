use std::path::Path;
use std::time::Duration;

use bonsaidb::client::url::Url;
use bonsaidb::client::{AsyncClient, AsyncRemoteDatabase};
use bonsaidb::core::connection::AsyncStorageConnection;
use bonsaidb::core::keyvalue::AsyncKeyValue;
use bonsaidb::local::config::{Builder, KeyValuePersistence, PersistenceThreshold};
use bonsaidb::server::{DefaultPermissions, Server, ServerConfiguration};
use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId};
use tokio::runtime::Runtime;
use ubyte::ToByteUnit;

pub fn read_blobs(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    read_blobs_local(c, data);
    read_blobs_networked(c, data);
}

fn read_blobs_local(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let runtime = Runtime::new().unwrap();
    let server = runtime.block_on(initialize_server(true));
    let database = runtime.block_on(async { server.database::<()>("key-value").await.unwrap() });
    runtime.block_on(set_blob(&database, data));

    c.bench_function(
        BenchmarkId::new("bonsaidb-local", data.len().bytes()),
        |b| {
            b.to_async(&runtime).iter(|| get_blob(&database));
        },
    );
}

fn read_blobs_networked(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let runtime = Runtime::new().unwrap();
    let (quic_database, ws_database) = initialize_networked_server(&runtime, true);
    runtime.block_on(set_blob(&quic_database, data));

    c.bench_function(BenchmarkId::new("bonsaidb-quic", data.len().bytes()), |b| {
        b.iter(|| {
            runtime.block_on(get_blob(&quic_database));
        });
    });

    c.bench_function(BenchmarkId::new("bonsaidb-ws", data.len().bytes()), |b| {
        b.iter(|| {
            runtime.block_on(get_blob(&ws_database));
        });
    });
}

pub fn write_blobs(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    write_blobs_local(c, data);
    write_blobs_networked(c, data);
}

fn write_blobs_local(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let runtime = Runtime::new().unwrap();
    let server = runtime.block_on(initialize_server(false));
    let database = runtime.block_on(async { server.database::<()>("key-value").await.unwrap() });

    c.bench_function(
        BenchmarkId::new("bonsaidb-local", data.len().bytes()),
        |b| {
            b.to_async(&runtime).iter(|| set_blob(&database, data));
        },
    );
}

fn write_blobs_networked(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let runtime = Runtime::new().unwrap();
    let (quic_database, ws_database) = initialize_networked_server(&runtime, false);

    c.bench_function(BenchmarkId::new("bonsaidb-quic", data.len().bytes()), |b| {
        b.iter(|| {
            runtime.block_on(set_blob(&quic_database, data));
        });
    });

    c.bench_function(BenchmarkId::new("bonsaidb-ws", data.len().bytes()), |b| {
        b.iter(|| {
            runtime.block_on(set_blob(&ws_database, data));
        });
    });
}

async fn get_blob<C: AsyncKeyValue>(connection: &C) {
    // The set_key API provides serialization. Uisng this API, we can skip
    // serialization.
    connection.get_key("blob").await.unwrap().unwrap();
}

async fn set_blob<C: AsyncKeyValue>(connection: &C, blob: &[u8]) {
    // The set_key API provides serialization. Uisng this API, we can skip
    // serialization.
    connection.set_binary_key("blob", blob).await.unwrap();
}

pub fn increment(c: &mut BenchmarkGroup<WallTime>) {
    increment_local(c);
    increment_networked(c);
}

fn increment_local(c: &mut BenchmarkGroup<WallTime>) {
    let runtime = Runtime::new().unwrap();
    let server = runtime.block_on(initialize_server(false));
    let database = runtime.block_on(async { server.database::<()>("key-value").await.unwrap() });

    c.bench_function("bonsaidb-local", |b| {
        b.to_async(&runtime).iter(|| increment_key(&database));
    });
}

fn increment_networked(c: &mut BenchmarkGroup<WallTime>) {
    let runtime = Runtime::new().unwrap();
    let (quic_database, ws_database) = initialize_networked_server(&runtime, false);

    c.bench_function("bonsaidb-quic", |b| {
        b.iter(|| {
            runtime.block_on(increment_key(&quic_database));
        });
    });

    c.bench_function("bonsaidb-ws", |b| {
        b.iter(|| {
            runtime.block_on(increment_key(&ws_database));
        });
    });
}

async fn increment_key<C: AsyncKeyValue>(connection: &C) {
    connection.increment_key_by("u64", 1_u64).await.unwrap();
}

async fn initialize_server(persist_changes: bool) -> Server {
    let path = Path::new("key-value-benchmarks.bonsaidb");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }
    let server = Server::open(
        ServerConfiguration::new(path)
            .default_permissions(DefaultPermissions::AllowAll)
            .key_value_persistence(if persist_changes {
                KeyValuePersistence::immediate()
            } else {
                KeyValuePersistence::lazy([PersistenceThreshold::after_changes(usize::MAX)])
            })
            .with_schema::<()>()
            .unwrap(),
    )
    .await
    .unwrap();
    server.install_self_signed_certificate(false).await.unwrap();
    server
        .create_database::<()>("key-value", false)
        .await
        .unwrap();
    server
}

fn initialize_networked_server(
    runtime: &Runtime,
    persist_changes: bool,
) -> (AsyncRemoteDatabase, AsyncRemoteDatabase) {
    let server = runtime.block_on(initialize_server(persist_changes));
    let certificate = runtime
        .block_on(async { server.certificate_chain().await.unwrap() })
        .into_end_entity_certificate();
    let quic_server = server.clone();
    runtime.spawn(async move {
        quic_server.listen_on(7022).await.unwrap();
    });
    runtime.spawn(async move {
        server
            .listen_for_websockets_on("0.0.0.0:7023", false)
            .await
            .unwrap();
    });
    let quic_database = runtime.block_on(async {
        // Allow the server time to start listening
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let client = AsyncClient::build(Url::parse("bonsaidb://localhost:7022").unwrap())
            .with_certificate(certificate)
            .build()
            .unwrap();
        client.database::<()>("key-value").await.unwrap()
    });
    let ws_database = runtime.block_on(async {
        // Allow the server time to start listening
        let client = AsyncClient::build(Url::parse("ws://localhost:7023").unwrap())
            .build()
            .unwrap();
        client.database::<()>("key-value").await.unwrap()
    });
    (quic_database, ws_database)
}
