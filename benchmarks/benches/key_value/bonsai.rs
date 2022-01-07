use std::{path::Path, time::Duration};

use bonsaidb::{
    client::{url::Url, Client},
    core::{connection::StorageConnection, keyvalue::KeyValue},
    local::config::{Builder, KeyValuePersistence, PersistenceThreshold},
    server::{DefaultPermissions, Server, ServerConfiguration},
};
use criterion::{measurement::WallTime, BenchmarkGroup, BenchmarkId};
use ubyte::ToByteUnit;

pub fn write_blobs(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    write_blobs_local(c, data);
    write_blobs_networked(c, data);
}

fn write_blobs_local(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(initialize_server());
    let database = runtime.block_on(async { server.database::<()>("key-value").await.unwrap() });

    c.bench_function(
        BenchmarkId::new("bonsaidb-local", data.len().bytes()),
        |b| {
            b.to_async(&runtime).iter(|| set_blob(&database, data));
        },
    );
}

fn write_blobs_networked(c: &mut BenchmarkGroup<WallTime>, data: &[u8]) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let server = runtime.block_on(initialize_server());
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
        let client = Client::build(Url::parse("bonsaidb://localhost:7022").unwrap())
            .with_certificate(certificate)
            .finish()
            .await
            .unwrap();
        client.database::<()>("key-value").await.unwrap()
    });
    let ws_database = runtime.block_on(async {
        // Allow the server time to start listening
        let client = Client::build(Url::parse("ws://localhost:7023").unwrap())
            .finish()
            .await
            .unwrap();
        client.database::<()>("key-value").await.unwrap()
    });

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

async fn set_blob<C: KeyValue>(connection: &C, blob: &[u8]) {
    // The set_key API provides serialization. Uisng this API, we can skip
    // serialization.
    connection.set_binary_key("blob", blob).await.unwrap();
}

async fn initialize_server() -> Server {
    let path = Path::new("key-value-benchmarks.bonsaidb");
    if path.exists() {
        std::fs::remove_dir_all(path).unwrap();
    }
    let server = Server::open(
        ServerConfiguration::new(path)
            .default_permissions(DefaultPermissions::AllowAll)
            .key_value_persistence(KeyValuePersistence::lazy([
                PersistenceThreshold::after_changes(usize::MAX),
            ]))
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
