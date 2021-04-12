use std::{sync::atomic::Ordering, time::Duration};

use pliantdb_core::{
    networking::ServerConnection,
    schema::Schema,
    test_util::{Basic, ConnectionTest, TestDirectory},
};
use pliantdb_server::{
    test_util::{basic_server_connection_tests, initialize_basic_server, BASIC_SERVER_NAME},
    Server,
};
use url::Url;

use super::*;
use crate::client::RemoteDatabase;

#[tokio::test(flavor = "multi_thread")]
async fn server_connection_tests() -> anyhow::Result<()> {
    let directory = TestDirectory::new("client-test");
    let server = initialize_basic_server(directory.as_ref()).await?;
    let task_server = server.clone();
    let server_task = tokio::spawn(async move { task_server.listen_on(5000).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let url = Url::parse(&format!(
        "pliantdb://[::1]:5000?server={}",
        BASIC_SERVER_NAME
    ))?;

    let client_task_is_running = {
        let client = Client::new(&url, Some(server.certificate().await?)).await?;
        let client_task_is_running = client.background_task_running.clone();
        assert!(client_task_is_running.load(Ordering::Acquire));

        basic_server_connection_tests(client).await?;

        server.shutdown(None).await?;
        server_task.await??;
        client_task_is_running
    };

    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(!client_task_is_running.load(Ordering::Acquire));

    Ok(())
}

#[cfg(feature = "websockets")]
mod websockets {
    use super::*;

    struct WebsocketTestHarness {
        _server: Server,
        _directory: TestDirectory,
        db: RemoteDatabase<Basic>,
    }

    impl WebsocketTestHarness {
        pub async fn new(test: ConnectionTest) -> anyhow::Result<Self> {
            let directory = TestDirectory::new(format!("websocket-server-{}", test));
            let server = initialize_basic_server(directory.as_ref()).await?;
            let task_server = server.clone();
            tokio::spawn(async move {
                task_server
                    .listen_for_websockets_on(&format!("localhost:{}", test.port(6000)))
                    .await
                    .unwrap();
                println!("Test websocket server shut down.");
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            let url = Url::parse(&format!("ws://localhost:{}", test.port(6000)))?;
            let client = Client::new(&url, None).await?;

            client
                .create_database(&test.to_string(), Basic::schema_id())
                .await?;
            let db = client.database::<Basic>(&test.to_string()).await;

            Ok(Self {
                db,
                _server: server,
                _directory: directory,
            })
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<Basic>> {
            Ok(self.db.clone())
        }
    }

    pliantdb_core::define_connection_test_suite!(WebsocketTestHarness);
}

mod pliant {
    use super::*;
    struct PliantTestHarness {
        _server: Server,
        _directory: TestDirectory,
        db: RemoteDatabase<Basic>,
    }

    impl PliantTestHarness {
        pub async fn new(test: ConnectionTest) -> anyhow::Result<Self> {
            let directory = TestDirectory::new(format!("pliant-server-{}", test));
            let server = initialize_basic_server(directory.as_ref()).await?;
            let task_server = server.clone();
            tokio::spawn(async move { task_server.listen_on(test.port(5001)).await });

            tokio::time::sleep(Duration::from_millis(100)).await;

            let url = Url::parse(&format!(
                "pliantdb://localhost:{}?server={}",
                test.port(5001),
                BASIC_SERVER_NAME
            ))?;
            let client = Client::new(&url, Some(server.certificate().await?)).await?;

            client
                .create_database(&test.to_string(), Basic::schema_id())
                .await?;
            let db = client.database::<Basic>(&test.to_string()).await;

            Ok(Self {
                db,
                _server: server,
                _directory: directory,
            })
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<Basic>> {
            Ok(self.db.clone())
        }
    }

    pliantdb_core::define_connection_test_suite!(PliantTestHarness);
}
