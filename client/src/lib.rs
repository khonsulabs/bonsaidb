//! Client for `pliantdb-server`.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc))]
#![allow(
    clippy::missing_errors_doc, // TODO
    clippy::option_if_let_else,
)]

pub use url;

mod client;
mod error;

pub use self::{client::Client, error::Error};

#[cfg(test)]
mod tests {
    use std::{sync::atomic::Ordering, time::Duration};

    use once_cell::sync::Lazy;
    use pliantdb_core::{
        networking::ServerConnection,
        schema::Schema,
        test_util::{Basic, ConnectionTest, TestDirectory},
    };
    use pliantdb_server::{
        test_util::{basic_server_connection_tests, initialize_basic_server, BASIC_SERVER_NAME},
        Server,
    };
    use tokio::sync::Mutex;
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
            let client = Client::new(&url, server.certificate().await?)?;
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

    static SHARED_TEST_SERVER: Lazy<Mutex<Option<TestServer>>> = Lazy::new(|| Mutex::new(None));

    struct TestServer {
        server: Server,
        _directory: TestDirectory,
    }

    struct TestHarness {
        _server: Server,
        db: RemoteDatabase<Basic>,
    }

    impl TestHarness {
        pub async fn new(test: ConnectionTest) -> anyhow::Result<Self> {
            let mut shared_server = SHARED_TEST_SERVER.lock().await;
            if shared_server.is_none() {
                let directory = TestDirectory::new("shared-client-server");
                let server = initialize_basic_server(directory.as_ref()).await?;
                let task_server = server.clone();
                tokio::spawn(async move { task_server.listen_on(5001).await });
                *shared_server = Some(TestServer {
                    server,
                    _directory: directory,
                });
            }

            let server = shared_server.as_ref().unwrap().server.clone();
            server
                .create_database(&test.to_string(), Basic::schema_id())
                .await?;

            tokio::time::sleep(Duration::from_millis(100)).await;
            let url = Url::parse(&format!(
                "pliantdb://[::1]:5001?server={}",
                BASIC_SERVER_NAME
            ))?;
            let client = Client::new(&url, server.certificate().await?)?;
            let db = client.database::<Basic>(&test.to_string()).await;

            Ok(Self {
                db,
                _server: server,
            })
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<Basic>> {
            Ok(self.db.clone())
        }
    }

    pliantdb_core::define_connection_test_suite!(TestHarness);
}
