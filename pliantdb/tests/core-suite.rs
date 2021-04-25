use std::{sync::atomic::Ordering, time::Duration};

use once_cell::sync::Lazy;
use pliantdb::{
    client::{Client, RemoteDatabase},
    core::{
        fabruic::Certificate,
        networking::ServerConnection,
        schema::Schema,
        test_util::{Basic, HarnessTest, TestDirectory},
    },
    server::test_util::{
        basic_server_connection_tests, initialize_basic_server, BASIC_SERVER_NAME,
    },
};
use tokio::sync::Mutex;
use url::Url;

#[tokio::test]
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
        let client = Client::new(url, Some(server.certificate().await?)).await?;
        let client_task_is_running = client.background_task_running();
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

async fn initialize_shared_server() -> Certificate {
    static CERTIFICATE: Lazy<Mutex<Option<Certificate>>> = Lazy::new(|| Mutex::new(None));
    let mut certificate = CERTIFICATE.lock().await;
    if certificate.is_none() {
        let (sender, receiver) = flume::bounded(1);
        std::thread::spawn(|| run_shared_server(sender));

        *certificate = Some(receiver.recv_async().await.unwrap());
        // Give the server time to start listening
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }

    certificate.clone().unwrap()
}

fn run_shared_server(certificate_sender: flume::Sender<Certificate>) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let directory = TestDirectory::new("shared-server");
        let server = initialize_basic_server(directory.as_ref()).await.unwrap();
        certificate_sender
            .send(server.certificate().await.unwrap())
            .unwrap();

        #[cfg(feature = "websockets")]
        {
            let task_server = server.clone();
            tokio::spawn(async move {
                task_server
                    .listen_for_websockets_on("localhost:6001")
                    .await
                    .unwrap();
            });
        }

        server.listen_on(6000).await.unwrap();
    });

    Ok(())
}

#[cfg(feature = "websockets")]
mod websockets {
    use super::*;

    struct WebsocketTestHarness {
        db: RemoteDatabase<Basic>,
    }

    impl WebsocketTestHarness {
        pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
            initialize_shared_server().await;
            let url = Url::parse("ws://localhost:6001")?;
            let client = Client::new(url, None).await?;

            let dbname = format!("websockets-{}", test);
            client
                .create_database(&dbname, Basic::schema_name()?)
                .await?;
            let db = client.database::<Basic>(&dbname).await?;

            Ok(Self { db })
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<Basic>> {
            Ok(self.db.clone())
        }

        pub async fn shutdown(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    pliantdb_core::define_connection_test_suite!(WebsocketTestHarness);

    #[cfg(feature = "pubsub")]
    pliantdb_core::define_pubsub_test_suite!(WebsocketTestHarness);
    #[cfg(feature = "keyvalue")]
    pliantdb_core::define_kv_test_suite!(WebsocketTestHarness);
}

mod pliant {
    use super::*;
    struct PliantTestHarness {
        db: RemoteDatabase<Basic>,
    }

    impl PliantTestHarness {
        pub async fn new(test: HarnessTest) -> anyhow::Result<Self> {
            let certificate = initialize_shared_server().await;

            let url = Url::parse(&format!(
                "pliantdb://localhost:6000?server={}",
                BASIC_SERVER_NAME
            ))?;
            let client = Client::new(url, Some(certificate)).await?;

            let dbname = format!("pliant-{}", test);
            client
                .create_database(&dbname, Basic::schema_name()?)
                .await?;
            let db = client.database::<Basic>(&dbname).await?;

            Ok(Self { db })
        }

        pub async fn connect<'a, 'b>(&'a self) -> anyhow::Result<RemoteDatabase<Basic>> {
            Ok(self.db.clone())
        }

        pub async fn shutdown(&self) -> anyhow::Result<()> {
            Ok(())
        }
    }

    pliantdb_core::define_connection_test_suite!(PliantTestHarness);
    #[cfg(feature = "pubsub")]
    pliantdb_core::define_pubsub_test_suite!(PliantTestHarness);

    #[cfg(feature = "keyvalue")]
    pliantdb_core::define_kv_test_suite!(PliantTestHarness);
}
