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

    use pliantdb_core::{
        schema::Schema,
        test_util::{Basic, TestDirectory},
    };
    use pliantdb_networking::ServerConnection;
    use pliantdb_server::test_util::{initialize_basic_server, BASIC_SERVER_NAME};
    use url::Url;

    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn test() -> anyhow::Result<()> {
        let directory = TestDirectory::new("client-test");
        let server = initialize_basic_server(directory.as_ref()).await?;
        let task_server = server.clone();
        let server_task = tokio::spawn(async move { task_server.listen_on("[::1]:5000").await });
        // Give the server time to start listening
        tokio::time::sleep(Duration::from_millis(100)).await;
        let url = Url::parse(&format!(
            "pliantdb://[::1]:5000?server={}",
            BASIC_SERVER_NAME
        ))?;

        let client = Client::new(&url, server.certificate().await?)?;
        let client_task_is_running = client.background_task_running.clone();
        assert!(client_task_is_running.load(Ordering::Acquire));

        let databases = client.list_databases().await?;
        assert_eq!(databases.len(), 1);
        assert_eq!(databases[0].name.as_ref(), "tests");
        assert_eq!(databases[0].schema, Basic::schema_id());

        drop(client);

        println!("Calling shutdown");
        server.shutdown(None).await?;
        server_task.await??;

        assert!(!client_task_is_running.load(Ordering::Acquire));

        Ok(())
    }
}
