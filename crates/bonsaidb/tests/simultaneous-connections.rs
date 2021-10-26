//! Tests a single server with multiple simultaneous connections.

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        connection::ServerConnection,
        test_util::{self, Basic, TestDirectory},
    },
    server::{Configuration, DefaultPermissions, Server},
};

#[tokio::test]
async fn simultaneous_connections() -> anyhow::Result<()> {
    let dir = TestDirectory::new("simultaneous-connections.bonsaidb");
    let server = Server::open(
        dir.as_ref(),
        Configuration {
            default_permissions: DefaultPermissions::AllowAll,
            ..Configuration::default()
        },
    )
    .await?;
    server
        .install_self_signed_certificate("test", false)
        .await?;
    let certificate = server.certificate().await?;
    server.register_schema::<Basic>().await?;
    tokio::spawn(async move { server.listen_on(12345).await });

    let client = Client::build(Url::parse("bonsaidb://localhost:12345?server=test")?)
        .with_certificate(certificate)
        .finish()
        .await?;

    let mut tasks = Vec::new();
    for i in 0usize..10 {
        tasks.push(test_one_client(client.clone(), format!("test{}", i)));
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<()>, anyhow::Error>>()?;
    Ok(())
}

async fn test_one_client(client: Client, database_name: String) -> anyhow::Result<()> {
    for _ in 0u32..50 {
        client
            .create_database::<Basic>(&database_name)
            .await
            .unwrap();
        let db = client.database::<Basic>(&database_name).await?;
        test_util::store_retrieve_update_delete_tests(&db)
            .await
            .unwrap();
        client.delete_database(&database_name).await.unwrap();
    }
    Ok(())
}
