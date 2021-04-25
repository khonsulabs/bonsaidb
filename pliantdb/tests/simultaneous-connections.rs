//! Tests a single server with multiple simultaneous connections.

use pliantdb::{
    client::{url::Url, Client},
    core::{
        networking::ServerConnection,
        schema::Schema,
        test_util::{self, Basic, TestDirectory},
    },
    server::{Configuration, Server},
};

#[tokio::test]
async fn simultaneous_connections() -> anyhow::Result<()> {
    let dir = TestDirectory::new("simultaneous-connections.pliantdb");
    let server = Server::open(dir.as_ref(), Configuration::default()).await?;
    server
        .install_self_signed_certificate("test", false)
        .await?;
    let certificate = server.certificate().await?;
    server.register_schema::<Basic>().await?;
    tokio::spawn(async move { server.listen_on(12345).await });

    let client = Client::new(
        Url::parse("pliantdb://localhost:12345?server=test")?,
        Some(certificate),
    )
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
            .create_database(&database_name, Basic::schema_name()?)
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
