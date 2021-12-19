//! Tests a single server with multiple simultaneous connections.

use bonsaidb::{
    client::{url::Url, Client},
    core::{
        connection::StorageConnection,
        test_util::{self, Basic, BasicSchema, TestDirectory},
    },
    local::config::Builder,
    server::{DefaultPermissions, Server, ServerConfiguration},
};

#[tokio::test]
async fn simultaneous_connections() -> anyhow::Result<()> {
    let dir = TestDirectory::new("simultaneous-connections.bonsaidb");
    let server = Server::open(
        ServerConfiguration::new(&dir)
            .default_permissions(DefaultPermissions::AllowAll)
            .with_schema::<BasicSchema>()?,
    )
    .await?;
    server.install_self_signed_certificate(false).await?;
    let certificate = server
        .certificate_chain()
        .await?
        .into_end_entity_certificate();
    tokio::spawn(async move { server.listen_on(12345).await });

    let client = Client::build(Url::parse("bonsaidb://localhost:12345")?)
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
            .create_database::<Basic>(&database_name, false)
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
