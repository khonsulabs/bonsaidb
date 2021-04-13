use pliantdb_client::{url::Url, Client};
use pliantdb_core::{
    networking::ServerConnection,
    schema::Schema,
    test_util::{self, Basic, TestDirectory},
};
use pliantdb_server::Server;

// This isn't really an example, just a way to run some manual testing
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = TestDirectory::new("test-server.pliantdb");
    let server = Server::open(dir.as_ref()).await?;
    server
        .install_self_signed_certificate("test", false)
        .await?;
    let certificate = server.certificate().await?;
    server.register_schema::<Basic>().await?;
    tokio::spawn(async move { server.listen_on(12345).await });

    let client = Client::new(
        &Url::parse("pliantdb://localhost:12345?server=test")?,
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
    println!("Done!");
    Ok(())
}

async fn test_one_client(client: Client, database_name: String) -> anyhow::Result<()> {
    for _ in 0u32..100 {
        client
            .create_database(&database_name, Basic::schema_id())
            .await
            .unwrap();
        let db = client.database::<Basic>(&database_name).await;
        test_util::store_retrieve_update_delete_tests(&db)
            .await
            .unwrap();
        client.delete_database(&database_name).await.unwrap();
    }
    Ok(())
}