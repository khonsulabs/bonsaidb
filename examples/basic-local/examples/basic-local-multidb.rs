use std::time::SystemTime;

use bonsaidb::{
    core::{
        connection::{Connection, StorageConnection},
        schema::{Collection, CollectionName, InvalidNameError, Schematic},
        Error,
    },
    local::{config::Configuration, Storage},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    pub timestamp: SystemTime,
    pub contents: String,
}

impl Collection for Message {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "messages")
    }

    fn define_views(_schema: &mut Schematic) -> Result<(), Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), bonsaidb::core::Error> {
    let storage = Storage::open_local("basic.bonsaidb", Configuration::default()).await?;
    // Before you can create a database, you must register the schema you're
    // wanting to use.
    storage.register_schema::<Message>().await?;
    storage.create_database::<Message>("messages", true).await?;
    let messages = storage.database::<Message>("messages").await?;
    storage
        .create_database::<Message>("private-messages", true)
        .await?;
    let private_messages = storage.database::<Message>("private-messages").await?;

    insert_a_message(&messages, "Hello, World!").await?;
    insert_a_message(&private_messages, "Hey!").await?;

    Ok(())
}

// ANCHOR: reusable-code
async fn insert_a_message<C: Connection>(
    connection: &C,
    value: &str,
) -> Result<(), bonsaidb::core::Error> {
    Message {
        contents: String::from(value),
        timestamp: SystemTime::now(),
    }
    .insert_into(connection)
    .await?;
    Ok(())
}
// ANCHOR_END: reusable-code
