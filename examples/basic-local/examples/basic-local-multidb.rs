use std::time::SystemTime;

use bonsaidb::{
    core::{
        connection::{AsyncConnection, AsyncStorageConnection},
        schema::{Collection, SerializedCollection},
    },
    local::{
        config::{Builder, StorageConfiguration},
        AsyncStorage,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Collection)]
#[collection(name = "messages")]
struct Message {
    pub timestamp: SystemTime,
    pub contents: String,
}

#[tokio::main]
async fn main() -> Result<(), bonsaidb::core::Error> {
    let storage =
        AsyncStorage::open(StorageConfiguration::new("basic.bonsaidb").with_schema::<Message>()?)
            .await?;
    let messages = storage.create_database::<Message>("messages", true).await?;

    let private_messages = storage
        .create_database::<Message>("private-messages", true)
        .await?;

    insert_a_message(&messages, "Hello, World!").await?;
    insert_a_message(&private_messages, "Hey!").await?;

    Ok(())
}

// ANCHOR: reusable-code
async fn insert_a_message<C: AsyncConnection>(
    connection: &C,
    value: &str,
) -> Result<(), bonsaidb::core::Error> {
    Message {
        contents: String::from(value),
        timestamp: SystemTime::now(),
    }
    .push_into_async(connection)
    .await?;
    Ok(())
}
// ANCHOR_END: reusable-code

#[test]
fn runs() {
    main().unwrap()
}
