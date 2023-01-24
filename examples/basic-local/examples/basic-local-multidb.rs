use std::time::SystemTime;

use bonsaidb::core::connection::{Connection, StorageConnection};
use bonsaidb::core::schema::{Collection, SerializedCollection};
use bonsaidb::local::config::{Builder, StorageConfiguration};
use bonsaidb::local::Storage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Collection)]
#[collection(name = "messages")]
struct Message {
    pub timestamp: SystemTime,
    pub contents: String,
}

fn main() -> Result<(), bonsaidb::core::Error> {
    let storage =
        Storage::open(StorageConfiguration::new("basic.bonsaidb").with_schema::<Message>()?)?;
    let messages = storage.create_database::<Message>("messages", true)?;

    let private_messages = storage.create_database::<Message>("private-messages", true)?;

    insert_a_message(&messages, "Hello, World!")?;
    insert_a_message(&private_messages, "Hey!")?;

    Ok(())
}

// ANCHOR: reusable-code
fn insert_a_message<C: Connection>(
    connection: &C,
    value: &str,
) -> Result<(), bonsaidb::core::Error> {
    Message {
        contents: String::from(value),
        timestamp: SystemTime::now(),
    }
    .push_into(connection)?;
    Ok(())
}
// ANCHOR_END: reusable-code

#[test]
fn runs() {
    main().unwrap()
}
