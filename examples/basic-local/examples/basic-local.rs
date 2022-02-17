use std::time::SystemTime;

use bonsaidb::{
    core::schema::{Collection, SerializedCollection},
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "messages")]
struct Message {
    pub timestamp: SystemTime,
    pub contents: String,
}

#[tokio::main]
async fn main() -> Result<(), bonsaidb::core::Error> {
    let db = Database::open::<Message>(StorageConfiguration::new("basic.bonsaidb")).await?;

    // Insert a new `Message` into the database. `Message` is a `Collection`
    // implementor, which makes them act in a similar fashion to tables in other
    // databases. BonsaiDb stores each "row" as a `Document`. This document
    // will have a unique ID, some other metadata, and your stored value. In
    // this case, `Message` implements `Serialize` and `Deserialize`, so we can
    // use convenience methods that return a `CollectionDocument`, moving all
    // needs of serialization behind the scenes.
    let document = Message {
        contents: String::from("Hello, World!"),
        timestamp: SystemTime::now(),
    }
    .push_into(&db)
    .await?;

    // Retrieve the message using the id returned from the previous call. both
    // `document` and `message_doc` should be identical.
    let message_doc = Message::get(document.header.id, &db)
        .await?
        .expect("couldn't retrieve stored item");

    println!(
        "Inserted message '{:?}' with id {}",
        message_doc.contents, message_doc.header.id
    );

    Ok(())
}

#[test]
fn runs() {
    main().unwrap()
}
