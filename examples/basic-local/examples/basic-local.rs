use std::time::SystemTime;

use bonsaidb::{
    core::{
        schema::{Collection, CollectionName, InvalidNameError, Schematic},
        Error,
    },
    local::{config::Configuration, Database},
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
    let db = Database::open_local::<Message>("basic.bonsaidb".as_ref(), Configuration::default())
        .await?;

    // Insert a new `Message` into the database. `Message` is a `Collection`
    // implementor, which makes them act in a similar fashion to tables in other
    // databases. `BonsaiDb` stores each "row" as a `Document`. This document
    // will have a unique ID, some other metadata, and your stored value. In
    // this case, `Message` implements `Serialize` and `Deserialize`, so we can
    // use convenience methods that return a `CollectionDocument`, moving all
    // needs of serialization behind the scenes.
    let document = Message {
        contents: String::from("Hello, World!"),
        timestamp: SystemTime::now(),
    }
    .insert_into(&db)
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
