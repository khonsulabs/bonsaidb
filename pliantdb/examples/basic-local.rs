use std::time::SystemTime;

use pliantdb::{
    core::{
        connection::Connection,
        schema::{Collection, CollectionName, InvalidNameError, Schematic},
        Error,
    },
    local::{Configuration, Storage},
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
async fn main() -> Result<(), anyhow::Error> {
    let db = Storage::<Message>::open_local("basic.pliantdb", &Configuration::default()).await?;
    let messages = db.collection::<Message>();

    // Insert a new `Message` into the collection. The `push()` method used
    // below is made available through the `Connection` trait. While this
    // example is connecting to a locally-stored database, with `PliantDB` all
    // database access is made through this single trait so that your code
    // doesn't need to change if you migrate from a local database to a remote
    // database -- just change how you establish your connection.
    let new_doc_info = messages
        .push(&Message {
            contents: String::from("Hello, World!"),
            timestamp: SystemTime::now(),
        })
        .await?;

    // Retrieve the message using the id returned from the previous call
    let message = messages
        .get(new_doc_info.id)
        .await?
        .expect("couldn't retrieve stored item")
        .contents::<Message>()?;

    println!(
        "Inserted message '{}' with id {}",
        message.contents, new_doc_info.id
    );

    Ok(())
}
