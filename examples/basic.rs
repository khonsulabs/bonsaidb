use std::borrow::Cow;

use pliantdb::{
    connection::Connection,
    document::Document,
    schema::{collection, Collection, Database, MapResult, Schema, View},
    storage::Storage,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

struct Basic;

impl Database for Basic {
    fn define_collections(collections: &mut Schema) {
        collections.define_collection::<Todos>();
    }
}

#[derive(Serialize, Deserialize)]
struct Todo<'a> {
    pub completed: bool,
    pub task: &'a str,
    pub parent_id: Option<Uuid>,
}

struct Todos;

impl Collection for Todos {
    fn id() -> collection::Id {
        collection::Id::from("todos")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view::<TodosByParent, _>();
    }
}

struct TodosByParent;

impl<'k> View<'k, Todos> for TodosByParent {
    type MapKey = Option<Uuid>;
    type MapValue = ();
    type Reduce = ();

    fn version() -> usize {
        0
    }

    fn name() -> Cow<'static, str> {
        Cow::from("todos-by-parent")
    }

    fn map(document: &Document<Todos>) -> MapResult<'k, Option<Uuid>> {
        let todo = document.contents::<Todo>()?;
        Ok(Some(document.emit_key(todo.parent_id)))
    }
}

#[tokio::main]
async fn main() -> Result<(), pliantdb::Error> {
    let db = Storage::<Basic>::open_local("test")?;
    let todos = db.collection::<Todos>()?;
    let doc = todos
        .push(&Todo {
            completed: false,
            task: "Test Task",
            parent_id: None,
        })
        .await?;

    let doc = todos
        .get(&doc.id)
        .await?
        .expect("couldn't retrieve stored item");

    let todo = doc.contents::<Todo>()?;

    println!("Inserted task '{}' with id {}", todo.task, doc.header.id);

    Ok(())
}
