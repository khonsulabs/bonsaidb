use std::borrow::Cow;

use pliantdb::{
    connection::Connection,
    document::Document,
    schema::{collection, Collection, MapResult, Schema, View},
    storage::Storage,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Todo<'a> {
    pub completed: bool,
    pub task: &'a str,
    pub parent_id: Option<Uuid>,
}

impl<'a> Collection for Todo<'a> {
    fn id() -> collection::Id {
        collection::Id::from("todos")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view::<TodosByParent>();
    }
}

struct TodosByParent;

impl<'k> View<'k> for TodosByParent {
    type MapKey = Option<Uuid>;
    type MapValue = ();
    type Reduce = ();

    fn version() -> usize {
        0
    }

    fn name() -> Cow<'static, str> {
        Cow::from("todos-by-parent")
    }

    fn map(document: &Document<'_>) -> MapResult<'k, Option<Uuid>> {
        let todo = document.contents::<Todo>()?;
        Ok(Some(document.emit_key(todo.parent_id)))
    }
}

#[tokio::main]
async fn main() -> Result<(), pliantdb::Error> {
    let db = Storage::<Todo>::open_local("basic.pliantdb")?;
    let todos = db.collection::<Todo>()?;
    let header = todos
        .push(&Todo {
            completed: false,
            task: "Test Task",
            parent_id: None,
        })
        .await?;

    let doc = todos
        .get(header.id)
        .await?
        .expect("couldn't retrieve stored item");

    let todo = doc.contents::<Todo>()?;

    println!("Inserted todo '{}' with id {}", todo.task, header.id);

    Ok(())
}
