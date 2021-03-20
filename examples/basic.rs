use std::borrow::Cow;

use pliantdb::{
    connection::Connection,
    schema::{Collection, Collections, Database, Document, MapResult, View, Views},
    storage::Storage,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

struct Basic;

impl Database for Basic {
    fn name(&self) -> Cow<'static, str> {
        Cow::from("basic") // TODO name shouldn't be on Database
    }

    fn define_collections(&self, collections: &mut Collections) {
        collections.push(Todos);
    }
}

#[derive(Serialize, Deserialize)]
struct Todo<'a> {
    pub completed: bool,
    pub task: &'a str,
    pub parent_id: Option<Uuid>,
}

#[derive(Clone)]
struct Todos;

impl Collection for Todos {
    fn name(&self) -> Cow<'static, str> {
        Cow::from("todos")
    }

    fn define_views(&self, views: &mut Views<Self>) {
        views.push(TodosByParent);
    }
}

struct TodosByParent;

impl View<Todos> for TodosByParent {
    type MapKey = Option<Uuid>;
    type MapValue = ();
    type Reduce = ();

    fn name(&self) -> Cow<'static, str> {
        Cow::from("todos-by-parent")
    }

    fn map(&self, document: &Document<Todos>) -> MapResult<Option<Uuid>> {
        let todo = document.contents::<Todo>()?;
        Ok(Some(document.emit(todo.parent_id)))
    }
}

// mydatabase.collection::<Todos>().view::<TodosByParent>().fetch(&None, &None) -> Vec<Document<Todos>>

#[tokio::main]
async fn main() -> Result<(), pliantdb::Error> {
    let db = Storage::open_local("test", Basic)?;

    let doc = db
        .collection::<Todos>()?
        .push(&Todo {
            completed: false,
            task: "Test Task",
            parent_id: None,
        })
        .await?;

    let doc = db
        .collection::<Todos>()?
        .get(&doc.id)
        .await?
        .expect("couldn't retrieve stored item");

    let todo = doc.contents::<Todo>()?;

    println!("Inserted task '{}' with id {}", todo.task, doc.id);

    Ok(())
}
