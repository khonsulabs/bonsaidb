use std::borrow::Cow;

use pliantdb::{
    connection::Connection,
    schema::{Collection, Collections, Database, Document, MapResult, View, Views},
    storage::Storage,
};
use serde::{Deserialize, Serialize};

struct Basic;

impl Database for Basic {
    fn name(&self) -> Cow<'static, str> {
        Cow::from("basic")
    }

    fn define_collections(&self, collections: &mut Collections) {
        collections.push(Todos);
    }
}

#[derive(Serialize, Deserialize)]
struct Todo<'a> {
    pub completed: bool,
    pub task: &'a str,
}

struct Todos;

impl Collection for Todos {
    fn name(&self) -> Cow<'static, str> {
        Cow::from("todos")
    }

    fn define_views(&self, views: &mut Views<Self>) {
        views.push(IncompleteTodos);
    }
}

struct IncompleteTodos;

impl View<Todos> for IncompleteTodos {
    type MapKey = ();
    type MapValue = ();
    type Reduce = ();

    fn name(&self) -> Cow<'static, str> {
        Cow::from("uncompleted-todos")
    }

    fn map<'d>(&self, document: &'d Document<Todos>) -> MapResult<'d> {
        let todo: Todo<'d> = document.contents::<Todo>()?;
        if todo.completed {
            Ok(Some(document.emit_nothing()))
        } else {
            Ok(None)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), pliantdb::connection::Error> {
    let db = Storage::open_local("test", Basic)?;

    let doc = db
        .collection::<Todos>()?
        .push(&Todo {
            completed: false,
            task: "Test Task",
        })
        .await?;

    let doc = db
        .collection::<Todos>()?
        .get(&doc.id)
        .await?
        .expect("couldn't retrieve stored item");

    println!("Inserted task with id {}", doc.id);

    Ok(())
}
