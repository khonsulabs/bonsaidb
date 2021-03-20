use std::borrow::Cow;

use pliantdb::schema::{
    Collection, CollectionViews, Database, DatabaseCollections, Document, MapResult, View,
};
use serde::{Deserialize, Serialize};

struct Basic;

impl Database for Basic {
    fn add_collections(collections: &mut DatabaseCollections) {
        collections.push(TodoCollection);
    }
}

#[derive(Serialize, Deserialize)]
struct Todo<'a> {
    pub completed: bool,
    pub task: &'a str,
}

struct TodoCollection;

impl Collection for TodoCollection {
    fn name(&self) -> Cow<'static, str> {
        Cow::from("todos")
    }

    fn add_views(&self, views: &mut CollectionViews<Self>) {
        views.push(IncompleteTodos);
    }
}

struct IncompleteTodos;

impl View<TodoCollection> for IncompleteTodos {
    type MapKey = ();
    type MapValue = ();
    type Reduce = ();

    fn name(&self) -> Cow<'static, str> {
        Cow::from("uncompleted-todos")
    }

    fn map<'d>(&self, document: &'d Document<TodoCollection>) -> MapResult<'d> {
        let todo: Todo<'d> = document.contents::<Todo>()?;
        if todo.completed {
            Ok(Some(document.emit_nothing()))
        } else {
            Ok(None)
        }
    }
}

fn main() {}
