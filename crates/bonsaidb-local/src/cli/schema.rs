use std::str::FromStr;

use bonsaidb_core::connection::{AsyncStorageConnection, StorageConnection};
use bonsaidb_core::schema::{
    CollectionName, InvalidNameError, SchemaName, SchemaSummary, ViewName,
};
use clap::Parser;

/// A schema query against a storage instance.
#[derive(Parser, Debug)]
pub struct Command {
    /// The name of the schema to query.
    pub name: Option<SchemaName>,

    /// The item in the schema to query.
    pub item: Option<CollectionOrView>,
}

impl Command {
    /// Executes the command on `storage`.
    pub fn execute<SC: StorageConnection>(self, storage: &SC) -> Result<(), crate::Error> {
        let schemas = storage.list_available_schemas()?;
        self.handle_schema_command(schemas)
    }

    /// Executes the command on `storage`.
    pub async fn execute_async<SC: AsyncStorageConnection>(
        self,
        storage: &SC,
    ) -> Result<(), crate::Error> {
        let schemas = storage.list_available_schemas().await?;
        self.handle_schema_command(schemas)
    }

    fn handle_schema_command(self, schemas: Vec<SchemaSummary>) -> Result<(), crate::Error> {
        if let Some(name) = self.name {
            let Some(schema) = schemas.into_iter().find(|s| s.name == name) else {
                return Err(crate::Error::Core(
                    bonsaidb_core::Error::SchemaNotRegistered(name),
                ));
            };

            if let Some(item) = self.item {
                match item {
                    CollectionOrView::View(view) => {
                        let Some(collection) = schema.collection(&view.collection) else {
                            return Err(crate::Error::Core(
                                bonsaidb_core::Error::CollectionNotFound,
                            ));
                        };
                        let Some(view) = collection.view(&view) else {
                            return Err(crate::Error::Core(bonsaidb_core::Error::ViewNotFound));
                        };
                        println!("Version: {}", view.version);
                        println!("Policy: {}", view.policy);
                    }
                    CollectionOrView::Collection(collection) => {
                        let Some(collection) = schema.collection(&collection) else {
                            return Err(crate::Error::Core(
                                bonsaidb_core::Error::CollectionNotFound,
                            ));
                        };
                        let mut views = collection.views().collect::<Vec<_>>();
                        views.sort_by(|v1, v2| v1.name.cmp(&v2.name));
                        for view in views {
                            println!("{}", view.name);
                        }
                    }
                }
            } else {
                print_collection_list(&schema);
            }
        } else if let Some(item) = self.item {
            eprintln!("missing `schema` for inspecting {item:?}");
            std::process::exit(-1);
        } else {
            print_schema_list(schemas);
        }
        Ok(())
    }
}

fn print_schema_list(mut schemas: Vec<SchemaSummary>) {
    schemas.sort_by(|s1, s2| s1.name.cmp(&s2.name));

    for schema in schemas {
        println!("{}", schema.name);
    }
}

fn print_collection_list(schema: &SchemaSummary) {
    let mut collections = schema.collections().collect::<Vec<_>>();
    collections.sort_by(|c1, c2| c1.name.cmp(&c2.name));

    for collection in collections {
        println!("{}", collection.name);
    }
}

/// A name that is either a [`CollectionName`] or [`ViewName`].
#[derive(Debug, Clone)]
pub enum CollectionOrView {
    /// A view name.
    View(ViewName),
    /// A collection name.
    Collection(CollectionName),
}

impl FromStr for CollectionOrView {
    type Err = InvalidNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(view) = ViewName::from_str(s) {
            Ok(Self::View(view))
        } else {
            CollectionName::from_str(s).map(Self::Collection)
        }
    }
}
