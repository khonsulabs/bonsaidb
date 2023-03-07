use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::schema::{CollectionName, SchemaName, Schematic, ViewName};

/// A summary of a [`Schema`](crate::schema::Schema)/[`Schematic`].
///
/// This type is a serializable summary of a [`Schematic`] and is the result of
/// [`StorageConnection::list_available_schemas`](crate::connection::StorageConnection::list_available_schemas)/[`AsyncStorageConnection::list_available_schemas`](crate::connection::AsyncStorageConnection::list_available_schemas).
/// It can be used to query information stored in BonsaiDb without needing
/// access to the Rust types.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct SchemaSummary {
    /// The name of the [`Schema`](crate::schema::Schema) this summary is of.
    pub name: SchemaName,
    collections: HashMap<CollectionName, CollectionSummary>,
}

impl SchemaSummary {
    /// Returns the summary of named collection, if the schema contains it.
    #[must_use]
    pub fn collection(&self, name: &CollectionName) -> Option<&CollectionSummary> {
        self.collections.get(name)
    }

    /// Returns an iterator over all collections contained in this schema.
    pub fn collections(&self) -> impl Iterator<Item = &CollectionSummary> {
        self.collections.values()
    }
}

impl<'a> From<&'a Schematic> for SchemaSummary {
    fn from(schematic: &'a Schematic) -> Self {
        let mut summary = Self {
            name: schematic.name.clone(),
            collections: HashMap::new(),
        };

        for collection_name in schematic.collections() {
            let collection = summary
                .collections
                .entry(collection_name.clone())
                .or_insert_with(|| CollectionSummary {
                    name: collection_name.clone(),
                    views: HashMap::new(),
                });
            for view in schematic.views_in_collection(collection_name) {
                let name = view.view_name();
                collection.views.insert(
                    name.clone(),
                    ViewSummary {
                        name,
                        lazy: view.lazy(),
                        unique: view.unique(),
                        version: view.version(),
                    },
                );
            }
        }

        summary
    }
}

/// A summary of a [`Collection`](crate::schema::Collection).
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct CollectionSummary {
    /// The name of the [`Collection`](crate::schema::Collection) this is a summary of.
    pub name: CollectionName,
    views: HashMap<ViewName, ViewSummary>,
}

impl CollectionSummary {
    /// Returns the summary of the named view, if it is contained in this collection.
    #[must_use]
    pub fn view(&self, name: &ViewName) -> Option<&ViewSummary> {
        self.views.get(name)
    }

    /// Returns an iterator over all summaries of views in this collection.
    pub fn views(&self) -> impl Iterator<Item = &ViewSummary> {
        self.views.values()
    }
}

/// A summary of a [`ViewSchema`](crate::schema::ViewSchema).
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct ViewSummary {
    /// The name of the [`ViewSchema`](crate::schema::ViewSchema) this is a
    /// summary of.
    pub name: ViewName,
    /// The result of [`ViewSchema::lazy()`](crate::schema::ViewSchema::lazy)
    /// for this view.
    pub lazy: bool,
    /// The result of [`ViewSchema::unique()`](crate::schema::ViewSchema::unique)
    /// for this view.
    pub unique: bool,
    /// The result of [`ViewSchema::version()`](crate::schema::ViewSchema::version)
    /// for this view.
    pub version: u64,
}
