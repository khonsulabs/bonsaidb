use std::collections::HashMap;

use bonsaidb_core::schema::{CollectionName, Schematic};
use bonsaidb_roots::{Roots, StdFile, Tree};

use crate::{
    database::document_tree_name,
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name,
    },
    Error,
};

#[derive(Default)]
pub struct OpenTrees {
    pub trees: Vec<Tree<StdFile>>,
    pub trees_index_by_name: HashMap<String, usize>,
}

impl OpenTrees {
    pub fn open_tree(
        &mut self,
        sled: &Roots<StdFile>,
        name: &str,
    ) -> Result<(), bonsaidb_roots::Error> {
        #[allow(clippy::map_entry)] // unwrapping errors is much uglier using entry()
        if !self.trees_index_by_name.contains_key(name) {
            let tree = sled.tree(name.to_string());
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            self.trees.push(tree);
        }

        Ok(())
    }

    pub fn open_trees_for_document_change(
        &mut self,
        sled: &Roots<StdFile>,
        database: &str,
        collection: &CollectionName,
        schema: &Schematic,
    ) -> Result<(), Error> {
        self.open_tree(sled, &document_tree_name(database, collection))?;

        if let Some(views) = schema.views_in_collection(collection) {
            for view in views {
                let view_name = view.view_name()?;
                if view.unique() {
                    self.open_tree(sled, &view_omitted_docs_tree_name(database, &view_name))?;
                    self.open_tree(sled, &view_document_map_tree_name(database, &view_name))?;
                    self.open_tree(sled, &view_entries_tree_name(database, &view_name))?;
                } else {
                    self.open_tree(sled, &view_invalidated_docs_tree_name(database, &view_name))?;
                }
            }
        }

        Ok(())
    }
}
