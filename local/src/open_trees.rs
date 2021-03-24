use std::collections::HashMap;

use pliantdb_core::schema::collection;

use crate::storage::document_tree_name;

#[derive(Default)]
pub struct OpenTrees {
    pub trees: Vec<sled::Tree>,
    pub trees_index_by_name: HashMap<String, usize>,
}

impl OpenTrees {
    pub fn open_tree(&mut self, sled: &sled::Db, name: &str) -> Result<(), sled::Error> {
        #[allow(clippy::map_entry)] // unwrapping errors is much uglier using entry()
        if !self.trees_index_by_name.contains_key(name) {
            let tree = sled.open_tree(name.as_bytes())?;
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            self.trees.push(tree);
        }

        Ok(())
    }

    pub fn open_trees_for_document_change(
        &mut self,
        sled: &sled::Db,
        collection: &collection::Id,
    ) -> Result<(), sled::Error> {
        self.open_tree(sled, &document_tree_name(collection))
    }
}
