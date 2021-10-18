use std::{collections::HashMap, sync::Arc};

use bonsaidb_core::{
    document::KeyId,
    schema::{CollectionName, Schematic},
};
use nebari::{
    io::fs::StdFile,
    tree::{AnyTreeRoot, Root, Unversioned, Versioned},
};

use crate::{
    database::document_tree_name,
    vault::{TreeVault, Vault},
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name,
    },
    Error,
};

#[derive(Default)]
pub(crate) struct OpenTrees {
    pub trees: Vec<Box<dyn AnyTreeRoot<StdFile>>>,
    pub trees_index_by_name: HashMap<String, usize>,
}

impl OpenTrees {
    pub fn open_tree<R: Root>(
        &mut self,
        name: &str,
        encryption_key: Option<&KeyId>,
        vault: &Arc<Vault>,
    ) {
        if !self.trees_index_by_name.contains_key(name) {
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            let mut tree = R::tree(name.to_string());
            if let Some(encryption_key) = encryption_key {
                tree = tree.with_vault(TreeVault {
                    key: encryption_key.clone(),
                    vault: vault.clone(),
                });
            }
            self.trees.push(Box::new(tree));
        }
    }

    pub fn open_trees_for_document_change(
        &mut self,
        collection: &CollectionName,
        schema: &Schematic,
        encryption_key: Option<&KeyId>,
        vault: &Arc<Vault>,
    ) -> Result<(), Error> {
        self.open_tree::<Versioned>(&document_tree_name(collection), encryption_key, vault);

        if let Some(views) = schema.views_in_collection(collection) {
            for view in views {
                let view_name = view.view_name()?;
                if view.unique() {
                    self.open_tree::<Unversioned>(
                        &view_omitted_docs_tree_name(&view_name),
                        encryption_key,
                        vault,
                    );
                    self.open_tree::<Unversioned>(
                        &view_document_map_tree_name(&view_name),
                        encryption_key,
                        vault,
                    );
                    self.open_tree::<Unversioned>(
                        &view_entries_tree_name(&view_name),
                        encryption_key,
                        vault,
                    );
                } else {
                    self.open_tree::<Unversioned>(
                        &view_invalidated_docs_tree_name(&view_name),
                        encryption_key,
                        vault,
                    );
                }
            }
        }

        Ok(())
    }
}
