use std::collections::HashMap;

use bonsaidb_core::schema::{CollectionName, Schematic};
use nebari::{
    io::any::AnyFile,
    tree::{AnyTreeRoot, ByIdIndexer, Root},
};

#[cfg(any(feature = "encryption", feature = "compression"))]
use crate::storage::TreeVault;
use crate::{
    database::{document_tree_name, DocumentsTree},
    views::{view_entries_tree_name, ViewEntries, ViewIndexer},
};

#[derive(Default)]
pub(crate) struct OpenTrees {
    pub trees: Vec<Box<dyn AnyTreeRoot<AnyFile>>>,
    pub trees_index_by_name: HashMap<String, usize>,
}

impl OpenTrees {
    #[cfg_attr(not(feature = "encryption"), allow(unused_mut))]
    #[cfg_attr(feature = "encryption", allow(clippy::unnecessary_wraps))]
    pub fn open_tree<R: Root>(
        &mut self,
        name: &str,
        #[cfg(any(feature = "encryption", feature = "compression"))] vault: Option<TreeVault>,
    ) where
        R::Reducer: Default,
    {
        self.open_tree_with_reducer::<R>(
            name,
            <R::Reducer as Default>::default(),
            #[cfg(any(feature = "encryption", feature = "compression"))]
            vault,
        );
    }

    #[cfg_attr(not(feature = "encryption"), allow(unused_mut))]
    #[cfg_attr(feature = "encryption", allow(clippy::unnecessary_wraps))]
    pub fn open_tree_with_reducer<R: Root>(
        &mut self,
        name: &str,
        reducer: R::Reducer,
        #[cfg(any(feature = "encryption", feature = "compression"))] vault: Option<TreeVault>,
    ) {
        if !self.trees_index_by_name.contains_key(name) {
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            let mut tree = R::tree_with_reducer(name.to_string(), reducer);

            #[cfg(any(feature = "encryption", feature = "compression"))]
            if let Some(vault) = vault {
                tree = tree.with_vault(vault);
            }

            self.trees.push(Box::new(tree));
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn open_trees_for_document_change(
        &mut self,
        collection: &CollectionName,
        schema: &Schematic,
        #[cfg(any(feature = "encryption", feature = "compression"))] vault: Option<TreeVault>,
    ) {
        self.open_tree::<DocumentsTree>(
            &document_tree_name(collection),
            #[cfg(any(feature = "encryption", feature = "compression"))]
            vault.clone(),
        );

        if let Some(views) = schema.views_in_collection(collection) {
            for view in views {
                let view_name = view.view_name();
                if view.eager() {
                    self.open_tree_with_reducer::<ViewEntries>(
                        &view_entries_tree_name(&view_name),
                        ByIdIndexer(ViewIndexer::new(view.clone())),
                        #[cfg(any(feature = "encryption", feature = "compression"))]
                        vault.clone(),
                    );
                }
            }
        }
    }
}
