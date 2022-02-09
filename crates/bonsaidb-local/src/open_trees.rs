use std::collections::HashMap;
#[cfg(feature = "encryption")]
use std::sync::Arc;

use bonsaidb_core::{
    document::KeyId,
    schema::{CollectionName, Schematic},
};
use nebari::{
    io::any::AnyFile,
    tree::{AnyTreeRoot, Root, Unversioned, Versioned},
};

#[cfg(feature = "encryption")]
use crate::vault::{TreeVault, Vault};
use crate::{
    database::document_tree_name,
    views::{
        view_document_map_tree_name, view_entries_tree_name, view_invalidated_docs_tree_name,
        view_omitted_docs_tree_name,
    },
    Error,
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
        encryption_key: Option<&KeyId>,
        #[cfg(feature = "encryption")] vault: &Arc<Vault>,
    ) -> Result<(), Error> {
        if !self.trees_index_by_name.contains_key(name) {
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            let mut tree = R::tree(name.to_string());

            #[cfg(feature = "encryption")]
            if let Some(encryption_key) = encryption_key {
                tree = tree.with_vault(TreeVault {
                    key: encryption_key.clone(),
                    vault: vault.clone(),
                });
            }

            #[cfg(not(feature = "encryption"))]
            if encryption_key.is_some() {
                return Err(Error::EncryptionDisabled);
            }

            self.trees.push(Box::new(tree));
        }
        Ok(())
    }

    pub fn open_trees_for_document_change(
        &mut self,
        collection: &CollectionName,
        schema: &Schematic,
        encryption_key: Option<&KeyId>,
        #[cfg(feature = "encryption")] vault: &Arc<Vault>,
    ) -> Result<(), Error> {
        self.open_tree::<Versioned>(
            &document_tree_name(collection),
            encryption_key,
            #[cfg(feature = "encryption")]
            vault,
        )?;

        if let Some(views) = schema.views_in_collection(collection) {
            for view in views {
                let view_name = view.view_name();
                if view.unique() {
                    self.open_tree::<Unversioned>(
                        &view_omitted_docs_tree_name(&view_name),
                        encryption_key,
                        #[cfg(feature = "encryption")]
                        vault,
                    )?;
                    self.open_tree::<Unversioned>(
                        &view_document_map_tree_name(&view_name),
                        encryption_key,
                        #[cfg(feature = "encryption")]
                        vault,
                    )?;
                    self.open_tree::<Unversioned>(
                        &view_entries_tree_name(&view_name),
                        encryption_key,
                        #[cfg(feature = "encryption")]
                        vault,
                    )?;
                } else {
                    self.open_tree::<Unversioned>(
                        &view_invalidated_docs_tree_name(&view_name),
                        encryption_key,
                        #[cfg(feature = "encryption")]
                        vault,
                    )?;
                }
            }
        }

        Ok(())
    }
}
