use std::collections::HashMap;

use bonsaidb_core::{
    document::DocumentId,
    schema::CollectionName,
    transaction::{ChangedDocument, ChangedKey, Changes, Executed},
};
use serde::{Deserialize, Serialize};

/// Details about an executed transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutedTransactionV0 {
    /// The id of the transaction.
    pub id: u64,

    /// A list of containing ids of `Documents` changed.
    pub changes: ChangesV0,
}

impl From<ExecutedTransactionV0> for Executed {
    fn from(legacy: ExecutedTransactionV0) -> Self {
        Self {
            id: legacy.id,
            changes: legacy.changes.into(),
        }
    }
}

/// A list of changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChangesV0 {
    /// A list of changed documents.
    Documents(Vec<ChangedDocumentV0>),
    /// A list of changed keys.
    Keys(Vec<ChangedKey>),
}

impl From<ChangesV0> for Changes {
    fn from(legacy: ChangesV0) -> Self {
        match legacy {
            ChangesV0::Documents(legacy_documents) => {
                let mut changed_documents = Vec::with_capacity(legacy_documents.len());
                let mut collections = Vec::new();
                let mut collection_indexes = HashMap::new();
                for changed in legacy_documents {
                    let collection = if let Some(id) = collection_indexes.get(&changed.collection) {
                        *id
                    } else {
                        let id = u16::try_from(collections.len()).unwrap();
                        collection_indexes.insert(changed.collection.clone(), id);
                        collections.push(changed.collection);
                        id
                    };
                    changed_documents.push(ChangedDocument {
                        collection,
                        id: changed.id,
                        deleted: changed.deleted,
                    });
                }
                Self::Documents {
                    collections,
                    changes: changed_documents,
                }
            }
            ChangesV0::Keys(changes) => Self::Keys(changes),
        }
    }
}

/// A record of a changed document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedDocumentV0 {
    /// The id of the `Collection` of the changed `Document`.
    pub collection: CollectionName,

    /// The id of the changed `Document`.
    pub id: DocumentId,

    /// If the `Document` has been deleted, this will be `true`.
    pub deleted: bool,
}
