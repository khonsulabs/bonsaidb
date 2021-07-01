use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use crate::{
    document::{Header, KeyId},
    schema::CollectionName,
};

/// A list of operations to execute as a single unit. If any operation fails,
/// all changes are aborted. Reads that happen while the transaction is in
/// progress will return old data and not block.
#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Transaction<'a> {
    /// The operations in this transaction.
    pub operations: Vec<Operation<'a>>,
}

impl<'a> Transaction<'a> {
    /// Adds an operation to the transaction.
    pub fn push(&mut self, operation: Operation<'a>) {
        self.operations.push(operation);
    }
}

/// A single operation performed on a `Collection`.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Operation<'a> {
    /// The id of the `Collection`.
    pub collection: CollectionName,

    /// The command being performed.
    pub command: Command<'a>,
}

/// A command to execute within a `Collection`.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Command<'a> {
    /// Inserts a new document containing `contents`.
    Insert {
        /// The encryption key to use for the document.
        encryption_key: Option<KeyId>,

        /// The initial contents of the document.
        contents: Cow<'a, [u8]>,
    },

    /// Update an existing `Document` identified by `id`. `revision` must match
    /// the currently stored revision on the `Document`. If it does not, the
    /// command fill fail with a `DocumentConflict` error.
    Update {
        /// The header of the `Document`. The revision must match the current
        /// document. Changing the encryption key will encrypt the document with
        /// the new key.
        header: Cow<'a, Header>,

        /// The new contents to store within the `Document`.
        contents: Cow<'a, [u8]>,
    },

    /// Delete an existing `Document` identified by `id`. `revision` must match
    /// the currently stored revision on the `Document`. If it does not, the
    /// command fill fail with a `DocumentConflict` error.
    Delete {
        /// The current header of the `Document`.
        header: Cow<'a, Header>,
    },
}

/// Information about the result of each `Operation` in a transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OperationResult {
    /// An operation succeeded but had no information to output.
    Success,

    /// A `Document` was updated.
    DocumentUpdated {
        /// The id of the `Collection` of the updated `Document`.
        collection: CollectionName,

        /// The header of the updated `Document`.
        header: Header,
    },

    /// A `Document` was deleted.
    DocumentDeleted {
        /// The id of the `Collection` of the deleted `Document`.
        collection: CollectionName,

        /// The id of the deleted `Document`.
        id: u64,
    },
}

/// Details about an executed transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Executed<'a> {
    /// The id of the transaction.
    pub id: u64,

    /// A list of containing ids of `Documents` changed.
    pub changed_documents: Cow<'a, [ChangedDocument]>,
}

impl<'a> Executed<'a> {
    /// Convert this structure to be free of borrows.
    #[must_use]
    pub fn to_owned(&self) -> Executed<'static> {
        Executed {
            id: self.id,
            changed_documents: self.changed_documents.iter().cloned().collect(),
        }
    }
}

/// A record of a changed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedDocument {
    /// The id of the `Collection` of the changed `Document`.
    pub collection: CollectionName,

    /// The id of the changed `Document`.
    pub id: u64,

    /// If the `Document` has been deleted, this will be `true`.
    pub deleted: bool,
}
