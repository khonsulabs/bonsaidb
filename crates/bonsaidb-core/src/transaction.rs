use serde::{Deserialize, Serialize};

use crate::{
    document::Header,
    schema::{CollectionName, SerializedCollection},
    Error,
};

/// A list of operations to execute as a single unit. If any operation fails,
/// all changes are aborted. Reads that happen while the transaction is in
/// progress will return old data and not block.
#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[must_use]
pub struct Transaction {
    /// The operations in this transaction.
    pub operations: Vec<Operation>,
}

impl Transaction {
    /// Returns a new, empty transaction.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an operation to the transaction.
    pub fn push(&mut self, operation: Operation) {
        self.operations.push(operation);
    }

    /// Appends an operation to the transaction and returns self.
    pub fn with(mut self, operation: Operation) -> Self {
        self.push(operation);
        self
    }
}

impl From<Operation> for Transaction {
    fn from(operation: Operation) -> Self {
        Self {
            operations: vec![operation],
        }
    }
}

impl Transaction {
    /// Inserts a new document with `contents` into `collection`.  If `id` is
    /// `None` a unique id will be generated. If an id is provided and a
    /// document already exists with that id, a conflict error will be returned.
    pub fn insert(collection: CollectionName, id: Option<u64>, contents: Vec<u8>) -> Self {
        Self::from(Operation::insert(collection, id, contents))
    }

    /// Updates a document in `collection`.
    pub fn update(collection: CollectionName, header: Header, contents: Vec<u8>) -> Self {
        Self::from(Operation::update(collection, header, contents))
    }

    /// Deletes a document from a `collection`.
    pub fn delete(collection: CollectionName, header: Header) -> Self {
        Self::from(Operation::delete(collection, header))
    }
}

/// A single operation performed on a `Collection`.
#[derive(Clone, Serialize, Deserialize, Debug)]
#[must_use]
pub struct Operation {
    /// The id of the `Collection`.
    pub collection: CollectionName,

    /// The command being performed.
    pub command: Command,
}

impl Operation {
    /// Inserts a new document with `contents` into `collection`.  If `id` is
    /// `None` a unique id will be generated. If an id is provided and a
    /// document already exists with that id, a conflict error will be returned.
    pub const fn insert(collection: CollectionName, id: Option<u64>, contents: Vec<u8>) -> Self {
        Self {
            collection,
            command: Command::Insert { id, contents },
        }
    }

    /// Inserts a new document with the serialized representation of `contents`
    /// into `collection`.  If `id` is `None` a unique id will be generated. If
    /// an id is provided and a document already exists with that id, a conflict
    /// error will be returned.
    pub fn insert_serialized<C: SerializedCollection>(
        id: Option<u64>,
        contents: &C::Contents,
    ) -> Result<Self, Error> {
        let contents = C::serialize(contents)?;
        Ok(Self::insert(C::collection_name(), id, contents))
    }

    /// Updates a document in `collection`.
    pub const fn update(collection: CollectionName, header: Header, contents: Vec<u8>) -> Self {
        Self {
            collection,
            command: Command::Update { header, contents },
        }
    }

    /// Updates a document with the serialized representation of `contents` in
    /// `collection`.
    pub fn update_serialized<C: SerializedCollection>(
        header: Header,
        contents: &C::Contents,
    ) -> Result<Self, Error> {
        let contents = C::serialize(contents)?;
        Ok(Self::update(C::collection_name(), header, contents))
    }

    /// Deletes a document from a `collection`.
    pub const fn delete(collection: CollectionName, header: Header) -> Self {
        Self {
            collection,
            command: Command::Delete { header },
        }
    }
}

/// A command to execute within a `Collection`.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Command {
    /// Inserts a new document containing `contents`.
    Insert {
        /// An optional id for the document. If this is `None`, a unique id will
        /// be generated. If this is `Some()` and a document already exists with
        /// that id, a conflict error will be returned.
        id: Option<u64>,
        /// The initial contents of the document.
        #[serde(with = "serde_bytes")]
        contents: Vec<u8>,
    },

    /// Update an existing `Document` identified by `id`. `revision` must match
    /// the currently stored revision on the `Document`. If it does not, the
    /// command fill fail with a `DocumentConflict` error.
    Update {
        /// The header of the `Document`. The revision must match the current
        /// document.
        header: Header,

        /// The new contents to store within the `Document`.
        #[serde(with = "serde_bytes")]
        contents: Vec<u8>,
    },

    /// Delete an existing `Document` identified by `id`. `revision` must match
    /// the currently stored revision on the `Document`. If it does not, the
    /// command fill fail with a `DocumentConflict` error.
    Delete {
        /// The current header of the `Document`.
        header: Header,
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
pub struct Executed {
    /// The id of the transaction.
    pub id: u64,

    /// A list of containing ids of `Documents` changed.
    pub changes: Changes,
}

/// A list of changes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Changes {
    /// A list of changed documents.
    Documents(Vec<ChangedDocument>),
    /// A list of changed keys.
    Keys(Vec<ChangedKey>),
}

impl Changes {
    /// Returns the list of documents changed in this transaction, or None if
    /// the transaction was not a document transaction.
    #[must_use]
    pub fn documents(&self) -> Option<&[ChangedDocument]> {
        if let Self::Documents(docs) = self {
            Some(docs)
        } else {
            None
        }
    }

    /// Returns the list of keys changed in this transaction, or None if the
    /// transaction was not a `KeyValue` transaction.
    #[must_use]
    pub fn keys(&self) -> Option<&[ChangedKey]> {
        if let Self::Keys(keys) = self {
            Some(keys)
        } else {
            None
        }
    }
}

/// A record of a changed document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedDocument {
    /// The id of the `Collection` of the changed `Document`.
    pub collection: CollectionName,

    /// The id of the changed `Document`.
    pub id: u64,

    /// If the `Document` has been deleted, this will be `true`.
    pub deleted: bool,
}

/// A record of a changed `KeyValue` entry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangedKey {
    /// The namespace of the key.
    pub namespace: Option<String>,

    /// The key that was changed.
    pub key: String,

    /// True if the key was deleted.
    pub deleted: bool,
}
