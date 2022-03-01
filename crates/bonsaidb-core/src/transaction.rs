use arc_bytes::serde::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    document::{CollectionHeader, DocumentId, Header},
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
    pub fn insert(
        collection: CollectionName,
        id: Option<DocumentId>,
        contents: impl Into<Bytes>,
    ) -> Self {
        Self::from(Operation::insert(collection, id, contents))
    }

    /// Updates a document in `collection`.
    pub fn update(collection: CollectionName, header: Header, contents: impl Into<Bytes>) -> Self {
        Self::from(Operation::update(collection, header, contents))
    }

    /// Overwrites a document in `collection`. If a document with `id` exists,
    /// it will be overwritten. If a document with `id` doesn't exist, it will
    /// be created.
    pub fn overwrite(
        collection: CollectionName,
        id: DocumentId,
        contents: impl Into<Bytes>,
    ) -> Self {
        Self::from(Operation::overwrite(collection, id, contents))
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
    pub fn insert(
        collection: CollectionName,
        id: Option<DocumentId>,
        contents: impl Into<Bytes>,
    ) -> Self {
        Self {
            collection,
            command: Command::Insert {
                id,
                contents: contents.into(),
            },
        }
    }

    /// Inserts a new document with the serialized representation of `contents`
    /// into `collection`. If `id` is `None` a unique id will be generated. If
    /// an id is provided and a document already exists with that id, a conflict
    /// error will be returned.
    pub fn insert_serialized<C: SerializedCollection>(
        id: Option<C::PrimaryKey>,
        contents: &C::Contents,
    ) -> Result<Self, Error> {
        let id = id.map(DocumentId::new).transpose()?;
        let contents = C::serialize(contents)?;
        Ok(Self::insert(C::collection_name(), id, contents))
    }

    /// Pushes a new document with the serialized representation of `contents`
    /// into `collection`.
    ///
    /// ## Automatic Id Assignment
    ///
    /// This function calls [`SerializedCollection::natural_id()`] to try to
    /// retrieve a primary key value from `contents`. If an id is returned, the
    /// item is inserted with that id. If an id is not returned, an id will be
    /// automatically assigned, if possible, by the storage backend, which uses
    /// the [`Key`](crate::key::Key) trait to assign ids.
    pub fn push_serialized<C: SerializedCollection>(contents: &C::Contents) -> Result<Self, Error> {
        let id = C::natural_id(contents);
        let id = id.map(DocumentId::new).transpose()?;
        let contents = C::serialize(contents)?;
        Ok(Self::insert(C::collection_name(), id, contents))
    }

    /// Updates a document in `collection`.
    pub fn update(collection: CollectionName, header: Header, contents: impl Into<Bytes>) -> Self {
        Self {
            collection,
            command: Command::Update {
                header,
                contents: contents.into(),
            },
        }
    }

    /// Updates a document with the serialized representation of `contents` in
    /// `collection`.
    pub fn update_serialized<C: SerializedCollection>(
        header: CollectionHeader<C::PrimaryKey>,
        contents: &C::Contents,
    ) -> Result<Self, Error> {
        let contents = C::serialize(contents)?;
        Ok(Self::update(
            C::collection_name(),
            Header::try_from(header)?,
            contents,
        ))
    }

    /// Overwrites a document in `collection`. If a document with `id` exists,
    /// it will be overwritten. If a document with `id` doesn't exist, it will
    /// be created.
    pub fn overwrite(
        collection: CollectionName,
        id: DocumentId,
        contents: impl Into<Bytes>,
    ) -> Self {
        Self {
            collection,
            command: Command::Overwrite {
                id,
                contents: contents.into(),
            },
        }
    }

    /// Overwrites a document with the serialized representation of `contents`
    /// in `collection`. If a document with `id` exists, it will be overwritten.
    /// If a document with `id` doesn't exist, it will be created.
    pub fn overwrite_serialized<C: SerializedCollection>(
        id: C::PrimaryKey,
        contents: &C::Contents,
    ) -> Result<Self, Error> {
        let contents = C::serialize(contents)?;
        Ok(Self::overwrite(
            C::collection_name(),
            DocumentId::new(id)?,
            contents,
        ))
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
        id: Option<DocumentId>,
        /// The initial contents of the document.
        contents: Bytes,
    },

    /// Update an existing `Document` identified by `header`. `header.revision` must match
    /// the currently stored revision on the `Document`. If it does not, the
    /// command fill fail with a `DocumentConflict` error.
    Update {
        /// The header of the `Document`. The revision must match the current
        /// document.
        header: Header,

        /// The new contents to store within the `Document`.
        contents: Bytes,
    },

    /// Overwrite an existing `Document` identified by `id`. The revision will
    /// not be checked before the document is updated. If the document does not
    /// exist, it will be created.
    Overwrite {
        /// The id of the document to overwrite.
        id: DocumentId,

        /// The new contents to store within the `Document`.
        contents: Bytes,
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
        id: DocumentId,
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
    Documents(DocumentChanges),
    /// A list of changed keys.
    Keys(Vec<ChangedKey>),
}

impl Changes {
    /// Returns the list of documents changed in this transaction, or None if
    /// the transaction was not a document transaction.
    #[must_use]
    pub const fn documents(&self) -> Option<&DocumentChanges> {
        if let Self::Documents(changes) = self {
            Some(changes)
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

/// A list of changed documents.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocumentChanges {
    /// All of the collections changed.
    pub collections: Vec<CollectionName>,
    /// The individual document changes.
    pub documents: Vec<ChangedDocument>,
}

impl DocumentChanges {
    /// Returns the changed document and the name of the collection the change
    /// happened to.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<(&CollectionName, &ChangedDocument)> {
        self.documents.get(index).and_then(|doc| {
            self.collections
                .get(usize::from(doc.collection))
                .map(|collection| (collection, doc))
        })
    }

    /// Returns the number of changes in this collection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// Returns true if there are no changes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// Returns an interator over all of the changed documents.
    pub const fn iter(&self) -> DocumentChangesIter<'_> {
        DocumentChangesIter {
            changes: self,
            index: Some(0),
        }
    }
}

/// An iterator over [`DocumentChanges`].
#[must_use]
pub struct DocumentChangesIter<'a> {
    changes: &'a DocumentChanges,
    index: Option<usize>,
}

impl<'a> Iterator for DocumentChangesIter<'a> {
    type Item = (&'a CollectionName, &'a ChangedDocument);

    fn next(&mut self) -> Option<Self::Item> {
        self.index.and_then(|index| {
            let result = self.changes.get(index);
            if result.is_some() {
                self.index = index.checked_add(1);
            }
            result
        })
    }
}

/// A draining iterator over [`ChangedDocument`]s.
#[must_use]
pub struct DocumentChangesIntoIter {
    collections: Vec<CollectionName>,
    documents: std::vec::IntoIter<ChangedDocument>,
}

impl Iterator for DocumentChangesIntoIter {
    type Item = (CollectionName, ChangedDocument);

    fn next(&mut self) -> Option<Self::Item> {
        self.documents.next().and_then(|doc| {
            self.collections
                .get(usize::from(doc.collection))
                .map(|collection| (collection.clone(), doc))
        })
    }
}

impl IntoIterator for DocumentChanges {
    type Item = (CollectionName, ChangedDocument);

    type IntoIter = DocumentChangesIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        DocumentChangesIntoIter {
            collections: self.collections,
            documents: self.documents.into_iter(),
        }
    }
}

#[test]
fn document_changes_iter() {
    let changes = DocumentChanges {
        collections: vec![CollectionName::private("a"), CollectionName::private("b")],
        documents: vec![
            ChangedDocument {
                collection: 0,
                id: DocumentId::from_u64(0),
                deleted: false,
            },
            ChangedDocument {
                collection: 0,
                id: DocumentId::from_u64(1),
                deleted: false,
            },
            ChangedDocument {
                collection: 1,
                id: DocumentId::from_u64(2),
                deleted: false,
            },
            ChangedDocument {
                collection: 2,
                id: DocumentId::from_u64(3),
                deleted: false,
            },
        ],
    };

    assert_eq!(changes.len(), 4);
    assert!(!changes.is_empty());

    let mut a_changes = 0;
    let mut b_changes = 0;
    let mut ids = Vec::new();
    for (collection, document) in changes.iter() {
        assert!(!ids.contains(&document.id));
        ids.push(document.id);
        match collection.name.as_ref() {
            "a" => a_changes += 1,
            "b" => b_changes += 1,
            _ => unreachable!("invalid collection name {collection}"),
        }
    }
    assert_eq!(a_changes, 2);
    assert_eq!(b_changes, 1);

    let mut a_changes = 0;
    let mut b_changes = 0;
    let mut ids = Vec::new();
    for (collection, document) in changes {
        assert!(!ids.contains(&document.id));
        ids.push(document.id);
        match collection.name.as_ref() {
            "a" => a_changes += 1,
            "b" => b_changes += 1,
            _ => unreachable!("invalid collection name {collection}"),
        }
    }
    assert_eq!(a_changes, 2);
    assert_eq!(b_changes, 1);
}

/// A record of a changed document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedDocument {
    /// The index of the `CollectionName` within the `collections` field of [`Changes::Documents`].
    pub collection: u16,

    /// The id of the changed `Document`.
    pub id: DocumentId,

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
