use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{document::Header, schema::collection};

/// a list of operations to execute as a single unit. If any operation fails,
/// all changes are aborted. Reads that happen while the transaction is in
/// progress will return old data and not block.
#[derive(Default, Debug)]
pub struct Transaction<'a> {
    /// the operations in this transaction
    pub operations: Vec<Operation<'a>>,
}

impl<'a> Transaction<'a> {
    /// add an operation to the transaction
    pub fn push(&mut self, operation: Operation<'a>) {
        self.operations.push(operation);
    }
}

/// a single operation performed on a `Collection`
#[derive(Debug)]
pub struct Operation<'a> {
    /// the id of the `Collection`
    pub collection: collection::Id,
    /// the command being performed
    pub command: Command<'a>,
}

/// a command to execute within a `Collection`
#[derive(Debug, Serialize, Deserialize)]
pub enum Command<'a> {
    /// insert a new document containing `contents`
    Insert {
        /// the initial contents of the document
        #[serde(borrow)]
        contents: Cow<'a, [u8]>,
    },
    /// update an existing `Document` identified by `id`. `revision` must match the currently stored revision on the `Document`. If it does not, the command fill fail with a `DocumentConflict` error.
    Update {
        /// the current header of the `Document`
        header: Cow<'a, Header>,

        /// the new contents to store within the `Document`
        #[serde(borrow)]
        contents: Cow<'a, [u8]>,
    },
}

/// information about the result of each `Operation` in a transaction
#[derive(Debug, Serialize, Deserialize)]
pub enum OperationResult {
    /// an operation succeeded but had no information to output
    Success,

    /// a `Document` was updated
    DocumentUpdated {
        /// the id of the `Collection` of the updated `Document`
        collection: collection::Id,

        /// the header of the updated `Document`
        header: Header,
    },
}

/// details about an executed transaction
#[derive(Debug, Serialize, Deserialize)]
pub struct Executed<'a> {
    /// the id of the transaction
    pub id: u64,

    /// a list of containing ids of `Documents` changed
    #[serde(borrow)]
    pub changed_documents: Cow<'a, [ChangedDocument]>,
}

/// a record of a changed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedDocument {
    /// the id of the `Collection` of the changed `Document`
    pub collection: collection::Id,

    /// the id of the changed `Document`
    pub id: Uuid,
}
