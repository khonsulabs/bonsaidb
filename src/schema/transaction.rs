use std::borrow::Cow;

use uuid::Uuid;

use super::{collection, Revision};
use serde::{Deserialize, Serialize};

/// a list of operations to execute as a single unit. If any operation fails,
/// all changes are aborted. Reads that happen while the transaction is in
/// progress will return old data and not block.
#[derive(Default, Debug)]
pub struct Transaction<'a> {
    operations: Vec<Operation<'a>>,
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
    /// update an existing `Document` identified by `id`. `revision` must match the currently stored revision on the `Document`. If it does not, the command fill fail with a `Conflict` error.
    // TODO once we have a conflict error, update the docs
    Update {
        /// the id of the `Document`
        id: Uuid,

        /// the current `Revision` of the `Document`
        #[serde(borrow)]
        revision: Cow<'a, Revision>,

        /// the new contents to store within the `Document`
        #[serde(borrow)]
        contents: Cow<'a, [u8]>,
    },
}

/// details about an executed transaction
#[derive(Debug, Serialize, Deserialize)]
pub struct Executed<'a> {
    /// the id of the transaction
    pub id: usize,

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
