use std::{borrow::Cow, collections::HashMap, marker::PhantomData, path::Path, sync::Arc};

use async_trait::async_trait;
use sled::{
    transaction::{ConflictableTransactionError, TransactionError, TransactionalTree},
    Transactional,
};
use uuid::Uuid;

use crate::{
    connection::{Collection, Connection},
    document::{Document, Header},
    schema::{self, collection, Database, Schema},
    transaction::{self, ChangedDocument, Command, Operation, OperationResult, Transaction},
};

/// a local, file-based database
#[derive(Clone)]
pub struct Storage<DB> {
    sled: sled::Db,
    collections: Arc<Schema>,
    _schema: PhantomData<DB>,
}

impl<DB> Storage<DB>
where
    DB: Database,
{
    /// opens a local file as a pliantdb
    pub fn open_local<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let mut collections = Schema::default();
        DB::define_collections(&mut collections);

        sled::open(path).map(|sled| Self {
            sled,
            collections: Arc::new(collections),
            _schema: PhantomData::default(),
        })
    }
}

#[async_trait]
impl<'a, DB> Connection<'a> for Storage<DB>
where
    DB: Database,
{
    fn collection<C: schema::Collection + 'static>(
        &'a self,
    ) -> Result<Collection<'a, Self, C>, crate::Error>
    where
        Self: Sized,
    {
        if self.collections.contains::<C>() {
            Ok(Collection::new(self))
        } else {
            Err(crate::Error::CollectionNotFound)
        }
    }

    async fn insert<C: schema::Collection>(
        &self,
        contents: Vec<u8>,
    ) -> Result<Header, crate::Error> {
        // We need these things to occur:
        // * [x] Create a "transaction" that contains the save statement.
        // * [ ] Execute the transaction
        //   * [ ] The transaction will get its own sequential ID, and be stored in
        //     its own tree -- this is the primary mechanism of replication.
        //   * [x] Transactions are database-wide, not specific to a collection.
        //     This particular method only operates on a single collection, but
        //     in the future APIs that support creating transactions across
        //     collections should be supported.
        //   * [x] Transactions need to have a record of the document ids that were
        //     modified. Read-replicas will be synchronizing these transaction
        //     records and can create a list of documents they need to
        //     synchronize.
        //  * [ ] return the newly created Header
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: C::id(),
            command: Command::Insert {
                contents: Cow::from(contents),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            Ok(header.clone())
        } else {
            unreachable!(
                "apply_transaction on a single insert should yield a single DocumentUpdated entry"
            )
        }
    }

    async fn update(&self, doc: &mut Document<'_>) -> Result<(), crate::Error> {
        let mut tx = Transaction::default();
        tx.push(Operation {
            collection: doc.collection.clone(),
            command: Command::Update {
                header: Cow::Owned(doc.header.as_ref().clone()),
                contents: Cow::Owned(doc.contents.to_vec()),
            },
        });
        let results = self.apply_transaction(tx).await?;
        if let OperationResult::DocumentUpdated { header, .. } = &results[0] {
            doc.header = Cow::Owned(header.clone());
            Ok(())
        } else {
            unreachable!(
                "apply_transaction on a single update should yield a single DocumentUpdated entry"
            )
        }
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, crate::Error> {
        let sled = self.sled.clone();
        tokio::task::spawn_blocking(move || {
            let mut open_trees = OpenTrees::default();
            open_trees.open_tree(&sled, TRANSACTION_TREE_NAME)?;
            for op in &transaction.operations {
                match &op.command {
                    Command::Update { .. } | Command::Insert { .. } => {
                        open_trees.open_trees_for_document_change(&sled, &op.collection)?;
                    }
                }
            }

            match open_trees.trees.transaction(|trees| {
                let mut results = Vec::new();
                let mut changed_documents = Vec::new();
                for op in &transaction.operations {
                    let result = execute_operation(op, trees, &open_trees.trees_index_by_name)?;

                    if let OperationResult::DocumentUpdated { header, collection } = &result {
                        changed_documents.push(ChangedDocument {
                            collection: collection.clone(),
                            id: header.id,
                        });
                    }

                    results.push(result);
                }

                // Save a record of the transaction we just completed.
                let tree = &trees[open_trees.trees_index_by_name[TRANSACTION_TREE_NAME]];
                let executed = transaction::Executed {
                    id: tree.generate_id()?,
                    changed_documents: Cow::from(changed_documents),
                };
                let serialized: Vec<u8> = bincode::serialize(&executed)
                    .map_err(|err| ConflictableTransactionError::Abort(crate::Error::from(err)))?;
                tree.insert(&executed.id.to_be_bytes(), serialized)?;

                trees.iter().for_each(TransactionalTree::flush);
                Ok(results)
            }) {
                Ok(results) => Ok(results),
                Err(err) => match err {
                    TransactionError::Abort(err) => Err(err),
                    TransactionError::Storage(err) => Err(crate::Error::from(err)),
                },
            }
        })
        .await
        .unwrap()
    }

    async fn get<C: schema::Collection>(
        &self,
        id: Uuid,
    ) -> Result<Option<Document<'static>>, crate::Error> {
        let tree = self.sled.open_tree(document_tree_name(&C::id()))?;
        if let Some(vec) = tree.get(id.as_bytes())? {
            Ok(Some(bincode::deserialize::<Document<'_>>(&vec)?.to_owned()))
        } else {
            Ok(None)
        }
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u64>,
    ) -> Result<Vec<transaction::Executed<'static>>, crate::Error> {
        let result_limit = result_limit.unwrap_or(100).max(1000); // TODO what is a good max result set?
        if result_limit > 0 {
            let tree = self.sled.open_tree(TRANSACTION_TREE_NAME)?;
            let iter = if let Some(starting_id) = starting_id {
                tree.range(starting_id.to_be_bytes()..=u64::MAX.to_be_bytes())
            } else {
                tree.iter()
            };

            #[allow(clippy::cast_possible_truncation)] // this value is limited above
            let mut results = Vec::with_capacity(result_limit as usize);
            for row in iter {
                let (_, vec) = row?;
                results.push(bincode::deserialize::<transaction::Executed<'_>>(&vec)?.to_owned());

                if results.len() as u64 >= result_limit {
                    break;
                }
            }
            Ok(results)
        } else {
            // A request was made to return an empty result? This should probably be
            // an error, but technically this is a correct response.
            Ok(Vec::default())
        }
    }
}

fn execute_operation(
    operation: &Operation<'_>,
    trees: &[TransactionalTree],
    tree_index_map: &HashMap<String, usize>,
) -> Result<OperationResult, ConflictableTransactionError<crate::Error>> {
    let tree = &trees[tree_index_map[&document_tree_name(&operation.collection)]];
    match &operation.command {
        Command::Insert { contents } => {
            let doc = Document::new(Cow::Borrowed(contents), operation.collection.clone());
            save_doc(tree, &doc)?;
            let serialized: Vec<u8> = bincode::serialize(&doc)
                .map_err(|err| ConflictableTransactionError::Abort(crate::Error::from(err)))?;
            tree.insert(doc.header.id.as_bytes(), serialized)?;

            Ok(OperationResult::DocumentUpdated {
                collection: operation.collection.clone(),
                header: doc.header.as_ref().clone(),
            })
        }
        Command::Update { header, contents } => {
            if let Some(vec) = tree.get(&header.id.as_bytes())? {
                let doc = bincode::deserialize::<Document<'_>>(&vec)
                    .map_err(|err| ConflictableTransactionError::Abort(crate::Error::from(err)))?;
                if &doc.header == header {
                    if let Some(updated_doc) = doc.create_new_revision(contents.clone()) {
                        save_doc(tree, &updated_doc)?;
                        Ok(OperationResult::DocumentUpdated {
                            collection: operation.collection.clone(),
                            header: updated_doc.header.as_ref().clone(),
                        })
                    } else {
                        // If no new revision was made, it means an attempt to
                        // save a document with the same contents was made.
                        // We'll return a success but not actually give a new
                        // version
                        Ok(OperationResult::DocumentUpdated {
                            collection: operation.collection.clone(),
                            header: doc.header.as_ref().clone(),
                        })
                    }
                } else {
                    Err(ConflictableTransactionError::Abort(
                        crate::Error::DocumentConflict(operation.collection.clone(), header.id),
                    ))
                }
            } else {
                Err(ConflictableTransactionError::Abort(
                    crate::Error::DocumentNotFound(operation.collection.clone(), header.id),
                ))
            }
        }
    }
}

fn save_doc(
    tree: &TransactionalTree,
    doc: &Document<'_>,
) -> Result<(), ConflictableTransactionError<crate::Error>> {
    let serialized: Vec<u8> = bincode::serialize(doc)
        .map_err(|err| ConflictableTransactionError::Abort(crate::Error::from(err)))?;
    tree.insert(doc.header.id.as_bytes(), serialized)?;
    Ok(())
}

#[derive(Default)]
struct OpenTrees {
    trees: Vec<sled::Tree>,
    trees_index_by_name: HashMap<String, usize>,
}

impl OpenTrees {
    fn open_tree(&mut self, sled: &sled::Db, name: &str) -> Result<(), sled::Error> {
        #[allow(clippy::map_entry)] // unwrapping errors is much uglier using entry()
        if !self.trees_index_by_name.contains_key(name) {
            let tree = sled.open_tree(name.as_bytes())?;
            self.trees_index_by_name
                .insert(name.to_string(), self.trees.len());
            self.trees.push(tree);
        }

        Ok(())
    }

    fn open_trees_for_document_change(
        &mut self,
        sled: &sled::Db,
        collection: &collection::Id,
    ) -> Result<(), sled::Error> {
        self.open_tree(sled, &document_tree_name(collection))
    }
}

const TRANSACTION_TREE_NAME: &str = "transactions";

fn document_tree_name(collection: &collection::Id) -> String {
    collection.0.to_string()
}

/// errors that can occur from interacting with storage
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// an error occurred interacting with `sled`
    #[error("error from storage: {0}")]
    Sled(#[from] sled::Error),

    /// an error occurred serializing the underlying database structures
    #[error("error while serializing internal structures: {0}")]
    InternalSerialization(#[from] bincode::Error),

    /// an error occurred serializing the contents of a `Document` or results of a `View`
    #[error("error while serializing: {0}")]
    Serialization(#[from] serde_cbor::Error),
}
