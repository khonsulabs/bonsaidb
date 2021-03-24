use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;

use crate::{
    document::{Document, Header},
    schema::{self},
    transaction::{self, OperationResult, Transaction},
    Error,
};

/// Defines all interactions with a `Database`, regardless of whether it is local or remote.
#[async_trait]
pub trait Connection<'a>: Send + Sync {
    /// Accesses a collection for the connected `Database`.
    fn collection<C: schema::Collection + 'static>(
        &'a self,
    ) -> Result<Collection<'a, Self, C>, Error>
    where
        Self: Sized;

    /// Inserts a newly created document into the connected `Database` for the collection `C`.
    async fn insert<C: schema::Collection>(&self, contents: Vec<u8>) -> Result<Header, Error>;

    /// Updates an existing document in the connected `Database` for the
    /// collection `C`. Upon success, `doc.revision` will be updated with the
    /// new revision.
    async fn update(&self, doc: &mut Document<'_>) -> Result<(), Error>;

    /// Retrieves a stored document from collection `C` identified by `id`.
    async fn get<C: schema::Collection>(&self, id: u64)
        -> Result<Option<Document<'static>>, Error>;

    /// Applies a transaction to the database. If any operation in the transaction
    /// fails, none of the operations will be applied to the database.
    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, Error>;

    /// Lists executed transactions from this database. By default, a maximum of
    /// 1000 entries will be returned, but that limit can be overridden by
    /// setting `result_limit`. A hard limit of 100,000 results will be
    /// returned. To begin listing after another known `transaction_id`, pass
    /// `transaction_id + 1` into `starting_id`.
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<transaction::Executed<'static>>, Error>;
}

/// Interacts with a collection over a `Connection`.
pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection<'a>,
    Cl: schema::Collection,
{
    /// Creates a new instance using `connection`.
    pub fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    /// Adds a new `Document<Cl>` with the contents `item`.
    pub async fn push<S: Serialize + Sync>(&self, item: &S) -> Result<Header, crate::Error> {
        let contents = serde_cbor::to_vec(item)?;
        Ok(self.connection.insert::<Cl>(contents).await?)
    }

    /// Retrieves a `Document<Cl>` with `id` from the connection.
    pub async fn get(&self, id: u64) -> Result<Option<Document<'static>>, Error> {
        self.connection.get::<Cl>(id).await
    }
}
