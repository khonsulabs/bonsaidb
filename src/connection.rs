use std::marker::PhantomData;

use async_trait::async_trait;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    document::{Document, Header},
    schema::{self},
    transaction::{OperationResult, Transaction},
    Error,
};

/// a trait that defines all interactions with a `Database`, regardless of whether it is local or remote
#[async_trait]
pub trait Connection<'a>: Send + Sync {
    /// access a collection for the connected `Database`
    fn collection<C: schema::Collection + 'static>(
        &'a self,
    ) -> Result<Collection<'a, Self, C>, Error>
    where
        Self: Sized;

    /// insert a newly created document into the connected `Database` for the collection `C`
    async fn insert<C: schema::Collection>(&self, contents: Vec<u8>) -> Result<Header, Error>;

    /// update an existing document in the connected `Database` for the
    /// collection `C`. Upon success, `doc.revision` will be updated with the
    /// new revision.
    async fn update(&self, doc: &mut Document<'_>) -> Result<(), Error>;

    /// retrieve a stored document from collection `C` identified by `id`
    async fn get<C: schema::Collection>(
        &self,
        id: Uuid,
    ) -> Result<Option<Document<'static>>, Error>;

    /// apply a transaction to the database. If any operation in the transaction
    /// fails, none of the operations will be applied to the database.
    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, Error>;
}

/// a struct used to interact with a collection over a `Connection`
pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection<'a>,
    Cl: schema::Collection,
{
    pub(crate) fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    /// add a new `Document<Cl>` with the contents `item`
    pub async fn push<S: Serialize + Sync>(&self, item: &S) -> Result<Header, crate::Error> {
        let contents = serde_cbor::to_vec(item)?;
        Ok(self.connection.insert::<Cl>(contents).await?)
    }

    /// add a new `Document<Cl>` with the contents `item`
    pub async fn update(&self, doc: &mut Document<'_>) -> Result<(), crate::Error> {
        Ok(self.connection.update(doc).await?)
    }

    /// retrieve a `Document<Cl>` with `id` from the connection
    pub async fn get(&self, id: Uuid) -> Result<Option<Document<'static>>, Error> {
        self.connection.get::<Cl>(id).await
    }
}
