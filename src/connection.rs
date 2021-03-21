use std::marker::PhantomData;

use serde::Serialize;
use uuid::Uuid;

use crate::schema::{self, Document};
use async_trait::async_trait;

/// an enumeration of errors that are `Connection`-related
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// an error occurred interacting with the file-storage layer
    #[error("error from storage {0}")]
    Storage(#[from] sled::Error),

    /// an attempt to use a `Collection` with a `Database` that it wasn't defined within
    #[error("attempted to access a collection not registered with this schema")]
    CollectionNotFound,
}

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
    async fn insert<C: schema::Collection>(&self, doc: &Document<'a, C>) -> Result<(), Error>;

    /// update an existing document in the connected `Database` for the
    /// collection `C`. Upon success, `doc.revision` will be updated with the
    /// new revision.
    async fn update<C: schema::Collection>(&self, doc: &mut Document<'a, C>) -> Result<(), Error>;
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
    pub async fn push<S: Serialize + Sync>(
        &self,
        item: &S,
    ) -> Result<Document<'static, Cl>, crate::Error> {
        let doc = Document::new(item)?;
        self.connection.insert(&doc).await?;
        Ok(doc)
    }

    /// retrieve a `Document<Cl>` with `id` from the connection
    pub async fn get(&self, id: &Uuid) -> Result<Option<Document<'_, Cl>>, Error> {
        todo!()
    }
}
