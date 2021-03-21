use std::marker::PhantomData;

use serde::Serialize;
use uuid::Uuid;

use crate::schema::{self, Document};
use async_trait::async_trait;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error from storage {0}")]
    Storage(#[from] sled::Error),
    #[error("attempted to access a collection not registered with this schema")]
    CollectionNotFound,
}

#[async_trait]
pub trait Connection: Send + Sync {
    fn collection<C: schema::Collection + 'static>(&self) -> Result<Collection<'_, Self, C>, Error>
    where
        Self: Sized;

    async fn save<C: schema::Collection>(&self, doc: &Document<C>) -> Result<(), Error>;
    async fn update<C: schema::Collection>(&self, doc: &mut Document<C>) -> Result<(), Error>;
}

pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection,
    Cl: schema::Collection,
{
    pub fn new(connection: &'a Cn) -> Self {
        Self {
            connection,
            _phantom: PhantomData::default(),
        }
    }

    pub async fn push<S: Serialize + Sync>(&self, item: &S) -> Result<Document<Cl>, crate::Error> {
        let doc = Document::new(item)?;
        self.connection.save(&doc).await?;
        Ok(doc)
    }

    pub async fn get(&self, id: &Uuid) -> Result<Option<Document<Cl>>, Error> {
        todo!()
    }
}
