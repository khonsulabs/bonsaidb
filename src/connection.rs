use std::marker::PhantomData;

use serde::Serialize;
use uuid::Uuid;

use crate::schema::{self, Document};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error from storage {0}")]
    Storage(#[from] sled::Error),
    #[error("attempted to access a collection not registered with this schema")]
    CollectionNotFound,
}

pub trait Connection: Send + Sync {
    fn collection<C: schema::Collection + 'static>(&self) -> Result<Collection<'_, Self, C>, Error>
    where
        Self: Sized;
}

pub struct Collection<'a, Cn, Cl> {
    connection: &'a Cn,
    collection: &'a dyn schema::Collection,
    _phantom: PhantomData<Cl>, // allows for extension traits to be written for collections of specific types
}

impl<'a, Cn, Cl> Collection<'a, Cn, Cl>
where
    Cn: Connection,
    Cl: schema::Collection,
{
    pub fn new(connection: &'a Cn, collection: &'a dyn schema::Collection) -> Self {
        Self {
            connection,
            collection,
            _phantom: PhantomData::default(),
        }
    }

    pub async fn push<S: Serialize + Sync>(&self, item: &S) -> Result<Document<Cl>, Error> {
        todo!()
    }

    pub async fn get(&self, id: &Uuid) -> Result<Option<Document<Cl>>, Error> {
        todo!()
    }
}
