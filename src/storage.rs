use std::{path::Path, sync::Arc};

use crate::{
    connection::{Collection, Connection, Error},
    schema::{self, Collections, Database},
};

#[derive(Clone)]
pub struct Storage<DB> {
    sled: sled::Db,
    schema: DB,
    collections: Arc<Collections>,
}

impl<DB> Storage<DB>
where
    DB: Database,
{
    pub fn open_local<P: AsRef<Path>>(path: P, schema: DB) -> Result<Self, sled::Error> {
        let mut collections = Collections::default();
        schema.define_collections(&mut collections);

        sled::open(path).map(|sled| Self {
            sled,
            schema,
            collections: Arc::new(collections),
        })
    }
}

impl<DB> Connection for Storage<DB>
where
    DB: Database,
{
    fn collection<C: schema::Collection + 'static>(&self) -> Result<Collection<'_, Self, C>, Error>
    where
        Self: Sized,
    {
        match self.collections.get::<C>() {
            Some(collection) => Ok(Collection::new(self, collection)),
            None => Err(Error::CollectionNotFound),
        }
    }
}
