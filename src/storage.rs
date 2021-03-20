use async_trait::async_trait;
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
    // views: Arc<Views>,
}

impl<DB> Storage<DB>
where
    DB: Database,
{
    pub fn open_local<P: AsRef<Path>>(path: P, schema: DB) -> Result<Self, sled::Error> {
        let mut collections = Collections::default();
        schema.define_collections(&mut collections);
        // let views = Views::default();
        // for collection in collections.collections.values() {
        //     // TODO Collect the views from the collections, which will allow us to expose storage.view::<Type>() directly without needing to navigate the Collection first
        // }

        sled::open(path).map(|sled| Self {
            sled,
            schema,
            collections: Arc::new(collections),
        })
    }
}

#[async_trait]
impl<DB> Connection for Storage<DB>
where
    DB: Database,
{
    fn collection<C: schema::Collection + Clone + 'static>(
        &self,
    ) -> Result<Collection<'_, Self, C>, Error>
    where
        Self: Sized,
    {
        match self.collections.get::<C>() {
            Some(collection) => Ok(Collection::new(self, collection)),
            None => Err(Error::CollectionNotFound),
        }
    }

    async fn save<C: schema::Collection>(&self, doc: &schema::Document<C>) -> Result<(), Error> {
        todo!()
    }
}
