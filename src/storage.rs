use async_trait::async_trait;
use std::{marker::PhantomData, path::Path, sync::Arc};

use crate::{
    connection::{Collection, Connection, Error},
    schema::{self, Database, Schema},
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
    ) -> Result<Collection<'a, Self, C>, Error>
    where
        Self: Sized,
    {
        if self.collections.contains::<C>() {
            Ok(Collection::new(self))
        } else {
            Err(Error::CollectionNotFound)
        }
    }

    async fn insert<C: schema::Collection>(
        &self,
        doc: &schema::Document<'a, C>,
    ) -> Result<(), Error> {
        // We need these things to occur:
        // * Create a "transaction" that contains the save statement.
        // * Execute the transaction
        //   * The transaction will get its own sequential ID, and be stored in
        //     its own tree -- this is the primary mechanism of replication.
        //   * Transactions are database-wide, not specific to a collection.
        //     This particular method only operates on a single collection, but
        //     in the future APIs that support creating transactions across
        //     collections should be supported.
        //   * Transactions need to have a record of the document ids that were
        //     modified. Read-replicas will be synchronizing these transaction
        //     records and can create a list of documents they need to
        //     synchronize.
        todo!()
    }

    async fn update<C: schema::Collection>(
        &self,
        doc: &mut schema::Document<'a, C>,
    ) -> Result<(), Error> {
        todo!()
    }
}
