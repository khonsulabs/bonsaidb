use std::marker::PhantomData;

use async_trait::async_trait;
use pliantdb_local::core::{
    self,
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    schema::{Collection, Map, MappedDocument, MappedValue, Schema},
};

use crate::{error::ResultExt, Server};

#[cfg(feature = "pubsub")]
mod pubsub;
#[cfg(feature = "pubsub")]
pub use pubsub::*;

#[cfg(feature = "keyvalue")]
mod kv;

/// A database hosted on a server.
pub struct Database<'a, 'b, DB: Schema> {
    server: &'a Server,
    name: &'b str,
    _phantom: PhantomData<DB>,
}

impl<'a, 'b, DB> Database<'a, 'b, DB>
where
    DB: Schema,
{
    pub(crate) fn new(server: &'a Server, name: &'b str) -> Self {
        Self {
            server,
            name,
            _phantom: PhantomData::default(),
        }
    }
}

#[async_trait]
impl<'a, 'b, DB> Connection for Database<'a, 'b, DB>
where
    DB: Schema,
{
    async fn get<C: Collection>(&self, id: u64) -> Result<Option<Document<'static>>, core::Error> {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.get::<C>(id).await
    }

    async fn get_multiple<C: Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<Document<'static>>, core::Error> {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.get_multiple::<C>(ids).await
    }

    async fn query<V: core::schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, core::Error>
    where
        Self: Sized,
    {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.query::<V>(key, access_policy).await
    }

    async fn query_with_docs<V: core::schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedDocument<V::Key, V::Value>>, core::Error>
    where
        Self: Sized,
    {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.query_with_docs::<V>(key, access_policy).await
    }

    async fn reduce<V: core::schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, core::Error>
    where
        Self: Sized,
    {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.reduce::<V>(key, access_policy).await
    }

    async fn reduce_grouped<V: core::schema::View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, core::Error>
    where
        Self: Sized,
    {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.reduce_grouped::<V>(key, access_policy).await
    }

    async fn apply_transaction(
        &self,
        transaction: core::transaction::Transaction<'static>,
    ) -> Result<Vec<core::transaction::OperationResult>, core::Error> {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.apply_transaction(transaction).await
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<core::transaction::Executed<'static>>, core::Error> {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.list_executed_transactions(starting_id, result_limit)
            .await
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, core::Error> {
        let db = self
            .server
            .open_database::<DB>(self.name)
            .await
            .map_err_to_core()?;
        db.last_transaction_id().await
    }
}
