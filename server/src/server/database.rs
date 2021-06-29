use std::ops::Deref;
#[cfg(feature = "pubsub")]
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "keyvalue")]
use pliantdb_core::kv::Kv;
#[cfg(feature = "pubsub")]
use pliantdb_core::{circulate::Message, pubsub::PubSub, pubsub::Subscriber};
use pliantdb_core::{
    connection::{AccessPolicy, QueryKey},
    schema::{self, Collection, Map, MappedValue, Schema, View},
    transaction::Transaction,
};
use pliantdb_local::Database;

use crate::{Backend, CustomServer};

/// A database belonging to a [`CustomServer`].
#[allow(clippy::module_name_repetitions)]
pub struct ServerDatabase<'a, B: Backend, DB: Schema> {
    #[cfg_attr(not(feature = "pubsub"), allow(dead_code))]
    pub(crate) server: &'a CustomServer<B>,
    pub(crate) db: Database<DB>,
}

impl<'a, B: Backend, DB: Schema> Deref for ServerDatabase<'a, B, DB> {
    type Target = Database<DB>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

/// Uses `CustomServer`'s `PubSub` relay.
#[cfg(feature = "pubsub")]
#[async_trait]
impl<'a, B: Backend, DB: Schema> PubSub for ServerDatabase<'a, B, DB> {
    type Subscriber = ServerSubscriber<B>;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, pliantdb_core::Error> {
        Ok(self
            .server
            .create_subscriber(self.db.name().to_string())
            .await)
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        self.server
            .publish_message(self.db.name(), &topic.into(), serde_cbor::to_vec(payload)?)
            .await;
        Ok(())
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), pliantdb_core::Error> {
        self.server
            .publish_serialized_to_all(self.db.name(), &topics, serde_cbor::to_vec(payload)?)
            .await;
        Ok(())
    }
}

/// A `PubSub` subscriber for a [`CustomServer`].
#[cfg(feature = "pubsub")]
pub struct ServerSubscriber<B: Backend> {
    /// The unique ID of this subscriber.
    pub id: u64,
    pub(crate) database: String,
    pub(crate) server: CustomServer<B>,
    pub(crate) receiver: flume::Receiver<Arc<Message>>,
}

#[cfg(feature = "pubsub")]
#[async_trait]
impl<B: Backend> Subscriber for ServerSubscriber<B> {
    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        topic: S,
    ) -> Result<(), pliantdb_core::Error> {
        self.server
            .subscribe_to(self.id, &self.database, topic)
            .await
    }

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), pliantdb_core::Error> {
        self.server
            .unsubscribe_from(self.id, &self.database, topic)
            .await
    }

    fn receiver(&self) -> &'_ flume::Receiver<Arc<Message>> {
        &self.receiver
    }
}

/// Pass-through implementation
#[async_trait]
impl<'a, B: Backend, DB: Schema> pliantdb_core::connection::Connection
    for ServerDatabase<'a, B, DB>
{
    async fn get<C: Collection>(
        &self,
        id: u64,
    ) -> Result<Option<pliantdb_core::document::Document<'static>>, pliantdb_core::Error> {
        self.db.get::<C>(id).await
    }

    async fn get_multiple<C: Collection>(
        &self,
        ids: &[u64],
    ) -> Result<Vec<pliantdb_core::document::Document<'static>>, pliantdb_core::Error> {
        self.db.get_multiple::<C>(ids).await
    }

    async fn query<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        self.db.query::<V>(key, access_policy).await
    }

    async fn query_with_docs<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::MappedDocument<V::Key, V::Value>>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        self.db.query_with_docs::<V>(key, access_policy).await
    }

    async fn reduce<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, pliantdb_core::Error>
    where
        Self: Sized,
    {
        self.db.reduce::<V>(key, access_policy).await
    }

    async fn reduce_grouped<V: View>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, pliantdb_core::Error>
    where
        Self: Sized,
    {
        self.db.reduce_grouped::<V>(key, access_policy).await
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<pliantdb_core::transaction::OperationResult>, pliantdb_core::Error> {
        self.db.apply_transaction(transaction).await
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<pliantdb_core::transaction::Executed<'static>>, pliantdb_core::Error> {
        self.db
            .list_executed_transactions(starting_id, result_limit)
            .await
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error> {
        self.db.last_transaction_id().await
    }
}

/// Pass-through implementation
#[cfg(feature = "keyvalue")]
#[async_trait]
impl<'a, B: Backend, DB: Schema> Kv for ServerDatabase<'a, B, DB> {
    async fn execute_key_operation(
        &self,
        op: pliantdb_core::kv::KeyOperation,
    ) -> Result<pliantdb_core::kv::Output, pliantdb_core::Error> {
        self.db.execute_key_operation(op).await
    }
}
