use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use bonsaidb_core::{
    circulate::Message,
    connection::{AccessPolicy, QueryKey, Range, Sort},
    document::{AnyDocumentId, Header, OwnedDocument},
    keyvalue::KeyValue,
    pubsub::{PubSub, Subscriber},
    schema::{self, view::map::MappedDocuments, Map, MappedValue, SerializedView},
    transaction::Transaction,
};
use bonsaidb_local::Database;

use crate::{Backend, CustomServer, NoBackend};

/// A database belonging to a [`CustomServer`].
pub struct ServerDatabase<B: Backend = NoBackend> {
    pub(crate) server: CustomServer<B>,
    pub(crate) db: Database,
}

impl<B: Backend> Deref for ServerDatabase<B> {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

/// Uses `CustomServer`'s `PubSub` relay.
#[async_trait]
impl<B: Backend> PubSub for ServerDatabase<B> {
    type Subscriber = ServerSubscriber<B>;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        Ok(self
            .server
            .create_subscriber(self.db.name().to_string())
            .await)
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.server
            .publish_message(self.db.name(), &topic.into(), pot::to_vec(payload)?)
            .await;
        Ok(())
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.server
            .publish_serialized_to_all(self.db.name(), &topics, pot::to_vec(payload)?)
            .await;
        Ok(())
    }
}

/// A `PubSub` subscriber for a [`CustomServer`].
pub struct ServerSubscriber<B: Backend> {
    /// The unique ID of this subscriber.
    pub id: u64,
    pub(crate) database: String,
    pub(crate) server: CustomServer<B>,
    pub(crate) receiver: flume::Receiver<Arc<Message>>,
}

#[async_trait]
impl<B: Backend> Subscriber for ServerSubscriber<B> {
    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        topic: S,
    ) -> Result<(), bonsaidb_core::Error> {
        self.server
            .subscribe_to(self.id, &self.database, topic)
            .await
    }

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), bonsaidb_core::Error> {
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
impl<B: Backend> bonsaidb_core::connection::Connection for ServerDatabase<B> {
    async fn get<C, PrimaryKey>(
        &self,
        id: PrimaryKey,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.db.get::<C, PrimaryKey>(id).await
    }

    async fn get_multiple<C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send + Sync,
    {
        self.db.get_multiple::<C, _, _, _>(ids).await
    }

    async fn list<C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.db.list::<C, R, PrimaryKey>(ids, order, limit).await
    }

    async fn list_headers<C, R, PrimaryKey>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<u32>,
    ) -> Result<Vec<Header>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.db
            .list_headers::<C, R, PrimaryKey>(ids, order, limit)
            .await
    }

    async fn count<C, R, PrimaryKey>(&self, ids: R) -> Result<u64, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.db.count::<C, R, PrimaryKey>(ids).await
    }

    async fn query<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        self.db.query::<V>(key, order, limit, access_policy).await
    }

    async fn query_with_docs<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        self.db
            .query_with_docs::<V>(key, order, limit, access_policy)
            .await
    }

    async fn reduce<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        self.db.reduce::<V>(key, access_policy).await
    }

    async fn reduce_grouped<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        self.db.reduce_grouped::<V>(key, access_policy).await
    }

    async fn delete_docs<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        self.db.delete_docs::<V>(key, access_policy).await
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<bonsaidb_core::transaction::OperationResult>, bonsaidb_core::Error> {
        self.db.apply_transaction(transaction).await
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<bonsaidb_core::transaction::Executed>, bonsaidb_core::Error> {
        self.db
            .list_executed_transactions(starting_id, result_limit)
            .await
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        self.db.last_transaction_id().await
    }

    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        self.db.compact_collection::<C>().await
    }

    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.db.compact().await
    }

    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.db.compact_key_value_store().await
    }
}

/// Pass-through implementation
#[async_trait]
impl<B: Backend> KeyValue for ServerDatabase<B> {
    async fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        self.db.execute_key_operation(op).await
    }
}
