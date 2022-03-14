use std::ops::Deref;

use async_trait::async_trait;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    connection::{AccessPolicy, AsyncLowLevelDatabase, QueryKey, Range, Sort},
    document::{AnyDocumentId, DocumentId, OwnedDocument},
    keyvalue::AsyncKeyValue,
    permissions::Permissions,
    pubsub::AsyncPubSub,
    schema::{
        self,
        view::map::{MappedDocuments, MappedSerializedValue},
        CollectionName, Map, MappedValue, SerializedView, ViewName,
    },
    transaction::Transaction,
};
use bonsaidb_local::AsyncDatabase;
use derive_where::derive_where;

use crate::{Backend, CustomServer, NoBackend};

/// A database belonging to a [`CustomServer`].
#[derive_where(Debug)]
pub struct ServerDatabase<B: Backend = NoBackend> {
    pub(crate) server: CustomServer<B>,
    pub(crate) db: AsyncDatabase,
}

impl<B: Backend> ServerDatabase<B> {
    #[must_use]
    pub fn with_effective_permissions(&self, permissions: &Permissions) -> Self {
        Self {
            db: self.db.with_effective_permissions(permissions),
            server: self.server.clone(),
        }
    }
}

impl<B: Backend> Deref for ServerDatabase<B> {
    type Target = AsyncDatabase;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

/// Uses `CustomServer`'s `PubSub` relay.
#[async_trait]
impl<B: Backend> AsyncPubSub for ServerDatabase<B> {
    type Subscriber = bonsaidb_local::Subscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        let subscriber = self.db.create_subscriber().await?;
        Ok(subscriber)
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.publish(topic, payload).await
    }

    async fn publish_bytes<S: Into<String> + Send>(
        &self,
        topic: S,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.publish_bytes(topic, payload).await
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.publish_to_all(topics, payload).await
    }

    async fn publish_bytes_to_all(
        &self,
        topics: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.publish_bytes_to_all(topics, payload).await
    }
}

/// Pass-through implementation
#[async_trait]
impl<B: Backend> bonsaidb_core::connection::AsyncConnection for ServerDatabase<B> {
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
        limit: Option<usize>,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.db.list::<C, R, PrimaryKey>(ids, order, limit).await
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
        limit: Option<usize>,
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
        limit: Option<usize>,
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
        result_limit: Option<usize>,
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
impl<B: Backend> AsyncKeyValue for ServerDatabase<B> {
    async fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        self.db.execute_key_operation(op).await
    }
}

#[async_trait]
impl<B: Backend> AsyncLowLevelDatabase for ServerDatabase<B> {
    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        self.db.get_from_collection(id, collection).await
    }

    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<usize>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.db
            .list_from_collection(ids, order, limit, collection)
            .await
    }

    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.db.count_from_collection(ids, collection).await
    }

    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.db.get_multiple_from_collection(ids, collection).await
    }

    async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.compact_collection_by_name(collection).await
    }

    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        self.db
            .query_by_name(view, key, order, limit, access_policy)
            .await
    }

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        self.db
            .query_by_name_with_docs(view, key, order, limit, access_policy)
            .await
    }

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        self.db.reduce_by_name(view, key, access_policy).await
    }

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        self.db
            .reduce_grouped_by_name(view, key, access_policy)
            .await
    }

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.db.delete_docs_by_name(view, key, access_policy).await
    }
}
