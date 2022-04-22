use std::ops::Deref;

use async_trait::async_trait;
use bonsaidb_core::{
    connection::{
        AccessPolicy, AsyncLowLevelConnection, HasSchema, HasSession, Range, SerializedQueryKey,
        Sort,
    },
    document::{DocumentId, Header, OwnedDocument},
    keyvalue::AsyncKeyValue,
    permissions::Permissions,
    pubsub::AsyncPubSub,
    schema::{self, view::map::MappedSerializedValue, CollectionName, Schematic, ViewName},
    transaction::{OperationResult, Transaction},
};
use bonsaidb_local::{AsyncDatabase, Database};
use derive_where::derive_where;

use crate::{Backend, CustomServer, NoBackend};

/// A database belonging to a [`CustomServer`].
#[derive_where(Debug, Clone)]
pub struct ServerDatabase<B: Backend = NoBackend> {
    pub(crate) server: CustomServer<B>,
    pub(crate) db: AsyncDatabase,
}

impl<B: Backend> From<ServerDatabase<B>> for Database {
    fn from(server: ServerDatabase<B>) -> Self {
        Self::from(server.db)
    }
}

impl<'a, B: Backend> From<&'a ServerDatabase<B>> for Database {
    fn from(server: &'a ServerDatabase<B>) -> Self {
        Self::from(server.db.clone())
    }
}

impl<B: Backend> ServerDatabase<B> {
    /// Restricts an unauthenticated instance to having `effective_permissions`.
    /// Returns `None` if a session has already been established.
    #[must_use]
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Option<Self> {
        self.db
            .with_effective_permissions(effective_permissions)
            .map(|db| Self {
                db,
                server: self.server.clone(),
            })
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

    async fn publish_bytes(
        &self,
        topic: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.publish_bytes(topic, payload).await
    }

    async fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send + 'async_trait,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.db.publish_bytes_to_all(topics, payload).await
    }
}

impl<B: Backend> HasSession for ServerDatabase<B> {
    fn session(&self) -> Option<&bonsaidb_core::connection::Session> {
        self.server.session()
    }
}

/// Pass-through implementation
#[async_trait]
impl<B: Backend> bonsaidb_core::connection::AsyncConnection for ServerDatabase<B> {
    type Storage = CustomServer<B>;

    fn storage(&self) -> Self::Storage {
        self.server.clone()
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
impl<B: Backend> AsyncKeyValue for ServerDatabase<B> {
    async fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        self.db.execute_key_operation(op).await
    }
}

#[async_trait]
impl<B: Backend> AsyncLowLevelConnection for ServerDatabase<B> {
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
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.db
            .list_from_collection(ids, order, limit, collection)
            .await
    }

    async fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        self.db
            .list_headers_from_collection(ids, order, limit, collection)
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
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        self.db
            .query_by_name(view, key, order, limit, access_policy)
            .await
    }

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        self.db
            .query_by_name_with_docs(view, key, order, limit, access_policy)
            .await
    }

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        self.db.reduce_by_name(view, key, access_policy).await
    }

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        self.db
            .reduce_grouped_by_name(view, key, access_policy)
            .await
    }

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.db.delete_docs_by_name(view, key, access_policy).await
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        self.db.apply_transaction(transaction).await
    }
}

impl<B: Backend> HasSchema for ServerDatabase<B> {
    fn schematic(&self) -> &Schematic {
        self.db.schematic()
    }
}
