use bonsaidb_core::{
    connection::{
        AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection, Connection,
        LowLevelConnection, StorageConnection,
    },
    keyvalue::{AsyncKeyValue, KeyValue},
    pubsub::{AsyncPubSub, AsyncSubscriber, PubSub, Subscriber},
    schema::Schematic,
};
use tokio::runtime::Handle;

use crate::{Client, RemoteDatabase, RemoteSubscriber};

impl Client {
    pub(crate) fn tokio(&self) -> &Handle {
        self.data.tokio.as_ref().unwrap()
    }
}

impl StorageConnection for Client {
    type Database = RemoteDatabase;
    type Authenticated = Self;

    fn session(&self) -> Option<&bonsaidb_core::connection::Session> {
        AsyncStorageConnection::session(self)
    }

    fn admin(&self) -> Self::Database {
        self.tokio()
            .block_on(async { AsyncStorageConnection::admin(self).await })
    }

    fn database<DB: bonsaidb_core::schema::Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::database::<DB>(self, name).await })
    }

    fn create_database_with_schema(
        &self,
        name: &str,
        schema: bonsaidb_core::schema::SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncStorageConnection::create_database_with_schema(self, name, schema, only_if_needed)
                .await
        })
    }

    fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::delete_database(self, name).await })
    }

    fn list_databases(
        &self,
    ) -> Result<Vec<bonsaidb_core::connection::Database>, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::list_databases(self).await })
    }

    fn list_available_schemas(
        &self,
    ) -> Result<Vec<bonsaidb_core::schema::SchemaName>, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::list_available_schemas(self).await })
    }

    fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::create_user(self, username).await })
    }

    fn delete_user<'user, U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::delete_user(self, user).await })
    }

    #[cfg(feature = "password-hashing")]
    fn set_user_password<'user, U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncStorageConnection::set_user_password(self, user, password).await
        })
    }

    #[cfg(feature = "password-hashing")]
    fn authenticate<'user, U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: bonsaidb_core::connection::Authentication,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncStorageConnection::authenticate(self, user, authentication).await
        })
    }

    fn assume_identity(
        &self,
        identity: bonsaidb_core::connection::Identity,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::assume_identity(self, identity).await })
    }

    fn add_permission_group_to_user<
        'user,
        'group,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        G: bonsaidb_core::schema::Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncStorageConnection::add_permission_group_to_user(self, user, permission_group).await
        })
    }

    fn remove_permission_group_from_user<
        'user,
        'group,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        G: bonsaidb_core::schema::Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncStorageConnection::remove_permission_group_from_user(self, user, permission_group)
                .await
        })
    }

    fn add_role_to_user<
        'user,
        'role,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        R: bonsaidb_core::schema::Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncStorageConnection::add_role_to_user(self, user, role).await })
    }

    fn remove_role_from_user<
        'user,
        'role,
        U: bonsaidb_core::schema::Nameable<'user, u64> + Send + Sync,
        R: bonsaidb_core::schema::Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncStorageConnection::remove_role_from_user(self, user, role).await
        })
    }
}

impl Connection for RemoteDatabase {
    type Storage = Client;

    fn storage(&self) -> Self::Storage {
        self.client.clone()
    }

    fn session(&self) -> Option<&bonsaidb_core::connection::Session> {
        Some(&self.session)
    }

    fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<bonsaidb_core::transaction::Executed>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncConnection::list_executed_transactions(self, starting_id, result_limit).await
        })
    }

    fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncConnection::last_transaction_id(self).await })
    }

    fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncConnection::compact(self).await })
    }

    fn compact_collection<C: bonsaidb_core::schema::Collection>(
        &self,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncConnection::compact_collection::<C>(self).await })
    }

    fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncConnection::compact_key_value_store(self).await })
    }
}

impl LowLevelConnection for RemoteDatabase {
    fn schematic(&self) -> &Schematic {
        &self.schema
    }

    fn get<C, PrimaryKey>(
        &self,
        id: PrimaryKey,
    ) -> Result<Option<bonsaidb_core::document::OwnedDocument>, bonsaidb_core::Error>
    where
        C: bonsaidb_core::schema::Collection,
        PrimaryKey: Into<bonsaidb_core::document::AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.tokio()
            .block_on(async { AsyncLowLevelConnection::get::<C, PrimaryKey>(self, id).await })
    }

    fn get_multiple<C, PrimaryKey, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<bonsaidb_core::document::OwnedDocument>, bonsaidb_core::Error>
    where
        C: bonsaidb_core::schema::Collection,
        DocumentIds: IntoIterator<Item = PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PrimaryKey> + Send + Sync,
        PrimaryKey: Into<bonsaidb_core::document::AnyDocumentId<C::PrimaryKey>> + Send + Sync,
    {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::get_multiple::<C, _, _, _>(self, ids).await
        })
    }

    fn list<C, R, PrimaryKey>(
        &self,
        ids: R,
        order: bonsaidb_core::connection::Sort,
        limit: Option<u32>,
    ) -> Result<Vec<bonsaidb_core::document::OwnedDocument>, bonsaidb_core::Error>
    where
        C: bonsaidb_core::schema::Collection,
        R: Into<bonsaidb_core::connection::Range<PrimaryKey>> + Send,
        PrimaryKey: Into<bonsaidb_core::document::AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::list::<C, _, _>(self, ids, order, limit).await
        })
    }

    fn count<C, R, PrimaryKey>(&self, ids: R) -> Result<u64, bonsaidb_core::Error>
    where
        C: bonsaidb_core::schema::Collection,
        R: Into<bonsaidb_core::connection::Range<PrimaryKey>> + Send,
        PrimaryKey: Into<bonsaidb_core::document::AnyDocumentId<C::PrimaryKey>> + Send,
    {
        self.tokio()
            .block_on(async { AsyncLowLevelConnection::count::<C, _, _>(self, ids).await })
    }

    fn query<V: bonsaidb_core::schema::SerializedView>(
        &self,
        key: Option<bonsaidb_core::connection::QueryKey<V::Key>>,
        order: bonsaidb_core::connection::Sort,
        limit: Option<u32>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::Map<V::Key, V::Value>>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::query::<V>(self, key, order, limit, access_policy).await
        })
    }

    fn query_with_docs<V: bonsaidb_core::schema::SerializedView>(
        &self,
        key: Option<bonsaidb_core::connection::QueryKey<V::Key>>,
        order: bonsaidb_core::connection::Sort,
        limit: Option<u32>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<
        bonsaidb_core::schema::view::map::MappedDocuments<
            bonsaidb_core::document::OwnedDocument,
            V,
        >,
        bonsaidb_core::Error,
    > {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::query_with_docs::<V>(self, key, order, limit, access_policy)
                .await
        })
    }

    fn reduce<V: bonsaidb_core::schema::SerializedView>(
        &self,
        key: Option<bonsaidb_core::connection::QueryKey<V::Key>>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<V::Value, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::reduce::<V>(self, key, access_policy).await
        })
    }

    fn reduce_grouped<V: bonsaidb_core::schema::SerializedView>(
        &self,
        key: Option<bonsaidb_core::connection::QueryKey<V::Key>>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
    {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::reduce_grouped::<V>(self, key, access_policy).await
        })
    }

    fn delete_docs<V: bonsaidb_core::schema::SerializedView>(
        &self,
        key: Option<bonsaidb_core::connection::QueryKey<V::Key>>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::delete_docs::<V>(self, key, access_policy).await
        })
    }

    fn apply_transaction(
        &self,
        transaction: bonsaidb_core::transaction::Transaction,
    ) -> Result<Vec<bonsaidb_core::transaction::OperationResult>, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncLowLevelConnection::apply_transaction(self, transaction).await })
    }

    fn get_from_collection(
        &self,
        id: bonsaidb_core::document::DocumentId,
        collection: &bonsaidb_core::schema::CollectionName,
    ) -> Result<Option<bonsaidb_core::document::OwnedDocument>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::get_from_collection(self, id, collection).await
        })
    }

    fn list_from_collection(
        &self,
        ids: bonsaidb_core::connection::Range<bonsaidb_core::document::DocumentId>,
        order: bonsaidb_core::connection::Sort,
        limit: Option<u32>,
        collection: &bonsaidb_core::schema::CollectionName,
    ) -> Result<Vec<bonsaidb_core::document::OwnedDocument>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::list_from_collection(self, ids, order, limit, collection).await
        })
    }

    fn count_from_collection(
        &self,
        ids: bonsaidb_core::connection::Range<bonsaidb_core::document::DocumentId>,
        collection: &bonsaidb_core::schema::CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::count_from_collection(self, ids, collection).await
        })
    }

    fn get_multiple_from_collection(
        &self,
        ids: &[bonsaidb_core::document::DocumentId],
        collection: &bonsaidb_core::schema::CollectionName,
    ) -> Result<Vec<bonsaidb_core::document::OwnedDocument>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::get_multiple_from_collection(self, ids, collection).await
        })
    }

    fn compact_collection_by_name(
        &self,
        collection: bonsaidb_core::schema::CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::compact_collection_by_name(self, collection).await
        })
    }

    fn query_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<bonsaidb_core::connection::QueryKey<bonsaidb_core::arc_bytes::serde::Bytes>>,
        order: bonsaidb_core::connection::Sort,
        limit: Option<u32>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::view::map::Serialized>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::query_by_name(self, view, key, order, limit, access_policy)
                .await
        })
    }

    fn query_by_name_with_docs(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<bonsaidb_core::connection::QueryKey<bonsaidb_core::arc_bytes::serde::Bytes>>,
        order: bonsaidb_core::connection::Sort,
        limit: Option<u32>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<bonsaidb_core::schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error>
    {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::query_by_name_with_docs(
                self,
                view,
                key,
                order,
                limit,
                access_policy,
            )
            .await
        })
    }

    fn reduce_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<bonsaidb_core::connection::QueryKey<bonsaidb_core::arc_bytes::serde::Bytes>>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::reduce_by_name(self, view, key, access_policy).await
        })
    }

    fn reduce_grouped_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<bonsaidb_core::connection::QueryKey<bonsaidb_core::arc_bytes::serde::Bytes>>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<Vec<bonsaidb_core::schema::view::map::MappedSerializedValue>, bonsaidb_core::Error>
    {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::reduce_grouped_by_name(self, view, key, access_policy).await
        })
    }

    fn delete_docs_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<bonsaidb_core::connection::QueryKey<bonsaidb_core::arc_bytes::serde::Bytes>>,
        access_policy: bonsaidb_core::connection::AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::delete_docs_by_name(self, view, key, access_policy).await
        })
    }
}

impl PubSub for RemoteDatabase {
    type Subscriber = RemoteSubscriber;

    fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::create_subscriber(self).await })
    }

    fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::publish(self, topic, payload).await })
    }

    fn publish_bytes<S: Into<String> + Send>(
        &self,
        topic: S,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::publish_bytes(self, topic, payload).await })
    }

    fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::publish_to_all(self, topics, payload).await })
    }

    fn publish_bytes_to_all(
        &self,
        topics: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::publish_bytes_to_all(self, topics, payload).await })
    }
}

impl Subscriber for RemoteSubscriber {
    fn subscribe_to<S: Into<String> + Send>(&self, topic: S) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncSubscriber::subscribe_to(self, topic).await })
    }

    fn unsubscribe_from(&self, topic: &str) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncSubscriber::unsubscribe_from(self, topic).await })
    }

    fn receiver(&self) -> &'_ flume::Receiver<std::sync::Arc<bonsaidb_core::circulate::Message>> {
        AsyncSubscriber::receiver(self)
    }
}

impl KeyValue for RemoteDatabase {
    fn execute_key_operation(
        &self,
        op: bonsaidb_core::keyvalue::KeyOperation,
    ) -> Result<bonsaidb_core::keyvalue::Output, bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncKeyValue::execute_key_operation(self, op).await })
    }
}
