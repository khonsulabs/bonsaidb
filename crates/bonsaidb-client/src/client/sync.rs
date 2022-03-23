use bonsaidb_core::{
    connection::{
        AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection, Connection,
        IdentityReference, LowLevelConnection, Range, Sort, StorageConnection,
    },
    document::{DocumentId, Header, OwnedDocument},
    keyvalue::{AsyncKeyValue, KeyValue},
    pubsub::{AsyncPubSub, AsyncSubscriber, PubSub, Receiver, Subscriber},
    schema::{CollectionName, Schematic},
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
        identity: IdentityReference<'_>,
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
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::get_from_collection(self, id, collection).await
        })
    }

    fn list_from_collection(
        &self,
        ids: Range<bonsaidb_core::document::DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::list_from_collection(self, ids, order, limit, collection).await
        })
    }

    fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::list_headers_from_collection(
                self, ids, order, limit, collection,
            )
            .await
        })
    }

    fn count_from_collection(
        &self,
        ids: Range<bonsaidb_core::document::DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::count_from_collection(self, ids, collection).await
        })
    }

    fn get_multiple_from_collection(
        &self,
        ids: &[bonsaidb_core::document::DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::get_multiple_from_collection(self, ids, collection).await
        })
    }

    fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio().block_on(async {
            AsyncLowLevelConnection::compact_collection_by_name(self, collection).await
        })
    }

    fn query_by_name(
        &self,
        view: &bonsaidb_core::schema::ViewName,
        key: Option<bonsaidb_core::connection::QueryKey<bonsaidb_core::arc_bytes::serde::Bytes>>,
        order: Sort,
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
        order: Sort,
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

    fn publish_bytes(&self, topic: Vec<u8>, payload: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::publish_bytes(self, topic, payload).await })
    }

    fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncPubSub::publish_bytes_to_all(self, topics, payload).await })
    }
}

impl Subscriber for RemoteSubscriber {
    fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncSubscriber::subscribe_to_bytes(self, topic).await })
    }

    fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), bonsaidb_core::Error> {
        self.tokio()
            .block_on(async { AsyncSubscriber::unsubscribe_from_bytes(self, topic).await })
    }

    fn receiver(&self) -> &Receiver {
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
