use bonsaidb_client::{Client, RemoteDatabase};
use bonsaidb_core::async_trait::async_trait;
use bonsaidb_core::connection::{
    self, AccessPolicy, AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection,
    HasSchema, HasSession, IdentityReference, Range, SerializedQueryKey, Session, Sort,
};
use bonsaidb_core::document::{DocumentId, Header, OwnedDocument};
use bonsaidb_core::schema::view::map::MappedSerializedValue;
use bonsaidb_core::schema::{
    self, Collection, CollectionName, Nameable, Schema, SchemaName, Schematic, ViewName,
};
use bonsaidb_core::transaction::{Executed, OperationResult, Transaction};
use bonsaidb_server::{Backend, CustomServer, NoBackend, ServerDatabase};
use derive_where::derive_where;

/// A local server or a server over a network connection.
#[derive_where(Clone, Debug)]
pub enum AnyServerConnection<B: Backend> {
    /// A local server.
    Local(CustomServer<B>),
    /// A server accessed with a [`Client`].
    Networked(Client),
}

impl<B: Backend> HasSession for AnyServerConnection<B> {
    fn session(&self) -> Option<&Session> {
        match self {
            Self::Local(server) => server.session(),
            Self::Networked(client) => client.session(),
        }
    }
}

#[async_trait]
impl<B: Backend> AsyncStorageConnection for AnyServerConnection<B> {
    type Authenticated = Self;
    type Database = AnyDatabase<B>;

    async fn admin(&self) -> Self::Database {
        match self {
            Self::Local(server) => AnyDatabase::Local(server.admin().await),
            Self::Networked(client) => AnyDatabase::Networked(client.admin().await),
        }
    }

    async fn database<DB: Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.database::<DB>(name).await.map(AnyDatabase::Local),
            Self::Networked(client) => client
                .database::<DB>(name)
                .await
                .map(AnyDatabase::Networked),
        }
    }

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .create_database_with_schema(name, schema, only_if_needed)
                    .await
            }
            Self::Networked(client) => {
                client
                    .create_database_with_schema(name, schema, only_if_needed)
                    .await
            }
        }
    }

    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.delete_database(name).await,
            Self::Networked(client) => client.delete_database(name).await,
        }
    }

    async fn list_databases(&self) -> Result<Vec<connection::Database>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.list_databases().await,
            Self::Networked(client) => client.list_databases().await,
        }
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.list_available_schemas().await,
            Self::Networked(client) => client.list_available_schemas().await,
        }
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.create_user(username).await,
            Self::Networked(client) => client.create_user(username).await,
        }
    }

    async fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.delete_user(user).await,
            Self::Networked(client) => client.delete_user(user).await,
        }
    }

    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.set_user_password(user, password).await,
            Self::Networked(client) => client.set_user_password(user, password).await,
        }
    }

    #[cfg(any(feature = "token-authentication", feature = "password-hashing"))]
    async fn authenticate(
        &self,
        authentication: bonsaidb_core::connection::Authentication,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.authenticate(authentication).await.map(Self::Local),
            Self::Networked(client) => client
                .authenticate(authentication)
                .await
                .map(Self::Networked),
        }
    }

    async fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.assume_identity(identity).await.map(Self::Local),
            Self::Networked(client) => client.assume_identity(identity).await.map(Self::Networked),
        }
    }

    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .add_permission_group_to_user(user, permission_group)
                    .await
            }
            Self::Networked(client) => {
                client
                    .add_permission_group_to_user(user, permission_group)
                    .await
            }
        }
    }

    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .remove_permission_group_from_user(user, permission_group)
                    .await
            }
            Self::Networked(client) => {
                client
                    .remove_permission_group_from_user(user, permission_group)
                    .await
            }
        }
    }

    async fn add_role_to_user<
        'user,
        'role,
        U: Nameable<'user, u64> + Send + Sync,
        R: Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.add_role_to_user(user, role).await,
            Self::Networked(client) => client.add_role_to_user(user, role).await,
        }
    }

    async fn remove_role_from_user<
        'user,
        'role,
        U: Nameable<'user, u64> + Send + Sync,
        R: Nameable<'role, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: R,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.remove_role_from_user(user, role).await,
            Self::Networked(client) => client.remove_role_from_user(user, role).await,
        }
    }
}

/// A database connection that can be either from a local server or a server
/// over a network connection.
#[derive_where(Clone, Debug)]
pub enum AnyDatabase<B: Backend = NoBackend> {
    /// A local database.
    Local(ServerDatabase<B>),
    /// A networked database accessed with a [`Client`].
    Networked(RemoteDatabase),
}

impl<B: Backend> HasSession for AnyDatabase<B> {
    fn session(&self) -> Option<&Session> {
        match self {
            Self::Local(server) => server.session(),
            Self::Networked(client) => client.session(),
        }
    }
}

#[async_trait]
impl<B: Backend> AsyncConnection for AnyDatabase<B> {
    type Storage = AnyServerConnection<B>;

    fn storage(&self) -> Self::Storage {
        match self {
            Self::Local(server) => AnyServerConnection::Local(server.storage()),
            Self::Networked(client) => AnyServerConnection::Networked(client.storage()),
        }
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<Executed>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .list_executed_transactions(starting_id, result_limit)
                    .await
            }
            Self::Networked(client) => {
                client
                    .list_executed_transactions(starting_id, result_limit)
                    .await
            }
        }
    }

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.last_transaction_id().await,
            Self::Networked(client) => client.last_transaction_id().await,
        }
    }

    async fn compact_collection<C: Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.compact_collection::<C>().await,
            Self::Networked(client) => client.compact_collection::<C>().await,
        }
    }

    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.compact().await,
            Self::Networked(client) => client.compact().await,
        }
    }

    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.compact_key_value_store().await,
            Self::Networked(client) => client.compact_key_value_store().await,
        }
    }
}

#[async_trait]
impl<B: Backend> AsyncLowLevelConnection for AnyDatabase<B> {
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.apply_transaction(transaction).await,
            Self::Networked(client) => client.apply_transaction(transaction).await,
        }
    }

    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.get_from_collection(id, collection).await,
            Self::Networked(client) => client.get_from_collection(id, collection).await,
        }
    }

    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .list_from_collection(ids, order, limit, collection)
                    .await
            }
            Self::Networked(client) => {
                client
                    .list_from_collection(ids, order, limit, collection)
                    .await
            }
        }
    }

    async fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .list_headers_from_collection(ids, order, limit, collection)
                    .await
            }
            Self::Networked(client) => {
                client
                    .list_headers_from_collection(ids, order, limit, collection)
                    .await
            }
        }
    }

    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.count_from_collection(ids, collection).await,
            Self::Networked(client) => client.count_from_collection(ids, collection).await,
        }
    }

    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.get_multiple_from_collection(ids, collection).await,
            Self::Networked(client) => client.get_multiple_from_collection(ids, collection).await,
        }
    }

    async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.compact_collection_by_name(collection).await,
            Self::Networked(client) => client.compact_collection_by_name(collection).await,
        }
    }

    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .query_by_name(view, key, order, limit, access_policy)
                    .await
            }
            Self::Networked(client) => {
                client
                    .query_by_name(view, key, order, limit, access_policy)
                    .await
            }
        }
    }

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .query_by_name_with_docs(view, key, order, limit, access_policy)
                    .await
            }
            Self::Networked(client) => {
                client
                    .query_by_name_with_docs(view, key, order, limit, access_policy)
                    .await
            }
        }
    }

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.reduce_by_name(view, key, access_policy).await,
            Self::Networked(client) => client.reduce_by_name(view, key, access_policy).await,
        }
    }

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => {
                server
                    .reduce_grouped_by_name(view, key, access_policy)
                    .await
            }
            Self::Networked(client) => {
                client
                    .reduce_grouped_by_name(view, key, access_policy)
                    .await
            }
        }
    }

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<SerializedQueryKey>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.delete_docs_by_name(view, key, access_policy).await,
            Self::Networked(client) => client.delete_docs_by_name(view, key, access_policy).await,
        }
    }
}

impl<B: Backend> HasSchema for AnyDatabase<B> {
    fn schematic(&self) -> &Schematic {
        match self {
            Self::Local(server) => server.schematic(),
            Self::Networked(client) => client.schematic(),
        }
    }
}
