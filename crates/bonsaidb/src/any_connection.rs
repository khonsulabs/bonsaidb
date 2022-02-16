use bonsaidb_client::{Client, RemoteDatabase};
#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::{Authenticated, Authentication};
use bonsaidb_core::{
    async_trait::async_trait,
    connection::{self, AccessPolicy, Connection, QueryKey, Range, Sort, StorageConnection},
    document::{DocumentKey, OwnedDocument},
    schema::{
        view::map::MappedDocuments, Collection, Map, MappedValue, Nameable, Schema, SchemaName,
        SerializedView,
    },
    transaction::{Executed, OperationResult, Transaction},
};
use bonsaidb_server::{Backend, CustomServer, NoBackend, ServerDatabase};

/// A local server or a server over a network connection.
pub enum AnyServerConnection<B: Backend> {
    /// A local server.
    Local(CustomServer<B>),
    /// A server accessed with a [`Client`].
    Networked(Client<B::CustomApi>),
}

#[async_trait]
impl<B: Backend> StorageConnection for AnyServerConnection<B> {
    type Database = AnyDatabase<B>;

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

    #[cfg(feature = "password-hashing")]
    async fn authenticate<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Authenticated, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.authenticate(user, authentication).await,
            Self::Networked(client) => client.authenticate(user, authentication).await,
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
pub enum AnyDatabase<B: Backend = NoBackend> {
    /// A local database.
    Local(ServerDatabase<B>),
    /// A networked database accessed with a [`Client`].
    Networked(RemoteDatabase<B::CustomApi>),
}

#[async_trait]
impl<B: Backend> Connection for AnyDatabase<B> {
    async fn get<C, PK>(&self, id: PK) -> Result<Option<OwnedDocument>, bonsaidb_core::Error>
    where
        C: Collection,
        PK: Into<DocumentKey<C::PrimaryKey>> + Send,
    {
        match self {
            Self::Local(server) => server.get::<C, _>(id).await,
            Self::Networked(client) => client.get::<C, _>(id).await,
        }
    }

    async fn get_multiple<C, PK, DocumentIds, I>(
        &self,
        ids: DocumentIds,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: Collection,
        DocumentIds: IntoIterator<Item = PK, IntoIter = I> + Send + Sync,
        I: Iterator<Item = PK> + Send + Sync,
        PK: Into<DocumentKey<C::PrimaryKey>> + Send + Sync,
    {
        match self {
            Self::Local(server) => server.get_multiple::<C, _, _, _>(ids).await,
            Self::Networked(client) => client.get_multiple::<C, _, _, _>(ids).await,
        }
    }

    async fn list<C, R, PK>(
        &self,
        ids: R,
        order: Sort,
        limit: Option<usize>,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error>
    where
        C: Collection,
        R: Into<Range<PK>> + Send,
        PK: Into<DocumentKey<C::PrimaryKey>> + Send,
    {
        match self {
            Self::Local(server) => server.list::<C, _, _>(ids, order, limit).await,
            Self::Networked(client) => client.list::<C, _, _>(ids, order, limit).await,
        }
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
        match self {
            Self::Local(server) => server.query::<V>(key, order, limit, access_policy).await,
            Self::Networked(client) => client.query::<V>(key, order, limit, access_policy).await,
        }
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
        match self {
            Self::Local(server) => {
                server
                    .query_with_docs::<V>(key, order, limit, access_policy)
                    .await
            }
            Self::Networked(client) => {
                client
                    .query_with_docs::<V>(key, order, limit, access_policy)
                    .await
            }
        }
    }

    async fn reduce<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self {
            Self::Local(server) => server.reduce::<V>(key, access_policy).await,
            Self::Networked(client) => client.reduce::<V>(key, access_policy).await,
        }
    }

    async fn reduce_grouped<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self {
            Self::Local(server) => server.reduce_grouped::<V>(key, access_policy).await,
            Self::Networked(client) => client.reduce_grouped::<V>(key, access_policy).await,
        }
    }

    async fn delete_docs<V: SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        match self {
            Self::Local(server) => server.delete_docs::<V>(key, access_policy).await,
            Self::Networked(client) => client.delete_docs::<V>(key, access_policy).await,
        }
    }

    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        match self {
            Self::Local(server) => server.apply_transaction(transaction).await,
            Self::Networked(client) => client.apply_transaction(transaction).await,
        }
    }

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
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
