use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::Authentication;
use bonsaidb_core::{
    arc_bytes::serde::Bytes,
    circulate,
    connection::{
        self, AccessPolicy, AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection,
        Connection, Identity, LowLevelConnection, QueryKey, Range, Session, Sort,
        StorageConnection,
    },
    document::{AnyDocumentId, DocumentId, OwnedDocument},
    keyvalue::{AsyncKeyValue, KeyOperation, KeyValue, Output},
    permissions::Permissions,
    pubsub::{self, AsyncPubSub, AsyncSubscriber, PubSub},
    schema::{
        self,
        view::map::{MappedDocuments, MappedSerializedValue},
        CollectionName, Map, MappedValue, Nameable, Schema, SchemaName, Schematic, ViewName,
    },
    transaction::{self, OperationResult, Transaction},
};

use crate::{
    config::StorageConfiguration,
    database::DatabaseNonBlocking,
    storage::{AnyBackupLocation, StorageNonBlocking},
    Database, Error, Storage, Subscriber,
};

/// A file-based, multi-database, multi-user database engine. This type is
/// designed for use with [Tokio](https://tokio.rs). For non-asynchronous code,
/// see the [`Storage`] type instead. This type can be converted using
/// [`std::convert::From`].
///
/// ## Converting from `AsyncDatabase::open` to `AsyncStorage::open`
///
/// [`AsyncDatabase::open`](AsyncDatabase::open) is a simple method that uses
/// `AsyncStorage` to create a database named `default` with the schema
/// provided. These two ways of opening the database are the same:
///
/// ```rust
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
/// use bonsaidb_core::{connection::AsyncStorageConnection, schema::Schema};
/// // `bonsaidb_local` is re-exported to `bonsaidb::local` if using the omnibus crate.
/// use bonsaidb_local::{
///     config::{AsyncStorageConfiguration, Builder},
///     AsyncDatabase, AsyncStorage,
/// };
/// # async fn open<MySchema: Schema>() -> anyhow::Result<()> {
/// // This creates a Storage instance, creates a database, and returns it.
/// let db = AsyncDatabase::open::<MySchema>(StorageConfiguration::new("my-db.bonsaidb")).await?;
///
/// // This is the equivalent code being executed:
/// let storage =
///     AsyncStorage::open(StorageConfiguration::new("my-db.bonsaidb").with_schema::<MySchema>()?)
///         .await?;
/// storage.create_database::<MySchema>("default", true).await?;
/// let db = storage.database::<MySchema>("default").await?;
/// #     Ok(())
/// # }
/// ```
///
/// ## Using multiple databases
///
/// This example shows how to use `AsyncStorage` to create and use multiple databases
/// with multiple schemas:
///
/// ```rust
/// use bonsaidb_core::{
///     connection::AsyncStorageConnection,
///     schema::{Collection, Schema},
/// };
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
///     AsyncStorage,
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Schema)]
/// #[schema(name = "my-schema", collections = [BlogPost, Author])]
/// # #[schema(core = bonsaidb_core)]
/// struct MySchema;
///
/// #[derive(Debug, Serialize, Deserialize, Collection)]
/// #[collection(name = "blog-posts")]
/// # #[collection(core = bonsaidb_core)]
/// struct BlogPost {
///     pub title: String,
///     pub contents: String,
///     pub author_id: u64,
/// }
///
/// #[derive(Debug, Serialize, Deserialize, Collection)]
/// #[collection(name = "blog-posts")]
/// # #[collection(core = bonsaidb_core)]
/// struct Author {
///     pub name: String,
/// }
///
/// # async fn test_fn() -> Result<(), bonsaidb_core::Error> {
/// let storage = AsyncStorage::open(
///     StorageConfiguration::new("my-db.bonsaidb")
///         .with_schema::<BlogPost>()?
///         .with_schema::<MySchema>()?,
/// )
/// .await?;
///
/// storage
///     .create_database::<BlogPost>("ectons-blog", true)
///     .await?;
/// let ectons_blog = storage.database::<BlogPost>("ectons-blog").await?;
/// storage
///     .create_database::<MySchema>("another-db", true)
///     .await?;
/// let another_db = storage.database::<MySchema>("another-db").await?;
///
/// #     Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct AsyncStorage {
    storage: Storage,
    runtime: tokio::runtime::Handle,
}

impl AsyncStorage {
    /// Creates or opens a multi-database [`AsyncStorage`] with its data stored in `directory`.
    pub async fn open(configuration: StorageConfiguration) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || Storage::open(configuration))
            .await?
            .map(Self::from)
    }

    /// Restores all data from a previously stored backup `location`.
    pub async fn restore<L: AnyBackupLocation + 'static>(&self, location: L) -> Result<(), Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.storage.restore(&location))
            .await?
    }

    /// Stores a copy of all data in this instance to `location`.
    pub async fn backup<L: AnyBackupLocation + 'static>(&self, location: L) -> Result<(), Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.storage.backup(&location))
            .await?
    }

    /// Restricts an unauthenticated instance to having `effective_permissions`.
    /// Returns `None` if a session has already been established.
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Option<Self> {
        self.storage
            .with_effective_permissions(effective_permissions)
            .map(Self::from)
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn database_without_schema(&self, name: &str) -> Result<AsyncDatabase, Error> {
        let name = name.to_owned();
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.storage.database_without_schema(&name))
            .await?
            .map(AsyncDatabase::from)
    }
}

impl From<Storage> for AsyncStorage {
    fn from(storage: Storage) -> Self {
        Self {
            storage,
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl<'a> From<&'a Storage> for AsyncStorage {
    fn from(storage: &'a Storage) -> Self {
        Self {
            storage: storage.clone(),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl<'a> From<&'a AsyncStorage> for Storage {
    fn from(storage: &'a AsyncStorage) -> Self {
        storage.storage.clone()
    }
}

impl From<AsyncStorage> for Storage {
    fn from(storage: AsyncStorage) -> Self {
        storage.storage
    }
}

impl StorageNonBlocking for AsyncStorage {
    fn path(&self) -> &std::path::Path {
        self.storage.path()
    }
    fn assume_session(&self, session: Session) -> Result<Self, bonsaidb_core::Error> {
        self.storage.assume_session(session).map(|storage| Self {
            storage,
            runtime: self.runtime.clone(),
        })
    }
}

/// A database stored in BonsaiDb. This type is designed for use with
/// [Tokio](https://tokio.rs). For non-asynchronous code, see the [`Database`]
/// type instead. This type can be converted using [`std::convert::From`].
#[derive(Debug, Clone)]
pub struct AsyncDatabase {
    pub(crate) database: Database,
    runtime: tokio::runtime::Handle,
}

impl AsyncDatabase {
    /// Creates a `Storage` with a single-database named "default" with its data stored at `path`.
    pub async fn open<DB: Schema>(configuration: StorageConfiguration) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || Database::open::<DB>(configuration))
            .await?
            .map(Self::from)
    }

    /// Restricts an unauthenticated instance to having `effective_permissions`.
    /// Returns `None` if a session has already been established.
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Option<Self> {
        self.database
            .with_effective_permissions(effective_permissions)
            .map(Self::from)
    }
}

impl From<Database> for AsyncDatabase {
    fn from(database: Database) -> Self {
        Self {
            database,
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl From<AsyncDatabase> for Database {
    fn from(database: AsyncDatabase) -> Self {
        database.database
    }
}

impl<'a> From<&'a Database> for AsyncDatabase {
    fn from(database: &'a Database) -> Self {
        Self {
            database: database.clone(),
            runtime: tokio::runtime::Handle::current(),
        }
    }
}

impl DatabaseNonBlocking for AsyncDatabase {
    fn name(&self) -> &str {
        self.database.name()
    }
}

#[async_trait]
impl AsyncStorageConnection for AsyncStorage {
    type Database = AsyncDatabase;
    type Authenticated = Self;

    async fn admin(&self) -> Self::Database {
        let task_self = self.clone();
        AsyncDatabase::from(
            self.runtime
                .spawn_blocking(move || task_self.storage.admin())
                .await
                .unwrap(),
        )
    }

    fn session(&self) -> Option<&Session> {
        self.storage.session()
    }

    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        let name = name.to_owned();
        self.runtime
            .spawn_blocking(move || {
                StorageConnection::create_database_with_schema(
                    &task_self.storage,
                    &name,
                    schema,
                    only_if_needed,
                )
            })
            .await
            .map_err(Error::from)?
    }

    async fn database<DB: Schema>(
        &self,
        name: &str,
    ) -> Result<Self::Database, bonsaidb_core::Error> {
        let task_self = self.clone();
        let name = name.to_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.database::<DB>(&name))
            .await
            .map_err(Error::from)?
            .map(AsyncDatabase::from)
    }

    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        let name = name.to_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.delete_database(&name))
            .await
            .map_err(Error::from)?
    }

    async fn list_databases(&self) -> Result<Vec<connection::Database>, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.storage.list_databases())
            .await
            .map_err(Error::from)?
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.storage.list_available_schemas())
            .await
            .map_err(Error::from)?
    }

    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        let task_self = self.clone();
        let username = username.to_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.create_user(&username))
            .await
            .map_err(Error::from)?
    }

    async fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.delete_user(user))
            .await
            .map_err(Error::from)?
    }

    #[cfg(feature = "password-hashing")]
    async fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.set_user_password(user, password))
            .await
            .map_err(Error::from)?
    }

    #[cfg(feature = "password-hashing")]
    async fn authenticate<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Self, bonsaidb_core::Error> {
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .storage
                    .authenticate(user, authentication)
                    .map(Self::from)
            })
            .await
            .map_err(Error::from)?
    }

    async fn assume_identity(
        &self,
        identity: Identity,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.storage.assume_identity(identity).map(Self::from))
            .await
            .map_err(Error::from)?
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
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        let group = permission_group.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.add_permission_group_to_user(user, group))
            .await
            .map_err(Error::from)?
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
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        let group = permission_group.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .storage
                    .remove_permission_group_from_user(user, group)
            })
            .await
            .map_err(Error::from)?
    }

    async fn add_role_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        let role = role.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.add_role_to_user(user, role))
            .await
            .map_err(Error::from)?
    }

    async fn remove_role_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        let user = user.name()?.into_owned();
        let role = role.name()?.into_owned();
        self.runtime
            .spawn_blocking(move || task_self.storage.remove_role_from_user(user, role))
            .await
            .map_err(Error::from)?
    }
}

#[async_trait]
impl AsyncConnection for AsyncDatabase {
    type Storage = AsyncStorage;

    fn storage(&self) -> Self::Storage {
        AsyncStorage::from(self.database.storage())
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(starting_id, result_limit))
    )]
    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<u32>,
    ) -> Result<Vec<transaction::Executed>, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .list_executed_transactions(starting_id, result_limit)
            })
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self
            .database
            .roots()
            .transactions()
            .current_transaction_id())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || Connection::compact(&task_self.database))
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || Connection::compact_collection::<C>(&task_self.database))
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || Connection::compact_key_value_store(&task_self.database))
            .await
            .map_err(Error::from)?
    }
}

#[async_trait]
impl AsyncKeyValue for AsyncDatabase {
    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || KeyValue::execute_key_operation(&task_self.database, op))
            .await
            .map_err(Error::from)?
    }
}

#[async_trait]
impl AsyncPubSub for AsyncDatabase {
    type Subscriber = Subscriber;

    async fn create_subscriber(&self) -> Result<Self::Subscriber, bonsaidb_core::Error> {
        PubSub::create_subscriber(&self.database)
    }

    async fn publish<S: Into<String> + Send, P: serde::Serialize + Sync>(
        &self,
        topic: S,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        PubSub::publish(&self.database, topic, payload)
    }

    async fn publish_to_all<P: serde::Serialize + Sync>(
        &self,
        topics: Vec<String>,
        payload: &P,
    ) -> Result<(), bonsaidb_core::Error> {
        PubSub::publish_to_all(&self.database, topics, payload)
    }

    async fn publish_bytes<S: Into<String> + Send>(
        &self,
        topic: S,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        PubSub::publish_bytes(&self.database, topic, payload)
    }

    async fn publish_bytes_to_all(
        &self,
        topics: Vec<String>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        PubSub::publish_bytes_to_all(&self.database, topics, payload)
    }
}

#[async_trait]
impl AsyncSubscriber for Subscriber {
    async fn subscribe_to<S: Into<String> + Send>(
        &self,
        topic: S,
    ) -> Result<(), bonsaidb_core::Error> {
        pubsub::Subscriber::subscribe_to(self, topic)
    }

    async fn unsubscribe_from(&self, topic: &str) -> Result<(), bonsaidb_core::Error> {
        pubsub::Subscriber::unsubscribe_from(self, topic)
    }

    fn receiver(&self) -> &'_ flume::Receiver<Arc<circulate::Message>> {
        pubsub::Subscriber::receiver(self)
    }
}

#[async_trait]
impl AsyncLowLevelConnection for AsyncDatabase {
    fn schematic(&self) -> &Schematic {
        self.database.schematic()
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(transaction)))]
    async fn apply_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.database.apply_transaction(transaction))
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(id)))]
    async fn get<C, PrimaryKey>(
        &self,
        id: PrimaryKey,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error>
    where
        C: schema::Collection,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send + 'async_trait,
    {
        let task_self = self.clone();
        let id = id.into().to_document_id()?;
        self.runtime
            .spawn_blocking(move || task_self.database.get::<C, _>(id))
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids)))]
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
        let task_self = self.clone();
        let ids = ids
            .into_iter()
            .map(|id| id.into().to_document_id())
            .collect::<Result<Vec<_>, _>>()?;
        self.runtime
            .spawn_blocking(move || task_self.database.get_multiple::<C, _, _, _>(ids))
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids, order, limit)))]
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
        let task_self = self.clone();
        let ids = ids.into().map_result(|id| id.into().to_document_id())?;
        self.runtime
            .spawn_blocking(move || {
                LowLevelConnection::list::<C, _, _>(&task_self.database, ids, order, limit)
            })
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(ids)))]
    async fn count<C, R, PrimaryKey>(&self, ids: R) -> Result<u64, bonsaidb_core::Error>
    where
        C: schema::Collection,
        R: Into<Range<PrimaryKey>> + Send,
        PrimaryKey: Into<AnyDocumentId<C::PrimaryKey>> + Send,
    {
        let task_self = self.clone();
        let ids = ids.into().map_result(|id| id.into().to_document_id())?;
        self.runtime
            .spawn_blocking(move || LowLevelConnection::count::<C, _, _>(&task_self.database, ids))
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(key, order, limit, access_policy))
    )]
    #[must_use]
    async fn query<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<Map<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                LowLevelConnection::query::<V>(
                    &task_self.database,
                    key,
                    order,
                    limit,
                    access_policy,
                )
            })
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(key, order, limit, access_policy))
    )]
    async fn query_with_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<MappedDocuments<OwnedDocument, V>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                LowLevelConnection::query_with_docs::<V>(
                    &task_self.database,
                    key,
                    order,
                    limit,
                    access_policy,
                )
            })
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(key, access_policy)))]
    async fn reduce<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<V::Value, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                LowLevelConnection::reduce::<V>(&task_self.database, key, access_policy)
            })
            .await
            .map_err(Error::from)?
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(key, access_policy)))]
    async fn reduce_grouped<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<V::Key, V::Value>>, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                LowLevelConnection::reduce_grouped::<V>(&task_self.database, key, access_policy)
            })
            .await
            .map_err(Error::from)?
    }

    async fn delete_docs<V: schema::SerializedView>(
        &self,
        key: Option<QueryKey<V::Key>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error>
    where
        Self: Sized,
    {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                LowLevelConnection::delete_docs::<V>(&task_self.database, key, access_policy)
            })
            .await
            .map_err(Error::from)?
    }

    async fn get_from_collection(
        &self,
        id: DocumentId,
        collection: &CollectionName,
    ) -> Result<Option<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        self.runtime
            .spawn_blocking(move || task_self.database.get_from_collection(id, &collection))
            .await
            .map_err(Error::from)?
    }

    async fn list_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .list_from_collection(ids, order, limit, &collection)
            })
            .await
            .map_err(Error::from)?
    }

    async fn count_from_collection(
        &self,
        ids: Range<DocumentId>,
        collection: &CollectionName,
    ) -> Result<u64, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        self.runtime
            .spawn_blocking(move || task_self.database.count_from_collection(ids, &collection))
            .await
            .map_err(Error::from)?
    }

    async fn get_multiple_from_collection(
        &self,
        ids: &[DocumentId],
        collection: &CollectionName,
    ) -> Result<Vec<OwnedDocument>, bonsaidb_core::Error> {
        let task_self = self.clone();
        // TODO avoid the allocation here, switch to IntoIterator.
        let ids = ids.to_vec();
        let collection = collection.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .get_multiple_from_collection(&ids, &collection)
            })
            .await
            .map_err(Error::from)?
    }

    async fn compact_collection_by_name(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || task_self.database.compact_collection_by_name(collection))
            .await
            .map_err(Error::from)?
    }

    async fn query_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<schema::view::map::Serialized>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let view = view.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .query_by_name(&view, key, order, limit, access_policy)
            })
            .await
            .map_err(Error::from)?
    }

    async fn query_by_name_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        order: Sort,
        limit: Option<u32>,
        access_policy: AccessPolicy,
    ) -> Result<schema::view::map::MappedSerializedDocuments, bonsaidb_core::Error> {
        let task_self = self.clone();
        let view = view.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .query_by_name_with_docs(&view, key, order, limit, access_policy)
            })
            .await
            .map_err(Error::from)?
    }

    async fn reduce_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let view = view.clone();
        self.runtime
            .spawn_blocking(move || task_self.database.reduce_by_name(&view, key, access_policy))
            .await
            .map_err(Error::from)?
    }

    async fn reduce_grouped_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedSerializedValue>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let view = view.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .reduce_grouped_by_name(&view, key, access_policy)
            })
            .await
            .map_err(Error::from)?
    }

    async fn delete_docs_by_name(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Bytes>>,
        access_policy: AccessPolicy,
    ) -> Result<u64, bonsaidb_core::Error> {
        let task_self = self.clone();
        let view = view.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .delete_docs_by_name(&view, key, access_policy)
            })
            .await
            .map_err(Error::from)?
    }
}
