use std::sync::Arc;

use async_trait::async_trait;
use bonsaidb_core::connection::{
    self, AccessPolicy, AsyncConnection, AsyncLowLevelConnection, AsyncStorageConnection,
    Connection, HasSchema, HasSession, IdentityReference, LowLevelConnection, Range,
    SerializedQueryKey, Session, Sort, StorageConnection,
};
use bonsaidb_core::document::{DocumentId, Header, OwnedDocument};
use bonsaidb_core::keyvalue::{AsyncKeyValue, KeyOperation, KeyValue, Output};
use bonsaidb_core::permissions::Permissions;
use bonsaidb_core::pubsub::{self, AsyncPubSub, AsyncSubscriber, PubSub, Receiver};
use bonsaidb_core::schema::view::map::MappedSerializedValue;
use bonsaidb_core::schema::{
    self, CollectionName, Nameable, Schema, SchemaName, SchemaSummary, Schematic, ViewName,
};
use bonsaidb_core::transaction::{self, OperationResult, Transaction};

use crate::config::StorageConfiguration;
use crate::database::DatabaseNonBlocking;
use crate::storage::{AnyBackupLocation, StorageNonBlocking};
use crate::{Database, Error, Storage, Subscriber};

/// A file-based, multi-database, multi-user database engine. This type is
/// designed for use with [Tokio](https://tokio.rs). For blocking
/// (non-asynchronous) code, see the [`Storage`] type instead.
///
/// ## Converting between Blocking and Async Types
///
/// [`AsyncDatabase`] and [`Database`] can be converted to and from each other
/// using:
///
/// - [`AsyncStorage::into_blocking()`]
/// - [`AsyncStorage::to_blocking()`]
/// - [`AsyncStorage::as_blocking()`]
/// - [`Storage::into_async()`]
/// - [`Storage::to_async()`]
/// - [`Storage::into_async_with_runtime()`]
/// - [`Storage::to_async_with_runtime()`]
///
/// ## Converting from `AsyncDatabase::open` to `AsyncStorage::open`
///
/// [`AsyncDatabase::open`](AsyncDatabase::open) is a simple method that uses
/// `AsyncStorage` to create a database named `default` with the schema
/// provided. These two ways of opening the database are the same:
///
/// ```rust
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
/// use bonsaidb_core::connection::AsyncStorageConnection;
/// use bonsaidb_core::schema::Schema;
/// // `bonsaidb_local` is re-exported to `bonsaidb::local` if using the omnibus crate.
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
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
/// This example shows how to use `AsyncStorage` to create and use multiple
/// databases with multiple schemas:
///
/// ```rust
/// use bonsaidb_core::connection::AsyncStorageConnection;
/// use bonsaidb_core::schema::{Collection, Schema};
/// use bonsaidb_local::config::{Builder, StorageConfiguration};
/// use bonsaidb_local::AsyncStorage;
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
    pub(crate) storage: Storage,
    pub(crate) runtime: Arc<tokio::runtime::Handle>,
}

impl AsyncStorage {
    /// Creates or opens a multi-database [`AsyncStorage`] with its data stored in `directory`.
    pub async fn open(configuration: StorageConfiguration) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || Storage::open(configuration))
            .await?
            .map(Storage::into_async)
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
    #[must_use]
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Option<Self> {
        self.storage
            .with_effective_permissions(effective_permissions)
            .map(|storage| Self {
                storage,
                runtime: self.runtime.clone(),
            })
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    pub async fn database_without_schema(&self, name: &str) -> Result<AsyncDatabase, Error> {
        let name = name.to_owned();
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .storage
                    .database_without_schema(&name)
                    .map(Database::into_async)
            })
            .await?
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async.
    pub fn into_blocking(self) -> Storage {
        self.storage
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async.
    pub fn to_blocking(&self) -> Storage {
        self.storage.clone()
    }

    /// Returns a reference to this instance's blocking version, which is able
    /// to be used without async.
    pub fn as_blocking(&self) -> &Storage {
        &self.storage
    }
}

impl<'a> From<&'a AsyncStorage> for Storage {
    fn from(storage: &'a AsyncStorage) -> Self {
        storage.to_blocking()
    }
}

impl From<AsyncStorage> for Storage {
    fn from(storage: AsyncStorage) -> Self {
        storage.into_blocking()
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
/// [Tokio](https://tokio.rs). For blocking (non-asynchronous) code, see the
/// [`Database`] type instead.
///
/// ## Converting between Async and Blocking Types
///
/// [`AsyncDatabase`] and [`Database`] can be converted to and from each other
/// using:
///
/// - [`AsyncDatabase::into_blocking()`]
/// - [`AsyncDatabase::to_blocking()`]
/// - [`AsyncDatabase::as_blocking()`]
/// - [`Database::into_async()`]
/// - [`Database::to_async()`]
/// - [`Database::into_async_with_runtime()`]
/// - [`Database::to_async_with_runtime()`]
///
/// ## Using `Database` to create a single database
///
/// `Database`provides an easy mechanism to open and access a single database:
///
/// ```rust
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
/// use bonsaidb_core::schema::Collection;
/// // `bonsaidb_local` is re-exported to `bonsaidb::local` if using the omnibus crate.
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
///     AsyncDatabase,
/// };
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Debug, Serialize, Deserialize, Collection)]
/// #[collection(name = "blog-posts")]
/// # #[collection(core = bonsaidb_core)]
/// struct BlogPost {
///     pub title: String,
///     pub contents: String,
/// }
///
/// # async fn test_fn() -> Result<(), bonsaidb_core::Error> {
/// let db = AsyncDatabase::open::<BlogPost>(StorageConfiguration::new("my-db.bonsaidb")).await?;
/// #     Ok(())
/// # }
/// ```
///
/// Under the hood, this initializes a [`AsyncStorage`] instance pointing at
/// "./my-db.bonsaidb". It then returns (or creates) a database named "default"
/// with the schema `BlogPost`.
///
/// In this example, `BlogPost` implements the
/// [`Collection`](schema::Collection) trait, and all collections can be used as
/// a [`Schema`].
#[derive(Debug, Clone)]
pub struct AsyncDatabase {
    pub(crate) database: Database,
    pub(crate) runtime: Arc<tokio::runtime::Handle>,
}

impl AsyncDatabase {
    /// Creates a `Storage` with a single-database named "default" with its data stored at `path`.
    pub async fn open<DB: Schema>(configuration: StorageConfiguration) -> Result<Self, Error> {
        tokio::task::spawn_blocking(move || {
            Database::open::<DB>(configuration).map(Database::into_async)
        })
        .await?
    }

    /// Restricts an unauthenticated instance to having `effective_permissions`.
    /// Returns `None` if a session has already been established.
    #[must_use]
    pub fn with_effective_permissions(&self, effective_permissions: Permissions) -> Option<Self> {
        self.database
            .with_effective_permissions(effective_permissions)
            .map(|database| Self {
                database,
                runtime: self.runtime.clone(),
            })
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async.
    #[must_use]
    pub fn into_blocking(self) -> Database {
        self.database
    }

    /// Converts this instance into its blocking version, which is able to be
    /// used without async.
    #[must_use]
    pub fn to_blocking(&self) -> Database {
        self.database.clone()
    }

    /// Returns a reference to this instance's blocking version, which is able
    /// to be used without async.
    #[must_use]
    pub fn as_blocking(&self) -> &Database {
        &self.database
    }
}

impl From<AsyncDatabase> for Database {
    fn from(database: AsyncDatabase) -> Self {
        database.into_blocking()
    }
}

impl<'a> From<&'a AsyncDatabase> for Database {
    fn from(database: &'a AsyncDatabase) -> Self {
        database.to_blocking()
    }
}

impl DatabaseNonBlocking for AsyncDatabase {
    fn name(&self) -> &str {
        self.database.name()
    }
}

impl HasSession for AsyncStorage {
    fn session(&self) -> Option<&Session> {
        self.storage.session()
    }
}

#[async_trait]
impl AsyncStorageConnection for AsyncStorage {
    type Authenticated = Self;
    type Database = AsyncDatabase;

    async fn admin(&self) -> Self::Database {
        let task_self = self.clone();

        self.runtime
            .spawn_blocking(move || task_self.storage.admin())
            .await
            .unwrap()
            .into_async()
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
            .spawn_blocking(move || {
                task_self
                    .storage
                    .database::<DB>(&name)
                    .map(Database::into_async)
            })
            .await
            .map_err(Error::from)?
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

    async fn list_available_schemas(&self) -> Result<Vec<SchemaSummary>, bonsaidb_core::Error> {
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

    #[cfg(any(feature = "token-authentication", feature = "password-hashing"))]
    async fn authenticate(
        &self,
        authentication: bonsaidb_core::connection::Authentication,
    ) -> Result<Self, bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .storage
                    .authenticate(authentication)
                    .map(Storage::into_async)
            })
            .await
            .map_err(Error::from)?
    }

    async fn assume_identity(
        &self,
        identity: IdentityReference<'_>,
    ) -> Result<Self::Authenticated, bonsaidb_core::Error> {
        let task_self = self.clone();
        let identity = identity.into_owned();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .storage
                    .assume_identity(identity)
                    .map(Storage::into_async)
            })
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

impl HasSession for AsyncDatabase {
    fn session(&self) -> Option<&Session> {
        self.database.session()
    }
}

#[async_trait]
impl AsyncConnection for AsyncDatabase {
    type Storage = AsyncStorage;

    fn storage(&self) -> Self::Storage {
        AsyncStorage {
            storage: self.database.storage(),
            runtime: self.runtime.clone(),
        }
    }

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

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error> {
        Ok(self
            .database
            .roots()
            .transactions()
            .current_transaction_id())
    }

    async fn compact(&self) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || Connection::compact(&task_self.database))
            .await
            .map_err(Error::from)?
    }

    async fn compact_collection<C: schema::Collection>(&self) -> Result<(), bonsaidb_core::Error> {
        let task_self = self.clone();
        self.runtime
            .spawn_blocking(move || Connection::compact_collection::<C>(&task_self.database))
            .await
            .map_err(Error::from)?
    }

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

    async fn publish_bytes(
        &self,
        topic: Vec<u8>,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        PubSub::publish_bytes(&self.database, topic, payload)
    }

    async fn publish_bytes_to_all(
        &self,
        topics: impl IntoIterator<Item = Vec<u8>> + Send + 'async_trait,
        payload: Vec<u8>,
    ) -> Result<(), bonsaidb_core::Error> {
        PubSub::publish_bytes_to_all(&self.database, topics, payload)
    }
}

#[async_trait]
impl AsyncSubscriber for Subscriber {
    async fn subscribe_to_bytes(&self, topic: Vec<u8>) -> Result<(), bonsaidb_core::Error> {
        pubsub::Subscriber::subscribe_to_bytes(self, topic)
    }

    async fn unsubscribe_from_bytes(&self, topic: &[u8]) -> Result<(), bonsaidb_core::Error> {
        pubsub::Subscriber::unsubscribe_from_bytes(self, topic)
    }

    fn receiver(&self) -> &Receiver {
        pubsub::Subscriber::receiver(self)
    }
}

impl HasSchema for AsyncDatabase {
    fn schematic(&self) -> &Schematic {
        self.database.schematic()
    }
}

#[async_trait]
impl AsyncLowLevelConnection for AsyncDatabase {
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

    async fn list_headers_from_collection(
        &self,
        ids: Range<DocumentId>,
        order: Sort,
        limit: Option<u32>,
        collection: &CollectionName,
    ) -> Result<Vec<Header>, bonsaidb_core::Error> {
        let task_self = self.clone();
        let collection = collection.clone();
        self.runtime
            .spawn_blocking(move || {
                task_self
                    .database
                    .list_headers_from_collection(ids, order, limit, &collection)
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
        key: Option<SerializedQueryKey>,
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
        key: Option<SerializedQueryKey>,
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
        key: Option<SerializedQueryKey>,
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
        key: Option<SerializedQueryKey>,
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
        key: Option<SerializedQueryKey>,
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
