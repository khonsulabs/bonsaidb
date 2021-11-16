use std::{
    any::Any,
    collections::HashMap,
    fmt::{Debug, Display},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
pub use bonsaidb_core::circulate::Relay;
#[cfg(feature = "internal-apis")]
use bonsaidb_core::custodian_password::{LoginResponse, ServerLogin};
use bonsaidb_core::{
    admin::{
        self,
        database::{self, ByName, Database as DatabaseRecord},
        password_config::PasswordConfig,
        user::User,
        Admin, PermissionGroup, Role, ADMIN_DATABASE_NAME,
    },
    connection::{AccessPolicy, Connection, QueryKey, Range, ServerConnection, Sort},
    custodian_password::{RegistrationFinalization, RegistrationRequest, ServerRegistration},
    document::{Document, KeyId},
    kv::{KeyOperation, Output},
    permissions::Permissions,
    schema::{
        view::map, CollectionDocument, CollectionName, MappedValue, NamedCollection,
        NamedReference, Schema, SchemaName, Schematic, ViewName,
    },
    transaction::{Executed, OperationResult, Transaction},
};
use futures::TryFutureExt;
use itertools::Itertools;
use nebari::{
    io::{
        fs::{StdFile, StdFileManager},
        FileManager,
    },
    ChunkCache, ThreadPool,
};
use rand::{thread_rng, Rng};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Mutex, RwLock},
};

use crate::{
    config::Configuration,
    database::Context,
    jobs::manager::Manager,
    tasks::TaskManager,
    vault::{self, LocalVaultKeyStorage, TreeVault, Vault},
    Database, Error,
};

/// A file-based, multi-database, multi-user database engine.
#[derive(Debug, Clone)]
pub struct Storage {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    id: StorageId,
    path: PathBuf,
    threadpool: ThreadPool<StdFile>,
    file_manager: StdFileManager,
    pub(crate) tasks: TaskManager,
    pub(crate) vault: Arc<Vault>,
    schemas: RwLock<HashMap<SchemaName, Box<dyn DatabaseOpener>>>,
    available_databases: RwLock<HashMap<String, SchemaName>>,
    open_roots: Mutex<HashMap<String, Context>>,
    default_encryption_key: Option<KeyId>,
    chunk_cache: ChunkCache,
    pub(crate) check_view_integrity_on_database_open: bool,
    runtime: tokio::runtime::Handle,
    relay: Relay,
}

impl Drop for Data {
    fn drop(&mut self) {
        let open_roots = std::mem::take(&mut self.open_roots);

        self.runtime.spawn(async move {
            let mut open_roots = open_roots.lock().await;
            for (_, context) in open_roots.drain() {
                context.shutdown();
            }
        });
    }
}

impl Storage {
    /// Creates or opens a multi-database [`Storage`] with its data stored in `directory`.
    pub async fn open_local<P: AsRef<Path> + Send>(
        path: P,
        configuration: Configuration,
    ) -> Result<Self, Error> {
        let owned_path = path.as_ref().to_owned();

        let manager = Manager::default();
        for _ in 0..configuration.workers.worker_count {
            manager.spawn_worker();
        }
        let tasks = TaskManager::new(manager);

        fs::create_dir_all(&owned_path).await?;

        let id = Self::lookup_or_create_id(&configuration, &owned_path).await?;

        let vault_key_storage = match configuration.vault_key_storage {
            Some(storage) => storage,
            None => Box::new(
                LocalVaultKeyStorage::new(owned_path.join("vault-keys"))
                    .await
                    .map_err(|err| Error::Vault(vault::Error::Initializing(err.to_string())))?,
            ),
        };

        let vault = Arc::new(Vault::initialize(id, &owned_path, vault_key_storage).await?);

        let check_view_integrity_on_database_open = configuration.views.check_integrity_on_open;
        let default_encryption_key = configuration.default_encryption_key;
        let storage = tokio::task::spawn_blocking::<_, Result<Self, Error>>(move || {
            Ok(Self {
                data: Arc::new(Data {
                    id,
                    tasks,
                    vault,
                    default_encryption_key,
                    path: owned_path,
                    file_manager: StdFileManager::default(),
                    chunk_cache: ChunkCache::new(2000, 160_384),
                    threadpool: ThreadPool::default(),
                    schemas: RwLock::default(),
                    available_databases: RwLock::default(),
                    open_roots: Mutex::default(),
                    check_view_integrity_on_database_open,
                    runtime: tokio::runtime::Handle::current(),
                    relay: Relay::default(),
                }),
            })
        })
        .await??;

        storage.cache_available_databases().await?;

        storage.create_admin_database_if_needed().await?;

        Ok(storage)
    }

    /// Returns the path of the database storage.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.data.path
    }

    async fn lookup_or_create_id(
        configuration: &Configuration,
        path: &Path,
    ) -> Result<StorageId, Error> {
        Ok(StorageId(if let Some(id) = configuration.unique_id {
            // The configuraiton id override is not persisted to disk. This is
            // mostly to prevent someone from accidentally adding this
            // configuration, realizing it breaks things, and then wanting to
            // revert. This makes reverting to the old value easier.
            id
        } else {
            // Load/Store a randomly generated id into a file. While the value
            // is numerical, the file contents are the ascii decimal, making it
            // easier for a human to view, and if needed, edit.
            let id_path = path.join("server-id");

            if id_path.exists() {
                // This value is important enought to not allow launching the
                // server if the file can't be read or contains unexpected data.
                let existing_id = String::from_utf8(
                    File::open(id_path)
                        .and_then(|mut f| async move {
                            let mut bytes = Vec::new();
                            f.read_to_end(&mut bytes).await.map(|_| bytes)
                        })
                        .await
                        .expect("error reading server-id file"),
                )
                .expect("server-id contains invalid data");

                existing_id.parse().expect("server-id isn't numeric")
            } else {
                let id = { thread_rng().gen::<u64>() };
                File::create(id_path)
                    .and_then(|mut file| async move {
                        let id = id.to_string();
                        file.write_all(id.as_bytes()).await?;
                        file.shutdown().await
                    })
                    .await
                    .map_err(|err| {
                        Error::Core(bonsaidb_core::Error::Configuration(format!(
                            "Error writing server-id file: {}",
                            err
                        )))
                    })?;
                id
            }
        }))
    }

    async fn cache_available_databases(&self) -> Result<(), Error> {
        let available_databases = self
            .admin()
            .await
            .view::<ByName>()
            .query()
            .await?
            .into_iter()
            .map(|map| (map.key, map.value))
            .collect();
        let mut storage_databases = self.data.available_databases.write().await;
        *storage_databases = available_databases;
        Ok(())
    }

    async fn create_admin_database_if_needed(&self) -> Result<(), Error> {
        self.register_schema::<Admin>().await?;
        match self.database::<Admin>(ADMIN_DATABASE_NAME).await {
            Ok(_) => {}
            Err(Error::Core(bonsaidb_core::Error::DatabaseNotFound(_))) => {
                self.create_database::<Admin>(ADMIN_DATABASE_NAME, true)
                    .await?;
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    /// Returns the unique id of the server.
    ///
    /// This value is set from the [`Configuration`] or randomly generated when
    /// creating a server. It shouldn't be changed after a server is in use, as
    /// doing can cause issues. For example, the vault that manages encrypted
    /// storage uses the server ID to store the vault key. If the server ID
    /// changes, the vault key storage will need to be updated with the new
    /// server ID.
    #[must_use]
    pub fn unique_id(&self) -> StorageId {
        self.data.id
    }

    #[must_use]
    pub(crate) fn vault(&self) -> &Arc<Vault> {
        &self.data.vault
    }

    #[must_use]
    pub(crate) fn default_encryption_key(&self) -> Option<&KeyId> {
        self.data.default_encryption_key.as_ref()
    }

    /// Registers a schema for use within the server.
    pub async fn register_schema<DB: Schema>(&self) -> Result<(), Error> {
        let mut schemas = self.data.schemas.write().await;
        if schemas
            .insert(
                DB::schema_name()?,
                Box::new(StorageSchemaOpener::<DB>::new()?),
            )
            .is_none()
        {
            Ok(())
        } else {
            Err(Error::Core(bonsaidb_core::Error::SchemaAlreadyRegistered(
                DB::schema_name()?,
            )))
        }
    }

    /// Retrieves a database. This function checks that the database exists and
    /// that the schema matches.
    pub async fn database<DB: Schema>(&self, name: &'_ str) -> Result<Database<DB>, Error> {
        let available_databases = self.data.available_databases.read().await;

        if let Some(stored_schema) = available_databases.get(name) {
            if stored_schema == &DB::schema_name()? {
                Ok(
                    Database::new(name.to_owned(), self.open_roots(name).await?, self.clone())
                        .await?,
                )
            } else {
                Err(Error::Core(bonsaidb_core::Error::SchemaMismatch {
                    database_name: name.to_owned(),
                    schema: DB::schema_name()?,
                    stored_schema: stored_schema.clone(),
                }))
            }
        } else {
            Err(Error::Core(bonsaidb_core::Error::DatabaseNotFound(
                name.to_owned(),
            )))
        }
    }

    pub(crate) async fn open_roots(&self, name: &str) -> Result<Context, Error> {
        let mut open_roots = self.data.open_roots.lock().await;
        if let Some(roots) = open_roots.get(name) {
            Ok(roots.clone())
        } else {
            let task_self = self.clone();
            let task_name = name.to_string();
            let roots = tokio::task::spawn_blocking(move || {
                let mut config = nebari::Config::new(task_self.data.path.join(task_name))
                    .cache(task_self.data.chunk_cache.clone())
                    .shared_thread_pool(&task_self.data.threadpool)
                    .file_manager(task_self.data.file_manager.clone());
                if let Some(key) = task_self.default_encryption_key() {
                    config = config.vault(TreeVault {
                        key: key.clone(),
                        vault: task_self.vault().clone(),
                    });
                }
                config.open().map_err(Error::from)
            })
            .await
            .unwrap()?;
            let context = Context::new(roots);

            open_roots.insert(name.to_owned(), context.clone());

            Ok(context)
        }
    }

    // pub async fn database_with_foreign_schema<DB: Schema>(
    //     &self,
    //     name: &'_ str,
    // ) -> Result<Database<DB>, Error> {
    //     let available_databases = self.data.available_databases.read().await;

    //     if let Some(stored_schema) = available_databases.get(name) {
    //         Ok(Database::new(name.to_owned(), self.clone()).await?)
    //     } else {
    //         Err(Error::Core(bonsaidb_core::Error::DatabaseNotFound(
    //             name.to_owned(),
    //         )))
    //     }
    // }

    pub(crate) fn tasks(&self) -> &'_ TaskManager {
        &self.data.tasks
    }

    pub(crate) fn check_view_integrity_on_database_open(&self) -> bool {
        self.data.check_view_integrity_on_database_open
    }

    pub(crate) fn relay(&self) -> &'_ Relay {
        &self.data.relay
    }

    fn validate_name(name: &str) -> Result<(), Error> {
        if name.chars().enumerate().all(|(index, c)| {
            c.is_ascii_alphanumeric()
                || (index == 0 && c == '_')
                || (index > 0 && (c == '.' || c == '-'))
        }) {
            Ok(())
        } else {
            Err(Error::Core(bonsaidb_core::Error::InvalidDatabaseName(
                name.to_owned(),
            )))
        }
    }

    /// Returns the administration database.
    #[allow(clippy::missing_panics_doc)]
    pub async fn admin(&self) -> Database<Admin> {
        Database::new(
            ADMIN_DATABASE_NAME,
            self.open_roots(ADMIN_DATABASE_NAME).await.unwrap(),
            self.clone(),
        )
        .await
        .unwrap()
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    /// Opens a database through a generic-free trait.
    pub async fn database_without_schema(
        &self,
        name: &str,
    ) -> Result<Box<dyn OpenDatabase>, Error> {
        let schema = match self
            .admin()
            .await
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .first()
        {
            Some(entry) => entry.value.clone(),
            None => {
                return Err(Error::Core(bonsaidb_core::Error::DatabaseNotFound(
                    name.to_string(),
                )))
            }
        };

        let mut schemas = self.data.schemas.write().await;
        if let Some(schema) = schemas.get_mut(&schema) {
            schema.open(name.to_string(), self.clone()).await
        } else {
            Err(Error::Core(bonsaidb_core::Error::SchemaNotRegistered(
                schema,
            )))
        }
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    /// Authenticates a user.
    pub async fn internal_login_with_password(
        &self,
        username: &str,
        login_request: bonsaidb_core::custodian_password::LoginRequest,
    ) -> Result<(Option<u64>, ServerLogin, LoginResponse), bonsaidb_core::Error> {
        let admin = self.admin().await;
        let config = PasswordConfig::load(&admin).await?;

        let (user_id, existing_password_hash) =
            if let Some(user) = User::load(username, &admin).await? {
                (Some(user.header.id), user.contents.password_hash)
            } else {
                (None, None)
            };

        let (login, response) = ServerLogin::login(&config, existing_password_hash, login_request)?;
        Ok((user_id, login, response))
    }

    async fn update_user_with_named_id<
        'user,
        'other,
        Col: NamedCollection,
        U: Into<NamedReference<'user>> + Send + Sync,
        O: Into<NamedReference<'other>> + Send + Sync,
        F: FnOnce(&mut CollectionDocument<User>, u64) -> bool,
    >(
        &self,
        user: U,
        other: O,
        callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        let user = user.into();
        let other = other.into();
        let admin = self.admin().await;
        let (user, other) =
            futures::try_join!(User::load(user, &admin), other.id::<Col, _>(&admin),)?;
        match (user, other) {
            (Some(mut user), Some(other)) => {
                if callback(&mut user, other) {
                    user.update(&admin).await?;
                }
                Ok(())
            }
            // TODO make this a generic not found with a name parameter.
            _ => Err(bonsaidb_core::Error::UserNotFound),
        }
    }
}

#[async_trait]
trait DatabaseOpener: Send + Sync + Debug {
    fn schematic(&self) -> &'_ Schematic;
    async fn open(&self, name: String, storage: Storage) -> Result<Box<dyn OpenDatabase>, Error>;
}

#[derive(Debug)]
struct StorageSchemaOpener<DB: Schema> {
    schematic: Schematic,
    _phantom: PhantomData<DB>,
}

impl<DB> StorageSchemaOpener<DB>
where
    DB: Schema,
{
    fn new() -> Result<Self, Error> {
        let schematic = DB::schematic()?;
        Ok(Self {
            schematic,
            _phantom: PhantomData::default(),
        })
    }
}

#[async_trait]
impl<DB> DatabaseOpener for StorageSchemaOpener<DB>
where
    DB: Schema,
{
    fn schematic(&self) -> &'_ Schematic {
        &self.schematic
    }

    async fn open(&self, name: String, storage: Storage) -> Result<Box<dyn OpenDatabase>, Error> {
        let roots = storage.open_roots(&name).await?;
        let db = Database::<DB>::new(name, roots, storage).await?;
        Ok(Box::new(db))
    }
}

/// Methods for accessing a database without the `Schema` type present.
#[doc(hidden)]
#[async_trait]
pub trait OpenDatabase: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &'_ dyn Any;

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
        permissions: &Permissions,
    ) -> Result<Option<Document<'static>>, bonsaidb_core::Error>;

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
        permissions: &Permissions,
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error>;

    async fn list_from_collection(
        &self,
        ids: Range<u64>,
        order: Sort,
        limit: Option<usize>,
        collection: &CollectionName,
        permissions: &Permissions,
    ) -> Result<Vec<Document<'static>>, bonsaidb_core::Error>;

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
        permissions: &Permissions,
    ) -> Result<Vec<OperationResult>, bonsaidb_core::Error>;

    async fn query(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, bonsaidb_core::Error>;

    async fn query_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        order: Sort,
        limit: Option<usize>,
        access_policy: AccessPolicy,
        permissions: &Permissions,
    ) -> Result<Vec<map::MappedSerialized>, bonsaidb_core::Error>;

    async fn reduce(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, bonsaidb_core::Error>;

    async fn reduce_grouped(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, bonsaidb_core::Error>;

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, bonsaidb_core::Error>;

    async fn last_transaction_id(&self) -> Result<Option<u64>, bonsaidb_core::Error>;

    async fn execute_key_operation(&self, op: KeyOperation)
        -> Result<Output, bonsaidb_core::Error>;

    async fn compact_collection(
        &self,
        collection: CollectionName,
    ) -> Result<(), bonsaidb_core::Error>;

    async fn compact_key_value_store(&self) -> Result<(), bonsaidb_core::Error>;

    async fn compact(&self) -> Result<(), bonsaidb_core::Error>;
}

#[async_trait]
impl ServerConnection for Storage {
    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(name, schema, only_if_needed))
    )]
    async fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        Self::validate_name(name)?;

        {
            let schemas = self.data.schemas.read().await;
            if !schemas.contains_key(&schema) {
                return Err(bonsaidb_core::Error::SchemaNotRegistered(schema));
            }
        }

        let mut available_databases = self.data.available_databases.write().await;
        let admin = self.admin().await;
        if !admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()
            .await?
            .is_empty()
        {
            if only_if_needed {
                return Ok(());
            }

            return Err(bonsaidb_core::Error::DatabaseNameAlreadyTaken(
                name.to_string(),
            ));
        }

        admin
            .collection::<DatabaseRecord>()
            .push(&admin::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .await?;
        available_databases.insert(name.to_string(), schema);

        Ok(())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(name)))]
    async fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        let admin = self.admin().await;
        let mut available_databases = self.data.available_databases.write().await;
        available_databases.remove(name);

        let mut open_roots = self.data.open_roots.lock().await;
        open_roots.remove(name);

        let database_folder = self.path().join(name);
        if database_folder.exists() {
            let file_manager = self.data.file_manager.clone();
            tokio::task::spawn_blocking(move || file_manager.delete_directory(&database_folder))
                .await
                .unwrap()
                .map_err(Error::Nebari)?;
        }

        if let Some(entry) = admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query_with_docs()
            .await?
            .first()
        {
            admin.delete::<DatabaseRecord>(&entry.document).await?;

            Ok(())
        } else {
            return Err(bonsaidb_core::Error::DatabaseNotFound(name.to_string()));
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn list_databases(&self) -> Result<Vec<admin::Database>, bonsaidb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases
            .iter()
            .map(|(name, schema)| admin::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .collect())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases.values().unique().cloned().collect())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(username)))]
    async fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        let result = self
            .admin()
            .await
            .collection::<User>()
            .push(&User::default_with_username(username))
            .await?;
        Ok(result.id)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, password_request)))]
    async fn set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_request: RegistrationRequest,
    ) -> Result<bonsaidb_core::custodian_password::RegistrationResponse, bonsaidb_core::Error> {
        let admin = self.admin().await;

        match User::load(user, &admin).await? {
            Some(mut doc) => {
                let config = PasswordConfig::load(&admin).await.unwrap();
                let (register, response) = ServerRegistration::register(&config, password_request)?;

                doc.contents.pending_password_change_state = Some(register);
                doc.update(&admin).await?;

                Ok(response)
            }
            None => Err(bonsaidb_core::Error::UserNotFound),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(user, password_finalization))
    )]
    async fn finish_set_user_password<'user, U: Into<NamedReference<'user>> + Send + Sync>(
        &self,
        user: U,
        password_finalization: RegistrationFinalization,
    ) -> Result<(), bonsaidb_core::Error> {
        let user = user.into();
        let admin = self.admin().await;
        match User::load(user, &admin).await? {
            Some(mut doc) => {
                if let Some(registration) = doc.contents.pending_password_change_state.take() {
                    let file = registration.finish(password_finalization)?;
                    doc.contents.password_hash = Some(file);
                    doc.update(&admin).await?;

                    Ok(())
                } else {
                    Err(bonsaidb_core::Error::Password(String::from(
                        "no existing state found",
                    )))
                }
            }
            None => Err(bonsaidb_core::Error::UserNotFound),
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, permission_group)))]
    async fn add_permission_group_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.update_user_with_named_id::<PermissionGroup, _, _, _>(
            user,
            permission_group,
            |user, permission_group_id| {
                if user.contents.groups.contains(&permission_group_id) {
                    false
                } else {
                    user.contents.groups.push(permission_group_id);
                    true
                }
            },
        )
        .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, permission_group)))]
    async fn remove_permission_group_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        permission_group: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.update_user_with_named_id::<PermissionGroup, _, _, _>(
            user,
            permission_group,
            |user, permission_group_id| {
                let old_len = user.contents.groups.len();
                user.contents.groups.retain(|id| id != &permission_group_id);
                old_len != user.contents.groups.len()
            },
        )
        .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, role)))]
    async fn add_role_to_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.update_user_with_named_id::<PermissionGroup, _, _, _>(user, role, |user, role_id| {
            if user.contents.roles.contains(&role_id) {
                false
            } else {
                user.contents.roles.push(role_id);
                true
            }
        })
        .await
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, role)))]
    async fn remove_role_from_user<
        'user,
        'group,
        U: Into<NamedReference<'user>> + Send + Sync,
        G: Into<NamedReference<'group>> + Send + Sync,
    >(
        &self,
        user: U,
        role: G,
    ) -> Result<(), bonsaidb_core::Error> {
        self.update_user_with_named_id::<Role, _, _, _>(user, role, |user, role_id| {
            let old_len = user.contents.roles.len();
            user.contents.roles.retain(|id| id != &role_id);
            old_len != user.contents.roles.len()
        })
        .await
    }
}

#[test]
fn name_validation_tests() {
    assert!(matches!(Storage::validate_name("azAZ09.-"), Ok(())));
    assert!(matches!(
        Storage::validate_name("_internal-names-work"),
        Ok(())
    ));
    assert!(matches!(
        Storage::validate_name("-alphaunmericfirstrequired"),
        Err(Error::Core(bonsaidb_core::Error::InvalidDatabaseName(_)))
    ));
    assert!(matches!(
        Storage::validate_name("\u{2661}"),
        Err(Error::Core(bonsaidb_core::Error::InvalidDatabaseName(_)))
    ));
}

/// The unique id of a [`Storage`] instance.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct StorageId(u64);

impl StorageId {
    /// Returns the id as a u64.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Debug for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // let formatted_length = format!();
        write!(f, "{:016x}", self.0)
    }
}

impl Display for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
