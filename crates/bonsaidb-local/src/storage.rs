use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    fs::{self, File},
    io::{Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

pub use bonsaidb_core::circulate::Relay;
#[cfg(feature = "password-hashing")]
use bonsaidb_core::connection::{Authenticated, Authentication};
#[cfg(any(feature = "encryption", feature = "compression"))]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::{
    admin::{
        self,
        database::{self, ByName, Database as DatabaseRecord},
        Admin, ADMIN_DATABASE_NAME,
    },
    connection::{self, Connection, StorageConnection},
    schema::{Schema, SchemaName, Schematic},
};
#[cfg(feature = "multiuser")]
use bonsaidb_core::{
    admin::{user::User, PermissionGroup, Role},
    document::CollectionDocument,
    schema::{Nameable, NamedCollection},
};
use itertools::Itertools;
use nebari::{
    io::{
        any::{AnyFile, AnyFileManager},
        FileManager,
    },
    ChunkCache, ThreadPool,
};
use parking_lot::{Mutex, RwLock};
use rand::{thread_rng, Rng};

#[cfg(feature = "compression")]
use crate::config::Compression;
#[cfg(feature = "encryption")]
use crate::vault::{self, LocalVaultKeyStorage, Vault};
use crate::{
    config::{KeyValuePersistence, StorageConfiguration},
    database::Context,
    tasks::{manager::Manager, TaskManager},
    Database, Error,
};

#[cfg(feature = "password-hashing")]
mod argon;

mod backup;
pub use backup::{AnyBackupLocation, BackupLocation};

/// A file-based, multi-database, multi-user database engine.
///
/// ## Converting from `Database::open` to `Storage::open`
///
/// [`Database::open`](Database::open) is a simple method that uses `Storage` to
/// create a database named `default` with the schema provided. These two ways
/// of opening the database are the same:
///
/// ```rust
/// // `bonsaidb_core` is re-exported to `bonsaidb::core` or `bonsaidb_local::core`.
/// use bonsaidb_core::{connection::StorageConnection, schema::Schema};
/// // `bonsaidb_local` is re-exported to `bonsaidb::local` if using the omnibus crate.
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
///     Database, Storage,
/// };
/// # async fn open<MySchema: Schema>() -> anyhow::Result<()> {
/// // This creates a Storage instance, creates a database, and returns it.
/// let db = Database::open::<MySchema>(StorageConfiguration::new("my-db.bonsaidb")).await?;
///
/// // This is the equivalent code being executed:
/// let storage =
///     Storage::open(StorageConfiguration::new("my-db.bonsaidb").with_schema::<MySchema>()?)
///         .await?;
/// storage.create_database::<MySchema>("default", true).await?;
/// let db = storage.database::<MySchema>("default").await?;
/// #     Ok(())
/// # }
/// ```
///
/// ## Using multiple databases
///
/// This example shows how to use `Storage` to create and use multiple databases
/// with multiple schemas:
///
/// ```rust
/// use bonsaidb_core::{
///     connection::StorageConnection,
///     schema::{Collection, Schema},
/// };
/// use bonsaidb_local::{
///     config::{Builder, StorageConfiguration},
///     Storage,
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
/// let storage = Storage::open(
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
pub struct Storage {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    id: StorageId,
    path: PathBuf,
    parallelization: usize,
    threadpool: ThreadPool<AnyFile>,
    file_manager: AnyFileManager,
    pub(crate) tasks: TaskManager,
    schemas: RwLock<HashMap<SchemaName, Arc<dyn DatabaseOpener>>>,
    available_databases: RwLock<HashMap<String, SchemaName>>,
    open_roots: Mutex<HashMap<String, Context>>,
    #[cfg(feature = "password-hashing")]
    argon: argon::Hasher,
    #[cfg(feature = "encryption")]
    pub(crate) vault: Arc<Vault>,
    #[cfg(feature = "encryption")]
    default_encryption_key: Option<KeyId>,
    #[cfg(any(feature = "compression", feature = "encryption"))]
    tree_vault: Option<TreeVault>,
    pub(crate) key_value_persistence: KeyValuePersistence,
    chunk_cache: ChunkCache,
    pub(crate) check_view_integrity_on_database_open: bool,
    relay: Relay,
}

impl Storage {
    /// Creates or opens a multi-database [`Storage`] with its data stored in `directory`.
    pub fn open(configuration: StorageConfiguration) -> Result<Self, Error> {
        let owned_path = configuration
            .path
            .clone()
            .unwrap_or_else(|| PathBuf::from("db.bonsaidb"));
        let file_manager = if configuration.memory_only {
            AnyFileManager::memory()
        } else {
            AnyFileManager::std()
        };

        let manager = Manager::default();
        for _ in 0..configuration.workers.worker_count {
            manager.spawn_worker();
        }
        let tasks = TaskManager::new(manager);

        fs::create_dir_all(&owned_path)?;

        let id = Self::lookup_or_create_id(&configuration, &owned_path)?;

        #[cfg(feature = "encryption")]
        let vault = {
            let vault_key_storage = match configuration.vault_key_storage {
                Some(storage) => storage,
                None => Arc::new(
                    LocalVaultKeyStorage::new(owned_path.join("vault-keys"))
                        .map_err(|err| Error::Vault(vault::Error::Initializing(err.to_string())))?,
                ),
            };

            Arc::new(Vault::initialize(id, &owned_path, vault_key_storage)?)
        };

        let parallelization = configuration.workers.parallelization;
        let check_view_integrity_on_database_open = configuration.views.check_integrity_on_open;
        let key_value_persistence = configuration.key_value_persistence;
        #[cfg(feature = "password-hashing")]
        let argon = argon::Hasher::new(configuration.argon);
        #[cfg(feature = "encryption")]
        let default_encryption_key = configuration.default_encryption_key;
        #[cfg(all(feature = "compression", feature = "encryption"))]
        let tree_vault = TreeVault::new_if_needed(
            default_encryption_key.clone(),
            &vault,
            configuration.default_compression,
        );
        #[cfg(all(not(feature = "compression"), feature = "encryption"))]
        let tree_vault = TreeVault::new_if_needed(default_encryption_key.clone(), &vault);
        #[cfg(all(feature = "compression", not(feature = "encryption")))]
        let tree_vault = TreeVault::new_if_needed(configuration.default_compression);

        let storage = Self {
            data: Arc::new(Data {
                id,
                tasks,
                parallelization,
                #[cfg(feature = "password-hashing")]
                argon,
                #[cfg(feature = "encryption")]
                vault,
                #[cfg(feature = "encryption")]
                default_encryption_key,
                #[cfg(any(feature = "compression", feature = "encryption"))]
                tree_vault,
                path: owned_path,
                file_manager,
                chunk_cache: ChunkCache::new(2000, 160_384),
                threadpool: ThreadPool::new(parallelization),
                schemas: RwLock::new(configuration.initial_schemas),
                available_databases: RwLock::default(),
                open_roots: Mutex::default(),
                key_value_persistence,
                check_view_integrity_on_database_open,
                relay: Relay::default(),
            }),
        };

        storage.cache_available_databases()?;

        storage.create_admin_database_if_needed()?;

        Ok(storage)
    }

    /// Returns the path of the database storage.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.data.path
    }

    fn lookup_or_create_id(
        configuration: &StorageConfiguration,
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
                        .and_then(|mut f| {
                            let mut bytes = Vec::new();
                            f.read_to_end(&mut bytes).map(|_| bytes)
                        })
                        .expect("error reading server-id file"),
                )
                .expect("server-id contains invalid data");

                existing_id.parse().expect("server-id isn't numeric")
            } else {
                let id = { thread_rng().gen::<u64>() };
                File::create(id_path)
                    .and_then(|mut file| {
                        let id = id.to_string();
                        file.write_all(id.as_bytes())
                    })
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

    fn cache_available_databases(&self) -> Result<(), Error> {
        let available_databases = self
            .admin()
            .view::<ByName>()
            .query()?
            .into_iter()
            .map(|map| (map.key, map.value))
            .collect();
        let mut storage_databases = self.data.available_databases.write();
        *storage_databases = available_databases;
        Ok(())
    }

    fn create_admin_database_if_needed(&self) -> Result<(), Error> {
        self.register_schema::<Admin>()?;
        match self.database::<Admin>(ADMIN_DATABASE_NAME) {
            Ok(_) => {}
            Err(bonsaidb_core::Error::DatabaseNotFound(_)) => {
                self.create_database::<Admin>(ADMIN_DATABASE_NAME, true)?;
            }
            Err(err) => return Err(Error::Core(err)),
        }
        Ok(())
    }

    /// Returns the unique id of the server.
    ///
    /// This value is set from the [`StorageConfiguration`] or randomly
    /// generated when creating a server. It shouldn't be changed after a server
    /// is in use, as doing can cause issues. For example, the vault that
    /// manages encrypted storage uses the server ID to store the vault key. If
    /// the server ID changes, the vault key storage will need to be updated
    /// with the new server ID.
    #[must_use]
    pub fn unique_id(&self) -> StorageId {
        self.data.id
    }

    #[must_use]
    pub(crate) fn parallelization(&self) -> usize {
        self.data.parallelization
    }

    #[must_use]
    #[cfg(feature = "encryption")]
    pub(crate) fn vault(&self) -> &Arc<Vault> {
        &self.data.vault
    }

    #[must_use]
    #[cfg(any(feature = "encryption", feature = "compression"))]
    pub(crate) fn tree_vault(&self) -> Option<&TreeVault> {
        self.data.tree_vault.as_ref()
    }

    #[must_use]
    #[cfg(feature = "encryption")]
    pub(crate) fn default_encryption_key(&self) -> Option<&KeyId> {
        self.data.default_encryption_key.as_ref()
    }

    #[must_use]
    #[cfg(all(feature = "compression", not(feature = "encryption")))]
    #[allow(clippy::unused_self)]
    pub(crate) fn default_encryption_key(&self) -> Option<&KeyId> {
        None
    }

    /// Registers a schema for use within the server.
    pub fn register_schema<DB: Schema>(&self) -> Result<(), Error> {
        let mut schemas = self.data.schemas.write();
        if schemas
            .insert(
                DB::schema_name(),
                Arc::new(StorageSchemaOpener::<DB>::new()?),
            )
            .is_none()
        {
            Ok(())
        } else {
            Err(Error::Core(bonsaidb_core::Error::SchemaAlreadyRegistered(
                DB::schema_name(),
            )))
        }
    }

    #[cfg_attr(
        not(any(feature = "encryption", feature = "compression")),
        allow(unused_mut)
    )]
    pub(crate) fn open_roots(&self, name: &str) -> Result<Context, Error> {
        let mut open_roots = self.data.open_roots.lock();
        if let Some(roots) = open_roots.get(name) {
            Ok(roots.clone())
        } else {
            let task_self = self.clone();
            let task_name = name.to_string();

            let mut config = nebari::Config::new(task_self.data.path.join(task_name))
                .file_manager(task_self.data.file_manager.clone())
                .cache(task_self.data.chunk_cache.clone())
                .shared_thread_pool(&task_self.data.threadpool);

            #[cfg(any(feature = "encryption", feature = "compression"))]
            if let Some(vault) = task_self.data.tree_vault.clone() {
                config = config.vault(vault);
            }

            let roots = config.open().map_err(Error::from)?;
            let context = Context::new(roots, self.data.key_value_persistence.clone());

            open_roots.insert(name.to_owned(), context.clone());

            Ok(context)
        }
    }

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
    // TODO this should probably move to StorageConnection instaed of creating an async version
    pub fn admin(&self) -> Database {
        Database::new::<Admin, _>(
            ADMIN_DATABASE_NAME,
            self.open_roots(ADMIN_DATABASE_NAME).unwrap(),
            self,
        )
        .unwrap()
    }

    /// Opens a database through a generic-free trait.
    pub(crate) fn database_without_schema(&self, name: &str) -> Result<Database, Error> {
        // TODO switch to upgradable read now that we are on parking_lot
        let schema = {
            let available_databases = self.data.available_databases.read();
            available_databases
                .get(name)
                .ok_or_else(|| {
                    Error::Core(bonsaidb_core::Error::DatabaseNotFound(name.to_string()))
                })?
                .clone()
        };

        let mut schemas = self.data.schemas.write();
        if let Some(schema) = schemas.get_mut(&schema) {
            let db = schema.open(name.to_string(), self)?;
            Ok(db)
        } else {
            Err(Error::Core(bonsaidb_core::Error::SchemaNotRegistered(
                schema,
            )))
        }
    }

    #[cfg(feature = "internal-apis")]
    #[doc(hidden)]
    /// Opens a database through a generic-free trait.
    pub fn database_without_schema_internal(&self, name: &str) -> Result<Database, Error> {
        self.database_without_schema(name)
    }

    #[cfg(feature = "multiuser")]
    fn update_user_with_named_id<
        'user,
        'other,
        Col: NamedCollection<PrimaryKey = u64>,
        U: Nameable<'user, u64> + Send + Sync,
        O: Nameable<'other, u64> + Send + Sync,
        F: FnOnce(&mut CollectionDocument<User>, u64) -> bool,
    >(
        &self,
        user: U,
        other: O,
        callback: F,
    ) -> Result<(), bonsaidb_core::Error> {
        let admin = self.admin();
        let other = other.name()?;
        let user = User::load(user.name()?, &admin)?;
        let other = other.id::<Col, _>(&admin)?;
        match (user, other) {
            (Some(mut user), Some(other)) => {
                if callback(&mut user, other) {
                    user.update(&admin)?;
                }
                Ok(())
            }
            // TODO make this a generic not found with a name parameter.
            _ => Err(bonsaidb_core::Error::UserNotFound),
        }
    }
}

pub trait DatabaseOpener: Send + Sync + Debug {
    fn schematic(&self) -> &'_ Schematic;
    fn open(&self, name: String, storage: &Storage) -> Result<Database, Error>;
}

#[derive(Debug)]
pub struct StorageSchemaOpener<DB: Schema> {
    schematic: Schematic,
    _phantom: PhantomData<DB>,
}

impl<DB> StorageSchemaOpener<DB>
where
    DB: Schema,
{
    pub fn new() -> Result<Self, Error> {
        let schematic = DB::schematic()?;
        Ok(Self {
            schematic,
            _phantom: PhantomData::default(),
        })
    }
}

impl<DB> DatabaseOpener for StorageSchemaOpener<DB>
where
    DB: Schema,
{
    fn schematic(&self) -> &'_ Schematic {
        &self.schematic
    }

    fn open(&self, name: String, storage: &Storage) -> Result<Database, Error> {
        let roots = storage.open_roots(&name)?;
        let db = Database::new::<DB, _>(name, roots, storage)?;
        Ok(db)
    }
}

impl StorageConnection for Storage {
    type Database = Database;

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(name, schema, only_if_needed))
    )]
    fn create_database_with_schema(
        &self,
        name: &str,
        schema: SchemaName,
        only_if_needed: bool,
    ) -> Result<(), bonsaidb_core::Error> {
        Self::validate_name(name)?;

        // TODO upgrade read
        {
            let schemas = self.data.schemas.read();
            if !schemas.contains_key(&schema) {
                return Err(bonsaidb_core::Error::SchemaNotRegistered(schema));
            }
        }

        let mut available_databases = self.data.available_databases.write();
        let admin = self.admin();
        if !admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()?
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
            })?;
        available_databases.insert(name.to_string(), schema);

        Ok(())
    }

    fn database<DB: Schema>(&self, name: &str) -> Result<Self::Database, bonsaidb_core::Error> {
        let db = self.database_without_schema(name)?;
        if db.data.schema.name == DB::schema_name() {
            Ok(db)
        } else {
            Err(bonsaidb_core::Error::SchemaMismatch {
                database_name: name.to_owned(),
                schema: DB::schema_name(),
                stored_schema: db.data.schema.name.clone(),
            })
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(name)))]
    fn delete_database(&self, name: &str) -> Result<(), bonsaidb_core::Error> {
        let admin = self.admin();
        let mut available_databases = self.data.available_databases.write();
        available_databases.remove(name);

        let mut open_roots = self.data.open_roots.lock();
        open_roots.remove(name);

        let database_folder = self.path().join(name);
        if database_folder.exists() {
            let file_manager = self.data.file_manager.clone();
            file_manager
                .delete_directory(&database_folder)
                .map_err(Error::Nebari)?;
        }

        if let Some(entry) = admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query()?
            .first()
        {
            admin.delete::<DatabaseRecord, _>(&entry.source)?;

            Ok(())
        } else {
            Err(bonsaidb_core::Error::DatabaseNotFound(name.to_string()))
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn list_databases(&self) -> Result<Vec<connection::Database>, bonsaidb_core::Error> {
        let available_databases = self.data.available_databases.read();
        Ok(available_databases
            .iter()
            .map(|(name, schema)| connection::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .collect())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    fn list_available_schemas(&self) -> Result<Vec<SchemaName>, bonsaidb_core::Error> {
        let available_databases = self.data.available_databases.read();
        Ok(available_databases.values().unique().cloned().collect())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(username)))]
    #[cfg(feature = "multiuser")]
    fn create_user(&self, username: &str) -> Result<u64, bonsaidb_core::Error> {
        let result = self
            .admin()
            .collection::<User>()
            .push(&User::default_with_username(username))?;
        Ok(result.id)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user)))]
    #[cfg(feature = "multiuser")]
    fn delete_user<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
    ) -> Result<(), bonsaidb_core::Error> {
        let admin = self.admin();
        let doc = User::load(user, &admin)?.ok_or(bonsaidb_core::Error::UserNotFound)?;
        doc.delete(&admin)?;

        Ok(())
    }

    #[cfg(feature = "password-hashing")]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, password)))]
    fn set_user_password<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        password: bonsaidb_core::connection::SensitiveString,
    ) -> Result<(), bonsaidb_core::Error> {
        let admin = self.admin();
        let mut user = User::load(user, &admin)?.ok_or(bonsaidb_core::Error::UserNotFound)?;
        user.contents.argon_hash = Some(self.data.argon.hash(user.header.id, password)?);
        user.update(&admin)
    }

    #[cfg(all(feature = "multiuser", feature = "password-hashing"))]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user)))]
    fn authenticate<'user, U: Nameable<'user, u64> + Send + Sync>(
        &self,
        user: U,
        authentication: Authentication,
    ) -> Result<Authenticated, bonsaidb_core::Error> {
        let admin = self.admin();
        let user = User::load(user, &admin)?.ok_or(bonsaidb_core::Error::InvalidCredentials)?;
        match authentication {
            Authentication::Password(password) => {
                let saved_hash = user
                    .contents
                    .argon_hash
                    .clone()
                    .ok_or(bonsaidb_core::Error::InvalidCredentials)?;

                self.data
                    .argon
                    .verify(user.header.id, password, saved_hash)?;
                let permissions = user.contents.effective_permissions(&admin)?;
                Ok(Authenticated {
                    user_id: user.header.id,
                    permissions,
                })
            }
        }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, permission_group)))]
    #[cfg(feature = "multiuser")]
    fn add_permission_group_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
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
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, permission_group)))]
    #[cfg(feature = "multiuser")]
    fn remove_permission_group_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
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
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, role)))]
    #[cfg(feature = "multiuser")]
    fn add_role_to_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
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
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(user, role)))]
    #[cfg(feature = "multiuser")]
    fn remove_role_from_user<
        'user,
        'group,
        U: Nameable<'user, u64> + Send + Sync,
        G: Nameable<'group, u64> + Send + Sync,
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

#[derive(Debug, Clone)]
#[cfg(any(feature = "compression", feature = "encryption"))]
pub(crate) struct TreeVault {
    #[cfg(feature = "compression")]
    compression: Option<Compression>,
    #[cfg(feature = "encryption")]
    pub key: Option<KeyId>,
    #[cfg(feature = "encryption")]
    pub vault: Arc<Vault>,
}

#[cfg(all(feature = "compression", feature = "encryption"))]
impl TreeVault {
    pub(crate) fn new_if_needed(
        key: Option<KeyId>,
        vault: &Arc<Vault>,
        compression: Option<Compression>,
    ) -> Option<Self> {
        if key.is_none() && compression.is_none() {
            None
        } else {
            Some(Self {
                key,
                compression,
                vault: vault.clone(),
            })
        }
    }

    fn header(&self, compressed: bool) -> u8 {
        let mut bits = if self.key.is_some() { 0b1000_0000 } else { 0 };

        if compressed {
            if let Some(compression) = self.compression {
                bits |= compression as u8;
            }
        }

        bits
    }
}

#[cfg(all(feature = "compression", feature = "encryption"))]
impl nebari::Vault for TreeVault {
    type Error = Error;

    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        use std::borrow::Cow;

        // TODO this allocates too much. The vault should be able to do an
        // in-place encryption operation so that we can use a single buffer.
        let mut includes_compression = false;
        let compressed = match (payload.len(), self.compression) {
            (128..=usize::MAX, Some(Compression::Lz4)) => {
                includes_compression = true;
                Cow::Owned(lz4_flex::block::compress_prepend_size(payload))
            }
            _ => Cow::Borrowed(payload),
        };

        let mut complete = if let Some(key) = &self.key {
            self.vault.encrypt_payload(key, &compressed, None)?
        } else {
            compressed.into_owned()
        };

        let header = self.header(includes_compression);
        if header != 0 {
            let header = [b't', b'r', b'v', header];
            complete.splice(0..0, header);
        }

        Ok(complete)
    }

    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        use std::borrow::Cow;

        if payload.len() >= 4 && &payload[0..3] == b"trv" {
            let header = payload[3];
            let payload = &payload[4..];
            let encrypted = (header & 0b1000_0000) != 0;
            let compression = header & 0b0111_1111;
            let decrypted = if encrypted {
                Cow::Owned(self.vault.decrypt_payload(payload, None)?)
            } else {
                Cow::Borrowed(payload)
            };
            #[allow(clippy::single_match)] // Make it an error when we add a new algorithm
            return Ok(match Compression::from_u8(compression) {
                Some(Compression::Lz4) => {
                    lz4_flex::block::decompress_size_prepended(&decrypted).map_err(Error::from)?
                }
                None => decrypted.into_owned(),
            });
        }
        self.vault.decrypt_payload(payload, None)
    }
}

#[cfg(all(feature = "compression", not(feature = "encryption")))]
impl TreeVault {
    pub(crate) fn new_if_needed(compression: Option<Compression>) -> Option<Self> {
        compression.map(|compression| Self {
            compression: Some(compression),
        })
    }
}

#[cfg(all(feature = "compression", not(feature = "encryption")))]
impl nebari::Vault for TreeVault {
    type Error = Error;

    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(match (payload.len(), self.compression) {
            (128..=usize::MAX, Some(Compression::Lz4)) => {
                let mut destination =
                    vec![0; lz4_flex::block::get_maximum_output_size(payload.len()) + 8];
                let compressed_length =
                    lz4_flex::block::compress_into(payload, &mut destination[8..])
                        .expect("lz4-flex documents this shouldn't fail");
                destination.truncate(compressed_length + 8);
                destination[0..4].copy_from_slice(&[b't', b'r', b'v', Compression::Lz4 as u8]);
                // to_le_bytes() makes it compatible with lz4-flex decompress_size_prepended.
                let uncompressed_length =
                    u32::try_from(payload.len()).expect("nebari doesn't support >32 bit blocks");
                destination[4..8].copy_from_slice(&uncompressed_length.to_le_bytes());
                destination
            }
            // TODO this shouldn't copy
            _ => payload.to_vec(),
        })
    }

    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if payload.len() >= 4 && &payload[0..3] == b"trv" {
            let header = payload[3];
            let payload = &payload[4..];
            let encrypted = (header & 0b1000_0000) != 0;
            let compression = header & 0b0111_1111;
            if encrypted {
                return Err(Error::EncryptionDisabled);
            }

            #[allow(clippy::single_match)] // Make it an error when we add a new algorithm
            return Ok(match Compression::from_u8(compression) {
                Some(Compression::Lz4) => {
                    lz4_flex::block::decompress_size_prepended(payload).map_err(Error::from)?
                }
                None => payload.to_vec(),
            });
        }
        Ok(payload.to_vec())
    }
}

#[cfg(all(not(feature = "compression"), feature = "encryption"))]
impl TreeVault {
    pub(crate) fn new_if_needed(key: Option<KeyId>, vault: &Arc<Vault>) -> Option<Self> {
        key.map(|key| Self {
            key: Some(key),
            vault: vault.clone(),
        })
    }

    #[allow(dead_code)] // This implementation is sort of documentation for what it would be. But our Vault payload already can detect if a parsing error occurs, so we don't need a header if only encryption is enabled.
    fn header(&self) -> u8 {
        if self.key.is_some() {
            0b1000_0000
        } else {
            0
        }
    }
}

#[cfg(all(not(feature = "compression"), feature = "encryption"))]
impl nebari::Vault for TreeVault {
    type Error = Error;

    fn encrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        if let Some(key) = &self.key {
            self.vault.encrypt_payload(key, payload, None)
        } else {
            // TODO does this need to copy?
            Ok(payload.to_vec())
        }
    }

    fn decrypt(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        self.vault.decrypt_payload(payload, None)
    }
}
