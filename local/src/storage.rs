use std::{any::Any, collections::HashMap, fmt::Debug, marker::PhantomData, path::Path, sync::Arc};

use async_trait::async_trait;
use itertools::Itertools;
#[cfg(feature = "pubsub")]
pub use pliantdb_core::circulate::Relay;
#[cfg(feature = "keyvalue")]
use pliantdb_core::kv::{KeyOperation, Output};
use pliantdb_core::{
    connection::{AccessPolicy, Connection, QueryKey},
    document::Document,
    networking,
    schema::{view::map, CollectionName, MappedValue, Schema, SchemaName, Schematic, ViewName},
    transaction::{Executed, OperationResult, Transaction},
};
use pliantdb_jobs::manager::Manager;
use tokio::sync::RwLock;

use crate::{
    admin::{
        database::{self, ByName, Database as DatabaseRecord},
        Admin,
    },
    error::ResultExt,
    tasks::TaskManager,
    Configuration, Database, Error,
};

#[cfg(feature = "keyvalue")]
pub mod kv;

/// A file-based, multi-database, multi-user database engine.
#[derive(Debug, Clone)]
pub struct Storage {
    data: Arc<Data>,
}

#[derive(Debug)]
struct Data {
    sled: sled::Db,
    pub(crate) tasks: TaskManager,
    schemas: RwLock<HashMap<SchemaName, Box<dyn DatabaseOpener>>>,
    available_databases: RwLock<HashMap<String, SchemaName>>,
    pub(crate) check_view_integrity_on_database_open: bool,
    #[cfg(feature = "keyvalue")]
    kv_expirer: std::sync::RwLock<Option<flume::Sender<kv::ExpirationUpdate>>>,
    #[cfg(feature = "pubsub")]
    relay: Relay,
}

impl Storage {
    /// Creates or opens a multi-database [`Storage`] with its data stored in `directory`.
    pub async fn open_local<P: AsRef<Path> + Send>(
        path: P,
        configuration: &Configuration,
    ) -> Result<Self, Error> {
        let owned_path = path.as_ref().to_owned();

        let manager = Manager::default();
        for _ in 0..configuration.workers.worker_count {
            manager.spawn_worker();
        }
        let tasks = TaskManager::new(manager);

        let check_view_integrity_on_database_open = configuration.views.check_integrity_on_open;
        let storage = tokio::task::spawn_blocking(move || {
            sled::open(owned_path)
                .map(|sled| Self {
                    data: Arc::new(Data {
                        sled,
                        tasks,
                        schemas: RwLock::default(),
                        available_databases: RwLock::default(),
                        check_view_integrity_on_database_open,
                        #[cfg(feature = "keyvalue")]
                        kv_expirer: std::sync::RwLock::default(),
                        #[cfg(feature = "pubsub")]
                        relay: Relay::default(),
                    }),
                })
                .map_err(Error::from)
        })
        .await
        .map_err(|err| pliantdb_core::Error::Database(err.to_string()))??;

        #[cfg(feature = "keyvalue")]
        storage
            .data
            .tasks
            .spawn_key_value_expiration_loader(&storage)
            .await;

        {
            let available_databases = storage
                .admin()
                .await
                .view::<ByName>()
                .query()
                .await?
                .into_iter()
                .map(|map| (map.key, map.value))
                .collect();
            let mut storage_databases = storage.data.available_databases.write().await;
            *storage_databases = available_databases;
        }

        Ok(storage)
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
            Err(Error::Core(pliantdb_core::Error::SchemaAlreadyRegistered(
                DB::schema_name()?,
            )))
        }
    }

    /// Retrieves a database. This function only verifies that the database exists
    pub async fn database<DB: Schema>(&self, name: &'_ str) -> Result<Database<DB>, Error> {
        let available_databases = self.data.available_databases.read().await;

        if let Some(stored_schema) = available_databases.get(name) {
            if stored_schema == &DB::schema_name()? {
                Ok(Database::new(name.to_owned(), self.clone()).await?)
            } else {
                Err(Error::Core(pliantdb_core::Error::SchemaMismatch {
                    database_name: name.to_owned(),
                    schema: DB::schema_name()?,
                    stored_schema: stored_schema.clone(),
                }))
            }
        } else {
            Err(Error::Core(pliantdb_core::Error::DatabaseNotFound(
                name.to_owned(),
            )))
        }
    }

    pub(crate) fn sled(&self) -> &'_ sled::Db {
        &self.data.sled
    }

    pub(crate) fn tasks(&self) -> &'_ TaskManager {
        &self.data.tasks
    }

    pub(crate) fn check_view_integrity_on_database_open(&self) -> bool {
        self.data.check_view_integrity_on_database_open
    }

    #[cfg(feature = "pubsub")]
    pub(crate) fn relay(&self) -> &'_ Relay {
        &self.data.relay
    }

    #[cfg(feature = "keyvalue")]
    pub(crate) fn update_key_expiration(&self, update: kv::ExpirationUpdate) {
        {
            let sender = self.data.kv_expirer.read().unwrap();
            if let Some(sender) = sender.as_ref() {
                let _ = sender.send(update);
                return;
            }
        }

        // If we fall through, we need to initialize the expirer task
        let mut sender = self.data.kv_expirer.write().unwrap();
        if sender.is_none() {
            let (kv_sender, kv_expirer_receiver) = flume::unbounded();
            let thread_sled = self.data.sled.clone();
            tokio::task::spawn_blocking(move || {
                kv::expiration_thread(kv_expirer_receiver, thread_sled)
            });
            *sender = Some(kv_sender);
        }

        let _ = sender.as_ref().unwrap().send(update);
    }

    fn validate_name(name: &str) -> Result<(), Error> {
        if name
            .chars()
            .enumerate()
            .all(|(index, c)| c.is_ascii_alphanumeric() || (index > 0 && (c == '.' || c == '-')))
        {
            Ok(())
        } else {
            Err(Error::Core(pliantdb_core::Error::InvalidDatabaseName(
                name.to_owned(),
            )))
        }
    }

    async fn admin(&self) -> Database<Admin> {
        Database::new("admin", self.clone()).await.unwrap()
    }
}

#[async_trait]
trait DatabaseOpener: Send + Sync + Debug {
    fn schematic(&self) -> &'_ Schematic;
    async fn open(
        &self,
        name: String,
        storage: Storage,
    ) -> Result<Arc<Box<dyn OpenDatabase>>, Error>;
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

    async fn open(
        &self,
        name: String,
        storage: Storage,
    ) -> Result<Arc<Box<dyn OpenDatabase>>, Error> {
        let db = Database::<DB>::new(name, storage).await?;
        Ok(Arc::new(Box::new(db)))
    }
}

#[async_trait]
pub trait OpenDatabase: Send + Sync + Debug + 'static {
    fn as_any(&self) -> &'_ dyn Any;

    async fn get_from_collection_id(
        &self,
        id: u64,
        collection: &CollectionName,
    ) -> Result<Option<Document<'static>>, pliantdb_core::Error>;

    async fn get_multiple_from_collection_id(
        &self,
        ids: &[u64],
        collection: &CollectionName,
    ) -> Result<Vec<Document<'static>>, pliantdb_core::Error>;

    async fn apply_transaction(
        &self,
        transaction: Transaction<'static>,
    ) -> Result<Vec<OperationResult>, pliantdb_core::Error>;

    async fn query(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<map::Serialized>, pliantdb_core::Error>;

    async fn query_with_docs(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<networking::MappedDocument>, pliantdb_core::Error>;

    async fn reduce(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<u8>, pliantdb_core::Error>;

    async fn reduce_grouped(
        &self,
        view: &ViewName,
        key: Option<QueryKey<Vec<u8>>>,
        access_policy: AccessPolicy,
    ) -> Result<Vec<MappedValue<Vec<u8>, Vec<u8>>>, pliantdb_core::Error>;

    async fn list_executed_transactions(
        &self,
        starting_id: Option<u64>,
        result_limit: Option<usize>,
    ) -> Result<Vec<Executed<'static>>, pliantdb_core::Error>;

    async fn last_transaction_id(&self) -> Result<Option<u64>, pliantdb_core::Error>;

    #[cfg(feature = "keyvalue")]
    async fn execute_key_operation(&self, op: KeyOperation)
        -> Result<Output, pliantdb_core::Error>;
}

#[async_trait]
impl networking::ServerConnection for Storage {
    async fn create_database(
        &self,
        name: &str,
        schema: SchemaName,
    ) -> Result<(), pliantdb_core::Error> {
        Self::validate_name(name)?;

        {
            let schemas = self.data.schemas.read().await;
            if !schemas.contains_key(&schema) {
                return Err(pliantdb_core::Error::SchemaNotRegistered(schema));
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
            return Err(pliantdb_core::Error::DatabaseNameAlreadyTaken(
                name.to_string(),
            ));
        }

        admin
            .collection::<DatabaseRecord>()
            .push(&networking::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .await?;
        available_databases.insert(name.to_string(), schema);

        Ok(())
    }

    async fn delete_database(&self, name: &str) -> Result<(), pliantdb_core::Error> {
        let mut available_databases = self.data.available_databases.write().await;

        let prefix = format!("{}::", name);
        let sled = self.data.sled.clone();
        tokio::task::spawn_blocking(move || {
            for name in sled.tree_names() {
                if name.starts_with(prefix.as_bytes()) {
                    sled.drop_tree(name)?;
                }
            }
            Result::<_, sled::Error>::Ok(())
        })
        .await
        .unwrap()
        .map_err_to_core()?;

        let admin = self.admin().await;
        if let Some(entry) = admin
            .view::<database::ByName>()
            .with_key(name.to_ascii_lowercase())
            .query_with_docs()
            .await?
            .first()
        {
            admin.delete(&entry.document).await?;
            available_databases.remove(name);

            Ok(())
        } else {
            return Err(pliantdb_core::Error::DatabaseNotFound(name.to_string()));
        }
    }

    async fn list_databases(&self) -> Result<Vec<networking::Database>, pliantdb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases
            .iter()
            .map(|(name, schema)| networking::Database {
                name: name.to_string(),
                schema: schema.clone(),
            })
            .collect())
    }

    async fn list_available_schemas(&self) -> Result<Vec<SchemaName>, pliantdb_core::Error> {
        let available_databases = self.data.available_databases.read().await;
        Ok(available_databases.values().unique().cloned().collect())
    }
}

#[test]
fn name_validation_tests() {
    assert!(matches!(Storage::validate_name("azAZ09.-"), Ok(())));
    assert!(matches!(
        Storage::validate_name(".alphaunmericfirstrequired"),
        Err(Error::Core(pliantdb_core::Error::InvalidDatabaseName(_)))
    ));
    assert!(matches!(
        Storage::validate_name("-alphaunmericfirstrequired"),
        Err(Error::Core(pliantdb_core::Error::InvalidDatabaseName(_)))
    ));
    assert!(matches!(
        Storage::validate_name("\u{2661}"),
        Err(Error::Core(pliantdb_core::Error::InvalidDatabaseName(_)))
    ));
}
