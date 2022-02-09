use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(feature = "encryption")]
use bonsaidb_core::document::KeyId;
use bonsaidb_core::schema::{Schema, SchemaName};
use sysinfo::{RefreshKind, System, SystemExt};

#[cfg(feature = "encryption")]
use crate::vault::AnyVaultKeyStorage;
use crate::{
    storage::{DatabaseOpener, StorageSchemaOpener},
    Error,
};

#[cfg(feature = "password-hashing")]
mod argon;
#[cfg(feature = "password-hashing")]
pub use argon::*;

/// Configuration options for [`Storage`](crate::storage::Storage).
#[derive(Debug)]
#[non_exhaustive]
pub struct StorageConfiguration {
    /// The path to the database. Defaults to `db.bonsaidb` if not specified.
    pub path: Option<PathBuf>,

    /// Prevents storing data on the disk. This is intended for testing purposes
    /// primarily. Keep in mind that the underlying storage format is
    /// append-only.
    pub memory_only: bool,

    /// The unique id of the server. If not specified, the server will randomly
    /// generate a unique id on startup. If the server generated an id and this
    /// value is subsequently set, the generated id will be overridden by the
    /// one specified here.
    pub unique_id: Option<u64>,

    /// The vault key storage to use. If not specified,
    /// [`LocalVaultKeyStorage`](crate::vault::LocalVaultKeyStorage) will be
    /// used with the server's data folder as the path. This is **incredibly
    /// insecure and should not be used outside of testing**.
    ///
    /// For secure encryption, it is important to store the vault keys in a
    /// location that is separate from the database. If the keys are on the same
    /// hardware as the encrypted content, anyone with access to the disk will
    /// be able to decrypt the stored data.
    #[cfg(feature = "encryption")]
    pub vault_key_storage: Option<Box<dyn AnyVaultKeyStorage>>,

    /// The default encryption key for the database. If specified, all documents
    /// will be stored encrypted at-rest using the key specified. Having this
    /// key specified will also encrypt views. Without this, views will be
    /// stored unencrypted.
    #[cfg(feature = "encryption")]
    pub default_encryption_key: Option<KeyId>,

    /// Configuration options related to background tasks.
    pub workers: Tasks,

    /// Configuration options related to views.
    pub views: Views,

    /// Controls how the key-value store persists keys, on a per-database basis.
    pub key_value_persistence: KeyValuePersistence,

    /// Password hashing configuration.
    #[cfg(feature = "password-hashing")]
    pub argon: ArgonConfiguration,

    pub(crate) initial_schemas: HashMap<SchemaName, Box<dyn DatabaseOpener>>,
}

impl Default for StorageConfiguration {
    fn default() -> Self {
        let system_specs = RefreshKind::new().with_cpu().with_memory();
        let mut system = System::new_with_specifics(system_specs);
        system.refresh_specifics(system_specs);
        Self {
            path: None,
            memory_only: false,
            unique_id: None,
            #[cfg(feature = "encryption")]
            vault_key_storage: None,
            #[cfg(feature = "encryption")]
            default_encryption_key: None,
            workers: Tasks::default_for(&system),
            views: Views::default(),
            key_value_persistence: KeyValuePersistence::default(),
            #[cfg(feature = "password-hashing")]
            argon: ArgonConfiguration::default_for(&system),
            initial_schemas: HashMap::default(),
        }
    }
}

impl StorageConfiguration {
    /// Registers the schema provided.
    pub fn register_schema<S: Schema>(&mut self) -> Result<(), Error> {
        self.initial_schemas
            .insert(S::schema_name(), Box::new(StorageSchemaOpener::<S>::new()?));
        Ok(())
    }
}

/// Configuration options for background tasks.
#[derive(Debug)]
pub struct Tasks {
    /// Defines how many workers should be spawned to process tasks. This
    /// defaults to the 2x the number of cpu cores available to the system or 4, whichever is larger.
    pub worker_count: usize,
}

impl SystemDefault for Tasks {
    fn default_for(system: &System) -> Self {
        Self {
            worker_count: system
                .physical_core_count()
                .unwrap_or_else(|| system.processors().len())
                * 2,
        }
    }
}

/// Configuration options for views.
#[derive(Clone, Debug, Default)]
pub struct Views {
    /// If true, the database will scan all views during the call to
    /// `open_local`. This will cause database opening to take longer, but once
    /// the database is open, no request will need to wait for the integrity to
    /// be checked. However, for faster startup time, you may wish to delay the
    /// integrity scan. Default value is `false`.
    pub check_integrity_on_open: bool,
}

/// Rules for persisting key-value changes. Default persistence is to
/// immediately persist all changes. While this ensures data integrity, the
/// overhead of the key-value store can be significantly reduced by utilizing
/// lazy persistence strategies that delay writing changes until certain
/// thresholds have been met.
///
/// ## Immediate persistence
///
/// The default persistence mode will trigger commits always:
///
/// ```rust
/// # use bonsaidb_local::config::KeyValuePersistence;
/// # use std::time::Duration;
/// assert!(!KeyValuePersistence::default().should_commit(0, Duration::ZERO));
/// assert!(KeyValuePersistence::default().should_commit(1, Duration::ZERO));
/// ```
///
/// ## Lazy persistence
///
/// Lazy persistence allows setting multiple thresholds, allowing for customized
/// behavior that can help tune performance, especially under write-heavy loads.
///
/// It is good practice to include one [`PersistenceThreshold`] that has no
/// duration, as it will ensure that the in-memory cache cannot exceed a certain
/// size. This number is counted for each database indepenently.
///
/// ```rust
/// # use bonsaidb_local::config::{KeyValuePersistence, PersistenceThreshold};
/// # use std::time::Duration;
/// #
/// let persistence = KeyValuePersistence::lazy([
///     PersistenceThreshold::after_changes(1).and_duration(Duration::from_secs(120)),
///     PersistenceThreshold::after_changes(10).and_duration(Duration::from_secs(10)),
///     PersistenceThreshold::after_changes(100),
/// ]);
///
/// // After 1 change and 60 seconds, no changes would be committed:
/// assert!(!persistence.should_commit(1, Duration::from_secs(60)));
/// // But on or after 120 seconds, that change will be committed:
/// assert!(persistence.should_commit(1, Duration::from_secs(120)));
///
/// // After 10 changes and 10 seconds, changes will be committed:
/// assert!(persistence.should_commit(10, Duration::from_secs(10)));
///
/// // Once 100 changes have been accumulated, this ruleset will always commit
/// // regardless of duration.
/// assert!(persistence.should_commit(100, Duration::ZERO));
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct KeyValuePersistence(KeyValuePersistenceInner);

#[derive(Debug, Clone)]
enum KeyValuePersistenceInner {
    Immediate,
    Lazy(Vec<PersistenceThreshold>),
}

impl Default for KeyValuePersistence {
    /// Returns [`KeyValuePersistence::immediate()`].
    fn default() -> Self {
        Self::immediate()
    }
}

impl KeyValuePersistence {
    /// Returns a ruleset that commits all changes immediately.
    pub const fn immediate() -> Self {
        Self(KeyValuePersistenceInner::Immediate)
    }

    /// Returns a ruleset that lazily commits data based on a list of thresholds.
    pub fn lazy<II>(rules: II) -> Self
    where
        II: IntoIterator<Item = PersistenceThreshold>,
    {
        let mut rules = rules.into_iter().collect::<Vec<_>>();
        rules.sort_by(|a, b| a.number_of_changes.cmp(&b.number_of_changes));
        Self(KeyValuePersistenceInner::Lazy(rules))
    }

    /// Returns true if these rules determine that the outstanding changes should be persisted.
    #[must_use]
    pub fn should_commit(
        &self,
        number_of_changes: usize,
        elapsed_since_last_commit: Duration,
    ) -> bool {
        self.duration_until_next_commit(number_of_changes, elapsed_since_last_commit)
            == Duration::ZERO
    }

    pub(crate) fn duration_until_next_commit(
        &self,
        number_of_changes: usize,
        elapsed_since_last_commit: Duration,
    ) -> Duration {
        if number_of_changes == 0 {
            Duration::MAX
        } else {
            match &self.0 {
                KeyValuePersistenceInner::Immediate => Duration::ZERO,
                KeyValuePersistenceInner::Lazy(rules) => {
                    let mut shortest_duration = Duration::MAX;
                    for rule in rules
                        .iter()
                        .take_while(|rule| rule.number_of_changes <= number_of_changes)
                    {
                        let remaining_time =
                            rule.duration.saturating_sub(elapsed_since_last_commit);
                        shortest_duration = shortest_duration.min(remaining_time);

                        if shortest_duration == Duration::ZERO {
                            break;
                        }
                    }
                    shortest_duration
                }
            }
        }
    }
}

/// A threshold controlling lazy commits. For a threshold to apply, both
/// `number_of_changes` must be met or surpassed and `duration` must have
/// elpased since the last commit.
///
/// A threshold with a duration of zero will not wait any time to persist
/// changes once the specified `number_of_changes` has been met or surpassed.
#[derive(Debug, Copy, Clone)]
#[must_use]
pub struct PersistenceThreshold {
    /// The minimum number of changes that must have occurred for this threshold to apply.
    pub number_of_changes: usize,
    /// The amount of time that must elapse since the last write for this threshold to apply.
    pub duration: Duration,
}

impl PersistenceThreshold {
    /// Returns a threshold that applies after a number of changes have elapsed.
    pub const fn after_changes(number_of_changes: usize) -> Self {
        Self {
            number_of_changes,
            duration: Duration::ZERO,
        }
    }

    /// Sets the duration of this threshold to `duration` and returns self.
    pub const fn and_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }
}

/// Storage configuration builder methods.
pub trait Builder: Default {
    /// Creates a default configuration with `path` set.
    fn new<P: AsRef<Path>>(path: P) -> Self {
        Self::default().path(path)
    }

    /// Sets [`StorageConfiguration::path`](StorageConfiguration#structfield.memory_only) to true and returns self.
    fn memory_only(self) -> Self;

    /// Registers the schema and returns self.
    fn with_schema<S: Schema>(self) -> Result<Self, Error>;

    /// Sets [`StorageConfiguration::path`](StorageConfiguration#structfield.path) to `path` and returns self.
    fn path<P: AsRef<Path>>(self, path: P) -> Self;
    /// Sets [`StorageConfiguration::unique_id`](StorageConfiguration#structfield.unique_id) to `unique_id` and returns self.
    fn unique_id(self, unique_id: u64) -> Self;
    /// Sets [`StorageConfiguration::vault_key_storage`](StorageConfiguration#structfield.vault_key_storage) to `key_storage` and returns self.
    #[cfg(feature = "encryption")]
    fn vault_key_storage<VaultKeyStorage: AnyVaultKeyStorage>(
        self,
        key_storage: VaultKeyStorage,
    ) -> Self;
    /// Sets [`StorageConfiguration::default_encryption_key`](StorageConfiguration#structfield.default_encryption_key) to `path` and returns self.
    #[cfg(feature = "encryption")]
    fn default_encryption_key(self, key: KeyId) -> Self;
    /// Sets [`Tasks::worker_count`] to `worker_count` and returns self.
    fn tasks_worker_count(self, worker_count: usize) -> Self;
    /// Sets [`Views::check_integrity_on_open`] to `check` and returns self.
    fn check_view_integrity_on_open(self, check: bool) -> Self;

    /// Sets [`StorageConfiguration::key_value_persistence`](StorageConfiguration#structfield.key_value_persistence) to `persistence` and returns self.
    fn key_value_persistence(self, persistence: KeyValuePersistence) -> Self;
}

impl Builder for StorageConfiguration {
    fn with_schema<S: Schema>(mut self) -> Result<Self, Error> {
        self.register_schema::<S>()?;
        Ok(self)
    }

    fn memory_only(mut self) -> Self {
        self.memory_only = true;
        self
    }

    fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_owned());
        self
    }

    fn unique_id(mut self, unique_id: u64) -> Self {
        self.unique_id = Some(unique_id);
        self
    }

    #[cfg(feature = "encryption")]
    fn vault_key_storage<VaultKeyStorage: AnyVaultKeyStorage>(
        mut self,
        key_storage: VaultKeyStorage,
    ) -> Self {
        self.vault_key_storage = Some(Box::new(key_storage));
        self
    }

    #[cfg(feature = "encryption")]
    fn default_encryption_key(mut self, key: KeyId) -> Self {
        self.default_encryption_key = Some(key);
        self
    }

    fn tasks_worker_count(mut self, worker_count: usize) -> Self {
        self.workers.worker_count = worker_count;
        self
    }

    fn check_view_integrity_on_open(mut self, check: bool) -> Self {
        self.views.check_integrity_on_open = check;
        self
    }

    fn key_value_persistence(mut self, persistence: KeyValuePersistence) -> Self {
        self.key_value_persistence = persistence;
        self
    }
}

pub(crate) trait SystemDefault: Sized {
    fn default_for(system: &System) -> Self;
    fn default() -> Self {
        let system_specs = RefreshKind::new().with_cpu().with_memory();
        let mut system = System::new_with_specifics(system_specs);
        system.refresh_specifics(system_specs);
        Self::default_for(&system)
    }
}
