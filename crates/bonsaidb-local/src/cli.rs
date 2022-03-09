use std::path::PathBuf;

use clap::Subcommand;

use crate::{backend, config::StorageConfiguration, AsyncStorage, Error, Storage};

/// Commands operating on local database storage.
#[derive(Subcommand, Debug)]
pub enum StorageCommand {
    /// Back up the storage.
    #[clap(subcommand)]
    Backup(Location),
    /// Restore the storage from backup.
    #[clap(subcommand)]
    Restore(Location),
}

/// A backup location.
#[derive(Subcommand, Debug)]
pub enum Location {
    /// A filesystem-based backup location.
    Path {
        /// The path to the backup directory.
        path: PathBuf,
    },
}

impl StorageCommand {
    /// Executes the command after opening a [`Storage`] instance using `config`.
    pub fn execute<Backend: backend::Backend>(
        &self,
        config: StorageConfiguration<Backend>,
    ) -> Result<(), Error> {
        let storage = Storage::<Backend>::open(config)?;
        self.execute_on(&storage)
    }

    /// Executes the command on `storage`.
    pub fn execute_on<Backend: backend::Backend>(
        &self,
        storage: &Storage<Backend>,
    ) -> Result<(), Error> {
        match self {
            StorageCommand::Backup(location) => location.backup(storage),
            StorageCommand::Restore(location) => location.restore(storage),
        }
    }

    /// Executes the command on `storage`.
    pub async fn execute_on_async<Backend: backend::Backend>(
        &self,
        storage: &AsyncStorage<Backend>,
    ) -> Result<(), Error> {
        match self {
            StorageCommand::Backup(location) => location.backup_async(storage).await,
            StorageCommand::Restore(location) => location.restore_async(storage).await,
        }
    }
}

impl Location {
    /// Backs-up `storage` to `self`.
    pub fn backup<Backend: backend::Backend>(
        &self,
        storage: &Storage<Backend>,
    ) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.backup(path),
        }
    }

    /// Restores `storage` from `self`.
    pub fn restore<Backend: backend::Backend>(
        &self,
        storage: &Storage<Backend>,
    ) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.restore(path),
        }
    }
    /// Backs-up `storage` to `self`.
    pub async fn backup_async<Backend: backend::Backend>(
        &self,
        storage: &AsyncStorage<Backend>,
    ) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.backup(path.clone()).await,
        }
    }

    /// Restores `storage` from `self`.
    pub async fn restore_async<Backend: backend::Backend>(
        &self,
        storage: &AsyncStorage<Backend>,
    ) -> Result<(), Error> {
        match self {
            Location::Path { path } => storage.restore(path.clone()).await,
        }
    }
}
