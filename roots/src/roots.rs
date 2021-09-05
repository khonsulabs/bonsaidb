use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::fs;

use crate::{transaction::TransactionManager, AsyncFile, Error, File, Vault};

pub struct Roots<F: AsyncFile = File> {
    data: Arc<Data<F>>,
}

struct Data<F: AsyncFile> {
    file_manager: F::Manager,
    vault: Option<Arc<dyn Vault>>,
    transactions: TransactionManager,
    path: PathBuf,
    _file: PhantomData<F>,
}

impl<F: AsyncFile> Roots<F> {
    pub async fn new<P: Into<PathBuf>>(
        path: P,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        let path = path.into();
        if !path.exists() {
            fs::create_dir(&path).await?;
        } else if !path.is_dir() {
            return Err(Error::message(format!(
                "'{:?}' already exists, but is not a directory.",
                path
            )));
        }

        let file_manager = <F::Manager as Default>::default();
        let transactions =
            TransactionManager::spawn::<F>(&path, file_manager.clone(), vault.clone()).await?;
        Ok(Self {
            data: Arc::new(Data {
                file_manager,
                vault,
                path,
                transactions,
                _file: PhantomData,
            }),
        })
    }

    pub async fn initialize_unencrypted<P: Into<PathBuf>>(path: P) -> Result<Self, Error> {
        Self::new(path, None).await
    }

    pub async fn initialize_encrypted<P: Into<PathBuf>, V: Vault>(
        path: P,
        vault: V,
    ) -> Result<Self, Error> {
        Self::new(path, Some(Arc::new(vault))).await
    }

    pub fn path(&self) -> &Path {
        &self.data.path
    }

    pub fn vault(&self) -> Option<Arc<dyn Vault>> {
        self.data.vault.clone()
    }

    // pub async fn execute(&self, transaction: PreparedTransaction) -> Result<
}

impl<F: AsyncFile> Clone for Roots<F> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}
