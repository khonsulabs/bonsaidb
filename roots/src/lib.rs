use std::{
    borrow::Cow,
    fmt::Display,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_file::{AsyncFile, File};
use tokio::fs;
use transactions::{OpenTransaction, TransactionState, Transactions, TreeLocks};

mod async_file;
mod transactions;
mod tree;

pub trait Vault: Send + Sync + 'static {
    fn current_key_id(&self) -> u32;
    fn encrypt(&self, payload: &[u8]) -> Vec<u8>;
    fn decrypt(&self, payload: &[u8]) -> Vec<u8>;
}

pub struct Roots<F: AsyncFile = File> {
    data: Arc<Data<F>>,
}

struct Data<F: AsyncFile> {
    file_manager: F::Manager,
    vault: Option<Arc<dyn Vault>>,
    transactions: TransactionState,
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

        // let transactions = Transactions::<F>::open(&path, vault.clone()).await?;
        let transactions = match Transactions::<F>::load_state(&path, vault.as_deref()).await {
            Ok(result) => result,
            Err(Error::DataIntegrity(err)) => return Err(Error::DataIntegrity(err)),
            _ => TransactionState::default(),
        };
        Ok(Self {
            data: Arc::new(Data {
                file_manager: <F::Manager as Default>::default(),
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

    pub async fn open(&self) -> Result<OpenRoots<F>, Error> {
        Ok(OpenRoots {
            roots: self.clone(),
            transactions: Transactions::open(
                self.path(),
                self.data.transactions.clone(),
                self.data.file_manager.clone(),
                self.vault(),
            )
            .await?,
        })
    }
}

impl<F: AsyncFile> Clone for Roots<F> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("an unrecoverable error with the data on disk has been found: {0}")]
    DataIntegrity(Box<Self>),
}

impl Error {
    pub fn message<S: Display>(message: S) -> Self {
        Self::Message(message.to_string())
    }
}

#[cfg(test)]
mod test_util;

pub struct OpenRoots<F: AsyncFile> {
    roots: Roots<F>,
    transactions: Transactions<F>,
}

impl<F: AsyncFile> OpenRoots<F> {
    pub async fn transaction<'t>(&'t mut self, trees: &[&[u8]]) -> OpenTransaction<'t, F> {
        let handle = self.transactions.new_transaction(trees).await;
        OpenTransaction {
            roots: self,
            handle,
        }
    }
}
