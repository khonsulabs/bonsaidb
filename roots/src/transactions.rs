use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    io::Write,
    marker::PhantomData,
    mem::size_of,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::{
    fs::OpenOptions,
    sync::{Mutex, MutexGuard},
};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned, U64};

use crate::{
    async_file::{AsyncFile, AsyncFileManager, FileWriter, OpenableFile},
    Error, OpenRoots, Vault,
};

const PAGE_SIZE: usize = 1024;

pub struct Transactions<F: AsyncFile> {
    file_manager: F::Manager,
    vault: Option<Arc<dyn Vault>>,
    state: TransactionState,
    log: <F::Manager as AsyncFileManager<F>>::FileHandle,
}

impl<F: AsyncFile> Transactions<F> {
    fn log_path(directory: &Path) -> PathBuf {
        directory.join("transactions")
    }

    pub async fn open(
        directory: &Path,
        state: TransactionState,
        file_manager: F::Manager,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        let log = file_manager.append(Self::log_path(directory)).await?;
        Ok(Self {
            vault,
            log,
            state,
            file_manager,
        })
    }

    pub async fn total_size(&self) -> u64 {
        let state = self.state.lock_for_write().await;
        *state
    }

    pub async fn load_state(
        directory: &Path,
        vault: Option<&dyn Vault>,
    ) -> Result<TransactionState, Error> {
        let log_path = Self::log_path(directory);
        let mut log_length = log_path.metadata()?.len();
        if log_length == 0 {
            return Err(Error::message("empty transaction log"));
        }

        let excess_length = log_length % PAGE_SIZE as u64;
        if excess_length > 0 {
            // Truncate the file to the proper page size. This should only happen in a recovery situation.
            eprintln!(
                "Transaction log has {} extra bytes. Truncating.",
                excess_length
            );
            let file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(&log_path)
                .await?;
            log_length -= excess_length;
            file.set_len(log_length).await?;
            file.sync_all().await?;
        }

        let mut log = F::read(log_path).await?;

        // Scan back block by block until we find a page header with a value of 1.
        let mut block_start = log_length - PAGE_SIZE as u64;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.resize(4, 0);
        let last_transaction_id = loop {
            // Read the page header
            scratch_buffer = match log.read_exact(block_start, scratch_buffer, 4).await {
                (Ok(_), buffer) => buffer,
                (Err(err), _) => return Err(err),
            };
            match scratch_buffer[0] {
                0 => {
                    if block_start == 0 {
                        panic!("transaction log contained data, but no valid pages were found")
                    }
                    block_start -= PAGE_SIZE as u64;
                    continue;
                }
                1 => {
                    // The length is the next 3 bytes.
                    let length = (scratch_buffer[1] as usize) << 16
                        | (scratch_buffer[2] as usize) << 8
                        | scratch_buffer[3] as usize;
                    if scratch_buffer.len() < length {
                        scratch_buffer.resize(length, 0);
                    }
                    scratch_buffer = match log
                        .read_exact(block_start + 4, scratch_buffer, length)
                        .await
                    {
                        (Ok(_), buffer) => buffer,
                        (Err(err), _) => return Err(err),
                    };
                    let payload = &scratch_buffer[0..length];
                    let decrypted = match vault {
                        Some(vault) => Cow::Owned(vault.decrypt(payload)),
                        None => Cow::Borrowed(payload),
                    };
                    let transaction = Transaction::deserialize(&decrypted)
                        .map_err(|err| Error::DataIntegrity(Box::new(err)))?;
                    break transaction.id;
                }
                _ => unreachable!("corrupt transaction log"),
            }
        };

        Ok(TransactionState::new(last_transaction_id + 1, log_length))
    }

    pub async fn push(&mut self, handle: TransactionHandle<'_>) -> Result<(), Error> {
        self.log
            .write(LogWriter {
                state: self.state.clone(),
                vault: self.vault.clone(),
                handle,
                _file: PhantomData,
            })
            .await
    }

    pub async fn close(self) -> Result<(), Error> {
        self.log.close().await
    }

    pub fn current_transaction_id(&self) -> u64 {
        self.state.current_transaction_id()
    }

    #[allow(clippy::needless_lifetimes)] // lies! I can't seem to get rid of the lifetimes.
    pub async fn new_transaction<'a>(
        &self,
        trees: &[&[u8]],
        locks: &'a mut TreeLocks,
    ) -> TransactionHandle<'a> {
        self.state.new_transaction(trees, locks).await
    }

    pub fn state(&self) -> TransactionState {
        self.state.clone()
    }
}

struct LogWriter<'a, F> {
    state: TransactionState,
    handle: TransactionHandle<'a>,
    vault: Option<Arc<dyn Vault>>,
    _file: PhantomData<F>,
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile> FileWriter<F> for LogWriter<'a, F> {
    async fn write(&self, log: &mut F) -> Result<(), Error> {
        let mut log_position = self.state.lock_for_write().await;
        let mut bytes = self.handle.transaction.serialize()?;
        if let Some(vault) = &self.vault {
            bytes = vault.encrypt(&bytes);
        }
        // Write out the transaction in pages.
        let total_length = bytes.len() + 3;
        let mut offset = 0;
        let mut scratch_buffer = vec![1_u8, 0, 0, 0];
        while offset < bytes.len() {
            // Write the page header
            let header_len = if offset == 0 {
                // The first page has the length of the payload as the next 3 bytes.
                let length = u32::try_from(bytes.len())
                    .map_err(|_| Error::message("transaction too large"))?;
                if length & 0xFF000000 != 0 {
                    return Err(Error::message("transaction too large"));
                }
                scratch_buffer[1] = (length >> 16) as u8;
                scratch_buffer[2] = (length >> 8) as u8;
                scratch_buffer[3] = (length & 0xFF) as u8;
                scratch_buffer = match log.write_all(*log_position, scratch_buffer, 0, 4).await {
                    (Ok(_), bytes) => bytes,
                    (Err(err), _) => return Err(err),
                };
                4
            } else {
                // Set page_header to have a 0 byte for future pages written.
                scratch_buffer[0] = 0;
                scratch_buffer = match log.write_all(*log_position, scratch_buffer, 0, 1).await {
                    (Ok(_), bytes) => bytes,
                    (Err(err), _) => return Err(err),
                };
                1
            };
            *log_position += header_len;

            // Write up to PAGE_SIZE - 1 bytes
            let total_bytes_left = total_length - (offset + 3);
            let bytes_to_write = total_bytes_left.min(PAGE_SIZE - 1);
            bytes = match log
                .write_all(*log_position, bytes, offset, bytes_to_write)
                .await
            {
                (Ok(_), bytes) => bytes,
                (Err(err), _) => return Err(err),
            };
            // Pad the page out if we wrote less than a page
            let pad_length = PAGE_SIZE - bytes_to_write - header_len as usize;
            if pad_length > 0 {
                scratch_buffer.resize(pad_length, 0);
                scratch_buffer = match log
                    .write_all(
                        *log_position + bytes_to_write as u64,
                        scratch_buffer,
                        0,
                        pad_length,
                    )
                    .await
                {
                    (Ok(_), bytes) => bytes,
                    (Err(err), _) => return Err(err),
                };
            }
            offset += bytes_to_write;
            *log_position += PAGE_SIZE as u64 - header_len;
        }

        drop(log_position);
        log.flush().await
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Transaction<'a> {
    pub id: u64,
    pub changes: TransactionEntries<'a>,
}

pub type TransactionEntries<'a> = BTreeMap<Cow<'a, [u8]>, Entries<'a>>;

impl<'a> Transaction<'a> {
    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        // Transaction ID
        buffer.write_u64::<BigEndian>(self.id)?;
        // Number of trees in the transaction
        buffer.write_u16::<BigEndian>(convert_usize(self.changes.len())?)?;
        for (tree, entries) in &self.changes {
            // Length of tree name
            buffer.write_u16::<BigEndian>(convert_usize(tree.len())?)?;
            // The tree name
            buffer.write_all(tree)?;
            // Number of `Entry`s
            buffer.write_u32::<BigEndian>(convert_usize(entries.len())?)?;
            for entry in entries.iter() {
                buffer.write_all(entry.as_bytes())?;
            }
        }
        Ok(buffer)
    }

    pub fn deserialize(mut buffer: &'a [u8]) -> Result<Self, Error> {
        let id = buffer.read_u64::<BigEndian>()?;
        let number_of_trees = buffer.read_u16::<BigEndian>()?;
        let mut changes = BTreeMap::new();
        for _ in 0..number_of_trees {
            let name_len = usize::from(buffer.read_u16::<BigEndian>()?);
            if name_len > buffer.len() {
                return Err(Error::message("invalid tree name length"));
            }
            let name_bytes = &buffer[0..name_len];
            let name = Cow::Borrowed(name_bytes);
            buffer = &buffer[name_len..];

            let entry_count = usize::try_from(buffer.read_u32::<BigEndian>()?).unwrap();
            let byte_len = entry_count * size_of::<Entry>();
            if byte_len > buffer.len() {
                return Err(Error::message("invalid entry length"));
            }
            let entries = LayoutVerified::new_slice(&buffer[..byte_len])
                .ok_or_else(|| Error::message("invalid entry slice"))?;
            buffer = &buffer[byte_len..];

            changes.insert(name, Entries::Borrowed(entries));
        }
        if buffer.is_empty() {
            Ok(Self { id, changes })
        } else {
            Err(Error::message("unexpected trailing bytes"))
        }
    }
}

#[derive(Debug)]
pub enum Entries<'a> {
    Owned(Vec<Entry>),
    Borrowed(LayoutVerified<&'a [u8], [Entry]>),
}

impl<'a> Clone for Entries<'a> {
    fn clone(&self) -> Self {
        match self {
            Self::Owned(owned) => Self::Owned(owned.clone()),
            Self::Borrowed(borrowed) => Self::Owned(borrowed.to_vec()),
        }
    }
}

impl<'a> Deref for Entries<'a> {
    type Target = [Entry];

    fn deref(&self) -> &Self::Target {
        match self {
            Entries::Owned(entries) => entries,
            Entries::Borrowed(entries) => entries,
        }
    }
}

impl<'a> Eq for Entries<'a> {}

impl<'a> PartialEq for Entries<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
pub struct Entry {
    pub document_id: U64<BigEndian>,
    pub sequence_id: U64<BigEndian>,
}

fn convert_usize<T: TryFrom<usize>>(value: usize) -> Result<T, Error> {
    T::try_from(value).map_err(|_| Error::message("too many changed trees in a transaction"))
}

#[test]
fn serialization_tests() {
    let mut transaction = Transaction {
        id: 1,
        changes: BTreeMap::default(),
    };
    transaction.changes.insert(
        Cow::Borrowed(b"tree1"),
        Entries::Owned(vec![Entry {
            document_id: U64::new(2),
            sequence_id: U64::new(3),
        }]),
    );
    let serialized = transaction.serialize().unwrap();
    let deserialized = Transaction::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    // Multiple trees
    let mut transaction = Transaction {
        id: 1,
        changes: BTreeMap::default(),
    };
    transaction.changes.insert(
        Cow::Borrowed(b"tree1"),
        Entries::Owned(vec![Entry {
            document_id: U64::new(2),
            sequence_id: U64::new(3),
        }]),
    );
    transaction.changes.insert(
        Cow::Borrowed(b"tree2"),
        Entries::Owned(vec![
            Entry {
                document_id: U64::new(4),
                sequence_id: U64::new(5),
            },
            Entry {
                document_id: U64::new(6),
                sequence_id: U64::new(7),
            },
        ]),
    );
    let serialized = transaction.serialize().unwrap();
    let deserialized = Transaction::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        async_file::{tokio::TokioFile, AsyncFile},
        test_util::RotatorVault,
    };

    #[tokio::test]
    async fn tokio_log_file_tests() {
        log_file_tests::<TokioFile>("tokio_log_file", None).await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn uring_log_file_tests() {
        tokio_uring::start(async {
            log_file_tests::<crate::async_file::uring::UringFile>("uring_log_file", None).await;
        })
    }

    #[tokio::test]
    async fn tokio_log_file_parallel_tests() {
        parallel_log_tests::<TokioFile>("parallel_tokio_log_file", None).await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn uring_log_file_parallel_tests() {
        tokio_uring::start(async {
            parallel_log_tests::<crate::async_file::uring::UringFile>(
                "parallel_uring_log_file",
                None,
            )
            .await;
        })
    }

    #[tokio::test]
    async fn encrypted_tokio_log_file_tests() {
        log_file_tests::<TokioFile>(
            "encrypted_tokio_log_file",
            Some(Arc::new(RotatorVault::new(13))),
        )
        .await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn encrypted_uring_log_file_tests() {
        tokio_uring::start(async {
            log_file_tests::<crate::async_file::uring::UringFile>(
                "encrypted_uring_log_file",
                Some(Arc::new(RotatorVault::new(13))),
            )
            .await;
        })
    }

    async fn log_file_tests<F: AsyncFile>(file_name: &str, vault: Option<Arc<dyn Vault>>) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        let file_manager = <F::Manager as Default>::default();
        tokio::fs::create_dir(&temp_dir).await.unwrap();
        for id in 0..100 {
            let state = Transactions::<F>::load_state(&temp_dir, vault.as_deref())
                .await
                .unwrap_or_default();
            let mut transactions =
                Transactions::<F>::open(&temp_dir, state, file_manager.clone(), vault.clone())
                    .await
                    .unwrap();
            assert_eq!(transactions.current_transaction_id(), id);
            let mut transaction_locks = Vec::new();
            let mut tx = transactions
                .new_transaction(&[b"hello"], &mut transaction_locks)
                .await;

            tx.transaction.changes.insert(
                Cow::Borrowed(b"tree1"),
                Entries::Owned(vec![Entry {
                    document_id: U64::new(2),
                    sequence_id: U64::new(3),
                }]),
            );

            transactions.push(tx).await.unwrap();
            transactions.close().await.unwrap();
        }

        if vault.is_some() {
            // Test that we can't open it without encryption
            assert!(Transactions::<F>::load_state(&temp_dir, None)
                .await
                .is_err());
        }
    }

    async fn parallel_log_tests<F: AsyncFile>(file_name: &str, vault: Option<Arc<dyn Vault>>) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        tokio::fs::create_dir(&temp_dir).await.unwrap();
        let file_manager = <F::Manager as Default>::default();
        let state = TransactionState::default();
        let mut futures = FuturesUnordered::new();
        for _id in 0..100 {
            futures.push(async {
                let mut transactions = Transactions::<F>::open(
                    &temp_dir,
                    state.clone(),
                    file_manager.clone(),
                    vault.clone(),
                )
                .await
                .unwrap();
                let mut transaction_locks = Vec::new();
                let mut tx = transactions
                    .new_transaction(&[b"hello"], &mut transaction_locks)
                    .await;

                tx.transaction.changes.insert(
                    Cow::Borrowed(b"tree1"),
                    Entries::Owned(vec![Entry {
                        document_id: U64::new(2),
                        sequence_id: U64::new(3),
                    }]),
                );

                transactions.push(tx).await.unwrap();
                transactions.close().await.unwrap();
            });
        }

        while futures.next().await.is_some() {}

        assert_eq!(state.current_transaction_id(), 100);
        // TODO test scanning the file
    }
}

#[derive(Clone, Debug)]
pub struct TransactionState {
    state: Arc<ActiveState>,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::new(0, 0)
    }
}

impl TransactionState {
    pub fn new(current_transaction_id: u64, log_position: u64) -> Self {
        Self {
            state: Arc::new(ActiveState {
                tree_locks: Mutex::default(),
                current_transaction_id: AtomicU64::new(current_transaction_id),
                log_position: Mutex::new(log_position),
            }),
        }
    }

    pub fn current_transaction_id(&self) -> u64 {
        self.state.current_transaction_id.load(Ordering::SeqCst)
    }
    async fn fetch_tree_locks<'a>(&'a self, trees: &'a [&[u8]], locks: &mut TreeLocks) {
        let mut tree_locks = self.state.tree_locks.lock().await;
        for tree in trees {
            if let Some(lock) = tree_locks.get(&Cow::Borrowed(*tree)) {
                locks.push(lock.clone());
            } else {
                let lock = Arc::new(Mutex::new(()));
                tree_locks.insert(Cow::Owned(tree.to_vec()), lock.clone());
                locks.push(lock);
            }
        }
    }

    #[allow(clippy::needless_lifetimes)] // lies! I can't seem to get rid of the lifetimes.
    pub async fn new_transaction<'a>(
        &self,
        trees: &[&[u8]],
        out_locks: &'a mut Vec<Arc<Mutex<()>>>,
    ) -> TransactionHandle<'a> {
        self.fetch_tree_locks(trees, out_locks).await;
        let mut lock_futures = out_locks
            .iter()
            .map(|lock| async move { lock.lock().await })
            .collect::<FuturesUnordered<_>>();

        let mut locked_trees = Vec::new();
        while let Some(tree) = lock_futures.next().await {
            locked_trees.push(tree);
        }

        TransactionHandle {
            locked_trees,
            transaction: Transaction {
                id: self
                    .state
                    .current_transaction_id
                    .fetch_add(1, Ordering::SeqCst),
                changes: TransactionEntries::default(),
            },
        }
    }
}

impl TransactionState {
    async fn lock_for_write(&self) -> MutexGuard<'_, u64> {
        self.state.log_position.lock().await
    }
}

#[derive(Debug)]
pub struct ActiveState {
    current_transaction_id: AtomicU64,
    tree_locks: Mutex<HashMap<Cow<'static, [u8]>, TreeLock>>,
    log_position: Mutex<u64>,
}

pub struct TransactionHandle<'a> {
    pub transaction: Transaction<'a>,
    pub locked_trees: Vec<LockedTree<'a>>,
}

pub type LockedTree<'a> = MutexGuard<'a, ()>;
pub type TreeLock = Arc<Mutex<()>>;
pub type TreeLocks = Vec<TreeLock>;

pub struct OpenTransaction<'a, F: AsyncFile> {
    pub handle: TransactionHandle<'a>,
    pub roots: &'a mut OpenRoots<F>,
}
