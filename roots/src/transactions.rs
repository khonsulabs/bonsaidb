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
        atomic::{AtomicBool, AtomicU64, Ordering},
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

#[derive(Clone)]
pub struct TransactionManager {
    state: TransactionState,
    transaction_sender: flume::Sender<(TransactionHandle<'static>, flume::Sender<()>)>,
}

impl TransactionManager {
    pub async fn spawn<F: AsyncFile>(
        directory: &Path,
        file_manager: F::Manager,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        let (transaction_sender, receiver) = flume::bounded(32);
        let log_path = Self::log_path(directory);
        let state = match Transactions::<F>::load_state(&log_path, vault.as_deref()).await {
            Ok(state) => state,
            Err(Error::DataIntegrity(err)) => return Err(Error::DataIntegrity(err)),
            _ => TransactionState::default(),
        };

        let thread_state = state.clone();
        std::thread::Builder::new()
            .name(String::from("bonsaidb-txlog"))
            .spawn(move || {
                transaction_writer_thread::<F>(
                    thread_state,
                    log_path,
                    receiver,
                    file_manager,
                    vault,
                )
            })
            .map_err(Error::message)?;

        Ok(Self {
            transaction_sender,
            state,
        })
    }

    pub async fn push(&self, transaction: TransactionHandle<'static>) {
        let (completion_sender, completion_receiver) = flume::bounded(1);
        self.transaction_sender
            .send((transaction, completion_sender))
            .unwrap();
        completion_receiver.recv_async().await.unwrap();
    }

    fn log_path(directory: &Path) -> PathBuf {
        directory.join("transactions")
    }
}

impl Deref for TransactionManager {
    type Target = TransactionState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

// TODO: when an error happens, we should try to recover.
fn transaction_writer_thread<F: AsyncFile>(
    state: TransactionState,
    log_path: PathBuf,
    transactions: flume::Receiver<(TransactionHandle<'static>, flume::Sender<()>)>,
    file_manager: F::Manager,
    vault: Option<Arc<dyn Vault>>,
) {
    F::Manager::run(async {
        let mut log = Transactions::<F>::open(&log_path, state, file_manager, vault)
            .await
            .unwrap();

        const BATCH: usize = 16;
        while let Ok(transaction) = transactions.recv_async().await {
            let mut transaction_batch = Vec::with_capacity(BATCH);
            transaction_batch.push(transaction.0);
            let mut completion_senders = Vec::with_capacity(BATCH);
            completion_senders.push(transaction.1);
            for _ in 0..BATCH - 1 {
                match transactions.try_recv() {
                    Ok((transaction, sender)) => {
                        transaction_batch.push(transaction);
                        completion_senders.push(sender);
                    }
                    // At this point either type of error we want to finish writing the transactions we have.
                    Err(_) => break,
                }
            }
            log.push(transaction_batch).await.unwrap();
            for completion_sender in completion_senders {
                let _ = completion_sender.send(());
            }
        }
    })
}

pub struct Transactions<F: AsyncFile> {
    file_manager: F::Manager,
    vault: Option<Arc<dyn Vault>>,
    state: TransactionState,
    log: <F::Manager as AsyncFileManager<F>>::FileHandle,
}

impl<F: AsyncFile> Transactions<F> {
    pub async fn open(
        log_path: &Path,
        state: TransactionState,
        file_manager: F::Manager,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        let log = file_manager.append(log_path).await?;
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
        log_path: &Path,
        vault: Option<&dyn Vault>,
    ) -> Result<TransactionState, Error> {
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

    pub async fn push(&mut self, handles: Vec<TransactionHandle<'_>>) -> Result<(), Error> {
        self.log
            .write(LogWriter {
                state: self.state.clone(),
                vault: self.vault.clone(),
                handles,
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
    pub async fn new_transaction<'a>(&self, trees: &[&[u8]]) -> TransactionHandle<'a> {
        self.state.new_transaction(trees).await
    }

    pub fn state(&self) -> TransactionState {
        self.state.clone()
    }
}

struct LogWriter<'a, F> {
    state: TransactionState,
    handles: Vec<TransactionHandle<'a>>,
    vault: Option<Arc<dyn Vault>>,
    _file: PhantomData<F>,
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile> FileWriter<F> for LogWriter<'a, F> {
    async fn write(&mut self, log: &mut F) -> Result<(), Error> {
        let mut log_position = self.state.lock_for_write().await;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.resize(PAGE_SIZE, 0);
        for handle in self.handles.drain(..) {
            let mut bytes = handle.transaction.serialize()?;
            if let Some(vault) = &self.vault {
                bytes = vault.encrypt(&bytes);
            }
            // Write out the transaction in pages.
            let total_length = bytes.len() + 3;
            let mut offset = 0;
            while offset < bytes.len() {
                // Write the page header
                let header_len = if offset == 0 {
                    // The first page has the length of the payload as the next 3 bytes.
                    let length = u32::try_from(bytes.len())
                        .map_err(|_| Error::message("transaction too large"))?;
                    if length & 0xFF000000 != 0 {
                        return Err(Error::message("transaction too large"));
                    }
                    scratch_buffer[0] = 1;
                    scratch_buffer[1] = (length >> 16) as u8;
                    scratch_buffer[2] = (length >> 8) as u8;
                    scratch_buffer[3] = (length & 0xFF) as u8;
                    4
                } else {
                    // Set page_header to have a 0 byte for future pages written.
                    scratch_buffer[0] = 0;
                    1
                };

                // Write up to PAGE_SIZE - header_len bytes
                let total_bytes_left = total_length - (offset + 3);
                let bytes_to_write = total_bytes_left.min(PAGE_SIZE - header_len as usize);
                scratch_buffer[header_len..bytes_to_write + header_len]
                    .copy_from_slice(&bytes[offset..offset + bytes_to_write]);
                scratch_buffer = match log
                    .write_all(*log_position, scratch_buffer, 0, PAGE_SIZE)
                    .await
                {
                    (Ok(_), bytes) => bytes,
                    (Err(err), _) => return Err(err),
                };
                offset += bytes_to_write;
                *log_position += PAGE_SIZE as u64;
            }
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
    use futures::future::join_all;

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
    async fn tokio_encrypted_log_manager_tests() {
        log_manager_tests::<TokioFile>(
            "encrypted_tokio_log_manager",
            Some(Arc::new(RotatorVault::new(13))),
        )
        .await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn uring_encrypted_log_manager_tests() {
        tokio_uring::start(async {
            log_manager_tests::<crate::async_file::uring::UringFile>(
                "encrypted_uring_log_manager",
                Some(Arc::new(RotatorVault::new(13))),
            )
            .await;
        })
    }

    async fn log_file_tests<F: AsyncFile>(file_name: &str, vault: Option<Arc<dyn Vault>>) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        let file_manager = <F::Manager as Default>::default();
        tokio::fs::create_dir(&temp_dir).await.unwrap();
        for id in 0..1_000_000 {
            let log_path = TransactionManager::log_path(&temp_dir);
            let state = Transactions::<F>::load_state(&log_path, vault.as_deref())
                .await
                .unwrap_or_default();
            let mut transactions =
                Transactions::<F>::open(&log_path, state, file_manager.clone(), vault.clone())
                    .await
                    .unwrap();
            assert_eq!(transactions.current_transaction_id(), id);
            let mut tx = transactions.new_transaction(&[b"hello"]).await;

            tx.transaction.changes.insert(
                Cow::Borrowed(b"tree1"),
                Entries::Owned(vec![Entry {
                    document_id: U64::new(2),
                    sequence_id: U64::new(3),
                }]),
            );

            transactions.push(vec![tx]).await.unwrap();
            transactions.close().await.unwrap();
        }

        if vault.is_some() {
            // Test that we can't open it without encryption
            assert!(Transactions::<F>::load_state(&temp_dir, None)
                .await
                .is_err());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tokio_log_manager_tests() {
        log_manager_tests::<TokioFile>("tokio_log_manager", None).await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn uring_log_manager_tests() {
        tokio_uring::start(async {
            log_manager_tests::<crate::async_file::uring::UringFile>("uring_log_manager", None)
                .await;
        })
    }

    async fn log_manager_tests<F: AsyncFile>(file_name: &str, vault: Option<Arc<dyn Vault>>) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        tokio::fs::create_dir(&temp_dir).await.unwrap();
        let file_manager = <F::Manager as Default>::default();
        let manager =
            TransactionManager::spawn::<F>(&temp_dir, file_manager.clone(), vault.clone())
                .await
                .unwrap();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let manager = manager.clone();
            handles.push(tokio::spawn(async move {
                for id in 0..100_000 {
                    let mut tx = manager.new_transaction(&[id.as_bytes()]).await;

                    tx.transaction.changes.insert(
                        Cow::Borrowed(b"tree1"),
                        Entries::Owned(vec![Entry {
                            document_id: U64::new(2),
                            sequence_id: U64::new(3),
                        }]),
                    );
                    manager.push(tx).await;
                }
            }));
        }
        join_all(handles).await;

        assert_eq!(manager.current_transaction_id(), 1_000_000);
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
                locks.push(lock.lock().await);
            } else {
                let lock = TreeLock::new();
                let locked = lock.lock().await;
                tree_locks.insert(Cow::Owned(tree.to_vec()), lock);
                locks.push(locked);
            }
        }
    }

    #[allow(clippy::needless_lifetimes)] // lies! I can't seem to get rid of the lifetimes.
    pub async fn new_transaction(&self, trees: &[&[u8]]) -> TransactionHandle<'static> {
        let mut locked_trees = Vec::with_capacity(trees.len());
        self.fetch_tree_locks(trees, &mut locked_trees).await;

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
    pub locked_trees: TreeLocks,
}

pub type TreeLocks = Vec<TreeLockHandle>;

#[derive(Debug)]
pub struct TreeLock {
    data: Arc<TreeLockData>,
}

impl TreeLock {
    pub fn new() -> Self {
        Self {
            data: Arc::new(TreeLockData {
                locked: AtomicBool::new(false),
                rt: tokio::runtime::Handle::current(),
                blocked: Mutex::default(),
            }),
        }
    }
    pub async fn lock(&self) -> TreeLockHandle {
        // Loop until we acquire a lock
        loop {
            // Try to acquire the lock without any possibility of blocking
            if self
                .data
                .locked
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            } else {
                let unblocked_receiver = {
                    let mut blocked = self.data.blocked.lock().await;
                    // Now that we've acquired this lock, it's possible the lock has
                    // been released. If there are already others waiting, we want
                    // to yield to allow those that were waiting before us to be
                    // woken up first (potentially).
                    if blocked.is_empty()
                        && self
                            .data
                            .locked
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                    {
                        break;
                    }
                    let (unblocked_sender, unblocked_receiver) = flume::bounded(1);
                    blocked.push(unblocked_sender);
                    unblocked_receiver
                };
                let _ = unblocked_receiver.recv_async().await;
            }
        }

        TreeLockHandle(TreeLock {
            data: self.data.clone(),
        })
    }
}

#[derive(Debug)]
struct TreeLockData {
    locked: AtomicBool,
    rt: tokio::runtime::Handle,
    blocked: Mutex<Vec<flume::Sender<()>>>,
}

#[derive(Debug)]
pub struct TreeLockHandle(TreeLock);

impl Drop for TreeLockHandle {
    fn drop(&mut self) {
        self.0.data.locked.store(false, Ordering::SeqCst);

        let data = self.0.data.clone();
        self.0.data.rt.spawn(async move {
            let mut blocked = data.blocked.lock().await;
            for blocked in blocked.drain(..) {
                let _ = blocked.send(());
            }
        });
    }
}

pub struct OpenTransaction<'a, F: AsyncFile> {
    pub handle: TransactionHandle<'a>,
    pub roots: &'a mut OpenRoots<F>,
}
