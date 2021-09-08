use std::{
    borrow::Cow, collections::BTreeMap, convert::TryFrom, io::Write, marker::PhantomData,
    mem::size_of, ops::Deref, path::Path, sync::Arc,
};

use async_trait::async_trait;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use tokio::fs::OpenOptions;
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned, U64};

use super::{State, TransactionHandle};
use crate::{
    async_file::{AsyncFile, AsyncFileManager, FileOp, OpenableFile},
    Error, Vault,
};

const PAGE_SIZE: usize = 1024;

pub struct TransactionLog<F: AsyncFile> {
    vault: Option<Arc<dyn Vault>>,
    state: State,
    log: <F::Manager as AsyncFileManager<F>>::FileHandle,
}

#[allow(clippy::future_not_send)]
impl<F: AsyncFile> TransactionLog<F> {
    pub async fn open(
        log_path: &Path,
        state: State,
        vault: Option<Arc<dyn Vault>>,
        file_manager: &F::Manager,
    ) -> Result<Self, Error> {
        let log = file_manager.append(log_path).await?;
        Ok(Self { vault, state, log })
    }

    pub async fn total_size(&self) -> u64 {
        let state = self.state.lock_for_write().await;
        *state
    }

    pub async fn initialize_state(
        state: &State,
        log_path: &Path,
        vault: Option<&dyn Vault>,
        file_manager: &F::Manager,
    ) -> Result<(), Error> {
        let mut log_length = file_manager.file_length(log_path).await?;
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

        let mut file = file_manager.read(log_path).await?;
        file.write(StateInitializer {
            state,
            log_length,
            vault: vault.as_deref(),
            _file: PhantomData,
        })
        .await
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

    pub fn state(&self) -> State {
        self.state.clone()
    }
}

struct StateInitializer<'a, F> {
    state: &'a State,
    log_length: u64,
    vault: Option<&'a dyn Vault>,
    _file: PhantomData<F>,
}
#[async_trait(?Send)]
impl<'a, F: AsyncFile> FileOp<F> for StateInitializer<'a, F> {
    type Output = ();
    async fn write(&mut self, log: &mut F) -> Result<(), Error> {
        // Scan back block by block until we find a page header with a value of 1.
        let mut block_start = self.log_length - PAGE_SIZE as u64;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.resize(4, 0);
        let last_transaction_id = loop {
            // Read the page header
            scratch_buffer = match log.read_exact(block_start, scratch_buffer, 4).await {
                (Ok(_), buffer) => buffer,
                (Err(err), _) => return Err(err),
            };
            #[allow(clippy::match_on_vec_items)]
            match scratch_buffer[0] {
                0 => {
                    if block_start == 0 {
                        panic!("transaction log contained data, but no valid pages were found");
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
                    let decrypted = match &self.vault {
                        Some(vault) => Cow::Owned(vault.decrypt(payload)),
                        None => Cow::Borrowed(payload),
                    };
                    let transaction =
                        LogEntry::deserialize(&decrypted).map_err(Error::data_integrity)?;
                    break transaction.id;
                }
                _ => unreachable!("corrupt transaction log"),
            }
        };

        self.state
            .initialize(last_transaction_id + 1, self.log_length)
            .await;
        Ok(())
    }
}

struct LogWriter<'a, F> {
    state: State,
    handles: Vec<TransactionHandle<'a>>,
    vault: Option<Arc<dyn Vault>>,
    _file: PhantomData<F>,
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile> FileOp<F> for LogWriter<'a, F> {
    type Output = ();
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
                    if length & 0xFF00_0000 != 0 {
                        return Err(Error::message("transaction too large"));
                    }
                    scratch_buffer[0] = 1;
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        scratch_buffer[1] = (length >> 16) as u8;
                        scratch_buffer[2] = (length >> 8) as u8;
                        scratch_buffer[3] = (length & 0xFF) as u8;
                    }
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
pub struct LogEntry<'a> {
    pub id: u64,
    pub changes: TransactionChanges<'a>,
}

pub type TransactionChanges<'a> = BTreeMap<Cow<'a, [u8]>, Entries<'a>>;

impl<'a> LogEntry<'a> {
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
        **self == **other
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
    let mut transaction = LogEntry {
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
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);

    // Multiple trees
    let mut transaction = LogEntry {
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
    let deserialized = LogEntry::deserialize(&serialized).unwrap();
    assert_eq!(transaction, deserialized);
}

#[cfg(test)]
#[allow(clippy::semicolon_if_nothing_returned, clippy::future_not_send)]
mod tests {
    use futures::future::join_all;

    use super::*;
    use crate::{
        async_file::{memory::MemoryFile, tokio::TokioFile, AsyncFile},
        test_util::RotatorVault,
        transaction::TransactionManager,
    };

    #[tokio::test]
    async fn tokio_log_file_tests() {
        log_file_tests::<TokioFile>("tokio_log_file", None).await;
    }

    #[tokio::test]
    async fn memory_log_file_tests() {
        log_file_tests::<MemoryFile>("memory_log_file", None).await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn uring_log_file_tests() {
        tokio_uring::start(async {
            log_file_tests::<crate::async_file::uring::UringFile>("uring_log_file", None).await;
        });
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
        });
    }

    async fn log_file_tests<F: AsyncFile>(file_name: &str, vault: Option<Arc<dyn Vault>>) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        let file_manager = <F::Manager as Default>::default();
        tokio::fs::create_dir(&temp_dir).await.unwrap();
        let log_path = {
            let directory: &Path = &temp_dir;
            directory.join("transactions")
        };

        for id in 0..1_000 {
            let state = State::default();
            let result = TransactionLog::<F>::initialize_state(
                &state,
                &log_path,
                vault.as_deref(),
                &file_manager,
            )
            .await;
            if id > 0 {
                result.unwrap();
            }
            let mut transactions =
                TransactionLog::<F>::open(&log_path, state, vault.clone(), &file_manager)
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
            let state = State::default();
            assert!(
                TransactionLog::<F>::initialize_state(&state, &temp_dir, None, &file_manager,)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tokio_log_manager_tests() {
        log_manager_tests::<TokioFile>("tokio_log_manager", None).await;
    }

    #[tokio::test]
    async fn memory_log_manager_tests() {
        log_manager_tests::<MemoryFile>("memory_log_manager", None).await;
    }

    #[test]
    #[cfg(feature = "uring")]
    fn uring_log_manager_tests() {
        tokio_uring::start(async {
            log_manager_tests::<crate::async_file::uring::UringFile>("uring_log_manager", None)
                .await;
        });
    }

    async fn log_manager_tests<F: AsyncFile + 'static>(
        file_name: &str,
        vault: Option<Arc<dyn Vault>>,
    ) {
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
                for id in 0..1_000 {
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

        assert_eq!(manager.current_transaction_id(), 10_000);
        // TODO test scanning the file
    }
}
