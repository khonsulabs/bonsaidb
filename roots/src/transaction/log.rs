use std::{
    borrow::Cow,
    collections::BTreeMap,
    convert::TryFrom,
    fs::OpenOptions,
    io::{SeekFrom, Write},
    marker::PhantomData,
    mem::size_of,
    ops::Deref,
    path::Path,
    sync::Arc,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned, U64};

use super::{State, TransactionHandle};
use crate::{
    managed_file::{FileManager, FileOp, ManagedFile, OpenableFile},
    Context, Error, Vault,
};

const PAGE_SIZE: usize = 1024;

pub struct TransactionLog<F: ManagedFile> {
    vault: Option<Arc<dyn Vault>>,
    state: State,
    log: <F::Manager as FileManager<F>>::FileHandle,
}

#[allow(clippy::future_not_send)]
impl<F: ManagedFile> TransactionLog<F> {
    pub fn open(
        log_path: &Path,
        state: State,
        context: Context<F::Manager>,
    ) -> Result<Self, Error> {
        let log = context.file_manager.append(log_path)?;
        Ok(Self {
            vault: context.vault,
            state,
            log,
        })
    }

    pub fn total_size(&self) -> u64 {
        let state = self.state.lock_for_write();
        *state
    }

    pub fn initialize_state(
        state: &State,
        log_path: &Path,
        context: &Context<F::Manager>,
    ) -> Result<(), Error> {
        let mut log_length = context.file_manager.file_length(log_path)?;
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
                .open(&log_path)?;
            log_length -= excess_length;
            file.set_len(log_length)?;
            file.sync_all()?;
        }

        let mut file = context.file_manager.read(log_path)?;
        file.execute(StateInitializer {
            state,
            log_length,
            vault: context.vault(),
            _file: PhantomData,
        })
    }

    pub fn push(&mut self, handles: Vec<TransactionHandle<'_>>) -> Result<(), Error> {
        self.log.execute(LogWriter {
            state: self.state.clone(),
            vault: self.vault.clone(),
            handles,
            _file: PhantomData,
        })
    }

    pub fn close(self) -> Result<(), Error> {
        self.log.close()
    }

    pub fn current_transaction_id(&self) -> u64 {
        self.state.current_transaction_id()
    }

    #[allow(clippy::needless_lifetimes)] // lies! I can't seem to get rid of the lifetimes.
    pub fn new_transaction<'a>(&self, trees: &[&[u8]]) -> TransactionHandle<'a> {
        self.state.new_transaction(trees)
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

impl<'a, F: ManagedFile> FileOp<F> for StateInitializer<'a, F> {
    type Output = ();
    fn execute(&mut self, log: &mut F) -> Result<(), Error> {
        // Scan back block by block until we find a page header with a value of 1.
        let mut block_start = self.log_length - PAGE_SIZE as u64;
        let mut scratch_buffer = Vec::new();
        scratch_buffer.resize(4, 0);
        let last_transaction_id = loop {
            log.seek(SeekFrom::Start(block_start))?;
            // Read the page header
            log.read_exact(&mut scratch_buffer[0..4])?;
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
                    log.read_exact(&mut scratch_buffer)?;
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
            .initialize(last_transaction_id + 1, self.log_length);
        Ok(())
    }
}

struct LogWriter<'a, F> {
    state: State,
    handles: Vec<TransactionHandle<'a>>,
    vault: Option<Arc<dyn Vault>>,
    _file: PhantomData<F>,
}

impl<'a, F: ManagedFile> FileOp<F> for LogWriter<'a, F> {
    type Output = ();
    fn execute(&mut self, log: &mut F) -> Result<(), Error> {
        let mut log_position = self.state.lock_for_write();
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
                log.write_all(&scratch_buffer)?;
                offset += bytes_to_write;
                *log_position += PAGE_SIZE as u64;
            }
        }

        drop(log_position);

        log.flush()?;

        Ok(())
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

    use super::*;
    use crate::{
        managed_file::{fs::StdFile, memory::MemoryFile, ManagedFile},
        test_util::RotatorVault,
        transaction::TransactionManager,
        ChunkCache,
    };

    fn tokio_log_file_tests() {
        log_file_tests::<StdFile>("tokio_log_file", None, None);
    }

    fn memory_log_file_tests() {
        log_file_tests::<MemoryFile>("memory_log_file", None, None);
    }

    fn tokio_encrypted_log_manager_tests() {
        log_manager_tests::<StdFile>(
            "encrypted_tokio_log_manager",
            Some(Arc::new(RotatorVault::new(13))),
            None,
        );
    }

    fn log_file_tests<F: ManagedFile>(
        file_name: &str,
        vault: Option<Arc<dyn Vault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        let file_manager = <F::Manager as Default>::default();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        std::fs::create_dir(&temp_dir).unwrap();
        let log_path = {
            let directory: &Path = &temp_dir;
            directory.join("transactions")
        };

        for id in 0..1_000 {
            let state = State::default();
            let result = TransactionLog::<F>::initialize_state(&state, &log_path, &context);
            if id > 0 {
                result.unwrap();
            }
            let mut transactions =
                TransactionLog::<F>::open(&log_path, state, context.clone()).unwrap();
            assert_eq!(transactions.current_transaction_id(), id);
            let mut tx = transactions.new_transaction(&[b"hello"]);

            tx.transaction.changes.insert(
                Cow::Borrowed(b"tree1"),
                Entries::Owned(vec![Entry {
                    document_id: U64::new(2),
                    sequence_id: U64::new(3),
                }]),
            );

            transactions.push(vec![tx]).unwrap();
            transactions.close().unwrap();
        }

        if context.vault.is_some() {
            // Test that we can't open it without encryption
            let state = State::default();
            assert!(TransactionLog::<F>::initialize_state(&state, &temp_dir, &context).is_err());
        }
    }

    fn tokio_log_manager_tests() {
        log_manager_tests::<StdFile>("tokio_log_manager", None, None);
    }

    fn memory_log_manager_tests() {
        log_manager_tests::<MemoryFile>("memory_log_manager", None, None);
    }

    fn log_manager_tests<F: ManagedFile + 'static>(
        file_name: &str,
        vault: Option<Arc<dyn Vault>>,
        cache: Option<ChunkCache>,
    ) {
        let temp_dir = crate::test_util::TestDirectory::new(file_name);
        std::fs::create_dir(&temp_dir).unwrap();
        let file_manager = <F::Manager as Default>::default();
        let context = Context {
            file_manager,
            vault,
            cache,
        };
        let manager = TransactionManager::spawn::<F>(&temp_dir, context).unwrap();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let manager = manager.clone();
            handles.push(std::thread::spawn(move || {
                for id in 0..1_000 {
                    let mut tx = manager.new_transaction(&[id.as_bytes()]);

                    tx.transaction.changes.insert(
                        Cow::Borrowed(b"tree1"),
                        Entries::Owned(vec![Entry {
                            document_id: U64::new(2),
                            sequence_id: U64::new(3),
                        }]),
                    );
                    manager.push(tx);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(manager.current_transaction_id(), 10_000);
        // TODO test scanning the file
    }
}
