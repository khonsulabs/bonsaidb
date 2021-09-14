//! Append-only B-Tree implementation
//!
//! The file format is inspired by
//! [Couchstore](https://github.com/couchbase/couchstore). The main difference
//! is that the file header has extra information to allow for cross-tree
//! transactions.
//!
//! ## Numbers and Alignment
//!
//! - All numbers are encoded in big-endian format/network byte order.
//! - All values are tightly packed. There is no padding or alignment that isn't
//!   explicitly included.
//!
//! ## File pages
//!
//! The file is written in pages that are 4,096 bytes long. Each page has single
//! `u8` representing the [`PageHeader`]. If data needs to span more than one
//! page, every 4,096 byte boundary must contain a [`PageHeader::Continuation`].
//!
//! ### File Headers
//!
//! If the header is a [`PageHeader::Header`], the contents of the block will be
//! a single chunk that contains a serialized [`TreeRoot`].
//!
//! ## Chunks
//!
//! Each time a document, B-Tree node, or header is written, it is written as a
//! chunk. If a [`Vault`] is in-use, each chunk will be pre-processed by the
//! vault before a `CRC-32-BZIP2` checksum is calculated. A chunk is limited to
//! 4 gigabytes of data (2^32).
//!
//! The chunk is written as:
//!
//! - `u32` - Data length, excluding the header.
//! - `u32` - CRC
//! - `[u8]` - Contents
//!
//! A data block may contain more than one chunk.

const VERSION: u8 = 0;

use std::{
    convert::TryFrom,
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use crc::{Crc, CRC_32_BZIP2};
use tokio::fs::OpenOptions;

use crate::{
    async_file::{AsyncFile, AsyncFileManager, FileOp, OpenableFile},
    tree::btree_root::BTreeRoot,
    Buffer, ChunkCache, Context, Error, Vault,
};

mod btree_entry;
mod btree_root;
mod by_id;
mod by_sequence;
mod interior;
mod key_entry;
mod modify;
mod serialization;
mod state;

use self::serialization::BinarySerialization;
pub use self::{
    modify::{Modification, Operation},
    state::State,
};

// The memory used by PagedWriter is PAGE_SIZE * PAGED_WRITER_BATCH_COUNT. E.g, 4096 * 4 = 16kb
const PAGE_SIZE: usize = 4096;
const PAGED_WRITER_BATCH_COUNT: usize = 4;

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_BZIP2);

enum PageHeader {
    // Using a strange value so that errors in the page writing/reading
    // algorithm are easier to detect.
    Continuation = 0xF1,
    Header = 1,
    Data = 2,
}

impl TryFrom<u8> for PageHeader {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xF1 => Ok(Self::Continuation),
            1 => Ok(Self::Header),
            2 => Ok(Self::Data),
            _ => Err(Error::data_integrity(format!(
                "invalid block header: {}",
                value
            ))),
        }
    }
}

macro_rules! commit_if_needed {
    ($self:ident, $about_to_write:expr) => {{
        if $self.offset == $self.scratch.len() {
            $self.commit().await?;
        } else if $about_to_write && $self.offset % PAGE_SIZE == 0 {
            $self.scratch[$self.offset] = PageHeader::Continuation as u8;
            $self.offset += 1;
        }
    }};
}

/// An append-only tree file.
///
/// ## Generics
/// - `F`: An [`AsyncFile`] implementor.
/// - `MAX_ORDER`: The maximum number of children a node in the tree can
///   contain. This implementation attempts to grow naturally towards this upper
///   limit. Changing this parameter does not automatically rebalance the tree,
///   but over time the tree will be updated.
pub struct TreeFile<F: AsyncFile, const MAX_ORDER: usize> {
    file: <F::Manager as AsyncFileManager<F>>::FileHandle,
    state: State<MAX_ORDER>,
    vault: Option<Arc<dyn Vault>>,
    cache: Option<ChunkCache>,
}

#[allow(clippy::future_not_send)]
impl<F: AsyncFile, const MAX_ORDER: usize> TreeFile<F, MAX_ORDER> {
    /// Returns a tree as contained in `file`.
    ///
    /// `state` should already be initialized using [`Self::initialize_state`] if the file exists.
    pub async fn new(
        file: <F::Manager as AsyncFileManager<F>>::FileHandle,
        state: State<MAX_ORDER>,
        vault: Option<Arc<dyn Vault>>,
        cache: Option<ChunkCache>,
    ) -> Result<Self, Error> {
        Ok(Self {
            file,
            state,
            vault,
            cache,
        })
    }

    /// Attempts to load the last saved state of this tree into `state`.
    pub async fn initialize_state(
        state: &State<MAX_ORDER>,
        file_path: &Path,
        context: &Context<F::Manager>,
    ) -> Result<(), Error> {
        let mut state = state.lock().await;
        if state.initialized() {
            return Ok(());
        }

        let mut file_length = context.file_manager.file_length(file_path).await?;
        if file_length == 0 {
            return Err(Error::message("empty transaction log"));
        }

        let excess_length = file_length % PAGE_SIZE as u64;
        if excess_length > 0 {
            // Truncate the file to the proper page size. This should only happen in a recovery situation.
            eprintln!(
                "Tree {:?} has {} extra bytes. Truncating.",
                file_path, excess_length
            );
            let file = OpenOptions::new()
                .append(true)
                .write(true)
                .open(&file_path)
                .await?;
            file_length -= excess_length;
            file.set_len(file_length).await?;
            file.sync_all().await?;
        }

        let mut tree = F::read(file_path).await?;

        // Scan back block by block until we find a header page.
        let mut block_start = file_length - PAGE_SIZE as u64;
        let mut scratch_buffer = vec![0_u8];
        let last_header = loop {
            // Read the page header
            scratch_buffer = match tree.read_exact(block_start, scratch_buffer, 1).await {
                (Ok(_), buffer) => buffer,
                (Err(err), _) => return Err(err),
            };
            #[allow(clippy::match_on_vec_items)]
            match PageHeader::try_from(scratch_buffer[0])? {
                PageHeader::Continuation | PageHeader::Data => {
                    if block_start == 0 {
                        return Err(Error::data_integrity(format!(
                            "Tree {:?} contained data, but no valid pages were found",
                            file_path
                        )));
                    }
                    block_start -= PAGE_SIZE as u64;
                    continue;
                }
                PageHeader::Header => {
                    let contents =
                        read_chunk(block_start + 1, &mut tree, context.vault(), context.cache())
                            .await?;
                    let root = BTreeRoot::deserialize(contents)
                        .map_err(|err| Error::DataIntegrity(Box::new(err)))?;
                    break root;
                }
            }
        };

        state.initialize(file_length, last_header);
        Ok(())
    }

    /// Returns the sequence that wrote this document.
    pub async fn push(
        &mut self,
        transaction_id: u64,
        key: Buffer<'static>,
        document: Buffer<'static>,
    ) -> Result<u64, Error> {
        self.file
            .execute(DocumentWriter {
                state: &self.state,
                vault: self.vault.as_deref(),
                cache: self.cache.as_ref(),
                modification: Some(Modification {
                    transaction_id,
                    keys: vec![key],
                    operation: Operation::Set(document),
                }),
            })
            .await
    }

    /// Returns the sequence that wrote this document.
    pub async fn modify(
        &mut self,
        modification: Modification<'static, Buffer<'static>>,
    ) -> Result<u64, Error> {
        self.file
            .execute(DocumentWriter {
                state: &self.state,
                vault: self.vault.as_deref(),
                cache: self.cache.as_ref(),
                modification: Some(modification),
            })
            .await
    }

    /// Gets the value stored for `key`.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Buffer<'static>>, Error> {
        self.file
            .execute(DocumentReader {
                state: &self.state,
                vault: self.vault.as_deref(),
                cache: self.cache.as_ref(),
                key,
            })
            .await
    }
}

struct DocumentWriter<'a, const MAX_ORDER: usize> {
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    modification: Option<Modification<'static, Buffer<'static>>>,
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile, const MAX_ORDER: usize> FileOp<F> for DocumentWriter<'a, MAX_ORDER> {
    type Output = u64;

    #[allow(clippy::shadow_unrelated)] // It is related, but clippy can't tell.
    async fn execute(&mut self, file: &mut F) -> Result<Self::Output, Error> {
        let mut state = self.state.lock().await;

        let mut data_block = PagedWriter::new(
            PageHeader::Data,
            file,
            self.vault.as_deref(),
            self.cache,
            state.current_position,
        )
        .await?;

        // Now that we have the document data's position, we can update the by_sequence and by_id indexes.
        state
            .header
            .modify(self.modification.take().unwrap(), &mut data_block)
            .await?;
        let new_header = state.header.serialize(&mut data_block).await?;
        let (file, after_data) = data_block.finish().await?;
        state.current_position = after_data;

        // Write a new header.
        let mut header_block = PagedWriter::new(
            PageHeader::Header,
            file,
            self.vault.as_deref(),
            self.cache,
            state.current_position,
        )
        .await?;
        header_block.write_chunk(&new_header).await?;

        let (file, after_header) = header_block.finish().await?;
        state.current_position = after_header;

        file.flush().await?;

        Ok(state.header.sequence)
    }
}

struct DocumentReader<'a, const MAX_ORDER: usize> {
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    key: &'a [u8],
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile, const MAX_ORDER: usize> FileOp<F> for DocumentReader<'a, MAX_ORDER> {
    type Output = Option<Buffer<'static>>;
    async fn execute(&mut self, file: &mut F) -> Result<Self::Output, Error> {
        let state = self.state.lock().await;
        state
            .header
            .get(self.key, file, self.vault, self.cache)
            .await
    }
}

#[allow(clippy::future_not_send)]
impl<'a, const MAX_ORDER: usize> DocumentWriter<'a, MAX_ORDER> {}

/// Writes data in pages, allowing for quick scanning through the file.
pub struct PagedWriter<'a, F: AsyncFile> {
    file: &'a mut F,
    vault: Option<&'a dyn Vault>,
    cache: Option<&'a ChunkCache>,
    position: u64,
    scratch: Vec<u8>,
    offset: usize,
}

impl<'a, F: AsyncFile> Deref for PagedWriter<'a, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.file
    }
}

impl<'a, F: AsyncFile> DerefMut for PagedWriter<'a, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file
    }
}

#[allow(clippy::future_not_send)]
impl<'a, F: AsyncFile> PagedWriter<'a, F> {
    async fn new(
        header: PageHeader,
        file: &'a mut F,
        vault: Option<&'a dyn Vault>,
        cache: Option<&'a ChunkCache>,
        position: u64,
    ) -> Result<PagedWriter<'a, F>, Error> {
        let mut writer = Self {
            file,
            vault,
            cache,
            position,
            scratch: vec![0; PAGE_SIZE * PAGED_WRITER_BATCH_COUNT],
            offset: 0,
        };
        writer.write_u8(header as u8).await?;
        Ok(writer)
    }

    fn current_position(&self) -> u64 {
        self.position + self.offset as u64
    }

    async fn write(&mut self, data: &[u8]) -> Result<usize, Error> {
        let bytes_written = data.len();
        commit_if_needed!(self, true);

        let new_offset = self.offset + data.len();
        let start_page = self.offset / PAGE_SIZE;
        let end_page = (new_offset - 1) / PAGE_SIZE;
        if start_page == end_page && new_offset <= self.scratch.len() {
            // Everything fits within the current page
            self.scratch[self.offset..new_offset].copy_from_slice(data);
            self.offset = new_offset;
        } else {
            // This won't fully fit within the page remainder. First, fill the remainder of the page
            let page_remaining = PAGE_SIZE - self.offset % PAGE_SIZE;
            let (fill_amount, mut remaining) = data.split_at(page_remaining);
            if !fill_amount.is_empty() {
                self.scratch[self.offset..self.offset + fill_amount.len()]
                    .copy_from_slice(fill_amount);
                self.offset += fill_amount.len();
            }

            // If the data is large enough to span multiple pages, continue to do so.
            while remaining.len() >= PAGE_SIZE {
                commit_if_needed!(self, true);

                let (one_page, after) = remaining.split_at(PAGE_SIZE - 1);
                remaining = after;

                self.scratch[self.offset..self.offset + PAGE_SIZE - 1].copy_from_slice(one_page);
                self.offset += PAGE_SIZE - 1;
            }

            commit_if_needed!(self, !remaining.is_empty());

            // If there's any data left, add it to the scratch
            if !remaining.is_empty() {
                let new_offset = self.offset + remaining.len();
                self.scratch[self.offset..new_offset].copy_from_slice(remaining);
                self.offset = new_offset;
            }

            commit_if_needed!(self, !remaining.is_empty());
        }
        Ok(bytes_written)
    }

    /// Writes a chunk of data to the file, after possibly encrypting it.
    /// Returns the position that this chunk can be read from in the file.
    #[allow(clippy::cast_possible_truncation)]
    async fn write_chunk(&mut self, contents: &[u8]) -> Result<u64, Error> {
        let possibly_encrypted = Buffer::from(
            self.vault
                .as_ref()
                .map_or_else(|| contents.to_vec(), |vault| vault.encrypt(contents)),
        );
        let length = u32::try_from(possibly_encrypted.len())
            .map_err(|_| Error::data_integrity("chunk too large"))?;
        let crc = CRC32.checksum(&possibly_encrypted);
        let mut position = self.current_position();
        // Ensure that the chunk header can be read contiguously
        let position_relative_to_page = position % PAGE_SIZE as u64;
        if position_relative_to_page + 8 > PAGE_SIZE as u64 {
            // Write the number of zeroes required to pad the current position
            // such that our header's 8 bytes will not be interrupted by a page
            // header.
            let bytes_needed = if position_relative_to_page == 0 {
                1
            } else {
                PAGE_SIZE - position_relative_to_page as usize
            };
            let zeroes = [0; 8];
            self.write(&zeroes[0..bytes_needed]).await?;
            position = self.current_position();
        }

        if position % PAGE_SIZE as u64 == 0 {
            // A page header will be written before our first byte.
            position += 1;
        }

        self.write_u32::<BigEndian>(length).await?;
        self.write_u32::<BigEndian>(crc).await?;
        self.write(&possibly_encrypted).await?;

        if let Some(cache) = self.cache {
            cache
                .insert(self.file.path(), position, possibly_encrypted)
                .await;
        }

        Ok(position)
    }

    async fn read_chunk(&mut self, position: u64) -> Result<Buffer<'static>, Error> {
        read_chunk(position, self.file, self.vault, self.cache).await
    }

    async fn write_u8(&mut self, value: u8) -> Result<usize, Error> {
        self.write(&[value]).await
    }

    async fn write_u16<B: ByteOrder>(&mut self, value: u16) -> Result<usize, Error> {
        let mut buffer = [0_u8; 2];
        B::write_u16(&mut buffer, value);
        self.write(&buffer).await
    }

    async fn write_u32<B: ByteOrder>(&mut self, value: u32) -> Result<usize, Error> {
        let mut buffer = [0_u8; 4];
        B::write_u32(&mut buffer, value);
        self.write(&buffer).await
    }

    async fn write_u64<B: ByteOrder>(&mut self, value: u64) -> Result<usize, Error> {
        let mut buffer = [0_u8; 8];
        B::write_u64(&mut buffer, value);
        self.write(&buffer).await
    }

    /// Writes the scratch buffer and resets `offset`.
    #[cfg_attr(not(debug_assertions), allow(unused_mut))]
    async fn commit(&mut self) -> Result<(), Error> {
        let mut buffer = std::mem::take(&mut self.scratch);
        let length = (self.offset + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE;

        // In debug builds, fill the padding with a recognizable number: the
        // answer to the ultimate question of life, the universe and everything.
        // For refence, 42 in hex is 0x2A.
        #[cfg(debug_assertions)]
        if self.offset < length {
            buffer[self.offset..length].fill(42);
        }

        let result = self.file.write_all(self.position, buffer, 0, length).await;
        self.scratch = result.1;

        if result.0.is_ok() {
            self.position += length as u64;
        }

        // Set the header to be a continuation block
        self.scratch[0] = PageHeader::Continuation as u8;
        self.offset = 1;
        result.0
    }

    async fn finish(mut self) -> Result<(&'a mut F, u64), Error> {
        if self.offset > 0 {
            self.commit().await?;
        }
        Ok((self.file, self.position))
    }
}

#[allow(clippy::future_not_send)]
#[allow(clippy::cast_possible_truncation)]
async fn read_chunk<F: AsyncFile>(
    position: u64,
    file: &mut F,
    vault: Option<&dyn Vault>,
    cache: Option<&ChunkCache>,
) -> Result<Buffer<'static>, Error> {
    if let Some(cache) = cache {
        if let Some(buffer) = cache.get(file.path(), position).await {
            return Ok(buffer);
        }
    }

    // Read the chunk header
    let mut scratch = Vec::new();
    scratch.reserve(PAGE_SIZE);
    scratch.resize(8, 0);
    let (result, mut scratch) = file.read_exact(position, scratch, 8).await;
    result?;
    let length = BigEndian::read_u32(&scratch[0..4]) as usize;
    let crc = BigEndian::read_u32(&scratch[4..8]);

    let mut data_start = position + 8;
    // If the data starts on a page boundary, there will have been a page
    // boundary inserted. Note: We don't read this byte, so technically it's
    // unchecked as part of this process.
    if data_start % PAGE_SIZE as u64 == 0 {
        data_start += 1;
    }

    let data_start_relative_to_page = data_start % PAGE_SIZE as u64;
    let data_end_relative_to_page = data_start_relative_to_page + length as u64;
    // Minus 2 may look like code cruft, but it's due to the correct value being
    // `data_end_relative_to_page as usize - 1`, except that if a write occurs
    // that ends exactly on a page boundary, we omit the boundary.
    let number_of_page_boundaries = (data_end_relative_to_page as usize - 2) / (PAGE_SIZE - 1);

    let data_page_start = data_start - data_start % PAGE_SIZE as u64;
    let total_bytes_to_read = length + number_of_page_boundaries;
    scratch.resize(total_bytes_to_read, 0);
    let (result, mut scratch) = file
        .read_exact(data_start, scratch, total_bytes_to_read)
        .await;
    result?;

    // We need to remove the `PageHeader::Continuation` bytes before continuing.
    let first_page_relative_end = data_page_start + PAGE_SIZE as u64 - data_start;
    for page_num in (0..number_of_page_boundaries).rev() {
        let continuation_byte = first_page_relative_end as usize + page_num * PAGE_SIZE;
        if scratch.remove(continuation_byte) != PageHeader::Continuation as u8 {
            return Err(Error::data_integrity(format!(
                "Expected PageHeader::Continuation at {}",
                position + continuation_byte as u64
            )));
        }
    }

    // This is an extra sanity check on the above algorithm, but given the
    // thoroughness of the unit test around this functionality, it's only
    // checked in debug mode. The CRC will still fail on bad reads, but noticing
    // the length is incorrect is a sign that the byte removal loop is bad.
    debug_assert_eq!(scratch.len(), length);

    let computed_crc = CRC32.checksum(&scratch);
    if crc != computed_crc {
        return Err(Error::data_integrity(format!(
            "crc32 failure on chunk at position {}",
            position
        )));
    }

    let decrypted = Buffer::from(match vault {
        Some(vault) => vault.decrypt(&scratch),
        None => scratch,
    });

    if let Some(cache) = cache {
        cache.insert(file.path(), position, decrypted.clone()).await;
    }

    Ok(decrypted)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use nanorand::{Pcg64, Rng};

    use super::*;
    use crate::{
        async_file::{memory::MemoryFile, tokio::TokioFileManager},
        TokioFile,
    };

    async fn test_paged_write(offset: usize, length: usize) -> Result<(), Error> {
        let mut file = MemoryFile::append(format!("test-{}-{}", offset, length)).await?;
        let mut paged_writer =
            PagedWriter::new(PageHeader::Header, &mut file, None, None, 0).await?;

        let mut scratch = Vec::new();
        scratch.resize(offset.max(length), 0);
        if offset > 0 {
            paged_writer.write(&scratch[..offset]).await?;
        }
        scratch.fill(1);
        let written_position = paged_writer.write_chunk(&scratch[..length]).await?;
        drop(paged_writer.finish().await?);

        let data = read_chunk(written_position, &mut file, None, None).await?;
        assert_eq!(data.len(), length);
        assert!(data.iter().all(|i| i == &1));
        drop(file);

        Ok(())
    }

    /// Tests the writing of pages at various boundaries. This should cover
    /// every edge case: offset is on page 1/2/3, write stays on first page or
    /// lands on page 1/2/3 end or extends onto page 4.
    #[tokio::test]
    async fn paged_writer() {
        test_paged_write(4087, 1).await.unwrap();
        for offset in 0..=PAGE_SIZE * 2 {
            for length in [
                1,
                PAGE_SIZE / 2,
                PAGE_SIZE,
                PAGE_SIZE * 3 / 2,
                PAGE_SIZE * 2,
            ] {
                if let Err(err) = test_paged_write(offset, length).await {
                    unreachable!(
                        "paged writer failure at offset {} length {}: {:?}",
                        offset, length, err
                    );
                }
            }
        }
    }

    async fn insert_one_record<F: AsyncFile, const MAX_ORDER: usize>(
        context: &Context<F::Manager>,
        file_path: &Path,
        ids: &mut HashSet<u64>,
        rng: &mut Pcg64,
    ) {
        let id = loop {
            let id = rng.generate::<u64>();
            if ids.insert(id) {
                break id;
            }
        };
        let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
        {
            let state = State::default();
            if ids.len() > 1 {
                TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context)
                    .await
                    .unwrap();
            }
            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .await
            .unwrap();
            tree.push(0, id_buffer.clone(), Buffer::from(b"hello world"))
                .await
                .unwrap();

            // This shouldn't have to scan the file, as the data fits in memory.
            let value = tree.get(&id_buffer).await.unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }

        // Try loading the file up and retrieving the data.
        {
            let state = State::default();
            TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context)
                .await
                .unwrap();

            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .await
            .unwrap();
            let value = tree.get(&id_buffer).await.unwrap();
            assert_eq!(&*value.unwrap(), b"hello world");
        }
    }

    #[tokio::test]
    async fn test() {
        const ORDER: usize = 4;

        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: TokioFileManager::default(),
            vault: None,
            cache: None,
        };
        let temp_dir = crate::test_util::TestDirectory::new("btree-tests");
        tokio::fs::create_dir(&temp_dir).await.unwrap();
        let file_path = temp_dir.join("tree");
        let mut ids = HashSet::new();
        // Insert up to the limit of a LEAF, which is ORDER - 1.
        for _ in 0..ORDER - 1 {
            insert_one_record::<TokioFile, ORDER>(&context, &file_path, &mut ids, &mut rng).await;
        }
        println!("Successfully inserted up to ORDER - 1 nodes.");

        // The next record will split the node
        insert_one_record::<TokioFile, ORDER>(&context, &file_path, &mut ids, &mut rng).await;
        println!("Successfully introduced one layer of depth.");

        // Insert a lot more.
        for _ in 0..1_000 {
            insert_one_record::<TokioFile, ORDER>(&context, &file_path, &mut ids, &mut rng).await;
        }
    }

    #[test]
    fn spam_insert_tokio() {
        spam_insert::<TokioFile>("tokio");
    }

    #[test]
    #[cfg(feature = "uring")]
    fn spam_insert_uring() {
        spam_insert::<crate::UringFile>("uring");
    }

    fn spam_insert<F: AsyncFile>(name: &str) {
        const ORDER: usize = 100;
        const RECORDS: usize = 1_000;
        F::Manager::run(async {
            let mut rng = Pcg64::new_seed(1);
            let ids = (0..RECORDS).map(|_| rng.generate::<u64>());
            let context = Context {
                file_manager: F::Manager::default(),
                vault: None,
                cache: Some(ChunkCache::new(100, 160_384)),
            };
            let temp_dir = crate::test_util::TestDirectory::new(format!("spam-inserts-{}", name));
            tokio::fs::create_dir(&temp_dir).await.unwrap();
            let file_path = temp_dir.join("tree");
            let state = State::default();
            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<F, ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .await
            .unwrap();
            for (_index, id) in ids.enumerate() {
                let id_buffer = Buffer::from(id.to_be_bytes().to_vec());
                tree.push(0, id_buffer.clone(), Buffer::from(b"hello world"))
                    .await
                    .unwrap();
            }
        });
    }

    #[test]
    fn bulk_insert_tokio() {
        bulk_insert::<TokioFile>("tokio");
    }

    #[test]
    #[cfg(feature = "uring")]
    fn bulk_insert_uring() {
        bulk_insert::<crate::UringFile>("uring");
    }

    fn bulk_insert<F: AsyncFile>(name: &str) {
        const ORDER: usize = 10;
        const RECORDS_PER_BATCH: usize = 10;
        const BATCHES: usize = 1000;
        F::Manager::run(async {
            let mut rng = Pcg64::new_seed(1);
            let context = Context {
                file_manager: F::Manager::default(),
                vault: None,
                cache: Some(ChunkCache::new(100, 160_384)),
            };
            let temp_dir = crate::test_util::TestDirectory::new(format!("bulk-inserts-{}", name));
            tokio::fs::create_dir(&temp_dir).await.unwrap();
            let file_path = temp_dir.join("tree");
            let state = State::default();
            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<F, ORDER>::new(
                file,
                state,
                context.vault.clone(),
                context.cache.clone(),
            )
            .await
            .unwrap();
            for id in 0..BATCHES {
                let mut ids = (0..RECORDS_PER_BATCH)
                    .map(|_| rng.generate::<u64>())
                    .collect::<Vec<_>>();
                ids.sort_unstable();
                let modification = Modification {
                    transaction_id: id as u64,
                    keys: ids
                        .iter()
                        .map(|id| Buffer::from(id.to_be_bytes().to_vec()))
                        .collect(),
                    operation: Operation::Set(Buffer::from(b"hello world")),
                };
                tree.modify(modification).await.unwrap();

                // Try five random gets
                for _ in 0..5 {
                    let index = rng.generate_range(0..ids.len());
                    let id = dbg!(Buffer::from(ids[index].to_be_bytes().to_vec()));
                    let value = tree.get(&id).await.unwrap();
                    assert_eq!(&*value.unwrap(), b"hello world");
                }
            }
        });
    }
}
