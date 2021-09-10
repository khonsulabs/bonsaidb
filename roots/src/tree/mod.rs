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

use std::{
    borrow::Cow,
    cmp::Ordering,
    convert::TryFrom,
    fmt::{Debug, Write},
    io::{self, ErrorKind, Read},
    ops::{Deref, DerefMut},
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_BZIP2};
use futures::{future::LocalBoxFuture, FutureExt};
use tokio::fs::OpenOptions;

use crate::{
    async_file::{AsyncFile, AsyncFileManager, FileOp, OpenableFile},
    error::InternalError,
    Context, Error, Vault,
};

mod by_id;
mod by_sequence;
mod interior;
mod state;

use self::{
    by_id::{ByIdIndex, ByIdStats},
    by_sequence::{BySequenceIndex, BySequenceStats},
    interior::Interior,
    state::State,
};

const PAGE_SIZE: usize = 4096;

const fn magic_code(version: u8) -> u32 {
    ('b' as u32) << 24 | ('d' as u32) << 16 | ('b' as u32) << 8 | version as u32
}

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

pub struct TreeFile<F: AsyncFile, const MAX_ORDER: usize> {
    file: <F::Manager as AsyncFileManager<F>>::FileHandle,
    state: State<MAX_ORDER>,
    vault: Option<Arc<dyn Vault>>,
}

#[allow(clippy::future_not_send)]
impl<F: AsyncFile, const MAX_ORDER: usize> TreeFile<F, MAX_ORDER> {
    pub async fn open(
        file: <F::Manager as AsyncFileManager<F>>::FileHandle,
        state: State<MAX_ORDER>,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        Ok(Self { file, state, vault })
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
                        panic!(
                            "Tree {:?} contained data, but no valid pages were found",
                            file_path
                        );
                    }
                    block_start -= PAGE_SIZE as u64;
                    continue;
                }
                PageHeader::Header => {
                    let contents = read_chunk(block_start + 1, &mut tree, context.vault()).await?;
                    let root = TreeRoot::deserialize(ScratchBuffer::from(contents))
                        .map_err(|err| Error::DataIntegrity(Box::new(err)))?;
                    break root;
                }
            }
        };

        state.initialize(file_length, last_header);
        Ok(())
    }

    /// Returns the sequence that wrote this document.
    pub async fn push(&mut self, key: ScratchBuffer, document: &[u8]) -> Result<u64, Error> {
        self.file
            .write(DocumentWriter {
                state: &self.state,
                vault: self.vault.as_deref(),
                key,
                document,
            })
            .await
    }

    /// Gets the value stored for `key`.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.file
            .write(DocumentReader {
                state: &self.state,
                vault: self.vault.as_deref(),
                key,
            })
            .await
    }
}

struct DocumentWriter<'a, const MAX_ORDER: usize> {
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    key: ScratchBuffer,
    document: &'a [u8],
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile, const MAX_ORDER: usize> FileOp<F> for DocumentWriter<'a, MAX_ORDER> {
    type Output = u64;

    #[allow(clippy::shadow_unrelated)] // It is related, but clippy can't tell.
    async fn write(&mut self, file: &mut F) -> Result<Self::Output, Error> {
        let mut state = self.state.lock().await;

        let mut data_block = PagedWriter::new(
            PageHeader::Data,
            file,
            self.vault.as_deref(),
            state.current_position,
        )
        .await?;

        let document_position = data_block.write_chunk(self.document).await?;

        // Now that we have the document data's position, we can update the by_sequence and by_id indexes.
        state
            .header
            .insert(
                self.key.clone(),
                u32::try_from(self.document.len()).unwrap(),
                document_position,
                &mut data_block,
            )
            .await?;
        let (file, after_data) = data_block.finish().await?;
        state.current_position = after_data;

        // Write a new header.
        let mut header_block = PagedWriter::new(
            PageHeader::Header,
            file,
            self.vault.as_deref(),
            state.current_position,
        )
        .await?;
        let data = state.header.serialize()?;
        header_block.write_chunk(&data).await?;

        let (file, after_header) = header_block.finish().await?;
        state.current_position = after_header;

        file.flush().await?;

        Ok(state.header.sequence)
    }
}

struct DocumentReader<'a, const MAX_ORDER: usize> {
    state: &'a State<MAX_ORDER>,
    vault: Option<&'a dyn Vault>,
    key: &'a [u8],
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile, const MAX_ORDER: usize> FileOp<F> for DocumentReader<'a, MAX_ORDER> {
    type Output = Option<Vec<u8>>;
    async fn write(&mut self, file: &mut F) -> Result<Self::Output, Error> {
        let state = self.state.lock().await;
        state.header.get(self.key, file, self.vault).await
    }
}

#[allow(clippy::future_not_send)]
impl<'a, const MAX_ORDER: usize> DocumentWriter<'a, MAX_ORDER> {}

struct PagedWriter<'a, F: AsyncFile> {
    file: &'a mut F,
    vault: Option<&'a dyn Vault>,
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
        position: u64,
    ) -> Result<PagedWriter<'a, F>, Error> {
        let mut writer = Self {
            file,
            vault,
            position,
            scratch: vec![0; PAGE_SIZE],
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
        let scratch_remaining = PAGE_SIZE - self.offset;
        let new_offset = self.offset + data.len();
        if new_offset <= PAGE_SIZE {
            self.scratch[self.offset..new_offset].copy_from_slice(data);
            self.offset = new_offset;
        } else {
            // This won't fully fit within the scratch buffer. First, fill the remainder of scratch and write it.
            let (fill_amount, mut remaining) = data.split_at(scratch_remaining);
            if !fill_amount.is_empty() {
                self.scratch[self.offset..PAGE_SIZE].copy_from_slice(fill_amount);
                self.offset = PAGE_SIZE;
            }
            self.commit().await?;

            // If the data is large enough to span multiple pages, continue to do so.
            while remaining.len() >= PAGE_SIZE - 1 {
                let (one_page, after) = remaining.split_at(PAGE_SIZE - 1);
                remaining = after;
                self.scratch[self.offset..PAGE_SIZE].copy_from_slice(one_page);
                self.offset = PAGE_SIZE;
                self.commit().await?;
            }

            // If there's any data left, add it to the scratch
            if !remaining.is_empty() {
                let new_offset = self.offset + remaining.len();
                self.scratch[self.offset..new_offset].copy_from_slice(remaining);
                self.offset = new_offset;
            }
        }
        Ok(bytes_written)
    }

    /// Writes a chunk of data to the file, after possibly encrypting it.
    /// Returns the position that this chunk can be read from in the file.
    #[allow(clippy::cast_possible_truncation)]
    async fn write_chunk(&mut self, contents: &[u8]) -> Result<u64, Error> {
        let possibly_encrypted = self.vault.as_ref().map_or_else(
            || Cow::Borrowed(contents),
            |vault| Cow::Owned(vault.encrypt(contents)),
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

        Ok(position)
    }

    async fn read_chunk(&mut self, position: u64) -> Result<Vec<u8>, Error> {
        read_chunk(position, self.file, self.vault).await
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

    /// Writes the page and resets `offset`.
    async fn commit(&mut self) -> Result<(), Error> {
        let mut buffer = std::mem::take(&mut self.scratch);

        // In debug builds, fill the padding with a recognizable number: the
        // answer to the ultimate question of life, the universe and everything.
        // For refence, 42 in hex is 0x2A.
        #[cfg(debug_assertions)]
        if self.offset < PAGE_SIZE {
            buffer[self.offset..].fill(42);
        }

        let result = self
            .file
            .write_all(self.position, buffer, 0, PAGE_SIZE)
            .await;
        self.scratch = result.1;

        if result.0.is_ok() {
            self.position += PAGE_SIZE as u64;
        }

        // Set the header to be a continuation block
        self.scratch[0] = PageHeader::Continuation as u8;
        self.offset = 1;
        result.0
    }

    async fn finish(mut self) -> Result<(&'a mut F, u64), Error> {
        self.commit().await?;
        Ok((self.file, self.position))
    }
}

#[allow(clippy::future_not_send)]
#[allow(clippy::cast_possible_truncation)]
async fn read_chunk<F: AsyncFile>(
    position: u64,
    file: &mut F,
    vault: Option<&dyn Vault>,
) -> Result<Vec<u8>, Error> {
    // Read the chunk header
    let mut scratch = Vec::new();
    scratch.resize(8, 0);
    let (result, mut scratch) = file.read_exact(position, scratch, 8).await;
    result?;
    let length = BigEndian::read_u32(&scratch[0..4]) as usize;
    let crc = BigEndian::read_u32(&scratch[4..8]);

    let mut data_start = position + 8;
    /// If the data starts on a page boundary, there will have been a page
    /// boundary inserted. Note: We don't read this byte, so technically it's
    /// unchecked as part of this process.
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

    match vault {
        Some(vault) => Ok(vault.decrypt(&scratch)),
        None => Ok(scratch),
    }
}

#[derive(Clone, Default, Debug)]
pub struct TreeRoot<const MAX_ORDER: usize> {
    transaction_id: u64,
    sequence: u64,
    by_sequence_root: BTreeEntry<BySequenceIndex, BySequenceStats, MAX_ORDER>,
    by_id_root: BTreeEntry<ByIdIndex, ByIdStats, MAX_ORDER>,
}

enum ChangeResult<I: BinarySerialization, R: BinarySerialization, const MAX_ORDER: usize> {
    Unchanged,
    Replace(BTreeEntry<I, R, MAX_ORDER>),
    Split(BTreeEntry<I, R, MAX_ORDER>, BTreeEntry<I, R, MAX_ORDER>),
}

#[allow(clippy::future_not_send)]
impl<const MAX_ORDER: usize> TreeRoot<MAX_ORDER> {
    async fn insert<F: AsyncFile>(
        &mut self,
        key: ScratchBuffer,
        document_size: u32,
        document_position: u64,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error> {
        let new_sequence = self
            .sequence
            .checked_add(1)
            .expect("sequence rollover prevented");

        // Insert into both trees
        match self
            .by_sequence_root
            .insert(
                ScratchBuffer::from(new_sequence.to_be_bytes().to_vec()),
                BySequenceIndex {
                    document_id: key.clone(),
                    document_size,
                    position: document_position,
                },
                writer,
            )
            .await?
        {
            ChangeResult::Unchanged => unreachable!(),
            ChangeResult::Replace(new_root) => {
                self.by_sequence_root = new_root;
            }
            ChangeResult::Split(lower, upper) => {
                self.by_sequence_root
                    .split_root(lower, upper, writer)
                    .await?;
            }
        }
        match self
            .by_id_root
            .insert(
                key,
                ByIdIndex {
                    sequence_id: new_sequence,
                    document_size,
                    position: document_position,
                },
                writer,
            )
            .await?
        {
            ChangeResult::Unchanged => unreachable!(),
            ChangeResult::Replace(new_root) => {
                self.by_id_root = new_root;
            }
            ChangeResult::Split(lower, upper) => {
                self.by_id_root.split_root(lower, upper, writer).await?;
            }
        }

        self.sequence = new_sequence;

        Ok(())
    }

    async fn get<F: AsyncFile>(
        &self,
        key: &[u8],
        file: &mut F,
        vault: Option<&dyn Vault>,
    ) -> Result<Option<Vec<u8>>, Error> {
        match self.by_id_root.get(key, file, vault).await? {
            Some(entry) => {
                let contents = read_chunk(entry.position, file, vault).await?;
                Ok(Some(contents))
            }
            None => Ok(None),
        }
    }

    pub fn deserialize(mut bytes: ScratchBuffer) -> Result<Self, Error> {
        let transaction_id = bytes.read_u64::<BigEndian>()?;
        let sequence = bytes.read_u64::<BigEndian>()?;
        let by_sequence_size = bytes.read_u32::<BigEndian>()? as usize;
        let by_id_size = bytes.read_u32::<BigEndian>()? as usize;
        if by_sequence_size + by_id_size != bytes.len() {
            return Err(Error::data_integrity(format!(
                "Header reported index sizes {} and {}, but data has {} remaining",
                by_sequence_size,
                by_id_size,
                bytes.len()
            )));
        };

        let mut by_sequence_bytes = bytes.read_bytes(by_sequence_size)?;
        let mut by_id_bytes = bytes.read_bytes(by_id_size)?;

        let by_sequence_root = BTreeEntry::deserialize_from(&mut by_sequence_bytes)?;
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id_bytes)?;

        Ok(Self {
            transaction_id,
            sequence,
            by_sequence_root,
            by_id_root,
        })
    }

    pub fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut output = Vec::new();
        output.write_u64::<BigEndian>(self.transaction_id)?;
        output.write_u64::<BigEndian>(self.sequence)?;
        // Reserve space for by_sequence and by_id sizes (2xu16).
        output.write_u64::<BigEndian>(0)?;

        let by_sequence_size = self.by_sequence_root.serialize_to(&mut output)?;
        let by_id_size = self.by_id_root.serialize_to(&mut output)?;

        let by_sequence_size = u32::try_from(by_sequence_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[16..20], by_sequence_size);
        let by_id_size = u32::try_from(by_id_size)
            .ok()
            .ok_or(Error::Internal(InternalError::HeaderTooLarge))?;
        BigEndian::write_u32(&mut output[20..24], by_id_size);

        Ok(output)
    }
}

/// A wrapper around a `Cow<'a, [u8]>` wrapper that implements Read, and has a
/// convenience method to take a slice of bytes as a `Cow<'a, [u8]>`.
#[derive(Clone)]
pub struct ScratchBuffer {
    buffer: Arc<Vec<u8>>,
    end: usize,
    position: usize,
}

impl Debug for ScratchBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut slice = self.as_slice();
        write!(f, "ScratchBuffer {{ length: {}, bytes: [", slice.len())?;
        while !slice.is_empty() {
            let (chunk, remaining) = slice.split_at(4.min(slice.len()));
            slice = remaining;
            for byte in chunk {
                write!(f, "{:x}", byte)?;
            }
            if !slice.is_empty() {
                f.write_char(' ')?;
            }
        }
        f.write_str("] }}")
    }
}

impl Eq for ScratchBuffer {}

impl PartialEq for ScratchBuffer {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for ScratchBuffer {
    fn cmp(&self, other: &Self) -> Ordering {
        if Arc::ptr_eq(&self.buffer, &other.buffer) {
            if self.position == other.position && self.end == other.end {
                Ordering::Equal
            } else {
                (&**self).cmp(&**other)
            }
        } else {
            (&**self).cmp(&**other)
        }
    }
}

impl PartialOrd for ScratchBuffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ScratchBuffer {
    pub fn read_bytes(&mut self, count: usize) -> Result<Self, std::io::Error> {
        let start = self.position;
        let end = self.position + count;
        if end > self.end {
            Err(std::io::Error::from(ErrorKind::UnexpectedEof))
        } else {
            self.position = end;
            Ok(Self {
                buffer: self.buffer.clone(),
                end,
                position: start,
            })
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[self.position..self.end]
    }
}

impl From<Vec<u8>> for ScratchBuffer {
    fn from(buffer: Vec<u8>) -> Self {
        Self {
            end: buffer.len(),
            buffer: Arc::new(buffer),
            position: 0,
        }
    }
}

impl Deref for ScratchBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl Read for ScratchBuffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let end = self.buffer.len().min(self.position + buf.len());
        let bytes_read = end - self.position;

        if bytes_read == 0 {
            return Err(io::Error::from(ErrorKind::UnexpectedEof));
        }

        buf.copy_from_slice(&self.buffer[self.position..end]);
        self.position = end;

        Ok(bytes_read)
    }
}

pub trait BinarySerialization: Sized {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error>;
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        self.serialize_to(&mut buffer)?;
        Ok(buffer)
    }
    fn deserialize_from(reader: &mut ScratchBuffer) -> Result<Self, Error>;
}

impl<I: BinarySerialization, R: BinarySerialization, const MAX_ORDER: usize> BinarySerialization
    for BTreeEntry<I, R, MAX_ORDER>
{
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match &self {
            Self::Leaf(leafs) => {
                debug_assert!(leafs.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer)?;
                }
            }
            Self::Interior(interiors) => {
                debug_assert!(interiors.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(0)?;
                bytes_written += 1;
                for interior in interiors {
                    bytes_written += interior.serialize_to(writer)?;
                }
            }
        }

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut ScratchBuffer) -> Result<Self, Error> {
        let node_header = reader.read_u8()?;
        match node_header {
            0 => {
                // Interior
                let mut nodes = Vec::new();
                while !reader.is_empty() {
                    nodes.push(Interior::deserialize_from(reader)?);
                }
                Ok(Self::Interior(nodes))
            }
            1 => {
                // Leaf
                let mut nodes = Vec::new();
                while !reader.is_empty() {
                    nodes.push(KeyEntry::deserialize_from(reader)?);
                }
                Ok(Self::Leaf(nodes))
            }
            _ => Err(Error::data_integrity("invalid node header")),
        }
    }
}

/// A B-Tree entry that stores a list of key-`I` pairs.
#[derive(Clone, Debug)]
pub enum BTreeEntry<I, R, const MAX_ORDER: usize> {
    /// An inline value. Overall, the B-Tree entry is a key-value pair.
    Leaf(Vec<KeyEntry<I>>),
    /// An interior node that contains pointers to other nodes.
    Interior(Vec<Interior<R>>),
}

impl<I, R, const MAX_ORDER: usize> Default for BTreeEntry<I, R, MAX_ORDER> {
    fn default() -> Self {
        Self::Leaf(Vec::new())
    }
}

pub trait Reducer<I> {
    fn reduce(indexes: &[&I]) -> Self;

    fn rereduce(reductions: &[&Self]) -> Self;
}

impl<I> Reducer<I> for () {
    fn reduce(_indexes: &[&I]) -> Self {}

    fn rereduce(_reductions: &[&Self]) -> Self {}
}

#[allow(clippy::future_not_send)]
impl<I, R, const MAX_ORDER: usize> BTreeEntry<I, R, MAX_ORDER>
where
    I: Clone + BinarySerialization + Debug,
    R: Clone + Reducer<I> + BinarySerialization + Debug,
{
    #[allow(clippy::too_many_lines)] // TODO refactor
    fn insert<'f, F: AsyncFile>(
        &'f self,
        key: ScratchBuffer,
        index: I,
        writer: &'f mut PagedWriter<'_, F>,
    ) -> LocalBoxFuture<'f, Result<ChangeResult<I, R, MAX_ORDER>, Error>> {
        async move {
            match self {
                BTreeEntry::Leaf(children) => {
                    let new_leaf = KeyEntry { key, index };
                    let mut children = children.iter().map(KeyEntry::clone).collect::<Vec<_>>();
                    match children.binary_search_by(|child| child.key.cmp(&new_leaf.key)) {
                        Ok(matching_index) => {
                            children[matching_index] = new_leaf;
                        }
                        Err(insert_at) => {
                            children.insert(insert_at, new_leaf);
                        }
                    }

                    if children.len() >= MAX_ORDER {
                        // We need to split this leaf into two leafs, moving a new interior node using the middle element.
                        let midpoint = children.len() / 2;
                        let (lower_half, upper_half) = children.split_at(midpoint);

                        Ok(ChangeResult::Split(
                            Self::Leaf(lower_half.to_vec()),
                            Self::Leaf(upper_half.to_vec()),
                        ))
                    } else {
                        Ok(ChangeResult::Replace(Self::Leaf(children)))
                    }
                }
                BTreeEntry::Interior(children) => {
                    let (containing_node_index, is_new_max) = children
                        .binary_search_by(|child| child.key.cmp(&key))
                        .map_or_else(
                            |not_found| {
                                if not_found == children.len() {
                                    // If we can't find a key less than what would fit
                                    // within our children, this key will become the new key
                                    // of the last child.
                                    (not_found - 1, true)
                                } else {
                                    (not_found, false)
                                }
                            },
                            |found| (found, false),
                        );

                    let child = &children[containing_node_index];
                    let chunk = writer.read_chunk(child.position).await?;
                    let mut reader = ScratchBuffer::from(chunk);
                    let entry = Self::deserialize_from(&mut reader)?;
                    let mut new_children = children.clone();
                    match entry.insert(key.clone(), index, writer).await? {
                        ChangeResult::Unchanged => unreachable!(),
                        ChangeResult::Replace(new_node) => {
                            let child_position = writer.write_chunk(&new_node.serialize()?).await?;
                            new_children[containing_node_index] = Interior {
                                key: if is_new_max {
                                    key.clone()
                                } else {
                                    child.key.clone()
                                },
                                position: child_position,
                                stats: new_node.stats(),
                            };
                        }
                        ChangeResult::Split(lower, upper) => {
                            // Write the two new children
                            let lower_position = writer.write_chunk(&lower.serialize()?).await?;
                            let upper_position = writer.write_chunk(&upper.serialize()?).await?;
                            // Replace the original child with the lower entry.
                            new_children[containing_node_index] = Interior {
                                key: lower.max_key().clone(),
                                position: lower_position,
                                stats: lower.stats(),
                            };
                            // Insert the upper entry at the next position.
                            new_children.insert(
                                containing_node_index + 1,
                                Interior {
                                    key: upper.max_key().clone(),
                                    position: upper_position,
                                    stats: upper.stats(),
                                },
                            );
                        }
                    };

                    if new_children.len() >= MAX_ORDER {
                        let midpoint = new_children.len() / 2;
                        let (_, upper_half) = new_children.split_at(midpoint);

                        // TODO this re-clones the upper-half children, but splitting a vec
                        // without causing multiple copies of data seems
                        // impossible without unsafe.
                        let upper_half = upper_half.to_vec();
                        assert_eq!(midpoint + upper_half.len(), new_children.len());
                        new_children.truncate(midpoint);

                        Ok(ChangeResult::Split(
                            Self::Interior(new_children),
                            Self::Interior(upper_half),
                        ))
                    } else {
                        Ok(ChangeResult::Replace(Self::Interior(new_children)))
                    }
                }
            }
        }
        .boxed_local()
    }

    fn stats(&self) -> R {
        match self {
            BTreeEntry::Leaf(children) => {
                R::reduce(&children.iter().map(|c| &c.index).collect::<Vec<_>>())
            }
            BTreeEntry::Interior(children) => {
                R::rereduce(&children.iter().map(|c| &c.stats).collect::<Vec<_>>())
            }
        }
    }

    fn max_key(&self) -> &ScratchBuffer {
        match self {
            BTreeEntry::Leaf(children) => &children.last().unwrap().key,
            BTreeEntry::Interior(children) => &children.last().unwrap().key,
        }
    }

    fn get<'f, F: AsyncFile>(
        &'f self,
        key: &'f [u8],
        file: &'f mut F,
        vault: Option<&'f dyn Vault>,
    ) -> LocalBoxFuture<'f, Result<Option<I>, Error>> {
        async move {
            match self {
                BTreeEntry::Leaf(children) => {
                    match children.binary_search_by(|child| (&*child.key).cmp(key)) {
                        Ok(matching) => {
                            let entry = &children[matching];
                            Ok(Some(entry.index.clone()))
                        }
                        Err(_) => Ok(None),
                    }
                }
                BTreeEntry::Interior(children) => {
                    let containing_node_index = children
                        .binary_search_by(|child| (&*child.key).cmp(key))
                        .unwrap_or_else(|not_found| not_found);

                    // This isn't guaranteed to succeed because we add one. If
                    // the key being searched for isn't contained, it will be
                    // greater than any of the node's keys.
                    if let Some(child) = children.get(containing_node_index) {
                        let chunk = read_chunk(child.position, file, vault).await?;
                        let mut reader = ScratchBuffer::from(chunk);
                        let entry = Self::deserialize_from(&mut reader)?;
                        entry.get(key, file, vault).await
                    } else {
                        Ok(None)
                    }
                }
            }
        }
        .boxed_local()
    }

    #[allow(clippy::too_many_lines)]
    async fn split_root<F: AsyncFile>(
        &mut self,
        lower: Self,
        upper: Self,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error> {
        // Write the two interiors as chunks
        let lower_half_position = writer.write_chunk(&lower.serialize()?).await?;
        let upper_half_position = writer.write_chunk(&upper.serialize()?).await?;

        // Regardless of what our current type is, the root will always be split
        // into interior nodes.
        *self = Self::Interior(vec![
            Interior {
                key: lower.max_key().clone(),
                position: lower_half_position,
                stats: lower.stats(),
            },
            Interior {
                key: upper.max_key().clone(),
                position: upper_half_position,
                stats: upper.stats(),
            },
        ]);
        Ok(())
    }
}

pub struct MappedKey<'a> {
    pub sequence: u64,
    pub contents: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct KeyEntry<I> {
    key: ScratchBuffer,
    index: I,
}

impl<I: BinarySerialization> BinarySerialization for KeyEntry<I> {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // Write the key
        let key_len = u16::try_from(self.key.len()).map_err(|_| Error::KeyTooLarge)?;
        writer.write_u16::<BigEndian>(key_len)?;
        writer.write_all(&self.key)?;
        bytes_written += 2 + key_len as usize;

        // Write the value
        bytes_written += self.index.serialize_to(writer)?;
        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut ScratchBuffer) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let key = reader.read_bytes(key_len)?;

        let value = I::deserialize_from(reader)?;

        Ok(Self { key, index: value })
    }
}

impl BinarySerialization for () {
    fn serialize_to<W: WriteBytesExt>(&self, _writer: &mut W) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(_reader: &mut ScratchBuffer) -> Result<Self, Error> {
        Ok(())
    }
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
        let mut paged_writer = PagedWriter::new(PageHeader::Header, &mut file, None, 0).await?;

        let mut scratch = Vec::new();
        scratch.resize(offset.max(length), 0);
        if offset > 0 {
            paged_writer.write(&scratch[..offset]).await?;
        }
        scratch.fill(1);
        let written_position = paged_writer.write_chunk(&scratch[..length]).await?;
        drop(paged_writer.finish().await?);

        let data = read_chunk(written_position, &mut file, None).await?;
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
        let id_buffer = ScratchBuffer::from(id.to_be_bytes().to_vec());
        {
            let state = State::default();
            if ids.len() > 1 {
                TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context)
                    .await
                    .unwrap();
            }
            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::open(file, state, context.vault.clone())
                .await
                .unwrap();
            tree.push(id_buffer.clone(), b"hello world").await.unwrap();

            // This shouldn't have to scan the file, as the data fits in memory.
            let value = tree.get(&id_buffer).await.unwrap();
            assert_eq!(&value.unwrap(), b"hello world");
        }

        // Try loading the file up and retrieving the data.
        {
            let state = State::default();
            TreeFile::<F, MAX_ORDER>::initialize_state(&state, file_path, context)
                .await
                .unwrap();

            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<F, MAX_ORDER>::open(file, state, context.vault.clone())
                .await
                .unwrap();
            let value = tree.get(&id_buffer).await.unwrap();
            assert_eq!(&value.unwrap(), b"hello world");
        }
    }

    #[tokio::test]
    async fn test() {
        const ORDER: usize = 4;

        let mut rng = Pcg64::new_seed(1);
        let context = Context {
            file_manager: TokioFileManager::default(),
            vault: None,
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
    fn spam_insert() {
        const ORDER: usize = 100;
        const RECORDS: usize = 1000;
        TokioFileManager::run(async {
            let mut rng = Pcg64::new_seed(1);
            let ids = (0..RECORDS).map(|_| rng.generate::<u64>());
            let context = Context {
                file_manager: TokioFileManager::default(),
                vault: None,
            };
            let temp_dir = crate::test_util::TestDirectory::new("spam-inserts");
            tokio::fs::create_dir(&temp_dir).await.unwrap();
            let file_path = temp_dir.join("tree");
            let state = State::default();
            let file = context.file_manager.append(file_path).await.unwrap();
            let mut tree = TreeFile::<TokioFile, ORDER>::open(file, state, context.vault.clone())
                .await
                .unwrap();
            for (_index, id) in ids.enumerate() {
                let id_buffer = ScratchBuffer::from(id.to_be_bytes().to_vec());
                tree.push(id_buffer.clone(), b"hello world").await.unwrap();
            }
        });
    }
}
