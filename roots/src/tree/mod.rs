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

use std::{borrow::Cow, convert::TryFrom, fmt::Debug, path::Path, sync::Arc};

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_BZIP2};
use tokio::fs::OpenOptions;

use crate::{
    async_file::{AsyncFile, AsyncFileManager, FileOp, OpenableFile},
    error::InternalError,
    Error, Vault,
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
    Continuation = 0,
    Header = 1,
    Data = 2,
}

impl TryFrom<u8> for PageHeader {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Continuation),
            1 => Ok(Self::Header),
            2 => Ok(Self::Data),
            _ => Err(Error::data_integrity(format!(
                "invalid block header: {}",
                value
            ))),
        }
    }
}

pub struct TreeFile<F: AsyncFile> {
    file: <F::Manager as AsyncFileManager<F>>::FileHandle,
    state: State,
    vault: Option<Arc<dyn Vault>>,
}

#[allow(clippy::future_not_send)]
impl<F: AsyncFile> TreeFile<F> {
    pub async fn open(
        file: <F::Manager as AsyncFileManager<F>>::FileHandle,
        state: State,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        Ok(Self { file, state, vault })
    }

    /// Attempts to load the last saved state of this tree into `state`.
    pub async fn initialize_state(
        state: &State,
        file_path: &Path,
        vault: Option<&dyn Vault>,
    ) -> Result<(), Error> {
        let mut state = state.lock().await;
        if state.initialized() {
            return Ok(());
        }

        let mut file_length = file_path.metadata()?.len();
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
                    let contents = read_chunk(block_start + 1, &mut tree, vault.as_deref()).await?;
                    let root = TreeRoot::deserialize(&contents)
                        .map_err(|err| Error::DataIntegrity(Box::new(err)))?;
                    break root.to_owned_lifetime();
                }
            }
        };

        state.initialize(file_length, last_header);
        Ok(())
    }

    /// Returns the sequence that wrote this document.
    pub async fn push(&mut self, key: &[u8], document: &[u8]) -> Result<u64, Error> {
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

struct DocumentWriter<'a> {
    state: &'a State,
    vault: Option<&'a dyn Vault>,
    key: &'a [u8],
    document: &'a [u8],
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile> FileOp<F> for DocumentWriter<'a> {
    type Output = u64;
    async fn write(&mut self, file: &mut F) -> Result<Self::Output, Error> {
        let mut state = self.state.lock().await;

        let mut data_block = PagedWriter::new(
            PageHeader::Data,
            file,
            self.vault.as_deref(),
            state.current_position,
        )
        .await?;

        let document_position = data_block.current_position();
        data_block.write_chunk(self.document).await?;

        // Now that we have the document data's position, we can update the by_sequence and by_id indexes.
        state
            .header
            .insert(
                self.key,
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

struct DocumentReader<'a> {
    state: &'a State,
    vault: Option<&'a dyn Vault>,
    key: &'a [u8],
}

#[async_trait(?Send)]
impl<'a, F: AsyncFile> FileOp<F> for DocumentReader<'a> {
    type Output = Option<Vec<u8>>;
    async fn write(&mut self, file: &mut F) -> Result<Self::Output, Error> {
        let state = self.state.lock().await;
        state.header.get(self.key, file, self.vault).await
    }
}

#[allow(clippy::future_not_send)]
impl<'a> DocumentWriter<'a> {}

struct PagedWriter<'a, F: AsyncFile> {
    file: &'a mut F,
    vault: Option<&'a dyn Vault>,
    position: u64,
    scratch: Vec<u8>,
    offset: usize,
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
            self.scratch[self.offset..PAGE_SIZE].copy_from_slice(fill_amount);
            self.commit().await?;

            // If the data is large enough to span multiple pages, continue to do so.
            while remaining.len() >= PAGE_SIZE - 1 {
                let (one_page, after) = remaining.split_at(PAGE_SIZE - 1);
                remaining = after;
                self.scratch[self.offset..PAGE_SIZE].copy_from_slice(one_page);
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

    async fn write_chunk(&mut self, contents: &[u8]) -> Result<(), Error> {
        let possibly_encrypted = self.vault.as_ref().map_or_else(
            || Cow::Borrowed(contents),
            |vault| Cow::Owned(vault.encrypt(contents)),
        );
        let length = u32::try_from(possibly_encrypted.len())
            .map_err(|_| Error::data_integrity("chunk too large"))?;
        let crc = CRC32.checksum(&possibly_encrypted);

        self.write_u32::<BigEndian>(length).await?;
        self.write_u32::<BigEndian>(crc).await?;
        self.write(&possibly_encrypted).await?;

        Ok(())
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
        let buffer = std::mem::take(&mut self.scratch);
        let result = self
            .file
            .write_all(self.position, buffer, 0, PAGE_SIZE)
            .await;
        self.scratch = result.1;

        if result.0.is_ok() {
            self.position += PAGE_SIZE as u64;
        }

        // Set the header to be a continuation block
        self.scratch[0] = 0;
        self.offset = 1;
        result.0
    }

    async fn finish(mut self) -> Result<(&'a mut F, u64), Error> {
        self.commit().await?;
        Ok((self.file, self.position))
    }
}

#[allow(clippy::future_not_send)]
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
    scratch.resize(length, 0);
    let (result, scratch) = file.read_exact(position + 8, scratch, length).await;
    result?;

    if crc != CRC32.checksum(&scratch) {
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

#[derive(Default, Debug)]
pub struct TreeRoot<'a> {
    transaction_id: u64,
    sequence: u64,
    by_sequence_root: BTreeEntry<'a, BySequenceIndex<'a>, BySequenceStats>,
    by_id_root: BTreeEntry<'a, ByIdIndex, ByIdStats>,
}

enum ChangeResult<'a, I: BinarySerialization<'a>, R: BinarySerialization<'a>> {
    Unchanged,
    Replace(BTreeEntry<'a, I, R>),
    Split(BTreeEntry<'a, I, R>, BTreeEntry<'a, I, R>),
}

impl<'a> Ownable for TreeRoot<'a> {
    type Output = TreeRoot<'static>;

    fn to_owned_lifetime(&self) -> Self::Output {
        TreeRoot {
            transaction_id: self.transaction_id,
            sequence: self.sequence,
            by_sequence_root: self.by_sequence_root.to_owned_lifetime(),
            by_id_root: self.by_id_root.to_owned_lifetime(),
        }
    }
}

#[allow(clippy::future_not_send)]
impl<'a> TreeRoot<'a> {
    async fn insert<F: AsyncFile>(
        &mut self,
        key: &[u8],
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
                &new_sequence.to_be_bytes(),
                BySequenceIndex {
                    document_id: Cow::Owned(key.to_vec()),
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
            ChangeResult::Split(_lower, _upper) => {
                todo!()
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
            ChangeResult::Split(_lower, _upper) => {
                todo!()
            }
        }

        self.sequence = new_sequence;

        Ok(())
    }

    async fn get<F: AsyncFile>(
        &self,
        key: &[u8],
        file: &'a mut F,
        vault: Option<&'a dyn Vault>,
    ) -> Result<Option<Vec<u8>>, Error> {
        match self.by_id_root.get(key).await? {
            Some(entry) => {
                let contents = read_chunk(entry.position, file, vault).await?;
                Ok(Some(contents))
            }
            None => Ok(None),
        }
    }

    pub fn deserialize(mut bytes: &'a [u8]) -> Result<Self, Error> {
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
        }
        let (mut by_sequence, mut by_id) = bytes.split_at(by_sequence_size);

        let by_sequence_root = BTreeEntry::deserialize_from(&mut by_sequence)?;
        let by_id_root = BTreeEntry::deserialize_from(&mut by_id)?;

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

enum Flags {
    Encrypted = 1,
}

pub trait BinarySerialization<'a>: Sized {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error>;
    fn serialize(&self) -> Result<Vec<u8>, Error> {
        let mut buffer = Vec::new();
        self.serialize_to(&mut buffer)?;
        Ok(buffer)
    }

    fn deserialize_from(reader: &mut &'a [u8]) -> Result<Self, Error>;
}

impl<'a, I: BinarySerialization<'a>, R: BinarySerialization<'a>> BinarySerialization<'a>
    for BTreeEntry<'a, I, R>
{
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match &self {
            Self::Leaf(leafs) => {
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer)?;
                }
            }
            Self::Interior(interiors) => {
                writer.write_u8(0)?;
                bytes_written += 1;
                for interior in interiors {
                    bytes_written += interior.serialize_to(writer)?;
                }
            }
        }

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut &'a [u8]) -> Result<Self, Error> {
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
pub enum BTreeEntry<'a, I, R> {
    /// An inline value. Overall, the B-Tree entry is a key-value pair.
    Leaf(Vec<KeyEntry<'a, I>>),
    /// An interior node that contains pointers to other nodes.
    Interior(Vec<Interior<'a, R>>),
}

impl<'a, I, R> Default for BTreeEntry<'a, I, R> {
    fn default() -> Self {
        Self::Leaf(Vec::new())
    }
}

/// The upper limit of the number of elements each node can directly contain.
const BTREE_ORDER: usize = 1_000;

pub trait Reducer<I> {
    fn reduce(indexes: &[&I]) -> Self;

    fn rereduce(reductions: &[&Self]) -> Self;
}

impl<I> Reducer<I> for () {
    fn reduce(_indexes: &[&I]) -> Self {}

    fn rereduce(_reductions: &[&Self]) -> Self {}
}

impl<'a, I: Ownable, R: Ownable> Ownable for BTreeEntry<'a, I, R> {
    type Output = BTreeEntry<'static, I::Output, R::Output>;

    fn to_owned_lifetime(&self) -> Self::Output {
        match self {
            Self::Leaf(children) => {
                BTreeEntry::Leaf(children.iter().map(|c| c.to_owned_lifetime()).collect())
            }
            Self::Interior(children) => {
                BTreeEntry::Interior(children.iter().map(|c| c.to_owned_lifetime()).collect())
            }
        }
    }
}

#[allow(clippy::future_not_send)]
impl<'a, I: Clone + BinarySerialization<'a>, R: Clone + Reducer<I> + BinarySerialization<'a>>
    BTreeEntry<'a, I, R>
{
    async fn insert<F: AsyncFile>(
        &self,
        key: &[u8],
        index: I,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<ChangeResult<'a, I, R>, Error> {
        match self {
            BTreeEntry::Leaf(children) => {
                let new_leaf = KeyEntry {
                    key: Cow::Owned(key.to_vec()),
                    index,
                };
                let mut children = children.iter().map(KeyEntry::to_owned).collect::<Vec<_>>();
                match children.binary_search_by_key(&key, |child| &child.key) {
                    Ok(matching_index) => {
                        children[matching_index] = new_leaf;
                    }
                    Err(insert_at) => {
                        children.insert(insert_at, new_leaf);
                    }
                }

                if children.len() >= BTREE_ORDER {
                    // We need to split this leaf into two leafs, moving a new interior node using the middle element.
                    // E.g., children.len() == 13, midpoint = (13 + 1) / 2 = 7
                    let midpoint = (children.len() + 1) / 2;
                    let (lower_half, upper_half) = children.split_at(midpoint);

                    // Calculate the statistics
                    let lower_half_stats =
                        R::reduce(&lower_half.iter().map(|l| &l.index).collect::<Vec<_>>());
                    let upper_half_stats =
                        R::reduce(&upper_half.iter().map(|l| &l.index).collect::<Vec<_>>());

                    // Write the two leafs as chunks
                    let lower_half_position = writer.current_position();
                    writer
                        .write_chunk(
                            &Self::Leaf(lower_half.iter().map(KeyEntry::to_owned).collect())
                                .serialize()?,
                        )
                        .await?;
                    let upper_half_position = writer.current_position();
                    writer
                        .write_chunk(&Self::Leaf(upper_half.to_vec()).serialize()?)
                        .await?;

                    Ok(ChangeResult::Replace(BTreeEntry::Interior(vec![
                        Interior {
                            key: Cow::Owned(lower_half.last().unwrap().key.to_vec()),
                            position: lower_half_position,
                            stats: lower_half_stats,
                        },
                        Interior {
                            key: Cow::Owned(upper_half.last().unwrap().key.to_vec()),
                            position: upper_half_position,
                            stats: upper_half_stats,
                        },
                    ])))
                } else {
                    Ok(ChangeResult::Replace(BTreeEntry::Leaf(children)))
                }
            }
            BTreeEntry::Interior(_pointers) => {
                // We need to find the location to insert this node at. It won't be inserted directly here.
                // let insert_into_index = pointers
                //     .binary_search_by_key(&key, |pointer| &pointer.key)
                //     .unwrap_or_else(|i| i);
                // load the node at the pointer
                // insert into the node
                // return the updated interior node
                todo!()
            }
        }
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

    async fn get(&self, key: &[u8]) -> Result<Option<I>, Error> {
        match self {
            BTreeEntry::Leaf(children) => {
                match children.binary_search_by_key(&key, |child| &child.key) {
                    Ok(matching) => {
                        let entry = &children[matching];
                        Ok(Some(entry.index.clone()))
                    }
                    Err(_) => Ok(None),
                }
            }
            BTreeEntry::Interior(_children) => {
                todo!()
            }
        }
    }
}

pub struct MappedKey<'a> {
    pub sequence: u64,
    pub contents: &'a [u8],
}

#[derive(Debug)]
pub struct KeyEntry<'a, I> {
    key: Cow<'a, [u8]>,
    index: I,
}

impl<'a, I: Clone> Clone for KeyEntry<'a, I> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            index: self.index.clone(),
        }
    }
}

impl<'a, I: Ownable> Ownable for KeyEntry<'a, I> {
    type Output = KeyEntry<'static, <I as Ownable>::Output>;

    fn to_owned_lifetime(&self) -> Self::Output {
        KeyEntry {
            key: Cow::Owned(self.key.to_vec()),
            index: self.index.to_owned_lifetime(),
        }
    }
}

impl<'a, I: BinarySerialization<'a>> BinarySerialization<'a> for KeyEntry<'a, I> {
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

    fn deserialize_from(reader: &mut &'a [u8]) -> Result<Self, Error> {
        let key_len = reader.read_u16::<BigEndian>()? as usize;
        if key_len > reader.len() {
            return Err(Error::data_integrity(format!(
                "key length {} found but only {} bytes remaining",
                key_len,
                reader.len()
            )));
        }
        let (key, remainder) = reader.split_at(key_len);
        *reader = remainder;

        let value = I::deserialize_from(reader)?;

        Ok(Self {
            key: Cow::Borrowed(key),
            index: value,
        })
    }
}

impl<'a> BinarySerialization<'a> for () {
    fn serialize_to<W: WriteBytesExt>(&self, _writer: &mut W) -> Result<usize, Error> {
        Ok(0)
    }

    fn deserialize_from(_reader: &mut &'a [u8]) -> Result<Self, Error> {
        Ok(())
    }
}

trait Ownable {
    type Output;
    fn to_owned_lifetime(&self) -> Self::Output;
}

trait NoLifetime: Clone {}

#[tokio::test]
async fn test() {
    use crate::async_file::tokio::{TokioFile, TokioFileManager};

    let manager = TokioFileManager::default();
    let temp_dir = crate::test_util::TestDirectory::new("btree-tests");
    tokio::fs::create_dir(&temp_dir).await.unwrap();

    {
        let state = State::default();
        let file = manager.append(temp_dir.join("tree")).await.unwrap();
        let mut tree = TreeFile::<TokioFile>::open(file, state, None)
            .await
            .unwrap();
        tree.push(b"test", b"hello world").await.unwrap();

        // This shouldn't have to scan the file, as the data fits in memory.
        let value = tree.get(b"test").await.unwrap();
        assert_eq!(&value.unwrap(), b"hello world");
    }

    // Try loading the file up and retrieving the data.
    {
        let file_path = temp_dir.join("tree");

        let state = State::default();
        TreeFile::<TokioFile>::initialize_state(&state, &file_path, None)
            .await
            .unwrap();

        let file = manager.append(&file_path).await.unwrap();
        let mut tree = TreeFile::<TokioFile>::open(file, state, None)
            .await
            .unwrap();
        let value = tree.get(b"test").await.unwrap();
        assert_eq!(&value.unwrap(), b"hello world");
    }
}
