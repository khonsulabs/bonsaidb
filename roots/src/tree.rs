//! Append-only BTree implementation
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
//! ## File blocks
//!
//! The file is written in blocks that are 4,096 bytes long. Each block has a
//! header:
//!
//! - 1 bit: Block Type
//!   - `0`: Data
//!   - `1`: Header
//! - 31 bits: Encryption Key ID
//! - 32 bits: Block CRC
//! - 32 bits: Block length
//!
//! The data is written after the header. Every 4,096 bytes a `0` byte will be
//! emitted to ensure that each offset at 4,096 can denote whether or not a
//! block contains a header. The block length field contains the actual data
//! length, not including the extra `0` bytes.
//!
//! ### Encryption
//!
//! When an encryption ID is specified, the chunks in the block are encrypted
//! according to the algorithm reported by the key store. The key store is asked
//! to encrypt and decrypt chunks with the given key ID.
//!
//! ### Data blocks
//!
//! Data blocks are written in chunks which contain this header:
//!
//! - 32 bits: Chunk Length
//! - 32 bits: CRC32 checksum
//!
//! The bytes of the chunk follow.
//!
//! ### Header Blocks
//!
//! Each header is prefixed by a 32 bit crc covering the Header struct and nodes
//!
//! - 32 bits: Magic code + version: `'bdb\0'`
//! - 64 bits: Transaction ID
//! - 1 bit: Reserved
//! - 31 bits: Encryption Key ID
//! - 64 bits: Next Sequence ID
//! - 16 bits: Size of By-Sequence B-Tree root
//! - 16 bits: Size of By-Id B-Tree root
//!
//! If Encryption Key ID is non-zero, the bytes that comprise the b-tree roots
//! will be encrypted as a single payload using the encryption key provided.
//!
//! The following bytes are the `By-Sequence` node bytes, followed by the
//! `By-Id` node bytes. The header only contains the root nodes. Any additional
//! nodes that are changed during a write will be contained within a data block.
//!
//! ## B-Tree Nodes
//!
//! - 8 bits: 1 if a Leaf Node, 0 if a Pointer Node.
//! - 12 bits: Key Size
//! - 28 bits: Value size
//! - Key data
//! - Leaf Nodes:
//!   - Value data
//! - Pointer Nodes:
//!   - 64 bits: Position on disk
//!   - 64 bits: Sub tree size. If subtree is a Leaf, it is the size of the node
//! - Non-root nodes:
//!   - 16 bits: Reduce value size
//!   - Reduce Value: Statistics that vary based on the index type.
//!
//! ## By-Id Index Values
//!
//! This B-tree is an index of documents by their IDs, ordered by byte
//! comparisons. The values are:
//!
//! - 64 bits: Sequence ID
//! - 32 bits: Document Size
//! - 64 bits: Position on disk. 0 means the document is deleted.
//!
//! ### Reduce Value
//!
//! The statistics tracked in the Reduce Value for By-Id Indexvalues are:
//!
//! - 64 bits: Alive document count
//! - 64 bits: Deleted document count
//! - 64 bits: Total disk size of document content.
//!
//! ## By-Sequence Index Values
//!
//! This B-tree is an index of documents by their sequence ID, ordered
//! ascending. The values are:
//!
//! - 16 bits: Document ID Size
//! - 32 bits: Document Size
//! - 64 bits: Position on disk. 0 means the document is deleted.
//!
//! ### Reduce Value
//!
//! The statistics tracked in the Reduce Value for By-Sequence Index values are:
//!
//! - 64 bits: Number of Records

use crate::async_file::AsyncFile;

const fn magic_code(version: u8) -> u32 {
    ('b' as u32) << 24 | ('d' as u32) << 16 | ('b' as u32) << 8 | version as u32
}

#[derive(Debug, zerocopy::FromBytes)]
#[repr(C)]
pub struct Header {
    magic_code: u32,
    transaction_id: u64,
    encryption_key: u32,
    next_sequence: u64,
    by_sequnce_size: u16,
    by_id_size: u16,
}

impl Header {
    pub const fn encryption_key(&self) -> u32 {
        self.encryption_key & 0x7FFFFFFF
    }
}

#[derive(Debug, zerocopy::FromBytes)]
#[repr(C)]
pub struct BlockHeader {
    type_and_encryption_key: u32,
    crc: u32,
    length: u32,
}

impl BlockHeader {
    pub const fn is_file_header(&self) -> bool {
        self.type_and_encryption_key & 0xF0000000 != 0
    }

    pub const fn encryption_key_id(&self) -> u32 {
        self.type_and_encryption_key & 0x7FFFFFFF
    }
}

#[derive(Debug, zerocopy::FromBytes)]
#[repr(C)]
pub struct ChunkHeader {
    crc: u32,
    length: u32,
}

pub struct Tree<F: AsyncFile> {
    file: F,
}
