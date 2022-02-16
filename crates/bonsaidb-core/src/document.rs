use core::cmp::Ordering;
use std::{
    borrow::Cow,
    fmt::{Display, Write},
    hash::Hash,
    ops::Deref,
    str::FromStr,
};

use arc_bytes::serde::{Bytes, CowBytes};
use serde::{de::Visitor, Deserialize, Serialize};

use crate::schema::{view::map::Mappings, Collection, Key, Map, SerializedCollection};

mod collection;
mod revision;
pub use collection::*;
pub use revision::Revision;

/// The header of a `Document`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The id of the Document. Unique across the collection `C`
    pub id: DocumentId,

    /// The revision of the stored document.
    pub revision: Revision,
}

/// A type that can return a [`Header`].
pub trait HasHeader {
    /// Returns the header for this instance.
    fn header(&self) -> Result<Header, crate::Error>;
}

impl HasHeader for Header {
    fn header(&self) -> Result<Header, crate::Error> {
        Ok(*self)
    }
}

/// View mapping emit functions. Used when implementing a view's `map()`
/// function.
pub trait Emit {
    /// Creates a `Map` result with an empty key and value.
    fn emit(&self) -> Result<Mappings<(), ()>, crate::Error> {
        self.emit_key_and_value((), ())
    }

    /// Creates a `Map` result with a `key` and an empty value.
    fn emit_key<K: for<'a> Key<'a>>(&self, key: K) -> Result<Mappings<K, ()>, crate::Error> {
        self.emit_key_and_value(key, ())
    }

    /// Creates a `Map` result with `value` and an empty key.
    fn emit_value<Value>(&self, value: Value) -> Result<Mappings<(), Value>, crate::Error> {
        self.emit_key_and_value((), value)
    }

    /// Creates a `Map` result with a `key` and `value`.
    fn emit_key_and_value<K: for<'a> Key<'a>, Value>(
        &self,
        key: K,
        value: Value,
    ) -> Result<Mappings<K, Value>, crate::Error>;
}

impl Emit for Header {
    fn emit_key_and_value<K: for<'a> Key<'a>, Value>(
        &self,
        key: K,
        value: Value,
    ) -> Result<Mappings<K, Value>, crate::Error> {
        Ok(Mappings::Simple(Some(Map::new(*self, key, value))))
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)?;
        f.write_char('@')?;
        self.revision.fmt(f)
    }
}

/// A header for a [`CollectionDocument`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CollectionHeader<PK> {
    /// The unique id of the document.
    pub id: PK,
    /// The revision of the document.
    pub revision: Revision,
}

impl<PK> Emit for CollectionHeader<PK>
where
    PK: for<'k> Key<'k>,
{
    fn emit_key_and_value<K: for<'a> Key<'a>, Value>(
        &self,
        key: K,
        value: Value,
    ) -> Result<Mappings<K, Value>, crate::Error> {
        let header = Header::try_from(self.clone())?;
        Ok(Mappings::Simple(Some(Map::new(header, key, value))))
    }
}

impl<PK> HasHeader for CollectionHeader<PK>
where
    PK: for<'k> Key<'k>,
{
    fn header(&self) -> Result<Header, crate::Error> {
        Header::try_from(self.clone())
    }
}

impl HasHeader for OwnedDocument {
    fn header(&self) -> Result<Header, crate::Error> {
        self.header.header()
    }
}

impl<'a> HasHeader for BorrowedDocument<'a> {
    fn header(&self) -> Result<Header, crate::Error> {
        self.header.header()
    }
}

impl<C> HasHeader for CollectionDocument<C>
where
    C: SerializedCollection,
{
    fn header(&self) -> Result<Header, crate::Error> {
        self.header.header()
    }
}

impl<PK> TryFrom<Header> for CollectionHeader<PK>
where
    PK: for<'k> Key<'k>,
{
    type Error = crate::Error;

    fn try_from(value: Header) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.deserialize::<PK>()?,
            revision: value.revision,
        })
    }
}

impl<PK> TryFrom<CollectionHeader<PK>> for Header
where
    PK: for<'k> Key<'k>,
{
    type Error = crate::Error;

    fn try_from(value: CollectionHeader<PK>) -> Result<Self, Self::Error> {
        Ok(Self {
            id: DocumentId::new(value.id)?,
            revision: value.revision,
        })
    }
}

/// A header with either a serialized or deserialized primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnyHeader<K> {
    /// A serialized header.
    Serialized(Header),
    /// A deserialized header.
    Collection(CollectionHeader<K>),
}

impl<K> AnyHeader<K>
where
    K: for<'k> Key<'k>,
{
    /// Returns the contained header as a [`Header`].
    pub fn into_header(self) -> Result<Header, crate::Error> {
        match self {
            AnyHeader::Serialized(header) => Ok(header),
            AnyHeader::Collection(header) => Header::try_from(header),
        }
    }
}

/// A document's ID that uniquely identifies it within its collection.
#[derive(Clone, Copy)]
pub struct DocumentId {
    length: u8,
    bytes: [u8; Self::MAX_LENGTH],
}

impl Deref for DocumentId {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[..usize::from(self.length)]
    }
}

impl Ord for DocumentId {
    fn cmp(&self, other: &Self) -> Ordering {
        (&**self).cmp(&**other)
    }
}

impl PartialOrd for DocumentId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for DocumentId {}

impl PartialEq for DocumentId {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl std::fmt::Debug for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DocumentId(")?;
        arc_bytes::print_bytes(self, f)?;
        f.write_char(')')
    }
}

impl Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(string) = std::str::from_utf8(self.as_ref()) {
            if string.bytes().all(|b| (32..=127).contains(&b)) {
                return f.write_str(string);
            }
        }

        if let Some((first_nonzero_byte, _)) = self
            .as_ref()
            .iter()
            .copied()
            .enumerate()
            .find(|(_index, b)| *b != 0)
        {
            if first_nonzero_byte > 0 {
                write!(f, "{:x}$", first_nonzero_byte)?;
            } else {
                f.write_char('$')?;
            }

            for (index, byte) in self[first_nonzero_byte..].iter().enumerate() {
                if index > 0 {
                    write!(f, "{:02x}", byte)?;
                } else {
                    write!(f, "{:x}", byte)?;
                }
            }
            Ok(())
        } else {
            // All zeroes
            write!(f, "{:x}$", self.len())
        }
    }
}

impl Hash for DocumentId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

// impl<'k> Key<'k> for DocumentId {
//     type Error = crate::Error;

//     const LENGTH: Option<usize> = None;

//     fn as_big_endian_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
//         Ok(Cow::Borrowed(self.as_ref()))
//     }

//     fn from_big_endian_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
//         Self::try_from(bytes)
//     }
// }

/// An invalid hexadecimal character was encountered.
#[derive(thiserror::Error, Debug)]
#[error("invalid hexadecimal bytes")]
pub struct InvalidHexadecimal;

const fn decode_hex_nibble(byte: u8) -> Result<u8, InvalidHexadecimal> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(InvalidHexadecimal),
    }
}

impl FromStr for DocumentId {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Ok(Self::default());
        }
        let mut id = Self::default();
        let bytes = s.as_bytes();

        if let Some((pound_offset, _)) = s.bytes().enumerate().find(|(_index, b)| *b == b'$') {
            if pound_offset > 2 {
                return Err(crate::Error::DocumentIdTooLong);
            }

            let preceding_zeroes = if pound_offset > 0 {
                let mut length = [0_u8];
                decode_big_endian_hex(&bytes[0..pound_offset], &mut length)?;
                usize::from(length[0])
            } else {
                0
            };

            let decoded_length = decode_big_endian_hex(&bytes[pound_offset + 1..], &mut id.bytes)?;
            if preceding_zeroes > 0 {
                let total_length = preceding_zeroes + usize::from(decoded_length);
                if total_length > Self::MAX_LENGTH {
                    return Err(crate::Error::DocumentIdTooLong);
                }
                // The full length indicated a longer ID, so we need to prefix some null bytes.
                id.bytes
                    .copy_within(0..usize::from(decoded_length), preceding_zeroes);
                id.bytes[0..preceding_zeroes].fill(0);
                id.length = u8::try_from(total_length).unwrap();
            } else {
                id.length = decoded_length;
            }
        } else if bytes.len() > Self::MAX_LENGTH {
            return Err(crate::Error::DocumentIdTooLong);
        } else {
            // UTF-8 representable
            id.length = u8::try_from(bytes.len()).unwrap();
            id.bytes[0..bytes.len()].copy_from_slice(bytes);
        }
        Ok(id)
    }
}

fn decode_big_endian_hex(bytes: &[u8], output: &mut [u8]) -> Result<u8, crate::Error> {
    let mut length = 0;
    let mut chunks = if bytes.len() & 1 == 0 {
        bytes.chunks_exact(2)
    } else {
        // Odd amount of bytes, special case the first char
        output[0] = decode_hex_nibble(bytes[0])?;
        length = 1;
        bytes[1..].chunks_exact(2)
    };
    for chunk in &mut chunks {
        let write_at = length;
        length += 1;
        if length > output.len() {
            return Err(crate::Error::DocumentIdTooLong);
        }
        let upper = decode_hex_nibble(chunk[0])?;
        let lower = decode_hex_nibble(chunk[1])?;
        output[write_at] = upper << 4 | lower;
    }
    if !chunks.remainder().is_empty() {
        return Err(crate::Error::from(InvalidHexadecimal));
    }
    Ok(u8::try_from(length).unwrap())
}

#[test]
fn document_id_parsing() {
    fn test_id(bytes: &[u8], display: &str) {
        let id = DocumentId::try_from(bytes).unwrap();
        let as_string = id.to_string();
        assert_eq!(as_string, display);
        let parsed = DocumentId::from_str(&as_string).unwrap();
        assert_eq!(&*parsed, bytes);
    }

    test_id(b"hello", "hello");
    test_id(b"\x00\x0a\xaf\xfa", "1$aaffa");
    test_id(&1_u128.to_be_bytes(), "f$1");
    test_id(&17_u8.to_be_bytes(), "$11");
    test_id(&[0_u8; 63], "3f$");
    // The above test is the same as this one, at the time of writing, but in
    // case we update MAX_LENGTH in the future, this extra test will ensure the
    // max-length formatting is always tested.
    test_id(
        &[0_u8; DocumentId::MAX_LENGTH],
        &format!("{:x}$", DocumentId::MAX_LENGTH),
    );
}

impl Default for DocumentId {
    fn default() -> Self {
        Self {
            length: 0,
            bytes: [0; Self::MAX_LENGTH],
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for DocumentId {
    type Error = crate::Error;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() <= Self::MAX_LENGTH {
            let mut new_id = Self {
                length: u8::try_from(bytes.len()).unwrap(),
                ..Self::default()
            };
            new_id.bytes[..bytes.len()].copy_from_slice(bytes);
            Ok(new_id)
        } else {
            Err(crate::Error::DocumentIdTooLong)
        }
    }
}

impl<const N: usize> TryFrom<[u8; N]> for DocumentId {
    type Error = crate::Error;

    fn try_from(bytes: [u8; N]) -> Result<Self, Self::Error> {
        Self::try_from(&bytes[..])
    }
}

impl DocumentId {
    const MAX_LENGTH: usize = 63;
    /// Returns a new instance with `value` as the identifier..
    pub fn new<K: for<'a> Key<'a>>(value: K) -> Result<Self, crate::Error> {
        let bytes = value
            .as_big_endian_bytes()
            .map_err(|err| crate::Error::Serialization(err.to_string()))?;
        Self::try_from(&bytes[..])
    }

    /// Returns a new document ID for a u64. This is equivalent to
    /// `DocumentId::new(id)`, but since this function accepts a non-generic
    /// type, it can help with type inference in some expressions.
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // Unwrap is impossible to fail.
    pub fn from_u64(id: u64) -> Self {
        Self::try_from(&id.to_be_bytes()[..]).unwrap()
    }

    /// Returns a new document ID for a u32. This is equivalent to
    /// `DocumentId::new(id)`, but since this function accepts a non-generic
    /// type, it can help with type inference in some expressions.
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // Unwrap is impossible to fail.
    pub fn from_u32(id: u32) -> Self {
        Self::try_from(&id.to_be_bytes()[..]).unwrap()
    }

    /// Returns the contained value, deserialized back to its original type.
    pub fn deserialize<'a, K: Key<'a>>(&'a self) -> Result<K, crate::Error> {
        K::from_big_endian_bytes(self.as_ref())
            .map_err(|err| crate::Error::Serialization(err.to_string()))
    }
}

impl Serialize for DocumentId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.as_ref())
    }
}

impl<'de> Deserialize<'de> for DocumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(DocumentIdVisitor)
    }
}

struct DocumentIdVisitor;

impl<'de> Visitor<'de> for DocumentIdVisitor {
    type Value = DocumentId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a document id (bytes)")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() <= DocumentId::MAX_LENGTH {
            let mut document_id = DocumentId {
                length: u8::try_from(v.len()).unwrap(),
                ..DocumentId::default()
            };
            document_id.bytes[..v.len()].copy_from_slice(v);
            Ok(document_id)
        } else {
            Err(E::invalid_length(v.len(), &"< 64 bytes"))
        }
    }

    // Provided for backwards compatibility. No new data is written with this.
    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DocumentId::from_u64(v))
    }
}

/// A unique id for a document, either serialized or deserialized.
pub enum DocumentKey<K> {
    /// A serialized id.
    Id(DocumentId),
    /// A deserialized id.
    Key(K),
}

impl<K> DocumentKey<K>
where
    K: for<'k> Key<'k>,
{
    /// Converts this value to a document id.
    pub fn to_document_id(&self) -> Result<DocumentId, crate::Error> {
        match self {
            Self::Id(id) => Ok(*id),
            Self::Key(key) => DocumentId::new(key.clone()),
        }
    }

    /// Converts this value to the primary key type.
    pub fn to_primary_key(&self) -> Result<K, crate::Error> {
        match self {
            Self::Id(id) => id.deserialize::<K>(),
            Self::Key(key) => Ok(key.clone()),
        }
    }
}

impl<K> From<K> for DocumentKey<K>
where
    K: for<'k> Key<'k>,
{
    fn from(key: K) -> Self {
        Self::Key(key)
    }
}

impl<K> From<DocumentId> for DocumentKey<K> {
    fn from(id: DocumentId) -> Self {
        Self::Id(id)
    }
}

/// Contains a serialized document in the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BorrowedDocument<'a> {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Header,

    /// The serialized bytes of the stored item.
    #[serde(borrow)]
    pub contents: CowBytes<'a>,
}

/// Contains a serialized document in the database.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnedDocument {
    /// The header of the document, which contains the id and `Revision`.
    pub header: Header,

    /// The serialized bytes of the stored item.
    pub contents: Bytes,
}

/// Common interface of a document in BonsaiDb.
pub trait Document<C>: Sized
where
    C: Collection,
{
    /// The bytes type used in the interface.
    type Bytes;

    /// Returns the unique key for this document.
    fn key(&self) -> DocumentKey<C::PrimaryKey>;
    /// Returns the header of this document.
    fn header(&self) -> AnyHeader<C::PrimaryKey>;
    /// Sets the header to the new header.
    fn set_header(&mut self, header: Header) -> Result<(), crate::Error>;
    /// Sets the header to the new collection header.
    fn set_collection_header(
        &mut self,
        header: CollectionHeader<C::PrimaryKey>,
    ) -> Result<(), crate::Error> {
        self.set_header(Header::try_from(header)?)
    }
    /// Returns the contents of this document, serialized.
    fn bytes(&self) -> Result<Vec<u8>, crate::Error>;
    /// Retrieves `contents` through deserialization into the type `D`.
    fn contents(&self) -> Result<C::Contents, crate::Error>
    where
        C: SerializedCollection,
        <C as SerializedCollection>::Contents: Clone;
    /// Stores `contents` into this document.
    fn set_contents(&mut self, contents: C::Contents) -> Result<(), crate::Error>
    where
        C: SerializedCollection;
}

impl<'a> AsRef<[u8]> for BorrowedDocument<'a> {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}

impl<'a, C> Document<C> for BorrowedDocument<'a>
where
    C: Collection,
{
    type Bytes = CowBytes<'a>;

    fn contents(&self) -> Result<C::Contents, crate::Error>
    where
        C: SerializedCollection,
    {
        <C as SerializedCollection>::deserialize(&self.contents)
    }

    fn set_contents(&mut self, contents: C::Contents) -> Result<(), crate::Error>
    where
        C: SerializedCollection,
    {
        self.contents = CowBytes::from(<C as SerializedCollection>::serialize(&contents)?);
        Ok(())
    }

    fn header(&self) -> AnyHeader<C::PrimaryKey> {
        AnyHeader::Serialized(self.header)
    }

    fn set_header(&mut self, header: Header) -> Result<(), crate::Error> {
        self.header = header;
        Ok(())
    }

    fn bytes(&self) -> Result<Vec<u8>, crate::Error> {
        Ok(self.contents.to_vec())
    }

    fn key(&self) -> DocumentKey<C::PrimaryKey> {
        DocumentKey::Id(self.header.id)
    }
}

impl<'a, C> Document<C> for OwnedDocument
where
    C: Collection,
{
    type Bytes = Vec<u8>;

    // fn new<Contents: Into<Self::Bytes>>(
    //     id: C::PrimaryKey,
    //     contents: Contents,
    // ) -> Result<Self, crate::Error> {
    //     let contents = Bytes(contents.into());
    //     Ok(Self {
    //         header: Header {
    //             id: DocumentId::new(id)?,
    //             revision: Revision::new(&contents),
    //         },
    //         contents,
    //     })
    // }

    // fn with_contents(id: C::PrimaryKey, contents: C::Contents) -> Result<Self, crate::Error>
    // where
    //     C: SerializedCollection,
    // {
    //     <BorrowedDocument<'_> as Document<C>>::with_contents(id, contents)
    //         .map(BorrowedDocument::into_owned)
    // }

    fn contents(&self) -> Result<C::Contents, crate::Error>
    where
        C: SerializedCollection,
    {
        <C as SerializedCollection>::deserialize(&self.contents)
    }

    fn set_contents(&mut self, contents: C::Contents) -> Result<(), crate::Error>
    where
        C: SerializedCollection,
    {
        self.contents = Bytes::from(<C as SerializedCollection>::serialize(&contents)?);
        Ok(())
    }

    fn key(&self) -> DocumentKey<C::PrimaryKey> {
        DocumentKey::Id(self.header.id)
    }

    fn header(&self) -> AnyHeader<C::PrimaryKey> {
        AnyHeader::Serialized(self.header)
    }

    fn set_header(&mut self, header: Header) -> Result<(), crate::Error> {
        self.header = header;
        Ok(())
    }

    fn bytes(&self) -> Result<Vec<u8>, crate::Error> {
        Ok(self.contents.to_vec())
    }
}

impl AsRef<Header> for OwnedDocument {
    fn as_ref(&self) -> &Header {
        &self.header
    }
}

impl AsMut<Header> for OwnedDocument {
    fn as_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

impl AsRef<[u8]> for OwnedDocument {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}

impl<'a> BorrowedDocument<'a> {
    /// Returns a new instance with the id and content bytes.
    pub fn new<Contents: Into<CowBytes<'a>>>(id: DocumentId, contents: Contents) -> Self {
        let contents = contents.into();
        let revision = Revision::new(&contents);
        Self {
            header: Header { id, revision },
            contents,
        }
    }

    /// Returns a new instance with `contents`, after serializing.
    pub fn with_contents<C>(id: C::PrimaryKey, contents: &C::Contents) -> Result<Self, crate::Error>
    where
        C: SerializedCollection,
    {
        let contents = <C as SerializedCollection>::serialize(contents)?;
        Ok(Self::new(DocumentId::new(id)?, contents))
    }

    /// Converts this document to an owned document.
    #[must_use]
    pub fn into_owned(self) -> OwnedDocument {
        OwnedDocument {
            header: self.header,
            contents: Bytes::from(self.contents),
        }
    }
}

impl<'a> AsRef<Header> for BorrowedDocument<'a> {
    fn as_ref(&self) -> &Header {
        &self.header
    }
}

impl<'a> AsMut<Header> for BorrowedDocument<'a> {
    fn as_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}

/// The ID of an encryption key.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum KeyId {
    /// A key with no id.
    None,
    /// The master key of the vault.
    Master,
    /// A specific named key in the vault.
    Id(Cow<'static, str>),
}

#[test]
fn emissions_tests() -> Result<(), crate::Error> {
    use crate::{schema::Map, test_util::Basic};

    let doc = BorrowedDocument::with_contents::<Basic>(1, &Basic::default())?;

    assert_eq!(
        doc.header.emit()?,
        Mappings::Simple(Some(Map::new(doc.header, (), ())))
    );

    assert_eq!(
        doc.header.emit_key(1)?,
        Mappings::Simple(Some(Map::new(doc.header, 1, ())))
    );

    assert_eq!(
        doc.header.emit_value(1)?,
        Mappings::Simple(Some(Map::new(doc.header, (), 1)))
    );

    assert_eq!(
        doc.header.emit_key_and_value(1, 2)?,
        Mappings::Simple(Some(Map::new(doc.header, 1, 2)))
    );

    Ok(())
}

#[test]
fn chained_mappings_test() -> Result<(), crate::Error> {
    use crate::{schema::Map, test_util::Basic};

    let doc = BorrowedDocument::with_contents::<Basic>(1, &Basic::default())?;

    assert_eq!(
        doc.header.emit()?.and(doc.header.emit()?),
        Mappings::List(vec![
            Map::new(doc.header, (), ()),
            Map::new(doc.header, (), ())
        ])
    );

    Ok(())
}

#[test]
fn header_display_test() {
    let original_contents = b"one";
    let revision = Revision::new(original_contents);
    let header = Header {
        id: DocumentId::new(42_u64).unwrap(),
        revision,
    };
    assert_eq!(
        header.to_string(),
        "7$2a@0-7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
    );
}
