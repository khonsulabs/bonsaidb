use std::{
    cmp::Ordering,
    fmt::{Display, Write},
    hash::Hash,
    ops::Deref,
    str::FromStr,
};

use actionable::Identifier;
use serde::{de::Visitor, Deserialize, Serialize};

use crate::key::Key;

/// The serialized representation of a document's unique ID.
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
        (**self).cmp(&**other)
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
        (**self).hash(state);
    }
}

impl<'a> From<DocumentId> for Identifier<'a> {
    fn from(id: DocumentId) -> Self {
        Identifier::from(id.to_vec())
    }
}

impl<'a> From<&'a DocumentId> for Identifier<'a> {
    fn from(id: &'a DocumentId) -> Self {
        Identifier::from(&**id)
    }
}

#[test]
fn document_id_identifier_tests() {
    assert_eq!(
        Identifier::from(DocumentId::new(String::from("hello")).unwrap()),
        Identifier::from("hello")
    );
    assert_eq!(
        Identifier::from(DocumentId::from_u64(1)),
        Identifier::from(1)
    );
}

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
    /// The maximum length, in bytes, that an id can contain.
    pub const MAX_LENGTH: usize = 63;

    /// Returns a new instance with `value` as the identifier..
    pub fn new<PrimaryKey: for<'a> Key<'a>>(value: PrimaryKey) -> Result<Self, crate::Error> {
        let bytes = value
            .as_ord_bytes()
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
    pub fn deserialize<'a, PrimaryKey: Key<'a>>(&'a self) -> Result<PrimaryKey, crate::Error> {
        PrimaryKey::from_ord_bytes(self.as_ref())
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
}

/// A unique id for a document, either serialized or deserialized.
pub enum AnyDocumentId<PrimaryKey> {
    /// A serialized id.
    Serialized(DocumentId),
    /// A deserialized id.
    Deserialized(PrimaryKey),
}

impl<PrimaryKey> AnyDocumentId<PrimaryKey>
where
    PrimaryKey: for<'k> Key<'k>,
{
    /// Converts this value to a document id.
    pub fn to_document_id(&self) -> Result<DocumentId, crate::Error> {
        match self {
            Self::Serialized(id) => Ok(*id),
            Self::Deserialized(key) => DocumentId::new(key.clone()),
        }
    }

    /// Converts this value to the primary key type.
    pub fn to_primary_key(&self) -> Result<PrimaryKey, crate::Error> {
        match self {
            Self::Serialized(id) => id.deserialize::<PrimaryKey>(),
            Self::Deserialized(key) => Ok(key.clone()),
        }
    }
}

impl<PrimaryKey> From<PrimaryKey> for AnyDocumentId<PrimaryKey>
where
    PrimaryKey: for<'k> Key<'k>,
{
    fn from(key: PrimaryKey) -> Self {
        Self::Deserialized(key)
    }
}

impl<PrimaryKey> From<DocumentId> for AnyDocumentId<PrimaryKey> {
    fn from(id: DocumentId) -> Self {
        Self::Serialized(id)
    }
}
