use std::borrow::Cow;
use std::fmt::{Display, Write};
use std::hash::Hash;
use std::mem::size_of;
use std::ops::Deref;
use std::str::FromStr;

use actionable::Identifier;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use tinyvec::{Array, TinyVec};

use crate::key::{ByteSource, Key, KeyEncoding, KeyKind, KeyVisitor};

/// The serialized representation of a document's unique ID.
#[derive(Default, Ord, Hash, Eq, PartialEq, PartialOrd, Clone)]
pub struct DocumentId(TinyVec<[u8; Self::INLINE_SIZE]>);

impl Deref for DocumentId {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
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
                write!(f, "{first_nonzero_byte:x}$")?;
            } else {
                f.write_char('$')?;
            }

            for (index, byte) in self[first_nonzero_byte..].iter().enumerate() {
                if index > 0 {
                    write!(f, "{byte:02x}")?;
                } else {
                    write!(f, "{byte:x}")?;
                }
            }
            Ok(())
        } else {
            // All zeroes
            write!(f, "{:x}$", self.len())
        }
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
        Identifier::from(DocumentId::new("hello").unwrap()),
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

        let bytes = s.as_bytes();
        if let Some((pound_offset, _)) = s.bytes().enumerate().find(|(_index, b)| *b == b'$') {
            if pound_offset > 5 {
                return Err(crate::Error::DocumentIdTooLong);
            }

            let preceding_zeroes = if pound_offset > 0 {
                let mut length = TinyVec::<[u8; 1]>::new();
                decode_big_endian_hex(&bytes[0..pound_offset], &mut length)?;
                let mut zeroes = [0_u8; size_of::<usize>()];
                let offset = zeroes.len() - length.len();
                zeroes[offset..].copy_from_slice(&length);
                usize::from_be_bytes(zeroes)
            } else {
                0
            };

            let mut id = TinyVec::new();
            decode_big_endian_hex(&bytes[pound_offset + 1..], &mut id)?;
            if preceding_zeroes > 0 {
                let total_length = preceding_zeroes + id.len();
                if total_length > Self::MAX_LENGTH {
                    return Err(crate::Error::DocumentIdTooLong);
                }
                // The full length indicated a longer ID, so we need to prefix some null bytes.
                id.splice(0..0, std::iter::repeat(0).take(preceding_zeroes));
            }
            Ok(Self(id))
        } else if bytes.len() > Self::MAX_LENGTH {
            Err(crate::Error::DocumentIdTooLong)
        } else {
            // UTF-8 representable
            Self::try_from(bytes)
        }
    }
}

fn decode_big_endian_hex<A: Array<Item = u8>>(
    bytes: &[u8],
    output: &mut TinyVec<A>,
) -> Result<(), crate::Error> {
    let mut chunks = if bytes.len() & 1 == 0 {
        bytes.chunks_exact(2)
    } else {
        // Odd amount of bytes, special case the first char
        output.push(decode_hex_nibble(bytes[0])?);
        bytes[1..].chunks_exact(2)
    };
    for chunk in &mut chunks {
        let upper = decode_hex_nibble(chunk[0])?;
        let lower = decode_hex_nibble(chunk[1])?;
        output.push(upper << 4 | lower);
    }
    if !chunks.remainder().is_empty() {
        return Err(crate::Error::from(InvalidHexadecimal));
    }
    Ok(())
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
        &vec![0_u8; DocumentId::MAX_LENGTH],
        &format!("{:x}$", DocumentId::MAX_LENGTH),
    );
}

impl<'a> TryFrom<&'a [u8]> for DocumentId {
    type Error = crate::Error;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() <= Self::MAX_LENGTH {
            Ok(Self(TinyVec::from(bytes)))
        } else {
            Err(crate::Error::DocumentIdTooLong)
        }
    }
}

impl<'a> TryFrom<Cow<'a, [u8]>> for DocumentId {
    type Error = crate::Error;

    fn try_from(bytes: Cow<'a, [u8]>) -> Result<Self, Self::Error> {
        Self::try_from(bytes.as_ref())
    }
}

impl<const N: usize> TryFrom<[u8; N]> for DocumentId {
    type Error = crate::Error;

    fn try_from(bytes: [u8; N]) -> Result<Self, Self::Error> {
        Self::try_from(&bytes[..])
    }
}

impl DocumentId {
    const INLINE_SIZE: usize = 16;
    /// The maximum size able to be stored in a document's unique id.
    pub const MAX_LENGTH: usize = 65_535;

    /// Returns a new instance with `value` as the identifier..
    pub fn new<PrimaryKey: for<'k> Key<'k>, PrimaryKeyRef: KeyEncoding<PrimaryKey> + ?Sized>(
        value: &PrimaryKeyRef,
    ) -> Result<Self, crate::Error> {
        let bytes = value
            .as_ord_bytes()
            .map_err(|err| crate::Error::other("key serialization", err))?;
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
    pub fn deserialize<'k, PrimaryKey: Key<'k>>(&'k self) -> Result<PrimaryKey, crate::Error> {
        PrimaryKey::from_ord_bytes(ByteSource::Borrowed(self.as_ref()))
            .map_err(|err| crate::Error::other("key serialization", err))
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
        Ok(DocumentId(TinyVec::from(v)))
    }
}

impl<'k> Key<'k> for DocumentId {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Self::try_from(bytes.as_ref())
    }
}

impl<PrimaryKey> KeyEncoding<PrimaryKey> for DocumentId
where
    PrimaryKey: for<'pk> Key<'pk>,
{
    type Error = crate::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&self) -> Result<Cow<'_, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }
}
