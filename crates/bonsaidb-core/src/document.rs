use std::{
    borrow::Cow,
    fmt::{Display, Write},
};

use arc_bytes::serde::{Bytes, CowBytes};
use serde::{Deserialize, Serialize};

use crate::schema::{view::map::Mappings, Key, Map, SerializedCollection};

mod collection;
mod revision;
pub use collection::*;
pub use revision::Revision;

/// The header of a `Document`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// The id of the Document. Unique across the collection `C`
    pub id: u64,

    /// The revision of the stored document.
    pub revision: Revision,
}

impl AsRef<Self> for Header {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl Header {
    /// Creates a `Map` result with an empty key and value.
    #[must_use]
    pub fn emit(self) -> Mappings<(), ()> {
        self.emit_key_and_value((), ())
    }

    /// Creates a `Map` result with a `key` and an empty value.
    #[must_use]
    pub fn emit_key<K: for<'a> Key<'a>>(self, key: K) -> Mappings<K, ()> {
        self.emit_key_and_value(key, ())
    }

    /// Creates a `Map` result with `value` and an empty key.
    #[must_use]
    pub fn emit_value<Value>(self, value: Value) -> Mappings<(), Value> {
        self.emit_key_and_value((), value)
    }

    /// Creates a `Map` result with a `key` and `value`.
    #[must_use]
    pub fn emit_key_and_value<K: for<'a> Key<'a>, Value>(
        self,
        key: K,
        value: Value,
    ) -> Mappings<K, Value> {
        Mappings::Simple(Some(Map::new(self, key, value)))
    }
}

impl Display for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.id.fmt(f)?;
        f.write_char('@')?;
        self.revision.fmt(f)
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
pub trait Document<'a>:
    AsRef<Header> + AsMut<Header> + AsRef<Header> + AsRef<[u8]> + Sized
{
    /// The bytes type used in the interface.
    type Bytes;

    /// Creates a new document with `contents`.
    #[must_use]
    fn new(id: u64, contents: impl Into<Self::Bytes>) -> Self;
    /// Creates a new document with serialized bytes from `contents`.
    fn with_contents<S: SerializedCollection<Contents = S>>(
        id: u64,
        contents: &S,
    ) -> Result<Self, crate::Error>;
    /// Retrieves `contents` through deserialization into the type `D`.
    fn contents<D>(&self) -> Result<D::Contents, crate::Error>
    where
        D: SerializedCollection<Contents = D>;
    /// Serializes and stores `contents` into this document.
    fn set_contents<S: SerializedCollection<Contents = S>>(
        &mut self,
        contents: &S,
    ) -> Result<(), crate::Error>;
}

impl<'a> AsRef<[u8]> for BorrowedDocument<'a> {
    fn as_ref(&self) -> &[u8] {
        &self.contents
    }
}

impl<'a> Document<'a> for BorrowedDocument<'a> {
    type Bytes = CowBytes<'a>;
    fn new(id: u64, contents: impl Into<CowBytes<'a>>) -> Self {
        let contents = contents.into();
        let revision = Revision::new(&contents);
        Self {
            header: Header { id, revision },
            contents,
        }
    }

    fn with_contents<S: SerializedCollection<Contents = S>>(
        id: u64,
        contents: &S,
    ) -> Result<Self, crate::Error> {
        let contents = <S as SerializedCollection>::serialize(contents)?;
        Ok(Self::new(id, contents))
    }

    fn contents<D>(&self) -> Result<D::Contents, crate::Error>
    where
        D: SerializedCollection<Contents = D>,
    {
        <D as SerializedCollection>::deserialize(&self.contents)
    }

    fn set_contents<S: SerializedCollection<Contents = S>>(
        &mut self,
        contents: &S,
    ) -> Result<(), crate::Error> {
        self.contents = CowBytes::from(<S as SerializedCollection>::serialize(contents)?);
        Ok(())
    }
}

impl Document<'static> for OwnedDocument {
    type Bytes = Vec<u8>;

    fn new(id: u64, contents: impl Into<Self::Bytes>) -> Self {
        let contents = Bytes(contents.into());
        Self {
            header: Header {
                id,
                revision: Revision::new(&contents),
            },
            contents,
        }
    }

    fn with_contents<S: SerializedCollection<Contents = S>>(
        id: u64,
        contents: &S,
    ) -> Result<Self, crate::Error> {
        BorrowedDocument::with_contents(id, contents).map(BorrowedDocument::into_owned)
    }

    fn contents<D>(&self) -> Result<D::Contents, crate::Error>
    where
        D: SerializedCollection<Contents = D>,
    {
        <D as SerializedCollection>::deserialize(&self.contents)
    }

    fn set_contents<S: SerializedCollection<Contents = S>>(
        &mut self,
        contents: &S,
    ) -> Result<(), crate::Error> {
        self.contents = Bytes::from(<S as SerializedCollection>::serialize(contents)?);
        Ok(())
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

    let doc = BorrowedDocument::with_contents(1, &Basic::default())?;

    assert_eq!(
        doc.header.emit(),
        Mappings::Simple(Some(Map::new(doc.header, (), ())))
    );

    assert_eq!(
        doc.header.emit_key(1),
        Mappings::Simple(Some(Map::new(doc.header, 1, ())))
    );

    assert_eq!(
        doc.header.emit_value(1),
        Mappings::Simple(Some(Map::new(doc.header, (), 1)))
    );

    assert_eq!(
        doc.header.emit_key_and_value(1, 2),
        Mappings::Simple(Some(Map::new(doc.header, 1, 2)))
    );

    Ok(())
}

#[test]
fn chained_mappings_test() -> Result<(), crate::Error> {
    use crate::{schema::Map, test_util::Basic};

    let doc = BorrowedDocument::with_contents(1, &Basic::default())?;

    assert_eq!(
        doc.header.emit().and(doc.header.emit()),
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
    let header = Header { id: 42, revision };
    assert_eq!(
        header.to_string(),
        "42@0-7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed"
    );
}
