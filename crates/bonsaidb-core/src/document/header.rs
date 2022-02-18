use std::fmt::{Display, Write};

use serde::{Deserialize, Serialize};

use crate::{
    document::{BorrowedDocument, CollectionDocument, DocumentId, OwnedDocument, Revision},
    key::Key,
    schema::{view::map::Mappings, Map, SerializedCollection},
};

/// The header of a `Document`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// The id of the Document. Unique across the collection the document is
    /// contained within.
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
        Ok(self.clone())
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
        Ok(Mappings::Simple(Some(Map::new(self.clone(), key, value))))
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
pub struct CollectionHeader<PrimaryKey> {
    /// The unique id of the document.
    pub id: PrimaryKey,
    /// The revision of the document.
    pub revision: Revision,
}

impl<PrimaryKey> Emit for CollectionHeader<PrimaryKey>
where
    PrimaryKey: for<'k> Key<'k>,
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

impl<PrimaryKey> HasHeader for CollectionHeader<PrimaryKey>
where
    PrimaryKey: for<'k> Key<'k>,
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

impl<PrimaryKey> TryFrom<Header> for CollectionHeader<PrimaryKey>
where
    PrimaryKey: for<'k> Key<'k>,
{
    type Error = crate::Error;

    fn try_from(value: Header) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.deserialize::<PrimaryKey>()?,
            revision: value.revision,
        })
    }
}

impl<PrimaryKey> TryFrom<CollectionHeader<PrimaryKey>> for Header
where
    PrimaryKey: for<'k> Key<'k>,
{
    type Error = crate::Error;

    fn try_from(value: CollectionHeader<PrimaryKey>) -> Result<Self, Self::Error> {
        Ok(Self {
            id: DocumentId::new(value.id)?,
            revision: value.revision,
        })
    }
}

/// A header with either a serialized or deserialized primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyHeader<PrimaryKey> {
    /// A serialized header.
    Serialized(Header),
    /// A deserialized header.
    Collection(CollectionHeader<PrimaryKey>),
}

impl<PrimaryKey> AnyHeader<PrimaryKey>
where
    PrimaryKey: for<'k> Key<'k>,
{
    /// Returns the contained header as a [`Header`].
    pub fn into_header(self) -> Result<Header, crate::Error> {
        match self {
            AnyHeader::Serialized(header) => Ok(header),
            AnyHeader::Collection(header) => Header::try_from(header),
        }
    }
}

#[test]
fn emissions_tests() -> Result<(), crate::Error> {
    use crate::{schema::Map, test_util::Basic};

    let doc = BorrowedDocument::with_contents::<Basic>(1, &Basic::default())?;

    assert_eq!(
        doc.header.emit()?,
        Mappings::Simple(Some(Map::new(doc.header.clone(), (), ())))
    );

    assert_eq!(
        doc.header.emit_key(1)?,
        Mappings::Simple(Some(Map::new(doc.header.clone(), 1, ())))
    );

    assert_eq!(
        doc.header.emit_value(1)?,
        Mappings::Simple(Some(Map::new(doc.header.clone(), (), 1)))
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
            Map::new(doc.header.clone(), (), ()),
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
