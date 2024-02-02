use std::collections::BTreeMap;
use std::fmt::Debug;

use arc_bytes::serde::Bytes;
use serde::{Deserialize, Serialize};

use crate::document::{CollectionHeader, DocumentId, Header, OwnedDocument};
use crate::schema::view::{self, ByteSource, Key, SerializedView, View, ViewSchema};
use crate::schema::Collection;

/// A document's entry in a View's mappings.
#[derive(Eq, PartialEq, Debug)]
pub struct Map<K = (), V = ()> {
    /// The header of the document that emitted this entry.
    pub source: Header,

    /// The key used to index the View.
    pub key: K,

    /// An associated value stored in the view.
    pub value: V,
}

impl<K, V> Map<K, V> {
    /// Serializes this map.
    pub(crate) fn serialized<'a, View>(&self) -> Result<Serialized, view::Error>
    where
        K: Key<'a>,
        View: SerializedView<Value = V>,
    {
        Ok(Serialized {
            source: self.source.clone(),
            key: Bytes::from(
                self.key
                    .as_ord_bytes()
                    .map_err(view::Error::key_serialization)?
                    .to_vec(),
            ),
            value: Bytes::from(View::serialize(&self.value)?),
        })
    }
}

impl<K, V> Map<K, V> {
    /// Creates a new Map entry for the document with id `source`.
    pub const fn new(source: Header, key: K, value: V) -> Self {
        Self { source, key, value }
    }
}

/// A document's entry in a View's mappings.
#[derive(Eq, PartialEq, Debug)]
pub struct CollectionMap<PrimaryKey, K = (), V = ()> {
    /// The header of the document that emitted this entry.
    pub source: CollectionHeader<PrimaryKey>,

    /// The key used to index the View.
    pub key: K,

    /// An associated value stored in the view.
    pub value: V,
}

/// This type is the result of `query()`. It is a list of mappings, which
/// contains:
///
/// - The key emitted during the map function.
/// - The value emitted during the map function.
/// - The source document header that the mappings originated from.
pub type ViewMappings<V> = Vec<
    CollectionMap<
        <<V as View>::Collection as Collection>::PrimaryKey,
        <V as View>::Key,
        <V as View>::Value,
    >,
>;

/// A collection of [`Map`]s.
#[derive(Debug, Eq, PartialEq)]
#[must_use]
pub enum Mappings<K = (), V = ()> {
    /// Zero or one mappings.
    Simple(Option<Map<K, V>>),
    /// More than one mapping.
    List(Vec<Map<K, V>>),
}

impl<K, V> Default for Mappings<K, V> {
    fn default() -> Self {
        Self::none()
    }
}

impl<K, V> Mappings<K, V> {
    /// Returns an empty collection of mappings.
    pub const fn none() -> Self {
        Self::Simple(None)
    }

    /// Appends `mapping` to the end of this collection.
    pub fn push(&mut self, mapping: Map<K, V>) {
        match self {
            Self::Simple(existing_mapping) => {
                *self = if let Some(existing_mapping) = existing_mapping.take() {
                    Self::List(vec![existing_mapping, mapping])
                } else {
                    Self::Simple(Some(mapping))
                };
            }
            Self::List(vec) => vec.push(mapping),
        }
    }

    /// Appends `mappings` to the end of this collection and returns self.
    pub fn and(mut self, mappings: Self) -> Self {
        self.extend(mappings);
        self
    }

    /// Returns an iterator for these mappings.
    pub fn iter(&self) -> MappingsIter<'_, K, V> {
        self.into_iter()
    }

    /// Returns the number of mappings contained.
    pub fn len(&self) -> usize {
        match self {
            Mappings::Simple(None) => 0,
            Mappings::Simple(Some(_)) => 1,
            Mappings::List(v) => v.len(),
        }
    }

    /// Returns true if there are no mappings in this collection.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<K, V> Extend<Map<K, V>> for Mappings<K, V> {
    fn extend<T: IntoIterator<Item = Map<K, V>>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        for map in iter {
            self.push(map);
        }
    }
}

impl<K, V> FromIterator<Map<K, V>> for Mappings<K, V> {
    fn from_iter<T: IntoIterator<Item = Map<K, V>>>(iter: T) -> Self {
        let mut mappings = Self::none();
        mappings.extend(iter);
        mappings
    }
}

impl<K, V> FromIterator<Self> for Mappings<K, V> {
    fn from_iter<T: IntoIterator<Item = Self>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        if let Some(mut collected) = iter.next() {
            for mappings in iter {
                collected.extend(mappings);
            }
            collected
        } else {
            Self::none()
        }
    }
}

impl<K, V> IntoIterator for Mappings<K, V> {
    type IntoIter = MappingsIntoIter<K, V>;
    type Item = Map<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Mappings::Simple(option) => MappingsIntoIter::Inline(option),
            Mappings::List(list) => MappingsIntoIter::Vec(list.into_iter()),
        }
    }
}

impl<'a, K, V> IntoIterator for &'a Mappings<K, V> {
    type IntoIter = MappingsIter<'a, K, V>;
    type Item = &'a Map<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Mappings::Simple(option) => MappingsIter::Inline(option.iter()),
            Mappings::List(list) => MappingsIter::Vec(list.iter()),
        }
    }
}

/// An iterator over [`Mappings`] that returns owned [`Map`] entries.
pub enum MappingsIntoIter<K = (), V = ()> {
    /// An iterator over a [`Mappings::Simple`] value.
    Inline(Option<Map<K, V>>),
    /// An iterator over a [`Mappings::List`] value.
    Vec(std::vec::IntoIter<Map<K, V>>),
}

impl<K, V> Iterator for MappingsIntoIter<K, V> {
    type Item = Map<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MappingsIntoIter::Inline(opt) => opt.take(),
            MappingsIntoIter::Vec(iter) => iter.next(),
        }
    }
}

/// An iterator over [`Mappings`] that returns [`Map`] entry references.
pub enum MappingsIter<'a, K = (), V = ()> {
    /// An iterator over a [`Mappings::Simple`] value.
    Inline(std::option::Iter<'a, Map<K, V>>),
    /// An iterator over a [`Mappings::List`] value.
    Vec(std::slice::Iter<'a, Map<K, V>>),
}

impl<'a, K, V> Iterator for MappingsIter<'a, K, V> {
    type Item = &'a Map<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MappingsIter::Inline(i) => i.next(),
            MappingsIter::Vec(i) => i.next(),
        }
    }
}

/// A collection of mappings and the associated documents.
pub struct MappedDocuments<D, V: View> {
    /// The collection of mappings.
    pub mappings: ViewMappings<V>,
    /// All associated documents by ID.
    ///
    /// Documents can appear in a mapping query multiple times. As a result, they are stored separately to avoid duplication.
    pub documents: BTreeMap<<V::Collection as Collection>::PrimaryKey, D>,
}

impl<D, V: View> MappedDocuments<D, V> {
    /// The number of mappings contained in this collection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Returns true if there are no mappings in this collection.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the mapped document at`index`, or `None` if `index >=
    /// self.len()`.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn get(&self, index: usize) -> Option<MappedDocument<'_, D, V::Key, V::Value>> {
        if index < self.len() {
            let mapping = &self.mappings[index];
            let document = self
                .documents
                .get(&mapping.source.id)
                .expect("missing mapped document");
            Some(MappedDocument {
                key: &mapping.key,
                value: &mapping.value,
                document,
            })
        } else {
            None
        }
    }

    /// Returns an iterator over the contained mapped documents.
    #[must_use]
    pub const fn iter(&self) -> MappedDocumentsIter<'_, D, V> {
        MappedDocumentsIter {
            docs: self,
            index: 0,
        }
    }
}

impl<D, V: View> Debug for MappedDocuments<D, V>
where
    V::Key: Debug,
    V::Value: Debug,
    D: Debug,
    <V::Collection as Collection>::PrimaryKey: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedDocuments")
            .field("mappings", &self.mappings)
            .field("documents", &self.documents)
            .finish()
    }
}

/// An iterator of mapped documents.
pub struct MappedDocumentsIter<'a, D, V: View> {
    docs: &'a MappedDocuments<D, V>,
    index: usize,
}

impl<'a, D, V: View> IntoIterator for &'a MappedDocuments<D, V> {
    type IntoIter = MappedDocumentsIter<'a, D, V>;
    type Item = MappedDocument<'a, D, V::Key, V::Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, D, V: View> Iterator for MappedDocumentsIter<'a, D, V> {
    type Item = MappedDocument<'a, D, V::Key, V::Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let doc = self.docs.get(self.index);
        self.index = self.index.saturating_add(1);
        doc
    }
}

/// A mapped document returned from a view query.
pub struct MappedDocument<'a, D, K, V> {
    /// The key that this document mapped to.
    pub key: &'a K,
    /// The associated value of this key.
    pub value: &'a V,
    /// The source document of this mapping.
    pub document: &'a D,
}

/// Represents a document's entry in a View's mappings, serialized and ready to store.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Serialized {
    /// The header of the document that emitted this entry.
    pub source: Header,

    /// The key used to index the View.Operation
    pub key: Bytes,

    /// An associated value stored in the view.Operation
    pub value: Bytes,
}

impl Serialized {
    /// Deserializes this map.
    pub fn deserialized<View: SerializedView>(
        &self,
    ) -> Result<Map<View::Key, View::Value>, view::Error> {
        Ok(Map::new(
            self.source.clone(),
            <View::Key as Key>::from_ord_bytes(ByteSource::Borrowed(&self.key))
                .map_err(view::Error::key_serialization)?,
            View::deserialize(&self.value)?,
        ))
    }
}

/// A serialized [`MappedDocument`](MappedDocument).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedSerializedDocuments {
    /// The serialized mapped value.
    pub mappings: Vec<Serialized>,
    /// The source document.
    pub documents: BTreeMap<DocumentId, OwnedDocument>,
}

impl MappedSerializedDocuments {
    /// Deserialize into a [`MappedDocument`](MappedDocument).
    pub fn deserialized<View: SerializedView>(
        self,
    ) -> Result<MappedDocuments<OwnedDocument, View>, crate::Error> {
        let mappings = self
            .mappings
            .iter()
            .map(|mapping| {
                let deserialized = Serialized::deserialized::<View>(mapping)?;
                Ok(CollectionMap {
                    source: deserialized.source.try_into()?,
                    key: deserialized.key,
                    value: deserialized.value,
                })
            })
            .collect::<Result<Vec<_>, crate::Error>>()?;

        Ok(MappedDocuments {
            mappings,
            documents: self
                .documents
                .into_iter()
                .map(|(key, value)| {
                    let key = key.deserialize()?;
                    Ok((key, value))
                })
                .collect::<Result<BTreeMap<_, _>, crate::Error>>()?,
        })
    }
}

/// A key value pair
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MappedValue<K, V> {
    /// The key responsible for generating the value
    pub key: K,

    /// The value generated by the `View`
    pub value: V,
}

impl<K, V> MappedValue<K, V> {
    /// Returns a new instance with the key/value pair.
    pub const fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

/// A mapped value in a [`View`].
pub type ViewMappedValue<'doc, V> =
    MappedValue<<V as ViewSchema>::MappedKey<'doc>, <<V as ViewSchema>::View as View>::Value>;

/// A serialized [`MappedValue`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedSerializedValue {
    /// The serialized key.
    pub key: Bytes,
    /// The serialized value.
    pub value: Bytes,
}
