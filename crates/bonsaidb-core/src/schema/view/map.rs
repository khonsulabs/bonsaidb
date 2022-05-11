use std::{collections::BTreeMap, fmt::Debug};

use arc_bytes::serde::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    document::{DocumentId, Header, OwnedDocument},
    schema::view::{self, Key, SerializedView, View},
};

/// A document's entry in a View's mappings.
#[derive(PartialEq, Debug)]
pub struct Map<K: for<'a> Key<'a> = (), V = ()> {
    /// The header of the document that emitted this entry.
    pub source: Header,

    /// The key used to index the View.
    pub key: K,

    /// An associated value stored in the view.
    pub value: V,
}

impl<K: for<'a> Key<'a>, V> Map<K, V> {
    /// Serializes this map.
    pub(crate) fn serialized<View: SerializedView<Value = V>>(
        &self,
    ) -> Result<Serialized, view::Error> {
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

impl<K: for<'a> Key<'a>, V> Map<K, V> {
    /// Creates a new Map entry for the document with id `source`.
    pub fn new(source: Header, key: K, value: V) -> Self {
        Self { source, key, value }
    }
}

/// A collection of [`Map`]s.
#[derive(Debug, PartialEq)]
#[must_use]
pub enum Mappings<K: for<'a> Key<'a> = (), V = ()> {
    /// Zero or one mappings.
    Simple(Option<Map<K, V>>),
    /// More than one mapping.
    List(Vec<Map<K, V>>),
}

impl<K: for<'a> Key<'a>, V> Default for Mappings<K, V> {
    fn default() -> Self {
        Self::none()
    }
}

impl<K: for<'a> Key<'a>, V> Mappings<K, V> {
    /// Returns an empty collection of mappings.
    pub fn none() -> Self {
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
}

impl<K: for<'a> Key<'a>, V> Extend<Map<K, V>> for Mappings<K, V> {
    fn extend<T: IntoIterator<Item = Map<K, V>>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        for map in iter {
            self.push(map);
        }
    }
}

impl<K: for<'a> Key<'a>, V> FromIterator<Map<K, V>> for Mappings<K, V> {
    fn from_iter<T: IntoIterator<Item = Map<K, V>>>(iter: T) -> Self {
        let mut mappings = Self::none();
        mappings.extend(iter);
        mappings
    }
}

impl<K: for<'a> Key<'a>, V> FromIterator<Self> for Mappings<K, V> {
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

impl<K: for<'a> Key<'a>, V> IntoIterator for Mappings<K, V> {
    type Item = Map<K, V>;

    type IntoIter = MappingsIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Mappings::Simple(option) => MappingsIter::Inline(option),
            Mappings::List(list) => MappingsIter::Vec(list.into_iter()),
        }
    }
}

/// An iterator over [`Mappings`].
pub enum MappingsIter<K: for<'a> Key<'a> = (), V = ()> {
    /// An iterator over a [`Mappings::Simple`] value.
    Inline(Option<Map<K, V>>),
    /// An iterator over a [`Mappings::List`] value.
    Vec(std::vec::IntoIter<Map<K, V>>),
}

impl<K: for<'a> Key<'a>, V> Iterator for MappingsIter<K, V> {
    type Item = Map<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            MappingsIter::Inline(opt) => opt.take(),
            MappingsIter::Vec(iter) => iter.next(),
        }
    }
}

/// A collection of mappings and the associated documents.
#[derive(Debug)]
pub struct MappedDocuments<D, V: View> {
    /// The collection of mappings.
    pub mappings: Vec<Map<V::Key, V::Value>>,
    /// All associated documents by ID.
    ///
    /// Documents can appear in a mapping query multiple times. As a result, they are stored separately to avoid duplication.
    pub documents: BTreeMap<DocumentId, D>,
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
}

/// An iterator of mapped documents.
pub struct MappedDocumentsIter<'a, D, V: View> {
    docs: &'a MappedDocuments<D, V>,
    index: usize,
}

impl<'a, D, V: View> IntoIterator for &'a MappedDocuments<D, V> {
    type Item = MappedDocument<'a, D, V::Key, V::Value>;

    type IntoIter = MappedDocumentsIter<'a, D, V>;

    fn into_iter(self) -> Self::IntoIter {
        MappedDocumentsIter {
            docs: self,
            index: 0,
        }
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
            <View::Key as Key>::from_ord_bytes(&self.key)
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
            .map(Serialized::deserialized::<View>)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(MappedDocuments {
            mappings,
            documents: self.documents,
        })
    }
}

/// A key value pair
#[derive(Clone, PartialEq, Debug)]
pub struct MappedValue<K: for<'a> Key<'a>, V> {
    /// The key responsible for generating the value
    pub key: K,

    /// The value generated by the `View`
    pub value: V,
}

impl<K: for<'a> Key<'a>, V> MappedValue<K, V> {
    /// Returns a new instance with the key/value pair.
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

/// A serialized [`MappedValue`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedSerializedValue {
    /// The serialized key.
    pub key: Bytes,
    /// The serialized value.
    pub value: Bytes,
}
