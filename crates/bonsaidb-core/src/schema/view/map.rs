use std::{fmt::Debug, marker::PhantomData};

use arc_bytes::serde::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    document::{CollectionDocument, Header, OwnedDocument},
    schema::{
        view::{self, Key, SerializedView, View},
        SerializedCollection,
    },
};

/// A document's entry in a View's mappings.
#[derive(PartialEq, Debug)]
pub struct Map<K: for<'a> Key<'a> = (), V = ()> {
    /// The id of the document that emitted this entry.
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
                    .as_big_endian_bytes()
                    .map_err(view::Error::key_serialization)?
                    .to_vec(),
            ),
            value: Bytes::from(View::serialize(&self.value)?),
        })
    }
}

/// A collection of [`Map`]s.
#[derive(Debug, PartialEq)]
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

/// A document's entry in a View's mappings.
#[derive(Debug)]
pub struct MappedDocument<V: View> {
    /// The id of the document that emitted this entry.
    pub document: OwnedDocument,

    /// The key used to index the View.
    pub key: V::Key,

    /// An associated value stored in the view.
    pub value: V::Value,

    _view: PhantomData<V>,
}

impl<V> MappedDocument<V>
where
    V: View,
{
    /// Returns a new instance.
    pub fn new(document: OwnedDocument, key: V::Key, value: V::Value) -> Self {
        Self {
            document,
            key,
            value,
            _view: PhantomData,
        }
    }
}

impl<K: for<'a> Key<'a>, V> Map<K, V> {
    /// Creates a new Map entry for the document with id `source`.
    pub fn new(source: Header, key: K, value: V) -> Self {
        Self { source, key, value }
    }
}

/// A document's entry in a View's mappings.
#[derive(Debug)]
pub struct MappedCollectionDocument<V>
where
    V: View,
    V::Collection: SerializedCollection,
    <V::Collection as SerializedCollection>::Contents: Debug,
{
    /// The id of the document that emitted this entry.
    pub document: CollectionDocument<V::Collection>,

    /// The key used to index the View.
    pub key: V::Key,

    /// An associated value stored in the view.
    pub value: V::Value,

    _view: PhantomData<V>,
}

impl<V: View> TryFrom<MappedDocument<V>> for MappedCollectionDocument<V>
where
    V::Collection: SerializedCollection,
    <V::Collection as SerializedCollection>::Contents: Debug,
{
    type Error = crate::Error;

    fn try_from(map: MappedDocument<V>) -> Result<Self, Self::Error> {
        Ok(Self {
            document: CollectionDocument::<V::Collection>::try_from(&map.document)?,
            key: map.key,
            value: map.value,
            _view: PhantomData,
        })
    }
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
            <View::Key as Key>::from_big_endian_bytes(&self.key)
                .map_err(view::Error::key_serialization)?,
            View::deserialize(&self.value)?,
        ))
    }
}

/// A serialized [`MappedDocument`](MappedDocument).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedSerialized {
    /// The serialized mapped value.
    pub mapping: MappedSerializedValue,
    /// The source document.
    pub source: OwnedDocument,
}

impl MappedSerialized {
    /// Deserialize into a [`MappedDocument`](MappedDocument).
    pub fn deserialized<View: SerializedView>(self) -> Result<MappedDocument<View>, crate::Error> {
        let key = Key::from_big_endian_bytes(&self.mapping.key).map_err(
            |err: <View::Key as Key<'_>>::Error| {
                crate::Error::Database(view::Error::key_serialization(err).to_string())
            },
        )?;
        let value = View::deserialize(&self.mapping.value)?;

        Ok(MappedDocument {
            document: self.source,
            key,
            value,
            _view: PhantomData,
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

/// A mapped value in a [`View`].
#[allow(type_alias_bounds)] // False positive, required for associated types
pub type ViewMappedValue<V: View> = MappedValue<V::Key, V::Value>;

/// A serialized [`MappedValue`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedSerializedValue {
    /// The serialized key.
    pub key: Bytes,
    /// The serialized value.
    pub value: Bytes,
}
