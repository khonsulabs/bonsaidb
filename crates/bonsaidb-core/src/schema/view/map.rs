use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    document::{Document, Header},
    schema::view::{self, Key},
};

/// A document's entry in a View's mappings.
#[derive(PartialEq, Debug)]
pub struct Map<K: Key = (), V: Serialize = ()> {
    /// The id of the document that emitted this entry.
    pub source: Header,

    /// The key used to index the View.
    pub key: K,

    /// An associated value stored in the view.
    pub value: V,
}

impl<K: Key, V: Serialize> Map<K, V> {
    /// Serializes this map.
    pub fn serialized(&self) -> Result<Serialized, view::Error> {
        Ok(Serialized {
            source: self.source.clone(),
            key: self
                .key
                .as_big_endian_bytes()
                .map_err(view::Error::key_serialization)?
                .to_vec(),
            value: pot::to_vec(&self.value)?,
        })
    }
}

/// A collection of [`Map`]s.
#[derive(Debug, PartialEq)]
pub enum Mappings<K: Key = (), V: Serialize = ()> {
    /// Zero or one mappings.
    Simple(Option<Map<K, V>>),
    /// More than one mapping.
    List(Vec<Map<K, V>>),
}

impl<K: Key, V: Serialize> Mappings<K, V> {
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

impl<K: Key, V: Serialize> Extend<Map<K, V>> for Mappings<K, V> {
    fn extend<T: IntoIterator<Item = Map<K, V>>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        for map in iter {
            self.push(map);
        }
    }
}

impl<K: Key, V: Serialize> FromIterator<Map<K, V>> for Mappings<K, V> {
    fn from_iter<T: IntoIterator<Item = Map<K, V>>>(iter: T) -> Self {
        let mut mappings = Self::none();
        mappings.extend(iter);
        mappings
    }
}

impl<K: Key, V: Serialize> FromIterator<Self> for Mappings<K, V> {
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

impl<K: Key, V: Serialize> IntoIterator for Mappings<K, V> {
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
pub enum MappingsIter<K: Key = (), V: Serialize = ()> {
    /// An iterator over a [`Mappings::Simple`] value.
    Inline(Option<Map<K, V>>),
    /// An iterator over a [`Mappings::List`] value.
    Vec(std::vec::IntoIter<Map<K, V>>),
}

impl<K: Key, V: Serialize> Iterator for MappingsIter<K, V> {
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
pub struct MappedDocument<K: Key = (), V: Serialize = ()> {
    /// The id of the document that emitted this entry.
    pub document: Document<'static>,

    /// The key used to index the View.
    pub key: K,

    /// An associated value stored in the view.
    pub value: V,
}

impl<K: Key, V: Serialize> Map<K, V> {
    /// Creates a new Map entry for the document with id `source`.
    pub fn new(source: Header, key: K, value: V) -> Self {
        Self { source, key, value }
    }
}

/// Represents a document's entry in a View's mappings, serialized and ready to store.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Serialized {
    /// The header of the document that emitted this entry.
    pub source: Header,

    /// The key used to index the View.
    pub key: Vec<u8>,

    /// An associated value stored in the view.
    pub value: Vec<u8>,
}

impl Serialized {
    /// Deserializes this map.
    pub fn deserialized<K: Key, V: Serialize + DeserializeOwned>(
        &self,
    ) -> Result<Map<K, V>, view::Error> {
        Ok(Map {
            source: self.source.clone(),
            key: K::from_big_endian_bytes(&self.key).map_err(view::Error::key_serialization)?,
            value: pot::from_slice(&self.value)?,
        })
    }
}

/// A serialized [`MappedDocument`](MappedDocument).
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MappedSerialized {
    /// The serialized key.
    pub key: Vec<u8>,
    /// The serialized value.
    pub value: Vec<u8>,
    /// The source document.
    pub source: Document<'static>,
}

impl MappedSerialized {
    /// Deserialize into a [`MappedDocument`](MappedDocument).
    pub fn deserialized<K: Key, V: Serialize + DeserializeOwned>(
        self,
    ) -> Result<MappedDocument<K, V>, crate::Error> {
        let key = Key::from_big_endian_bytes(&self.key).map_err(|err: K::Error| {
            crate::Error::Database(view::Error::key_serialization(err).to_string())
        })?;
        let value = pot::from_slice(&self.value)
            .map_err(|err| crate::Error::Database(view::Error::from(err).to_string()))?;

        Ok(MappedDocument {
            document: self.source,
            key,
            value,
        })
    }
}

/// A key value pair
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct MappedValue<K: Key, V> {
    /// The key responsible for generating the value
    pub key: K,

    /// The value generated by the `View`
    pub value: V,
}
