use std::{borrow::Cow, convert::TryInto};

use serde::{Deserialize, Serialize};

/// A document's entry in a View's mappings.
#[derive(PartialEq, Debug)]
pub struct Map<K: Key = (), V: Serialize = ()> {
    /// The id of the document that emitted this entry.
    pub source: u64,

    /// The key used to index the View.
    pub key: K,

    /// An associated value stored in the view.
    pub value: V,
}

impl<K: Key, V: Serialize> Map<K, V> {
    /// Creates a new Map entry for the document with id `source`.
    pub fn new(source: u64, key: K, value: V) -> Self {
        Self { source, key, value }
    }
}

/// Represents a document's entry in a View's mappings, serialized and ready to store.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Serialized {
    /// The id of the document that emitted this entry.
    pub source: u64,

    /// The key used to index the View.
    pub key: Vec<u8>,

    /// An associated value stored in the view.
    pub value: Vec<u8>,
}

/// A trait that enables a type to convert itself to a big-endian/network byte order.
pub trait Key: Clone + Send + Sync {
    /// Convert `self` into an `IVec` containing bytes ordered in big-endian/network byte order.
    fn as_big_endian_bytes(&self) -> Cow<'_, [u8]>;

    /// Convert a slice of bytes into `Self` by interpretting `bytes` in big-endian/network byte order.
    fn from_big_endian_bytes(bytes: &[u8]) -> Self;
}

impl<'k> Key for Cow<'k, [u8]> {
    fn as_big_endian_bytes(&self) -> Cow<'k, [u8]> {
        self.clone()
    }

    fn from_big_endian_bytes(bytes: &[u8]) -> Self {
        Cow::Owned(bytes.to_vec())
    }
}

impl Key for () {
    fn as_big_endian_bytes(&self) -> Cow<'_, [u8]> {
        Cow::default()
    }

    fn from_big_endian_bytes(_: &[u8]) -> Self {}
}

#[cfg(feature = "uuid")]
impl<'k> Key for uuid::Uuid {
    fn as_big_endian_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_big_endian_bytes(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes.try_into().unwrap())
    }
}

impl<'k, T> Key for Option<T>
where
    T: Key,
{
    /// # Panics
    ///
    /// Panics if `T::into_big_endian_bytes` returns an empty `IVec`.
    // TODO consider removing this panic limitation by adding a single byte to
    // each key (at the end preferrably) so that we can distinguish between None
    // and a 0-byte type
    fn as_big_endian_bytes(&self) -> Cow<'_, [u8]> {
        self.as_ref()
            .map(|contents| {
                let contents = contents.as_big_endian_bytes();
                assert!(!contents.is_empty());
                contents
            })
            .unwrap_or_default()
    }

    fn from_big_endian_bytes(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            None
        } else {
            Some(T::from_big_endian_bytes(bytes))
        }
    }
}

macro_rules! impl_key_for_primitive {
    ($type:ident) => {
        impl Key for $type {
            fn as_big_endian_bytes(&self) -> Cow<'_, [u8]> {
                Cow::from(self.to_be_bytes().to_vec())
            }

            fn from_big_endian_bytes(bytes: &[u8]) -> Self {
                $type::from_be_bytes(bytes.try_into().unwrap())
            }
        }
    };
}

impl_key_for_primitive!(i8);
impl_key_for_primitive!(u8);
impl_key_for_primitive!(i16);
impl_key_for_primitive!(u16);
impl_key_for_primitive!(i32);
impl_key_for_primitive!(u32);
impl_key_for_primitive!(i64);
impl_key_for_primitive!(u64);
impl_key_for_primitive!(i128);
impl_key_for_primitive!(u128);

#[test]
#[allow(clippy::cognitive_complexity)] // I disagree - @ecton
fn primitive_key_encoding_tests() {
    macro_rules! test_primitive_extremes {
        ($type:ident) => {
            assert_eq!(
                &$type::MAX.to_be_bytes(),
                $type::MAX.as_big_endian_bytes().as_ref()
            );
            assert_eq!(
                $type::MAX,
                $type::from_big_endian_bytes(&$type::MAX.as_big_endian_bytes())
            );
            assert_eq!(
                $type::MIN,
                $type::from_big_endian_bytes(&$type::MIN.as_big_endian_bytes())
            );
        };
    }

    test_primitive_extremes!(i8);
    test_primitive_extremes!(u8);
    test_primitive_extremes!(i16);
    test_primitive_extremes!(u16);
    test_primitive_extremes!(i32);
    test_primitive_extremes!(u32);
    test_primitive_extremes!(i64);
    test_primitive_extremes!(u64);
    test_primitive_extremes!(i128);
    test_primitive_extremes!(u128);
}

#[test]
fn optional_key_encoding_tests() {
    assert!(Option::<i8>::None.as_big_endian_bytes().is_empty());
    assert_eq!(
        Some(1_i8),
        Option::from_big_endian_bytes(&Some(1_i8).as_big_endian_bytes())
    );
}

#[test]
#[allow(clippy::unit_cmp)] // this is more of a compilation test
fn unit_key_encoding_tests() {
    assert!(().as_big_endian_bytes().is_empty());
    assert_eq!((), <() as Key>::from_big_endian_bytes(&[]));
}

#[test]
fn vec_key_encoding_tests() {
    const ORIGINAL_VALUE: &[u8] = b"pliantdb";
    let vec = Cow::<'_, [u8]>::from(ORIGINAL_VALUE);
    assert_eq!(
        vec.clone(),
        Cow::from_big_endian_bytes(&vec.as_big_endian_bytes())
    );
}
