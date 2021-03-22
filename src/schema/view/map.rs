use std::{borrow::Cow, convert::TryInto, marker::PhantomData};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// a structure representing a document's entry in a View's mappings
#[derive(PartialEq, Debug)]
pub struct Map<'k, K: Key<'k> = (), V: Serialize = ()> {
    /// the id of the document that emitted this entry
    pub source: Uuid,

    /// the key used to index the View
    pub key: K,

    /// an associated value stored in the view
    pub value: V,

    _phantom: PhantomData<&'k K>,
}

impl<'k, K: Key<'k>, V: Serialize> Map<'k, K, V> {
    /// creates a new Map entry for the document with id `source`
    pub fn new(source: Uuid, key: K, value: V) -> Self {
        Self {
            source,
            key,
            value,
            _phantom: PhantomData::default(),
        }
    }
}

/// a structure representing a document's entry in a View's mappings, serialized and ready to store
#[derive(Serialize, Deserialize, Debug)]
pub struct Serialized<'k> {
    /// the id of the document that emitted this entry
    pub source: Uuid,

    /// the key used to index the View
    #[serde(borrow)]
    pub key: Cow<'k, [u8]>,

    /// an associated value stored in the view
    pub value: serde_cbor::Value,
}

/// a trait that enables a type to convert itself to a consistent endianness. Expected to be consistent with the implementation of `FromEndianBytes`.
pub trait Key<'k> {
    /// convert `self` into an `IVec` containing bytes ordered in a consistent, cross-platform manner
    fn into_endian_bytes(self) -> Cow<'k, [u8]>;
    /// convert a slice of bytes into `Self` by interpretting `bytes` in a consistent, cross-platform manner
    fn from_endian_bytes(bytes: &'k [u8]) -> Self;
}

impl<'k> Key<'k> for Cow<'k, [u8]> {
    fn into_endian_bytes(self) -> Cow<'k, [u8]> {
        self
    }

    fn from_endian_bytes(bytes: &'k [u8]) -> Self {
        Cow::from(bytes)
    }
}

impl<'k> Key<'k> for () {
    fn into_endian_bytes(self) -> Cow<'k, [u8]> {
        Cow::default()
    }

    fn from_endian_bytes(_: &'k [u8]) -> Self {}
}

impl<'k> Key<'k> for Uuid {
    fn into_endian_bytes(self) -> Cow<'k, [u8]> {
        Cow::from(self.as_u128().to_be_bytes().to_vec())
    }

    fn from_endian_bytes(bytes: &'k [u8]) -> Self {
        Self::from_u128(u128::from_be_bytes(bytes.try_into().unwrap()))
    }
}

impl<'k, T> Key<'k> for Option<T>
where
    T: Key<'k>,
{
    /// # Panics
    ///
    /// panics if `T::to_endian_bytes` returns an empty `IVec`
    // TODO consider removing this panic limitation by adding a single byte to
    // each key (at the end preferrably) so that we can distinguish between None
    // and a 0-byte type
    fn into_endian_bytes(self) -> Cow<'k, [u8]> {
        self.map(|contents| {
            let contents = contents.into_endian_bytes();
            assert!(!contents.is_empty());
            contents
        })
        .unwrap_or_default()
    }

    fn from_endian_bytes(bytes: &'k [u8]) -> Self {
        if bytes.is_empty() {
            None
        } else {
            Some(T::from_endian_bytes(bytes))
        }
    }
}

macro_rules! impl_key_for_primitive {
    ($type:ident) => {
        impl<'k> Key<'k> for $type {
            fn into_endian_bytes(self) -> Cow<'k, [u8]> {
                Cow::from(self.to_be_bytes().to_vec())
            }

            fn from_endian_bytes(bytes: &'k [u8]) -> Self {
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
                $type::MAX.into_endian_bytes().as_ref()
            );
            assert_eq!(
                $type::MAX,
                $type::from_endian_bytes(&$type::MAX.into_endian_bytes())
            );
            assert_eq!(
                $type::MIN,
                $type::from_endian_bytes(&$type::MIN.into_endian_bytes())
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
    assert!(Option::<i8>::None.into_endian_bytes().is_empty());
    assert_eq!(
        Some(1_i8),
        Option::from_endian_bytes(&Some(1_i8).into_endian_bytes())
    );
}

#[test]
#[allow(clippy::unit_cmp)] // this is more of a compilation test
fn unit_key_encoding_tests() {
    assert!(().into_endian_bytes().is_empty());
    assert_eq!((), <() as Key>::from_endian_bytes(&[]));
}

#[test]
fn vec_key_encoding_tests() {
    const ORIGINAL_VALUE: &[u8] = b"pliantdb";
    let vec = Cow::<'_, [u8]>::from(ORIGINAL_VALUE);
    assert_eq!(
        vec.clone(),
        Cow::from_endian_bytes(&vec.into_endian_bytes())
    );
}
