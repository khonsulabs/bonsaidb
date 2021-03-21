use std::{borrow::Cow, convert::TryInto, marker::PhantomData};

use serde::Serialize;
use sled::IVec;
use uuid::Uuid;

/// a structure representing a document's entry in a View's mappings
#[derive(PartialEq, Debug)]
pub struct Map<'k, K: ToEndianBytes<'k> = (), V: Serialize = ()> {
    /// the id of the document that emitted this entry
    pub source: Uuid,

    /// the key used to index the View
    pub key: K,

    /// an associated value stored in the view
    pub value: V,

    _phantom: PhantomData<&'k K>,
}

impl<'k, K: ToEndianBytes<'k>, V: Serialize> Map<'k, K, V> {
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
pub struct Serialized {
    /// the id of the document that emitted this entry
    pub source: Uuid,

    /// the key used to index the View
    // TODO change this to bytes
    pub key: IVec,

    /// an associated value stored in the view
    pub value: serde_cbor::Value,
}

/// a trait that enables a type to convert itself to a consistent endianness. Expected to be consistent with the implementation of `FromEndianBytes`.
pub trait ToEndianBytes<'a> {
    /// convert `self` into an `IVec` containing bytes ordered in a consistent, cross-platform manner
    fn to_endian_bytes(&self) -> IVec;
    /// convert a slice of bytes into `Self` by interpretting `bytes` in a consistent, cross-platform manner
    fn from_endian_bytes(bytes: &'a [u8]) -> Self;
}

impl<'a> ToEndianBytes<'a> for Cow<'a, [u8]> {
    fn to_endian_bytes(&self) -> IVec {
        IVec::from(self.as_ref())
    }

    fn from_endian_bytes(bytes: &'a [u8]) -> Self {
        Cow::from(bytes)
    }
}

impl<'a> ToEndianBytes<'a> for IVec {
    fn to_endian_bytes(&self) -> IVec {
        self.clone()
    }

    fn from_endian_bytes(bytes: &'a [u8]) -> Self {
        Self::from(bytes)
    }
}

impl<'a> ToEndianBytes<'a> for () {
    fn to_endian_bytes(&self) -> IVec {
        IVec::default()
    }

    fn from_endian_bytes(_: &'a [u8]) -> Self {}
}

impl<'a> ToEndianBytes<'a> for Uuid {
    fn to_endian_bytes(&self) -> IVec {
        IVec::from(&self.as_u128().to_be_bytes())
    }

    fn from_endian_bytes(bytes: &'a [u8]) -> Self {
        Self::from_u128(u128::from_be_bytes(bytes.try_into().unwrap()))
    }
}

impl<'a, T> ToEndianBytes<'a> for Option<T>
where
    T: ToEndianBytes<'a>,
{
    fn to_endian_bytes(&self) -> IVec {
        self.as_ref()
            .map(|contents| contents.to_endian_bytes())
            .unwrap_or_default()
    }

    fn from_endian_bytes(bytes: &'a [u8]) -> Self {
        if bytes.is_empty() {
            None
        } else {
            Some(T::from_endian_bytes(bytes))
        }
    }
}

macro_rules! impl_mapkey_for_primitive {
    ($type:ident) => {
        impl<'a> ToEndianBytes<'a> for $type {
            fn to_endian_bytes(&self) -> IVec {
                IVec::from(&$type::to_be_bytes(*self))
            }

            fn from_endian_bytes(bytes: &'a [u8]) -> Self {
                $type::from_be_bytes(bytes.try_into().unwrap())
            }
        }
    };
}

impl_mapkey_for_primitive!(i8);
impl_mapkey_for_primitive!(u8);
impl_mapkey_for_primitive!(i16);
impl_mapkey_for_primitive!(u16);
impl_mapkey_for_primitive!(i32);
impl_mapkey_for_primitive!(u32);
impl_mapkey_for_primitive!(i64);
impl_mapkey_for_primitive!(u64);
impl_mapkey_for_primitive!(i128);
impl_mapkey_for_primitive!(u128);
