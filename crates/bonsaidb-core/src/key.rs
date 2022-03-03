use std::{
    borrow::Cow, convert::Infallible, io::ErrorKind, num::TryFromIntError, string::FromUtf8Error,
};

use arc_bytes::{
    serde::{Bytes, CowBytes},
    ArcBytes,
};
use num_traits::{FromPrimitive, ToPrimitive};
use ordered_varint::{Signed, Unsigned, Variable};
use serde::{Deserialize, Serialize};

use crate::{connection::Range, AnyError};

/// A trait that enables a type to convert itself into a `memcmp`-compatible
/// sequence of bytes.
pub trait Key<'k>: Clone + std::fmt::Debug + Send + Sync {
    /// The error type that can be produced by either serialization or
    /// deserialization.
    type Error: AnyError;

    /// The size of the key, if constant. If this type doesn't produce the same
    /// number of bytes for each value, this should be `None`.
    const LENGTH: Option<usize>;

    /// Convert `self` into a `Cow<[u8]>` containing bytes that are able to be
    /// compared via `memcmp` in a way that is comptaible with its own Ord
    /// implementation.
    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error>;

    /// Deserialize a sequence of bytes previously encoded with
    /// [`Self::as_ord_bytes`].
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error>;

    /// Return the first value in sequence for this type. Not all types
    /// implement this.
    fn first_value() -> Result<Self, NextValueError> {
        Err(NextValueError::Unsupported)
    }

    /// Return the next value in sequence for this type. Not all types implement
    /// this. Instead of wrapping/overflowing, None should be returned.
    fn next_value(&self) -> Result<Self, NextValueError> {
        Err(NextValueError::Unsupported)
    }
}

/// The error types for [`Key::next_value()`].
#[derive(Clone, thiserror::Error, Debug, Serialize, Deserialize)]
pub enum NextValueError {
    /// The key type does not support this operation.
    #[error("the key type does not support automatic ids")]
    Unsupported,
    /// Generating a new value would wrap the underlying value.
    #[error("the key type has run out of unique values")]
    WouldWrap,
}

/// A type that can be used as a prefix range in range-based queries.
pub trait IntoPrefixRange: Sized {
    /// Returns the value as a prefix-range, which will match all values that
    /// start with `self`.
    fn into_prefix_range(self) -> Range<Self>;
}

fn next_byte_sequence(start: &[u8]) -> Option<Vec<u8>> {
    let mut end = start.to_vec();
    // Modify the last byte by adding one. If it would wrap, we proceed to the
    // next byte.
    while let Some(last_byte) = end.pop() {
        if let Some(next) = last_byte.checked_add(1) {
            end.push(next);
            return Some(end);
        }
    }

    None
}

impl<'k> Key<'k> for Cow<'k, [u8]> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(self.clone())
    }

    fn from_ord_bytes(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(Cow::Owned(bytes.to_vec()))
    }
}

impl<'k> IntoPrefixRange for Cow<'k, [u8]> {
    fn into_prefix_range(self) -> Range<Self> {
        if let Some(next) = next_byte_sequence(&self) {
            Range::from(self..Cow::Owned(next))
        } else {
            Range::from(self..)
        }
    }
}

#[test]
fn cow_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(Cow::<'_, [u8]>::Borrowed(b"a")
        .into_prefix_range()
        .contains(&Cow::Borrowed(b"aa")));
    assert!(!Cow::<'_, [u8]>::Borrowed(b"a")
        .into_prefix_range()
        .contains(&Cow::Borrowed(b"b")));
    assert!(Cow::<'_, [u8]>::Borrowed(b"\xff")
        .into_prefix_range()
        .contains(&Cow::Borrowed(b"\xff\xff")));
    assert!(!Cow::<'_, [u8]>::Borrowed(b"\xff")
        .into_prefix_range()
        .contains(&Cow::Borrowed(b"\xfe")));
}

impl<'a> Key<'a> for Vec<u8> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(bytes.to_vec())
    }
}

impl<'k> IntoPrefixRange for Vec<u8> {
    fn into_prefix_range(self) -> Range<Self> {
        if let Some(next) = next_byte_sequence(&self) {
            Range::from(self..next)
        } else {
            Range::from(self..)
        }
    }
}

#[test]
fn vec_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(b"a".to_vec().into_prefix_range().contains(&b"aa".to_vec()));
    assert!(!b"a".to_vec().into_prefix_range().contains(&b"b".to_vec()));
    assert!(b"\xff"
        .to_vec()
        .into_prefix_range()
        .contains(&b"\xff\xff".to_vec()));
    assert!(!b"\xff"
        .to_vec()
        .into_prefix_range()
        .contains(&b"\xfe".to_vec()));
}

impl<'a> Key<'a> for ArcBytes<'a> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes))
    }
}

impl<'k> IntoPrefixRange for ArcBytes<'k> {
    fn into_prefix_range(self) -> Range<Self> {
        if let Some(next) = next_byte_sequence(&self) {
            Range::from(self..Self::owned(next))
        } else {
            Range::from(self..)
        }
    }
}

#[test]
fn arcbytes_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(ArcBytes::from(b"a")
        .into_prefix_range()
        .contains(&ArcBytes::from(b"aa")));
    assert!(!ArcBytes::from(b"a")
        .into_prefix_range()
        .contains(&ArcBytes::from(b"b")));
    assert!(ArcBytes::from(b"\xff")
        .into_prefix_range()
        .contains(&ArcBytes::from(b"\xff\xff")));
    assert!(!ArcBytes::from(b"\xff")
        .into_prefix_range()
        .contains(&ArcBytes::from(b"\xfe")));
}

impl<'a> Key<'a> for CowBytes<'a> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(self.0.clone())
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes))
    }
}

impl<'k> IntoPrefixRange for CowBytes<'k> {
    fn into_prefix_range(self) -> Range<Self> {
        if let Some(next) = next_byte_sequence(&self) {
            Range::from(self..Self::from(next))
        } else {
            Range::from(self..)
        }
    }
}

#[test]
fn cowbytes_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(CowBytes::from(&b"a"[..])
        .into_prefix_range()
        .contains(&CowBytes::from(&b"aa"[..])));
    assert!(!CowBytes::from(&b"a"[..])
        .into_prefix_range()
        .contains(&CowBytes::from(&b"b"[..])));
    assert!(CowBytes::from(&b"\xff"[..])
        .into_prefix_range()
        .contains(&CowBytes::from(&b"\xff\xff"[..])));
    assert!(!CowBytes::from(&b"\xff"[..])
        .into_prefix_range()
        .contains(&CowBytes::from(&b"\xfe"[..])));
}

impl<'a> Key<'a> for Bytes {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes))
    }
}

impl IntoPrefixRange for Bytes {
    fn into_prefix_range(self) -> Range<Self> {
        if let Some(next) = next_byte_sequence(&self) {
            Range::from(self..Self::from(next))
        } else {
            Range::from(self..)
        }
    }
}

#[test]
fn bytes_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(Bytes::from(b"a".to_vec())
        .into_prefix_range()
        .contains(&Bytes::from(b"aa".to_vec())));
    assert!(!Bytes::from(b"a".to_vec())
        .into_prefix_range()
        .contains(&Bytes::from(b"b".to_vec())));
    assert!(Bytes::from(b"\xff".to_vec())
        .into_prefix_range()
        .contains(&Bytes::from(b"\xff\xff".to_vec())));
    assert!(!Bytes::from(b"\xff".to_vec())
        .into_prefix_range()
        .contains(&Bytes::from(b"\xfe".to_vec())));
}

impl<'a> Key<'a> for String {
    type Error = FromUtf8Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Self::from_utf8(bytes.to_vec())
    }
}

impl IntoPrefixRange for String {
    fn into_prefix_range(self) -> Range<Self> {
        let mut bytes = self.as_bytes().to_vec();
        for (index, char) in self.char_indices().rev() {
            let mut next_char = u32::from(char) + 1;
            if next_char == 0xd800 {
                next_char = 0xE000;
            } else if next_char > u32::from(char::MAX) {
                continue;
            }

            let mut char_bytes = [0; 6];
            bytes.splice(
                index..,
                char::try_from(next_char)
                    .unwrap()
                    .encode_utf8(&mut char_bytes)
                    .bytes(),
            );
            return Range::from(self..Self::from_utf8(bytes).unwrap());
        }

        Range::from(self..)
    }
}

#[test]
fn string_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(String::from("a")
        .into_prefix_range()
        .contains(&String::from("aa")));
    assert!(!String::from("a")
        .into_prefix_range()
        .contains(&String::from("b")));
    assert!(String::from("\u{d799}")
        .into_prefix_range()
        .contains(&String::from("\u{d799}a")));
    assert!(!String::from("\u{d799}")
        .into_prefix_range()
        .contains(&String::from("\u{e000}")));
    assert!(String::from("\u{10ffff}")
        .into_prefix_range()
        .contains(&String::from("\u{10ffff}a")));
    assert!(!String::from("\u{10ffff}")
        .into_prefix_range()
        .contains(&String::from("\u{10fffe}")));
}

impl<'a> Key<'a> for () {
    type Error = Infallible;

    const LENGTH: Option<usize> = Some(0);

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::default())
    }

    fn from_ord_bytes(_: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(())
    }
}

impl<'a> Key<'a> for bool {
    type Error = Infallible;

    const LENGTH: Option<usize> = Some(1);

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        if *self {
            Ok(Cow::Borrowed(&[1_u8]))
        } else {
            Ok(Cow::Borrowed(&[0_u8]))
        }
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() || bytes[0] == 0 {
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

macro_rules! impl_key_for_tuple {
    ($(($index:tt, $varname:ident, $generic:ident)),+) => {
        impl<'a, $($generic),+> Key<'a> for ($($generic),+)
        where
            $($generic: Key<'a>),+
        {
            type Error = CompositeKeyError;

            const LENGTH: Option<usize> = match ($($generic::LENGTH),+) {
                ($(Some($varname)),+) => Some($($varname +)+ 0),
                _ => None,
            };

            fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
                let mut bytes = Vec::new();

                $(encode_composite_key_field(&self.$index, &mut bytes)?;)+

                Ok(Cow::Owned(bytes))
            }

            fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
                $(let ($varname, bytes) = decode_composite_key_field::<$generic>(bytes)?;)+

                if bytes.is_empty() {
                    Ok(($($varname),+))
                } else {
                    Err(CompositeKeyError::new(std::io::Error::from(
                        ErrorKind::InvalidData,
                    )))
                }
            }
        }
    };
}

impl_key_for_tuple!((0, t1, T1), (1, t2, T2));
impl_key_for_tuple!((0, t1, T1), (1, t2, T2), (2, t3, T3));
impl_key_for_tuple!((0, t1, T1), (1, t2, T2), (2, t3, T3), (3, t4, T4));
impl_key_for_tuple!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5)
);
impl_key_for_tuple!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5),
    (5, t6, T6)
);
impl_key_for_tuple!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5),
    (5, t6, T6),
    (6, t7, T7)
);
impl_key_for_tuple!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5),
    (5, t6, T6),
    (6, t7, T7),
    (7, t8, T8)
);

fn encode_composite_key_field<'a, T: Key<'a>>(
    value: &'a T,
    bytes: &mut Vec<u8>,
) -> Result<(), CompositeKeyError> {
    let t2 = T::as_ord_bytes(value).map_err(CompositeKeyError::new)?;
    if T::LENGTH.is_none() {
        (t2.len() as u64)
            .encode_variable(bytes)
            .map_err(CompositeKeyError::new)?;
    }
    bytes.extend(t2.iter().copied());
    Ok(())
}

fn decode_composite_key_field<'a, T: Key<'a>>(
    mut bytes: &'a [u8],
) -> Result<(T, &[u8]), CompositeKeyError> {
    let length = if let Some(length) = T::LENGTH {
        length
    } else {
        usize::try_from(u64::decode_variable(&mut bytes)?)?
    };
    let (t2, remaining) = bytes.split_at(length);
    Ok((
        T::from_ord_bytes(t2).map_err(CompositeKeyError::new)?,
        remaining,
    ))
}

#[test]
#[allow(clippy::too_many_lines, clippy::cognitive_complexity)] // couldn't figure out how to macro-ize it
fn composite_key_tests() {
    fn roundtrip<T: for<'a> Key<'a> + Ord + Eq + std::fmt::Debug>(mut cases: Vec<T>) {
        let mut encoded = {
            cases
                .iter()
                .map(|tuple| tuple.as_ord_bytes().unwrap().to_vec())
                .collect::<Vec<Vec<u8>>>()
        };
        cases.sort();
        encoded.sort();
        let decoded = encoded
            .iter()
            .map(|encoded| T::from_ord_bytes(encoded).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(cases, decoded);
    }

    let values = [Unsigned::from(0_u8), Unsigned::from(16_u8)];
    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            cases.push((t1, t2));
        }
    }
    roundtrip(cases);

    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            for t3 in values {
                cases.push((t1, t2, t3));
            }
        }
    }
    roundtrip(cases);

    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            for t3 in values {
                for t4 in values {
                    cases.push((t1, t2, t3, t4));
                }
            }
        }
    }
    roundtrip(cases);

    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            for t3 in values {
                for t4 in values {
                    for t5 in values {
                        cases.push((t1, t2, t3, t4, t5));
                    }
                }
            }
        }
    }
    roundtrip(cases);

    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            for t3 in values {
                for t4 in values {
                    for t5 in values {
                        for t6 in values {
                            cases.push((t1, t2, t3, t4, t5, t6));
                        }
                    }
                }
            }
        }
    }
    roundtrip(cases);

    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            for t3 in values {
                for t4 in values {
                    for t5 in values {
                        for t6 in values {
                            for t7 in values {
                                cases.push((t1, t2, t3, t4, t5, t6, t7));
                            }
                        }
                    }
                }
            }
        }
    }
    roundtrip(cases);

    let mut cases = Vec::new();
    for t1 in values {
        for t2 in values {
            for t3 in values {
                for t4 in values {
                    for t5 in values {
                        for t6 in values {
                            for t7 in values {
                                for t8 in values {
                                    cases.push((t1, t2, t3, t4, t5, t6, t7, t8));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    roundtrip(cases);
}

/// An error occurred inside of one of the composite key fields.
#[derive(thiserror::Error, Debug)]
#[error("key error: {0}")]
pub struct CompositeKeyError(Box<dyn AnyError>);

impl CompositeKeyError {
    pub(crate) fn new<E: AnyError>(error: E) -> Self {
        Self(Box::new(error))
    }
}

impl From<TryFromIntError> for CompositeKeyError {
    fn from(err: TryFromIntError) -> Self {
        Self::new(err)
    }
}

impl From<std::io::Error> for CompositeKeyError {
    fn from(err: std::io::Error) -> Self {
        Self::new(err)
    }
}

impl<'a> Key<'a> for Signed {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&self) -> Result<Cow<'a, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes)
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Self::from(0_i128))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        i128::try_from(*self)
            .ok()
            .and_then(|key| key.checked_add(1))
            .map(Self::from)
            .ok_or(NextValueError::WouldWrap)
    }
}

impl<'a> Key<'a> for Unsigned {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes)
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Self::from(0_u128))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        u128::try_from(*self)
            .ok()
            .and_then(|key| key.checked_add(1))
            .map(Self::from)
            .ok_or(NextValueError::WouldWrap)
    }
}

#[cfg(feature = "uuid")]
impl<'k> Key<'k> for uuid::Uuid {
    type Error = std::array::TryFromSliceError;

    const LENGTH: Option<usize> = Some(16);

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }

    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from_bytes(bytes.try_into()?))
    }
}

impl<'a, T> Key<'a> for Option<T>
where
    T: Key<'a>,
{
    type Error = T::Error;

    const LENGTH: Option<usize> = T::LENGTH;

    /// # Panics
    ///
    /// Panics if `T::into_big_endian_bytes` returns an empty `IVec`.
    // TODO consider removing this panic limitation by adding a single byte to
    // each key (at the end preferrably) so that we can distinguish between None
    // and a 0-byte type
    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        if let Some(contents) = self {
            let contents = contents.as_ord_bytes()?;
            assert!(!contents.is_empty());
            Ok(contents)
        } else {
            Ok(Cow::default())
        }
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            Ok(None)
        } else {
            Ok(Some(T::from_ord_bytes(bytes)?))
        }
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Some(T::first_value()?))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.as_ref().map(T::next_value).transpose()
    }
}

/// Adds `Key` support to an enum. Requires implementing
/// [`ToPrimitive`](num_traits::ToPrimitive) and
/// [`FromPrimitive`](num_traits::FromPrimitive), or using a crate like
/// [num-derive](https://crates.io/crates/num-derive) to do it automatically.
/// Take care when using enums as keys: if the order changes or if the meaning
/// of existing numerical values changes, make sure to update any related views'
/// version number to ensure the values are re-evaluated.
pub trait EnumKey: ToPrimitive + FromPrimitive + Clone + std::fmt::Debug + Send + Sync {}

/// An error that indicates an unexpected number of bytes were present.
#[derive(thiserror::Error, Debug)]
#[error("incorrect byte length")]
pub struct IncorrectByteLength;

/// An error that indicates an unexpected enum variant value was found.
#[derive(thiserror::Error, Debug)]
#[error("unknown enum variant")]
pub struct UnknownEnumVariant;

impl From<std::array::TryFromSliceError> for IncorrectByteLength {
    fn from(_: std::array::TryFromSliceError) -> Self {
        Self
    }
}

// ANCHOR: impl_key_for_enumkey
impl<'a, T> Key<'a> for T
where
    T: EnumKey,
{
    type Error = std::io::Error;
    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        let integer = self
            .to_u64()
            .map(Unsigned::from)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, IncorrectByteLength))?;
        Ok(Cow::Owned(integer.to_variable_vec()?))
    }

    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let primitive = u64::decode_variable(bytes)?;
        Self::from_u64(primitive)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, UnknownEnumVariant))
    }
}
// ANCHOR_END: impl_key_for_enumkey

macro_rules! impl_key_for_primitive {
    ($type:ident) => {
        impl<'a> Key<'a> for $type {
            type Error = IncorrectByteLength;
            const LENGTH: Option<usize> = Some(std::mem::size_of::<$type>());

            fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
                Ok(Cow::from(self.to_be_bytes().to_vec()))
            }

            fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
                Ok($type::from_be_bytes(bytes.try_into()?))
            }

            fn first_value() -> Result<Self, NextValueError> {
                Ok(0)
            }

            fn next_value(&self) -> Result<Self, NextValueError> {
                self.checked_add(1).ok_or(NextValueError::WouldWrap)
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
fn primitive_key_encoding_tests() -> anyhow::Result<()> {
    macro_rules! test_primitive_extremes {
        ($type:ident) => {
            assert_eq!(
                &$type::MAX.to_be_bytes(),
                $type::MAX.as_ord_bytes()?.as_ref()
            );
            assert_eq!(
                $type::MAX,
                $type::from_ord_bytes(&$type::MAX.as_ord_bytes()?)?
            );
            assert_eq!(
                $type::MIN,
                $type::from_ord_bytes(&$type::MIN.as_ord_bytes()?)?
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

    Ok(())
}

#[test]
fn optional_key_encoding_tests() -> anyhow::Result<()> {
    assert!(Option::<i8>::None.as_ord_bytes()?.is_empty());
    assert_eq!(
        Some(1_i8),
        Option::from_ord_bytes(&Some(1_i8).as_ord_bytes()?)?
    );
    Ok(())
}

#[test]
#[allow(clippy::unit_cmp)] // this is more of a compilation test
fn unit_key_encoding_tests() -> anyhow::Result<()> {
    assert!(().as_ord_bytes()?.is_empty());
    assert_eq!((), <() as Key>::from_ord_bytes(&[])?);
    Ok(())
}

#[test]
#[allow(clippy::unit_cmp)] // this is more of a compilation test
fn bool_key_encoding_tests() -> anyhow::Result<()> {
    let true_as_bytes = true.as_ord_bytes()?;
    let false_as_bytes = false.as_ord_bytes()?;
    assert!(bool::from_ord_bytes(&true_as_bytes)?);
    assert!(!bool::from_ord_bytes(&false_as_bytes)?);
    Ok(())
}

#[test]
fn vec_key_encoding_tests() -> anyhow::Result<()> {
    const ORIGINAL_VALUE: &[u8] = b"bonsaidb";
    let vec = Cow::<'_, [u8]>::from(ORIGINAL_VALUE);
    assert_eq!(vec.clone(), Cow::from_ord_bytes(&vec.as_ord_bytes()?)?);
    Ok(())
}

#[test]
#[allow(clippy::use_self)] // Weird interaction with num_derive
fn enum_derive_tests() -> anyhow::Result<()> {
    #[derive(Debug, Clone, num_derive::ToPrimitive, num_derive::FromPrimitive)]
    enum SomeEnum {
        One = 1,
        NineNineNine = 999,
    }

    impl EnumKey for SomeEnum {}

    let encoded = SomeEnum::One.as_ord_bytes()?;
    let value = SomeEnum::from_ord_bytes(&encoded)?;
    assert!(matches!(value, SomeEnum::One));

    let encoded = SomeEnum::NineNineNine.as_ord_bytes()?;
    let value = SomeEnum::from_ord_bytes(&encoded)?;
    assert!(matches!(value, SomeEnum::NineNineNine));

    Ok(())
}
