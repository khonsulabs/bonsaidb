/// [`Key`] implementations for time types.
pub mod time;

use std::{
    borrow::Cow,
    convert::Infallible,
    io::{ErrorKind, Write},
    num::TryFromIntError,
    string::FromUtf8Error,
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
pub trait KeyEncoding<'k, K>: std::fmt::Debug + Send + Sync
where
    K: Key<'k>,
{
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
}

/// A trait that enables a type to convert itself into a `memcmp`-compatible
/// sequence of bytes.
pub trait Key<'k>: KeyEncoding<'k, Self> + Clone + std::fmt::Debug + Send + Sync {
    /// Deserialize a sequence of bytes previously encoded with
    /// [`KeyEncoding::as_ord_bytes`].
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

impl<'a, 'k, K> KeyEncoding<'k, K> for &'a K
where
    K: Key<'k>,
{
    type Error = K::Error;

    const LENGTH: Option<usize> = K::LENGTH;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        (*self).as_ord_bytes()
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
    fn from_ord_bytes(bytes: &[u8]) -> Result<Self, Self::Error> {
        Ok(Cow::Owned(bytes.to_vec()))
    }
}

impl<'k> KeyEncoding<'k, Self> for Cow<'k, [u8]> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(self.clone())
    }
}

macro_rules! impl_u8_slice_key_encoding {
    ($type:ty) => {
        impl<'a, 'k> KeyEncoding<'k, $type> for &'a [u8] {
            type Error = Infallible;

            const LENGTH: Option<usize> = None;

            fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
                Ok(Cow::Borrowed(self))
            }
        }
    };
}

impl_u8_slice_key_encoding!(Cow<'k, [u8]>);

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
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(bytes.to_vec())
    }
}

impl<'a> KeyEncoding<'a, Self> for Vec<u8> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }
}

impl_u8_slice_key_encoding!(Vec<u8>);

impl<'k> IntoPrefixRange for Vec<u8> {
    fn into_prefix_range(self) -> Range<Self> {
        if let Some(next) = next_byte_sequence(&self) {
            Range::from(self..next)
        } else {
            Range::from(self..)
        }
    }
}

impl<'a, const N: usize> Key<'a> for [u8; N] {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() == N {
            let mut array = [0; N];
            array.copy_from_slice(bytes);
            Ok(array)
        } else {
            Err(IncorrectByteLength)
        }
    }
}

impl<'a, const N: usize> KeyEncoding<'a, Self> for [u8; N] {
    type Error = IncorrectByteLength;

    const LENGTH: Option<usize> = Some(N);

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
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
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes))
    }
}

impl<'a> KeyEncoding<'a, Self> for ArcBytes<'a> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }
}

impl_u8_slice_key_encoding!(ArcBytes<'k>);

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
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes))
    }
}

impl<'a> KeyEncoding<'a, Self> for CowBytes<'a> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(self.0.clone())
    }
}

impl_u8_slice_key_encoding!(CowBytes<'k>);

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
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes))
    }
}

impl<'a> KeyEncoding<'a, Self> for Bytes {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }
}

impl_u8_slice_key_encoding!(Bytes);

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
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Self::from_utf8(bytes.to_vec())
    }
}

impl<'a> KeyEncoding<'a, Self> for String {
    type Error = FromUtf8Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

impl<'a, 'k> KeyEncoding<'k, String> for &'a str {
    type Error = FromUtf8Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
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

impl<'k> Key<'k> for Cow<'k, str> {
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        std::str::from_utf8(bytes).map(Cow::Borrowed)
    }
}

impl<'k> KeyEncoding<'k, Self> for Cow<'k, str> {
    type Error = std::str::Utf8Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
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
    fn from_ord_bytes(_: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(())
    }
}

impl<'a> KeyEncoding<'a, Self> for () {
    type Error = Infallible;

    const LENGTH: Option<usize> = Some(0);

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        Ok(Cow::default())
    }
}

impl<'a> Key<'a> for bool {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() || bytes[0] == 0 {
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

impl<'a> KeyEncoding<'a, Self> for bool {
    type Error = Infallible;

    const LENGTH: Option<usize> = Some(1);

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        if *self {
            Ok(Cow::Borrowed(&[1_u8]))
        } else {
            Ok(Cow::Borrowed(&[0_u8]))
        }
    }
}

macro_rules! impl_key_for_tuple {
    ($(($index:tt, $varname:ident, $generic:ident)),+) => {
        impl<'a, $($generic),+> Key<'a> for ($($generic),+,)
        where
            $($generic: Key<'a>),+
        {
            fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
                $(let ($varname, bytes) = decode_composite_field::<$generic>(bytes)?;)+

                if bytes.is_empty() {
                    Ok(($($varname),+,))
                } else {
                    Err(CompositeKeyError::new(std::io::Error::from(
                        ErrorKind::InvalidData,
                    )))
                }
            }
        }

        impl<'a, $($generic),+> KeyEncoding<'a, Self> for ($($generic),+,)
        where
            $($generic: Key<'a>),+
        {
            type Error = CompositeKeyError;

            const LENGTH: Option<usize> = match ($($generic::LENGTH),+,) {
                ($(Some($varname)),+,) => Some($($varname +)+ 0),
                _ => None,
            };

            fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
                let mut bytes = Vec::new();

                $(encode_composite_field(&self.$index, &mut bytes)?;)+

                Ok(Cow::Owned(bytes))
            }
        }
    };
}

impl_key_for_tuple!((0, t1, T1));
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

/// Encodes a value using the `Key` trait in such a way that multiple values can
/// still be ordered at the byte level when chained together.
///
/// ```rust
/// # use bonsaidb_core::key::{encode_composite_field, decode_composite_field};
///
/// let value1 = String::from("hello");
/// let value2 = 42_u32;
/// let mut key_bytes = Vec::new();
/// encode_composite_field(&value1, &mut key_bytes).unwrap();
/// encode_composite_field(&value2, &mut key_bytes).unwrap();
///
/// let (decoded_string, remaining_bytes) = decode_composite_field::<String>(&key_bytes).unwrap();
/// assert_eq!(decoded_string, value1);
/// let (decoded_u32, remaining_bytes) = decode_composite_field::<u32>(&remaining_bytes).unwrap();
/// assert_eq!(decoded_u32, value2);
/// assert!(remaining_bytes.is_empty());
/// ```
pub fn encode_composite_field<'a, T: Key<'a>, Bytes: Write>(
    value: &'a T,
    bytes: &mut Bytes,
) -> Result<(), CompositeKeyError> {
    let t2 = T::as_ord_bytes(value).map_err(CompositeKeyError::new)?;
    if T::LENGTH.is_none() {
        (t2.len() as u64)
            .encode_variable(bytes)
            .map_err(CompositeKeyError::new)?;
    }
    bytes.write_all(&t2)?;
    Ok(())
}

/// Decodes a value previously encoded using [`encode_composite_field()`].
/// The result is a tuple with the first element being the decoded value, and
/// the second element is the remainig byte slice.
///
/// ```rust
/// # use bonsaidb_core::key::{encode_composite_field, decode_composite_field};
///
/// let value1 = String::from("hello");
/// let value2 = 42_u32;
/// let mut key_bytes = Vec::new();
/// encode_composite_field(&value1, &mut key_bytes).unwrap();
/// encode_composite_field(&value2, &mut key_bytes).unwrap();
///
/// let (decoded_string, remaining_bytes) = decode_composite_field::<String>(&key_bytes).unwrap();
/// assert_eq!(decoded_string, value1);
/// let (decoded_u32, remaining_bytes) = decode_composite_field::<u32>(&remaining_bytes).unwrap();
/// assert_eq!(decoded_u32, value2);
/// assert!(remaining_bytes.is_empty());
/// ```
pub fn decode_composite_field<'a, T: Key<'a>>(
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
#[allow(clippy::cognitive_complexity)] // There's no way to please clippy with this
fn composite_key_tests() {
    assert!((0_u32, 0_u32).as_ord_bytes().unwrap() < (1_u32, 0_u32).as_ord_bytes().unwrap());
    /// This test generates various combinations of every tuple type supported.
    /// Each test calls this function, which builds a second Vec containing all
    /// of the encoded key values. Next, both the original vec and the encoded
    /// vec are sorted. The encoded entries are decoded, and the sorted original
    /// vec is compared against the decoded vec to ensure the ordering is the
    /// same.
    ///
    /// The macros just make it less verbose to create the nested loops which
    /// create the tuples.
    fn verify_key_ordering<T: for<'a> Key<'a> + Ord + Eq + std::fmt::Debug>(mut cases: Vec<T>) {
        let mut encoded = {
            cases
                .iter()
                .map(|tuple| tuple.as_ord_bytes().unwrap().to_vec())
                .collect::<Vec<Vec<u8>>>()
        };
        cases.sort();
        encoded.sort();
        println!("Tested {} entries", cases.len());
        let decoded = encoded
            .iter()
            .map(|encoded| T::from_ord_bytes(encoded).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(cases, decoded);
    }

    let values = [0_u16, 0xFF00, 0x0FF0, 0xFF];
    macro_rules! test_enum_variations {
        ($($ident:ident),+) => {
            let mut cases = Vec::new();
            test_enum_variations!(for cases $($ident),+; $($ident),+);
            verify_key_ordering(cases);
        };
        (for $cases:ident $first:ident, $($ident:ident),+; $($variable:ident),+) => {
            for $first in values {
                test_enum_variations!(for $cases $($ident),+; $($variable),+);
            }
        };
        (for $cases:ident $first:ident; $($ident:ident),+) => {
            for $first in values {
                $cases.push(($($ident),+,));
            }
        };
    }
    macro_rules! recursive_test_enum_variations {
        ($ident:ident, $($last_ident:ident),+) => {
            recursive_test_enum_variations!($($last_ident),+);
            test_enum_variations!($ident, $($last_ident),+);
        };
        ($ident:ident) => {
            test_enum_variations!($ident);
        }
    }
    recursive_test_enum_variations!(t1, t2, t3, t4, t5, t6, t7, t8);
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

impl<'a> KeyEncoding<'a, Self> for Signed {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&self) -> Result<Cow<'a, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

impl<'a> Key<'a> for Unsigned {
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

impl<'a> KeyEncoding<'a, Self> for Unsigned {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

#[cfg(feature = "uuid")]
impl<'k> Key<'k> for uuid::Uuid {
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        Ok(Self::from_bytes(bytes.try_into()?))
    }
}

#[cfg(feature = "uuid")]
impl<'k> KeyEncoding<'k, Self> for uuid::Uuid {
    type Error = std::array::TryFromSliceError;

    const LENGTH: Option<usize> = Some(16);

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

impl<'a, T> Key<'a> for Option<T>
where
    T: Key<'a>,
    Self: KeyEncoding<'a, Self, Error = <T as KeyEncoding<'a, T>>::Error>,
{
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() || bytes[0] == 0 {
            Ok(None)
        } else {
            Ok(Some(T::from_ord_bytes(&bytes[1..])?))
        }
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Some(T::first_value()?))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.as_ref().map(T::next_value).transpose()
    }
}

impl<'a, K, T> KeyEncoding<'a, Option<K>> for Option<T>
where
    T: KeyEncoding<'a, K>,
    K: for<'k> Key<'k>,
{
    type Error = T::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        if let Some(contents) = self {
            let mut contents = contents.as_ord_bytes()?.to_vec();
            contents.insert(0, 1);
            Ok(Cow::Owned(contents))
        } else {
            Ok(Cow::Borrowed(b"\0"))
        }
    }
}

/// A type that preserves the original implementation of [`Key`] for
/// `Option<T>`. This should not be used in new code and will be removed in a
/// future version.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
#[deprecated = "this type should not be used in new code and should only be used in transitionary code."]
#[allow(deprecated)]
pub struct LegacyOptionKey<T>(pub Option<T>);

#[allow(deprecated)]
impl<'a, T> Key<'a> for LegacyOptionKey<T>
where
    T: Key<'a>,
    Self: KeyEncoding<'a, Self, Error = <T as KeyEncoding<'a, T>>::Error>,
{
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.is_empty() {
            Ok(Self(None))
        } else {
            Ok(Self(Some(T::from_ord_bytes(bytes)?)))
        }
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Self(Some(T::first_value()?)))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.0.as_ref().map(T::next_value).transpose().map(Self)
    }
}

#[allow(deprecated)]
impl<'a, K, T> KeyEncoding<'a, LegacyOptionKey<K>> for LegacyOptionKey<T>
where
    T: KeyEncoding<'a, K>,
    K: for<'k> Key<'k>,
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
        if let Some(contents) = &self.0 {
            let contents = contents.as_ord_bytes()?;
            assert!(!contents.is_empty());
            Ok(contents)
        } else {
            Ok(Cow::default())
        }
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
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let primitive = u64::decode_variable(bytes)?;
        Self::from_u64(primitive)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, UnknownEnumVariant))
    }
}

impl<'a, T> KeyEncoding<'a, Self> for T
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
}
// ANCHOR_END: impl_key_for_enumkey

macro_rules! impl_key_for_primitive {
    ($type:ident) => {
        impl<'a> Key<'a> for $type {
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
        impl<'a> KeyEncoding<'a, Self> for $type {
            type Error = IncorrectByteLength;
            const LENGTH: Option<usize> = Some(std::mem::size_of::<$type>());

            fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
                Ok(Cow::from(self.to_be_bytes().to_vec()))
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
    let some_string = Some("hello").as_ord_bytes()?;
    let empty_string = Some("").as_ord_bytes()?;
    let none_string = Option::<String>::None.as_ord_bytes()?;
    assert_eq!(
        Option::<String>::from_ord_bytes(&some_string)
            .unwrap()
            .as_deref(),
        Some("hello")
    );
    assert_eq!(
        Option::<String>::from_ord_bytes(&empty_string)
            .unwrap()
            .as_deref(),
        Some("")
    );
    assert_eq!(
        Option::<String>::from_ord_bytes(&none_string).unwrap(),
        None
    );

    #[allow(deprecated)]
    {
        let some_string = LegacyOptionKey(Some("hello")).as_ord_bytes()?;
        let none_string = LegacyOptionKey::<String>(None).as_ord_bytes()?;
        assert_eq!(
            LegacyOptionKey::<String>::from_ord_bytes(&some_string)
                .unwrap()
                .0
                .as_deref(),
            Some("hello")
        );
        assert_eq!(
            LegacyOptionKey::<String>::from_ord_bytes(&none_string)
                .unwrap()
                .0,
            None
        );
    }
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
    assert_eq!(
        vec.clone(),
        Cow::<'_, [u8]>::from_ord_bytes(&vec.as_ord_bytes()?)?
    );
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
