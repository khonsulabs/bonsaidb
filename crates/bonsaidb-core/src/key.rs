/// [`Key`] implementations for time types.
pub mod time;

mod deprecated;

use std::{
    borrow::{Borrow, Cow},
    convert::Infallible,
    io::ErrorKind,
    num::TryFromIntError,
    string::FromUtf8Error,
};

use arc_bytes::{
    serde::{Bytes, CowBytes},
    ArcBytes,
};
pub use bonsaidb_macros::Key;
pub use deprecated::*;
use num_traits::{FromPrimitive, ToPrimitive};
use ordered_varint::{Signed, Unsigned, Variable};
use serde::{Deserialize, Serialize};

use crate::{
    connection::{Bound, BoundRef, MaybeOwned, RangeRef},
    AnyError,
};

/// A trait that enables a type to convert itself into a `memcmp`-compatible
/// sequence of bytes.
pub trait KeyEncoding<'k, K>: Send + Sync
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
///
/// # Deriving this trait
///
/// This trait can be derived on structs and enums whose members all implement
/// `Key`. It is important to note that the order of individual fields and enum
/// variants is important, so special care must be taken when updating enums and
/// structs when trying to preserve backwards compatibility with existing data.
///
/// ```rust
/// use bonsaidb_core::key::Key;
///
/// #[derive(Key, Clone, Debug)]
/// # #[key(core = bonsaidb_core)]
/// struct CompositeKey {
///     user_id: u64,
///     task_id: u32,
/// }
/// ```
///
/// Each field or enum variant is encoded and decoded in the order in which it
/// appears in the source code. The implementation uses [`CompositeKeyEncoder`]
/// and [`CompositeKeyDecoder`] to encode each field.
///
/// ## `allow_null_bytes`
///
/// The derive macro offers an argument `allow_null_bytes`, which defaults to
/// false. Null bytes can cause problematic sort behavior when multiple
/// variable-length encoded fields are encoded as a composite key. Consider this
/// example:
///
/// ```rust
/// use bonsaidb_core::key::Key;
///
/// #[derive(Key, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
/// #[key(allow_null_bytes = true)]
/// # #[key(core = bonsaidb_core)]
/// struct CompositeKey {
///     a: String,
///     b: String,
/// }
/// ```
///
/// With this structure, we can cause sorting misbehaviors using this data set:
///
/// | `a`      | `b`   | Encoded Bytes         |
/// |----------|-------|-----------------------|
/// | `"a"`    | `"c"` | `6100 6300 0101`      |
/// | `"a\0b"` | `"a"` | `6100 6200 610003 01` |
/// | `"b"`    | `"a"` | `6200 6100 0101`      |
///
/// In this table, `a` and `b` are ordered as `CompositeKey` would be ordered
/// when compared using `Ord`. However, the order of the encoded bytes does not
/// match. Without specifying `allow_null_bytes = true`, [`CompositeKeyEncoder`]
/// will return an error when a null byte is encountered.
///
/// This null-byte edge case only applies to variable length [`Key`]s
/// ([`KeyEncoding::LENGTH`] is `None`).
pub trait Key<'k>: KeyEncoding<'k, Self> + Clone + Send + Sync {
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

impl<'a, 'k, K, KE> KeyEncoding<'k, K> for &'a KE
where
    KE: KeyEncoding<'k, K> + ?Sized + PartialEq,
    K: Key<'k> + PartialEq<KE>,
{
    type Error = KE::Error;

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
pub trait IntoPrefixRange<'a, TOwned>: PartialEq
where
    TOwned: Borrow<Self> + PartialEq<Self>,
{
    /// Returns the value as a prefix-range, which will match all values that
    /// start with `self`.
    fn to_prefix_range(&'a self) -> RangeRef<'a, TOwned, Self>;
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

impl<'a, 'k> IntoPrefixRange<'a, Self> for Cow<'k, [u8]> {
    fn to_prefix_range(&'a self) -> RangeRef<'a, Self> {
        if let Some(next) = next_byte_sequence(self) {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(Cow::Owned(next))),
            }
        } else {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::Unbounded,
            }
        }
    }
}

impl<'a, 'k, TOwned, TBorrowed> Key<'k> for MaybeOwned<'a, TOwned, TBorrowed>
where
    TBorrowed: KeyEncoding<'k, TOwned, Error = TOwned::Error> + PartialEq + ?Sized,
    TOwned: Key<'k> + PartialEq<TBorrowed>,
{
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        TOwned::from_ord_bytes(bytes).map(Self::Owned)
    }
}

impl<'a, 'k, TOwned, TBorrowed> KeyEncoding<'k, Self> for MaybeOwned<'a, TOwned, TBorrowed>
where
    TBorrowed: KeyEncoding<'k, TOwned, Error = TOwned::Error> + PartialEq + ?Sized,
    TOwned: Key<'k> + PartialEq<TBorrowed>,
{
    type Error = TOwned::Error;

    const LENGTH: Option<usize> = TBorrowed::LENGTH;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        match self {
            MaybeOwned::Owned(value) => value.as_ord_bytes(),
            MaybeOwned::Borrowed(value) => value.as_ord_bytes(),
        }
    }
}

#[test]
fn cow_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(Cow::<'_, [u8]>::Borrowed(b"a")
        .to_prefix_range()
        .contains(&Cow::Borrowed(&b"aa"[..])));
    assert!(!Cow::<'_, [u8]>::Borrowed(b"a")
        .to_prefix_range()
        .contains(&Cow::Borrowed(&b"b"[..])));
    assert!(Cow::<'_, [u8]>::Borrowed(b"\xff")
        .to_prefix_range()
        .contains(&Cow::Borrowed(&b"\xff\xff"[..])));
    assert!(!Cow::<'_, [u8]>::Borrowed(b"\xff")
        .to_prefix_range()
        .contains(&Cow::Borrowed(&b"\xfe"[..])));
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

impl<'a> IntoPrefixRange<'a, Self> for Vec<u8> {
    fn to_prefix_range(&'a self) -> RangeRef<'a, Self> {
        if let Some(next) = next_byte_sequence(self) {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(next)),
            }
        } else {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::Unbounded,
            }
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
    assert!(b"a".to_vec().to_prefix_range().contains(&b"aa".to_vec()));
    assert!(!b"a".to_vec().to_prefix_range().contains(&b"b".to_vec()));
    assert!(b"\xff"
        .to_vec()
        .to_prefix_range()
        .contains(&b"\xff\xff".to_vec()));
    assert!(!b"\xff"
        .to_vec()
        .to_prefix_range()
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

impl<'a, 'k> IntoPrefixRange<'a, Self> for ArcBytes<'k> {
    fn to_prefix_range(&'a self) -> RangeRef<'a, Self> {
        if let Some(next) = next_byte_sequence(self) {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(Self::owned(next))),
            }
        } else {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::Unbounded,
            }
        }
    }
}

#[test]
fn arcbytes_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(ArcBytes::from(b"a")
        .to_prefix_range()
        .contains(&ArcBytes::from(b"aa")));
    assert!(!ArcBytes::from(b"a")
        .to_prefix_range()
        .contains(&ArcBytes::from(b"b")));
    assert!(ArcBytes::from(b"\xff")
        .to_prefix_range()
        .contains(&ArcBytes::from(b"\xff\xff")));
    assert!(!ArcBytes::from(b"\xff")
        .to_prefix_range()
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

impl<'a, 'k> IntoPrefixRange<'a, Self> for CowBytes<'k> {
    fn to_prefix_range(&'a self) -> RangeRef<'_, Self> {
        if let Some(next) = next_byte_sequence(self) {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(Self::from(next))),
            }
        } else {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::Unbounded,
            }
        }
    }
}

#[test]
fn cowbytes_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(CowBytes::from(&b"a"[..])
        .to_prefix_range()
        .contains(&CowBytes::from(&b"aa"[..])));
    assert!(!CowBytes::from(&b"a"[..])
        .to_prefix_range()
        .contains(&CowBytes::from(&b"b"[..])));
    assert!(CowBytes::from(&b"\xff"[..])
        .to_prefix_range()
        .contains(&CowBytes::from(&b"\xff\xff"[..])));
    assert!(!CowBytes::from(&b"\xff"[..])
        .to_prefix_range()
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

impl<'a> IntoPrefixRange<'a, Self> for Bytes {
    fn to_prefix_range(&'a self) -> RangeRef<'a, Self> {
        if let Some(next) = next_byte_sequence(self) {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(Self::from(next))),
            }
        } else {
            RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::Unbounded,
            }
        }
    }
}

#[test]
fn bytes_prefix_range_tests() {
    use std::ops::RangeBounds;
    assert!(Bytes::from(b"a".to_vec())
        .to_prefix_range()
        .contains(&Bytes::from(b"aa".to_vec())));
    assert!(!Bytes::from(b"a".to_vec())
        .to_prefix_range()
        .contains(&Bytes::from(b"b".to_vec())));
    assert!(Bytes::from(b"\xff".to_vec())
        .to_prefix_range()
        .contains(&Bytes::from(b"\xff\xff".to_vec())));
    assert!(!Bytes::from(b"\xff".to_vec())
        .to_prefix_range()
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

impl<'k> KeyEncoding<'k, String> for str {
    type Error = FromUtf8Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

impl<'a> IntoPrefixRange<'a, Self> for String {
    fn to_prefix_range(&'a self) -> RangeRef<'a, Self> {
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
            return RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(Self::from_utf8(bytes).unwrap())),
            };
        }

        RangeRef {
            start: BoundRef::borrowed(Bound::Included(self)),
            end: BoundRef::Unbounded,
        }
    }
}

impl<'a> IntoPrefixRange<'a, String> for str {
    fn to_prefix_range(&'a self) -> RangeRef<'a, String, Self> {
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
            return RangeRef {
                start: BoundRef::borrowed(Bound::Included(self)),
                end: BoundRef::owned(Bound::Excluded(String::from_utf8(bytes).unwrap())),
            };
        }

        RangeRef {
            start: BoundRef::borrowed(Bound::Included(self)),
            end: BoundRef::Unbounded,
        }
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
        .to_prefix_range()
        .contains(&String::from("aa")));
    assert!(!String::from("a")
        .to_prefix_range()
        .contains(&String::from("b")));
    assert!(String::from("\u{d799}")
        .to_prefix_range()
        .contains(&String::from("\u{d799}a")));
    assert!(!String::from("\u{d799}")
        .to_prefix_range()
        .contains(&String::from("\u{e000}")));
    assert!(String::from("\u{10ffff}")
        .to_prefix_range()
        .contains(&String::from("\u{10ffff}a")));
    assert!(!String::from("\u{10ffff}")
        .to_prefix_range()
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
                let mut decoder = CompositeKeyDecoder::new(bytes);
                $(let $varname = decoder.decode::<$generic>()?;)+
                decoder.finish()?;

                    Ok(($($varname),+,))
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
                let mut encoder = CompositeKeyEncoder::default();

                $(encoder.encode(&self.$index)?;)+

                Ok(Cow::Owned(encoder.finish()))
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

/// Encodes multiple [`KeyEncoding`] implementors into a single byte buffer,
/// preserving the ordering guarantees necessary for [`Key`].
///
/// The produced bytes can be decoded using [`CompositeKeyDecoder`].
#[derive(Default)]
pub struct CompositeKeyEncoder {
    bytes: Vec<u8>,
    encoded_lengths: Vec<u16>,
    allow_null_bytes: bool,
}

impl CompositeKeyEncoder {
    /// Prevents checking for null bytes in variable length fields. [`Key`]
    /// types that have a fixed width have no edge cases with null bytes. See
    /// [`CompositeKeyFieldContainsNullByte`] for an explanation and example of
    /// the edge case introduced when allowing null bytes to be encoded.
    pub fn allow_null_bytes_in_variable_fields(&mut self) {
        self.allow_null_bytes = true;
    }

    /// Encodes a single [`KeyEncoding`] implementing value.
    ///
    /// ```rust
    /// # use bonsaidb_core::key::{CompositeKeyEncoder, CompositeKeyDecoder};
    ///
    /// let value1 = String::from("hello");
    /// let value2 = 42_u32;
    /// let mut encoder = CompositeKeyEncoder::default();
    /// encoder.encode(&value1).unwrap();
    /// encoder.encode(&value2).unwrap();
    /// let encoded = encoder.finish();
    ///
    /// let mut decoder = CompositeKeyDecoder::new(&encoded);
    /// let decoded_string = decoder.decode::<String>().unwrap();
    /// assert_eq!(decoded_string, value1);
    /// let decoded_u32 = decoder.decode::<u32>().unwrap();
    /// assert_eq!(decoded_u32, value2);
    /// decoder.finish().expect("trailing bytes");
    /// ```
    pub fn encode<'a, K: Key<'a>, T: KeyEncoding<'a, K> + ?Sized>(
        &mut self,
        value: &'a T,
    ) -> Result<(), CompositeKeyError> {
        let encoded = T::as_ord_bytes(value).map_err(CompositeKeyError::new)?;
        if T::LENGTH.is_none() {
            if !self.allow_null_bytes && encoded.iter().any(|b| *b == 0) {
                return Err(CompositeKeyError::new(std::io::Error::new(
                    ErrorKind::InvalidData,
                    CompositeKeyFieldContainsNullByte,
                )));
            }
            let encoded_length = u16::try_from(encoded.len())?;
            self.encoded_lengths.push(encoded_length);
        }
        self.bytes.extend_from_slice(&encoded);
        if T::LENGTH.is_none() {
            // With variable length data, we need the key to have a delimiter to
            // ensure that "a" sorts before "aa".
            self.bytes.push(0);
        }
        Ok(())
    }

    /// Finishes encoding the field and returns the encoded bytes.
    #[must_use]
    #[allow(clippy::missing_panics_doc)] // All unreachable
    pub fn finish(mut self) -> Vec<u8> {
        self.bytes.reserve_exact(self.encoded_lengths.len() * 2);
        for length in self.encoded_lengths.into_iter().rev() {
            match length {
                0..=0x7F => {
                    self.bytes.push(u8::try_from(length).unwrap());
                }
                0x80..=0x3FFF => {
                    self.bytes.push(u8::try_from(length >> 7).unwrap());
                    self.bytes
                        .push(u8::try_from((length & 0x7F) | 0x80).unwrap());
                }
                0x4000.. => {
                    self.bytes.push(u8::try_from(length >> 14).unwrap());
                    self.bytes
                        .push(u8::try_from(((length >> 7) & 0x7F) | 0x80).unwrap());
                    self.bytes
                        .push(u8::try_from((length & 0x7F) | 0x80).unwrap());
                }
            }
        }
        self.bytes
    }
}

/// Null bytes in variable fields encoded with [`CompositeKeyEncoder`] can cause
/// sort order to misbehave.
///
/// Consider these tuples:
///
/// | Tuple          | Encoded          |
/// |----------------|------------------|
/// | `("a", "b")`   | `9700 9800 0101` |
/// | `("a\0", "a")` | `9700 9700 0101` |
///
/// The encoded bytes, when compared, will produce a different sort result than
/// when comparing the tuples.
///
/// By default, [`CompositeKeyEncoder`] checks for null bytes and returns an
/// error when a null byte is found. See
/// [`CompositeKeyEncoder::allow_null_bytes_in_variable_fields()`] if you wish
/// to allow null bytes despite this edge case.
#[derive(thiserror::Error, Debug)]
#[error("a variable length field contained a null byte.")]
pub struct CompositeKeyFieldContainsNullByte;

/// Decodes multiple [`Key`] values from a byte slice previously encoded with
/// [`CompositeKeyEncoder`].
#[derive(Default)]
pub struct CompositeKeyDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CompositeKeyDecoder<'a> {
    /// Returns a new decoder for `bytes`.
    #[must_use]
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Decodes a value previously encoded using [`CompositeKeyEncoder`]. Calls
    /// to decode must be made in the same order as the values were encoded in.
    ///
    /// ```rust
    /// # use bonsaidb_core::key::{CompositeKeyEncoder, CompositeKeyDecoder};
    ///
    /// let value1 = String::from("hello");
    /// let value2 = 42_u32;
    /// let mut encoder = CompositeKeyEncoder::default();
    /// encoder.encode(&value1).unwrap();
    /// encoder.encode(&value2).unwrap();
    /// let encoded = encoder.finish();
    ///
    /// let mut decoder = CompositeKeyDecoder::new(&encoded);
    /// let decoded_string = decoder.decode::<String>().unwrap();
    /// assert_eq!(decoded_string, value1);
    /// let decoded_u32 = decoder.decode::<u32>().unwrap();
    /// assert_eq!(decoded_u32, value2);
    /// decoder.finish().expect("trailing bytes");
    /// ```
    pub fn decode<T: Key<'a>>(&mut self) -> Result<T, CompositeKeyError> {
        let length = if let Some(length) = T::LENGTH {
            length
        } else {
            // Read a variable-encoded length from the tail of the bytes.
            let mut length = 0;
            let mut bytes_read = 0;
            let mut found_end = false;
            while let Some(&tail) = self.bytes.last() {
                length |= usize::from(tail & 0x7F) << (bytes_read * 7);
                bytes_read += 1;
                self.bytes = &self.bytes[..self.bytes.len() - 1];
                if tail & 0x80 == 0 {
                    found_end = true;
                    break;
                }
            }
            if !found_end {
                return Err(CompositeKeyError::new(std::io::Error::from(
                    ErrorKind::UnexpectedEof,
                )));
            }
            length
        };
        let end = self.offset + length;
        if end <= self.bytes.len() {
            let decoded =
                T::from_ord_bytes(&self.bytes[self.offset..end]).map_err(CompositeKeyError::new)?;
            self.offset = end;
            if T::LENGTH.is_none() {
                // Variable fields always have an extra null byte to delimit the data
                self.offset += 1;
            }
            Ok(decoded)
        } else {
            Err(CompositeKeyError::new(std::io::Error::from(
                ErrorKind::UnexpectedEof,
            )))
        }
    }

    /// Verifies the underlying byte slice has been fully consumed.
    pub fn finish(self) -> Result<(), CompositeKeyError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CompositeKeyError::new(std::io::Error::from(
                ErrorKind::InvalidData,
            )))
        }
    }
}

#[test]
fn composite_key_null_check_test() {
    let mut encoder = CompositeKeyEncoder::default();
    encoder.encode(&vec![0_u8]).unwrap_err();

    let mut encoder = CompositeKeyEncoder::default();
    encoder.allow_null_bytes_in_variable_fields();
    encoder.encode(&vec![0_u8]).unwrap();
    let bytes = encoder.finish();
    let mut decoder = CompositeKeyDecoder::new(&bytes);
    let decoded_value = decoder.decode::<Cow<'_, [u8]>>().unwrap();
    assert_eq!(decoded_value.as_ref(), &[0]);
}

#[test]
#[allow(clippy::cognitive_complexity)] // There's no way to please clippy with this
fn composite_key_tests() {
    /// This test generates various combinations of every tuple type supported.
    /// Each test calls this function, which builds a second Vec containing all
    /// of the encoded key values. Next, both the original vec and the encoded
    /// vec are sorted. The encoded entries are decoded, and the sorted original
    /// vec is compared against the decoded vec to ensure the ordering is the
    /// same.
    ///
    /// The macros just make it less verbose to create the nested loops which
    /// create the tuples.
    fn verify_key_ordering<T: for<'a> Key<'a> + Ord + Eq + std::fmt::Debug>(
        mut cases: Vec<T>,
        already_ordered: bool,
    ) {
        let mut encoded = {
            cases
                .iter()
                .map(|tuple| tuple.as_ord_bytes().unwrap().to_vec())
                .collect::<Vec<Vec<u8>>>()
        };
        if !already_ordered {
            cases.sort();
        }
        encoded.sort();
        println!("Tested {} entries", cases.len());
        let decoded = encoded
            .iter()
            .map(|encoded| T::from_ord_bytes(encoded).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(cases, decoded);
    }

    // Simple mixed-length tests
    let a2 = (String::from("a"), 2_u8);
    let aa1 = (String::from("aa"), 1_u8);
    let aa2 = (String::from("aa"), 2_u8);
    let b1 = (String::from("b"), 1_u8);
    let b2 = (String::from("b"), 2_u8);
    verify_key_ordering(vec![a2.clone(), aa1.clone()], true);
    verify_key_ordering(vec![a2, aa1, aa2, b1, b2], true);

    // Two byte length (0x80)
    verify_key_ordering(
        vec![(vec![1; 127], vec![2; 128]), (vec![1; 128], vec![2; 127])],
        true,
    );
    // Three byte length (0x80)
    verify_key_ordering(
        vec![
            (vec![1; 16383], vec![1; 16384]),
            (vec![1; 16384], vec![2; 16383]),
        ],
        true,
    );

    let values = [0_u16, 0xFF00, 0x0FF0, 0xFF];
    macro_rules! test_enum_variations {
        ($($ident:ident),+) => {
            let mut cases = Vec::new();
            test_enum_variations!(for cases $($ident),+; $($ident),+);
            verify_key_ordering(cases, false);
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

impl<'a> Key<'a> for isize {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes)
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(0)
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.checked_add(1).ok_or(NextValueError::WouldWrap)
    }
}

impl<'a> KeyEncoding<'a, Self> for isize {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&self) -> Result<Cow<'a, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

impl<'a> Key<'a> for usize {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes)
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(0)
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.checked_add(1).ok_or(NextValueError::WouldWrap)
    }
}

impl<'a> KeyEncoding<'a, Self> for usize {
    type Error = std::io::Error;

    const LENGTH: Option<Self> = None;

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

impl<'a, T, K> KeyEncoding<'a, Option<K>> for Option<T>
where
    T: KeyEncoding<'a, K>,
    K: Key<'a>,
{
    type Error = T::Error;

    const LENGTH: Option<usize> = match T::LENGTH {
        Some(length) => Some(1 + length),
        None => None,
    };

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

const RESULT_OK: u8 = 0;
const RESULT_ERR: u8 = 1;

impl<'a, T, E> Key<'a> for Result<T, E>
where
    T: Key<'a>,
    E: Key<'a, Error = <T as KeyEncoding<'a, T>>::Error>,
    Self: KeyEncoding<'a, Self, Error = <T as KeyEncoding<'a, T>>::Error>,
{
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        match bytes.get(0) {
            Some(&RESULT_OK) => T::from_ord_bytes(&bytes[1..]).map(Ok),
            Some(_) => E::from_ord_bytes(&bytes[1..]).map(Err),
            None => {
                // Empty buffer, but we don't have an error type.
                E::from_ord_bytes(bytes).map(Err)
            }
        }
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Ok(T::first_value()?))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        match self {
            Ok(value) => value.next_value().map(Ok),
            Err(err) => err.next_value().map(Err),
        }
    }
}

impl<'a, T, E, TBorrowed, EBorrowed> KeyEncoding<'a, Result<T, E>> for Result<TBorrowed, EBorrowed>
where
    TBorrowed: KeyEncoding<'a, T>,
    T: Key<'a, Error = TBorrowed::Error>,
    EBorrowed: KeyEncoding<'a, E, Error = TBorrowed::Error>,
    E: Key<'a, Error = TBorrowed::Error>,
{
    type Error = <TBorrowed as KeyEncoding<'a, T>>::Error;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        let (header, contents) = match self {
            Ok(value) => (RESULT_OK, value.as_ord_bytes()?),
            Err(value) => (RESULT_ERR, value.as_ord_bytes()?),
        };
        let mut contents = contents.to_vec();
        contents.insert(0, header);
        Ok(Cow::Owned(contents))
    }
}

#[test]
fn result_key_tests() {
    let ok_0 = Result::<u8, u16>::first_value().unwrap();
    let ok_1 = ok_0.next_value().unwrap();
    assert_eq!(ok_1, Ok(1));
    let ok_2 = Result::<u8, u16>::Ok(2_u8);
    let err_1 = Result::<u8, u16>::Err(1_u16);
    let err_2 = err_1.next_value().unwrap();
    assert_eq!(err_2, Err(2));
    let ok_1_encoded = ok_1.as_ord_bytes().unwrap();
    let ok_2_encoded = ok_2.as_ord_bytes().unwrap();
    let err_1_encoded = err_1.as_ord_bytes().unwrap();
    let err_2_encoded = err_2.as_ord_bytes().unwrap();
    assert!(ok_1_encoded < ok_2_encoded);
    assert!(ok_2_encoded < err_1_encoded);
    assert!(err_1_encoded < err_2_encoded);
    assert_eq!(Result::from_ord_bytes(&ok_1_encoded).unwrap(), ok_1);
    assert_eq!(Result::from_ord_bytes(&ok_2_encoded).unwrap(), ok_2);
    assert_eq!(Result::from_ord_bytes(&err_1_encoded).unwrap(), err_1);
    assert_eq!(Result::from_ord_bytes(&err_2_encoded).unwrap(), err_2);
}

/// Adds `Key` support to an enum. Requires implementing
/// [`ToPrimitive`](num_traits::ToPrimitive) and
/// [`FromPrimitive`](num_traits::FromPrimitive), or using a crate like
/// [num-derive](https://crates.io/crates/num-derive) to do it automatically.
/// Take care when using enums as keys: if the order changes or if the meaning
/// of existing numerical values changes, make sure to update any related views'
/// version number to ensure the values are re-evaluated.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct EnumKey<T>(pub T)
where
    T: ToPrimitive + FromPrimitive + Clone + Eq + Ord + std::fmt::Debug + Send + Sync;

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
impl<'a, T> Key<'a> for EnumKey<T>
where
    T: ToPrimitive + FromPrimitive + Clone + Eq + Ord + std::fmt::Debug + Send + Sync,
{
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let primitive = u64::decode_variable(bytes)?;
        T::from_u64(primitive)
            .map(Self)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, UnknownEnumVariant))
    }
}

impl<'a, T> KeyEncoding<'a, Self> for EnumKey<T>
where
    T: ToPrimitive + FromPrimitive + Clone + Eq + Ord + std::fmt::Debug + Send + Sync,
{
    type Error = std::io::Error;
    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        let integer = self
            .0
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

    assert_eq!(
        usize::from_ord_bytes(&usize::MAX.as_ord_bytes().unwrap()).unwrap(),
        usize::MAX
    );
    assert_eq!(
        usize::from_ord_bytes(&usize::MIN.as_ord_bytes().unwrap()).unwrap(),
        usize::MIN
    );
    assert_eq!(
        isize::from_ord_bytes(&isize::MAX.as_ord_bytes().unwrap()).unwrap(),
        isize::MAX
    );
    assert_eq!(
        isize::from_ord_bytes(&isize::MIN.as_ord_bytes().unwrap()).unwrap(),
        isize::MIN
    );

    Ok(())
}

#[test]
fn optional_key_encoding_tests() -> anyhow::Result<()> {
    let some_string = (&Some("hello")).as_ord_bytes()?;
    let empty_string = (&Some("")).as_ord_bytes()?;
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
        let some_string = OptionKeyV1(Some("hello")).as_ord_bytes()?.to_vec();
        let none_string = OptionKeyV1::<String>(None).as_ord_bytes()?;
        assert_eq!(
            OptionKeyV1::<String>::from_ord_bytes(&some_string)
                .unwrap()
                .0
                .as_deref(),
            Some("hello")
        );
        assert_eq!(
            OptionKeyV1::<String>::from_ord_bytes(&none_string)
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
fn enum_derive_tests() -> anyhow::Result<()> {
    #[derive(
        Debug,
        Clone,
        num_derive::ToPrimitive,
        num_derive::FromPrimitive,
        Ord,
        PartialOrd,
        Eq,
        PartialEq,
    )]
    enum SomeEnum {
        One = 1,
        NineNineNine = 999,
    }

    let encoded = EnumKey(SomeEnum::One).as_ord_bytes()?;
    let value = EnumKey::<SomeEnum>::from_ord_bytes(&encoded)?;
    assert!(matches!(value.0, SomeEnum::One));

    let encoded = EnumKey(SomeEnum::NineNineNine).as_ord_bytes()?;
    let value = EnumKey::<SomeEnum>::from_ord_bytes(&encoded)?;
    assert!(matches!(value.0, SomeEnum::NineNineNine));

    Ok(())
}
