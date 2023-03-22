/// [`Key`] implementations for time types.
pub mod time;
mod varint;

mod deprecated;

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::ErrorKind;
use std::num::TryFromIntError;
use std::ops::Deref;
use std::string::FromUtf8Error;

use arc_bytes::serde::{Bytes, CowBytes};
use arc_bytes::ArcBytes;
pub use bonsaidb_macros::Key;
pub use deprecated::*;
use num_traits::{FromPrimitive, ToPrimitive};
use ordered_varint::{Signed, Unsigned, Variable};
use serde::{Deserialize, Serialize};
pub use varint::{VarInt, VariableInteger};

use crate::connection::{Bound, BoundRef, MaybeOwned, RangeRef};
use crate::AnyError;

/// A trait that enables a type to convert itself into a `memcmp`-compatible
/// sequence of bytes.
pub trait KeyEncoding<'k, K = Self>: Send + Sync
where
    K: Key<'k>,
{
    /// The error type that can be produced by either serialization or
    /// deserialization.
    type Error: AnyError;

    /// The size of the key, if constant. If this type doesn't produce the same
    /// number of bytes for each value, this should be `None`.
    const LENGTH: Option<usize>;

    /// Describes this type by invoking functions on `visitor` describing the
    /// key being encoded.
    ///
    /// See the [`KeyVisitor`] trait for more information.
    ///
    /// [`KeyDescription::for_key()`]/[`KeyDescription::for_encoding()`] are
    /// built-in functions
    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor;

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
/// ## Changing the `enum` representation type
///
/// By default, the derived `Key` implementation will use an `isize` for its
/// representation, which is encoded using [`ordered_varint`]. If you wish to
/// use a fixed-size encoding or use `usize`, `enum_repr` can be used to control
/// the type being encoded.
///
/// The default behavior produces compact output for simple enums, but can also
/// support growing to the limits of `isize`:
///
/// ```rust
/// use bonsaidb_core::key::{Key, KeyEncoding};
///
/// #[derive(Key, Clone, Debug)]
/// # #[key(core = bonsaidb_core)]
/// enum Color {
///     Red,
///     Green,
///     Blue,
/// }
///
/// let encoded = Color::Red.as_ord_bytes().unwrap();
/// assert_eq!(encoded.len(), 1);
/// ```
///
/// If a `#[repr(...)]` attribute exists and its parameter is a built-in integer
/// type, the `Key` derive will use that type for its representation instead:
///
/// ```rust
/// use bonsaidb_core::key::{Key, KeyEncoding};
///
/// #[derive(Key, Clone, Debug)]
/// # #[key(core = bonsaidb_core)]
/// #[repr(u32)]
/// enum Color {
///     Red = 0xFF0000FF,
///     Green = 0x00FF00FF,
///     Blue = 0x0000FFFF,
/// }
///
/// let encoded = Color::Red.as_ord_bytes().unwrap();
/// assert_eq!(encoded.len(), 4);
/// ```
///
/// If the type would rather use a different type for the key encoding than it
/// uses for in-memory representation, the `enum_repr` parameter can be used:
///
/// ```rust
/// use bonsaidb_core::key::{Key, KeyEncoding};
///
/// #[derive(Key, Clone, Debug)]
/// # #[key(core = bonsaidb_core)]
/// #[key(enum_repr = u32)]
/// #[repr(usize)]
/// enum Color {
///     Red = 0xFF0000FF,
///     Green = 0x00FF00FF,
///     Blue = 0x0000FFFF,
/// }
///
/// let encoded = Color::Red.as_ord_bytes().unwrap();
/// assert_eq!(encoded.len(), 4);
/// ```
///
/// ## `null_handling`
///
/// The derive macro offers an argument `null_handling`, which defaults to
/// `escape`. Escaping null bytes in variable length fields ensures all data
/// encoded will sort correctly. In situations where speed is more important
/// than sorting behavior, `allow` or `deny` can be specified. `allow` will
/// permit null bytes to pass through encoding and decoding without being
/// checked. `deny` will check for null bytes when encoding and return an error
/// if any are present.
///
/// Null bytes can cause problematic sort behavior when multiple variable-length
/// encoded fields are encoded as a composite key. Consider this example:
///
/// ```rust
/// use bonsaidb_core::key::Key;
///
/// #[derive(Key, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
/// #[key(null_handling = allow)]
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
/// match.
///
/// This null-byte edge case only applies to variable length [`Key`]s
/// ([`KeyEncoding::LENGTH`] is `None`).
pub trait Key<'k>: KeyEncoding<'k, Self> + Clone + Send + Sync {
    /// If true, this type can benefit from an owned `Vec<u8>`. This flag is
    /// used as a hint of whether to attempt to do memcpy operations in some
    /// decoding operations to avoid extra allocations.
    const CAN_OWN_BYTES: bool;

    /// Deserialize a sequence of bytes previously encoded with
    /// [`KeyEncoding::as_ord_bytes`].
    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error>;

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

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        KE::describe(visitor);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        (*self).as_ord_bytes()
    }
}

/// A Visitor for information about a [`KeyEncoding`].
///
/// This trait is used in [`KeyEncoding::describe`] to report information about
/// the type of data contained within the key. Using this information,
/// generically interpreting the bytes of a key type can be possible as long as
/// the key type uses [`CompositeKeyEncoder`].
///
/// This trait is not something that most users will ever need to implement.
/// Instead,
/// [`KeyDescription::for_key()`]/[`KeyDescription::for_encoding()`] are
/// built-in functions to retrieve the information reported by this trait in an
/// easier-to-use interface.
pub trait KeyVisitor {
    /// Report that a basic key type is encoded next in this key.
    fn visit_type(&mut self, kind: KeyKind);

    /// Report that a composite type made up of `count` fields is encoded next in
    /// this key.
    ///
    /// `KeyVisitor` implementations may panic if there are less than or more
    /// than `count` fields reported by either `visit_type()` or
    /// `visit_composite()`.
    fn visit_composite(&mut self, kind: CompositeKind, count: usize);

    /// Report that the current composite type has extra metadata.
    ///
    /// This can be used to encode const generic parameters that are helpful in
    /// interpretting the contents of a type. For example,
    /// [`LimitedResolutionTimestamp`](time::limited::LimitedResolutionTimestamp)
    /// uses this function to report the `TimeEpoch`'s nanoseconds since the
    /// Unix epoch.
    fn visit_composite_attribute(
        &mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<KeyAttibuteValue>,
    );
}

/// The type of a single field contained in a key.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum KeyKind {
    /// An `unit` type, encoded with no length.
    Unit,
    /// An `u8` encoded in big-endian encoding.
    U8,
    /// An `u16` encoded in big-endian encoding.
    U16,
    /// An `u32` encoded in big-endian encoding.
    U32,
    /// An `u64` encoded in big-endian encoding.
    U64,
    /// An `u128` encoded in big-endian encoding.
    U128,
    /// An `usize` encoded in big-endian encoding.
    Usize,
    /// An `i8` encoded in big-endian encoding.
    I8,
    /// An `i16` encoded in big-endian encoding.
    I16,
    /// An `i32` encoded in big-endian encoding.
    I32,
    /// An `i64` encoded in big-endian encoding.
    I64,
    /// An `i128` encoded in big-endian encoding.
    I128,
    /// An `isize` encoded in big-endian encoding.
    Isize,
    /// A [`Signed`] number encoded using [`ordered_varint`].
    Signed,
    /// An [`Unsigned`] number encoded using [`ordered_varint`].
    Unsigned,
    /// A `bool` encoded as a single byte.
    Bool,
    /// A String encoded using BonsaiDb's built-in `KeyEncoding`.
    String,
    /// A byte array encoded using BonsaiDb's built-in `KeyEncoding`.
    Bytes,
}

/// A value used as part of [`KeyVisitor::visit_composite_attribute`].
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum KeyAttibuteValue {
    /// An `u8` value.
    U8(u8),
    /// An `i8` value.
    I8(i8),
    /// An `u16` value.
    U16(u16),
    /// An `i16` value.
    I16(i16),
    /// An `u32` value.
    U32(u32),
    /// An `i32` value.
    I32(i32),
    /// An `u64` value.
    U64(u64),
    /// An `i64` value.
    I64(i64),
    /// An `u128` value.
    U128(u128),
    /// An `i128` value.
    I128(i128),
    /// An `usize` value.
    Usize(usize),
    /// An `isize` value.
    Isize(isize),
    /// A `bool` value.
    Bool(bool),
    /// A string value.
    Str(Cow<'static, str>),
    /// A byte array value.
    Bytes(Cow<'static, [u8]>),
}

macro_rules! impl_const_key_from {
    ($from:ty, $constkey:expr) => {
        impl From<$from> for KeyAttibuteValue {
            fn from(value: $from) -> Self {
                $constkey(value)
            }
        }
    };
}

impl_const_key_from!(u8, KeyAttibuteValue::U8);
impl_const_key_from!(i8, KeyAttibuteValue::I8);
impl_const_key_from!(u16, KeyAttibuteValue::U16);
impl_const_key_from!(i16, KeyAttibuteValue::I16);
impl_const_key_from!(u32, KeyAttibuteValue::U32);
impl_const_key_from!(i32, KeyAttibuteValue::I32);
impl_const_key_from!(u64, KeyAttibuteValue::U64);
impl_const_key_from!(i64, KeyAttibuteValue::I64);
impl_const_key_from!(u128, KeyAttibuteValue::U128);
impl_const_key_from!(i128, KeyAttibuteValue::I128);
impl_const_key_from!(usize, KeyAttibuteValue::Usize);
impl_const_key_from!(isize, KeyAttibuteValue::Isize);
impl_const_key_from!(bool, KeyAttibuteValue::Bool);
impl_const_key_from!(&'static str, |s: &'static str| KeyAttibuteValue::Str(
    Cow::Borrowed(s)
));
impl_const_key_from!(String, |s: String| KeyAttibuteValue::Str(Cow::Owned(s)));
impl_const_key_from!(&'static [u8], |b: &'static [u8]| KeyAttibuteValue::Bytes(
    Cow::Borrowed(b)
));
impl_const_key_from!(Vec<u8>, |b: Vec<u8>| KeyAttibuteValue::Bytes(Cow::Owned(b)));

/// A description of the kind of a composite key.
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum CompositeKind {
    /// An [`Option`], which always contains a single field.
    Option,
    /// An [`Result`], which reports two fields -- the first for the `Ok` type,
    /// and the second for the `Err` type.
    Result,
    /// A sequence of fields.
    Tuple,
    /// A sequence of fields, identified by the fully qualified name. E.g.,
    /// `"std::time::SystemTime"` is the value that the
    /// [`SystemTime`](std::time::SystemTime)'s `KeyEncoding` implementation
    /// reports.
    Struct(Cow<'static, str>),
}

/// A description of an encoded [`Key`].
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub enum KeyDescription {
    /// A basic key type.
    Basic(KeyKind),
    /// A composite key made up of more than one field.
    Composite(CompositeKeyDescription),
    /// Another type with a custom encoding format.
    Other(Cow<'static, str>),
}

impl KeyDescription {
    /// Returns a description of the given [`KeyEncoding`] implementor.
    #[must_use]
    pub fn for_encoding<KE: for<'k> KeyEncoding<'k, K>, K: for<'k> Key<'k>>() -> Self {
        let mut describer = KeyDescriber::default();
        KE::describe(&mut describer);
        describer
            .result
            .expect("invalid KeyEncoding::describe implementation -- imbalanced visit calls")
    }

    /// Returns the description of a given [`Key`] implementor.
    #[must_use]
    pub fn for_key<K: for<'k> Key<'k>>() -> Self {
        Self::for_encoding::<K, K>()
    }
}

/// A description of a multi-field key encoded using [`CompositeKeyEncoder`].
#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct CompositeKeyDescription {
    /// The kind of composite key.
    pub kind: CompositeKind,
    /// The fields contained within this key.
    pub fields: Vec<KeyDescription>,
    /// The attributes of this key.
    pub attributes: HashMap<Cow<'static, str>, KeyAttibuteValue>,
}

#[derive(Default)]
struct KeyDescriber {
    stack: Vec<CompositeKeyDescription>,
    result: Option<KeyDescription>,
}

impl KeyDescriber {
    fn record(&mut self, description: KeyDescription) {
        match self.stack.last_mut() {
            Some(composite) => {
                composite.fields.push(description);
                if composite.fields.len() == composite.fields.capacity() {
                    // The composite key is finished, pop the state.
                    let completed = self.stack.pop().expect("just matched");
                    self.record(KeyDescription::Composite(completed));
                }
            }
            None => {
                // Stack has been completed.
                assert!(self.result.replace(description).is_none());
            }
        }
    }
}

impl KeyVisitor for KeyDescriber {
    fn visit_type(&mut self, kind: KeyKind) {
        let description = KeyDescription::Basic(kind);
        self.record(description);
    }

    fn visit_composite(&mut self, kind: CompositeKind, count: usize) {
        self.stack.push(CompositeKeyDescription {
            kind,
            fields: Vec::with_capacity(count),
            attributes: HashMap::new(),
        });
    }

    fn visit_composite_attribute(
        &mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<KeyAttibuteValue>,
    ) {
        let current = self
            .stack
            .last_mut()
            .expect("visit_composite_attribute must be called only after visit_composite");
        current.attributes.insert(key.into(), value.into());
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

/// A source of bytes from a `&'borrowed [u8]`, `&'ephemeral [u8]` or `Vec<u8>`.
///
/// This is used by the [`Key`] trait to allow `'ephemeral` borrows of an owned
/// `Vec<u8>` while also allowing for code paths that support borrowing from a
/// `'borrowed` byte slice.
pub enum ByteSource<'borrowed, 'ephemeral> {
    /// Borrowed bytes valid for `'borrowed`.
    Borrowed(&'borrowed [u8]),

    /// Borrowed bytes valid for `'ephemeral`.
    Ephemeral(&'ephemeral [u8]),

    /// Owned bytes.
    Owned(Vec<u8>),
}

impl<'borrowed, 'ephemeral> ByteSource<'borrowed, 'ephemeral>
where
    'borrowed: 'ephemeral,
{
    /// Coerce a `ByteSource` into `Cow<'borrowed, [u8]>`
    ///
    /// This will allocate if this is an `'ephemeral` reference.
    #[must_use]
    pub fn into_borrowed(self) -> Cow<'borrowed, [u8]> {
        match self {
            Self::Borrowed(bytes) => Cow::Borrowed(bytes),
            Self::Ephemeral(bytes) => Cow::Owned(Vec::from(bytes)),
            Self::Owned(bytes) => Cow::Owned(bytes),
        }
    }

    /// Coerce a `ByteSource` into a `Vec<u8>`
    ///
    /// This will allocate if the source is `'borrowed` or `'ephemeral`.
    #[must_use]
    #[allow(clippy::match_same_arms)] // Lifetimes are different.
    pub fn into_owned(self) -> Vec<u8> {
        match self {
            Self::Borrowed(bytes) => Vec::from(bytes),
            Self::Ephemeral(bytes) => Vec::from(bytes),
            Self::Owned(bytes) => bytes,
        }
    }
}

impl<'borrowed, 'ephemeral> AsRef<[u8]> for ByteSource<'borrowed, 'ephemeral> {
    #[allow(clippy::match_same_arms)] // Lifetimes are different.
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Ephemeral(bytes) => bytes,
            Self::Owned(ref bytes) => bytes.as_slice(),
        }
    }
}

impl<'borrowed, 'ephemeral> Deref for ByteSource<'borrowed, 'ephemeral> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'borrowed, 'ephemeral> Default for ByteSource<'borrowed, 'ephemeral> {
    fn default() -> Self {
        Self::Owned(Vec::default())
    }
}

impl<'borrowed, 'ephemeral> From<Cow<'borrowed, [u8]>> for ByteSource<'borrowed, 'ephemeral> {
    fn from(cow: Cow<'borrowed, [u8]>) -> Self {
        match cow {
            Cow::Borrowed(bytes) => ByteSource::Borrowed(bytes),
            Cow::Owned(bytes) => ByteSource::Owned(bytes),
        }
    }
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
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Ok(bytes.into_borrowed())
    }
}

impl<'k> KeyEncoding<'k, Self> for Cow<'k, [u8]> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(self.clone())
    }
}

macro_rules! impl_u8_slice_key_encoding {
    ($type:ty) => {
        impl<'a, 'k> KeyEncoding<'k, $type> for &'a [u8] {
            type Error = Infallible;

            const LENGTH: Option<usize> = None;

            fn describe<Visitor>(visitor: &mut Visitor)
            where
                Visitor: KeyVisitor,
            {
                visitor.visit_type(KeyKind::Bytes)
            }

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
    const CAN_OWN_BYTES: bool = TOwned::CAN_OWN_BYTES;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
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

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        TBorrowed::describe(visitor);
    }

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

impl<'k> Key<'k> for Vec<u8> {
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Ok(bytes.into_owned())
    }
}

impl<'k> KeyEncoding<'k, Self> for Vec<u8> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self))
    }
}

impl_u8_slice_key_encoding!(Vec<u8>);

impl<'k> IntoPrefixRange<'k, Self> for Vec<u8> {
    fn to_prefix_range(&'k self) -> RangeRef<'k, Self> {
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

impl<'k, const N: usize> Key<'k> for [u8; N] {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        if bytes.as_ref().len() == N {
            let mut array = [0; N];
            array.copy_from_slice(bytes.as_ref());
            Ok(array)
        } else {
            Err(IncorrectByteLength)
        }
    }
}

impl<'k, const N: usize> KeyEncoding<'k, Self> for [u8; N] {
    type Error = IncorrectByteLength;

    const LENGTH: Option<usize> = Some(N);

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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

impl<'k> Key<'k> for ArcBytes<'k> {
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        Ok(Self::from(bytes.into_borrowed()))
    }
}

impl<'k> KeyEncoding<'k, Self> for ArcBytes<'k> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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

impl<'k> Key<'k> for CowBytes<'k> {
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        Ok(Self(bytes.into_borrowed()))
    }
}

impl<'k> KeyEncoding<'k, Self> for CowBytes<'k> {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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

impl<'k> Key<'k> for Bytes {
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        Ok(Self(bytes.into_owned()))
    }
}

impl<'k> KeyEncoding<'k, Self> for Bytes {
    type Error = Infallible;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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

impl<'k> Key<'k> for String {
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        Self::from_utf8(bytes.into_owned())
    }
}

impl<'k> KeyEncoding<'k, Self> for String {
    type Error = FromUtf8Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::String);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

impl<'k> KeyEncoding<'k, String> for str {
    type Error = FromUtf8Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::String);
    }

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
    const CAN_OWN_BYTES: bool = true;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        match bytes.into_borrowed() {
            Cow::Borrowed(bytes) => std::str::from_utf8(bytes).map(Cow::Borrowed),
            Cow::Owned(bytes) => String::from_utf8(bytes)
                .map(Cow::Owned)
                .map_err(|e| e.utf8_error()),
        }
    }
}

impl<'k> KeyEncoding<'k, Self> for Cow<'k, str> {
    type Error = std::str::Utf8Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::String);
    }

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

impl<'k> Key<'k> for () {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'b>(_: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        Ok(())
    }
}

impl<'k> KeyEncoding<'k, Self> for () {
    type Error = Infallible;

    const LENGTH: Option<usize> = Some(0);

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Unit);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::default())
    }
}

impl<'k> Key<'k> for bool {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        let bytes = bytes.as_ref();
        if bytes.is_empty() || bytes[0] == 0 {
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

impl<'k> KeyEncoding<'k, Self> for bool {
    type Error = Infallible;

    const LENGTH: Option<usize> = Some(1);

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Bool);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        if *self {
            Ok(Cow::Borrowed(&[1_u8]))
        } else {
            Ok(Cow::Borrowed(&[0_u8]))
        }
    }
}

macro_rules! count_args {
    () => (0usize);
    ( $arg:tt $($remaining:tt)* ) => (1usize + count_args!($($remaining)*));
}

macro_rules! impl_key_for_tuple {
    ($(($index:tt, $varname:ident, $generic:ident)),+) => {
        impl<'k, $($generic),+> Key<'k> for ($($generic),+,)
        where
            $($generic: Key<'k>),+
        {
            const CAN_OWN_BYTES: bool = false;
            fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
                let mut decoder = CompositeKeyDecoder::default_for(bytes);
                $(let $varname = decoder.decode::<$generic>()?;)+
                decoder.finish()?;

                Ok(($($varname),+,))
            }
        }

        impl<'k, $($generic),+> KeyEncoding<'k, Self> for ($($generic),+,)
        where
            $($generic: Key<'k>),+
        {
            type Error = CompositeKeyError;

            const LENGTH: Option<usize> = match ($($generic::LENGTH),+,) {
                ($(Some($varname)),+,) => Some($($varname +)+ 0),
                _ => None,
            };

            fn describe<Visitor>(visitor: &mut Visitor)
            where
                Visitor: KeyVisitor,
            {
                visitor.visit_composite(CompositeKind::Tuple, count_args!($($generic) +));
                $($generic::describe(visitor);)+
            }

            fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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
pub struct CompositeKeyEncoder<NullHandling = EscapeNullBytes> {
    bytes: Vec<u8>,
    encoded_lengths: Vec<u16>,
    null_handling: NullHandling,
}

impl<NullHandling> CompositeKeyEncoder<NullHandling>
where
    NullHandling: CompositeKeyNullHandler,
{
    /// Encodes a single [`KeyEncoding`] implementing value.
    ///
    /// ```rust
    /// # use bonsaidb_core::key::{ByteSource, CompositeKeyEncoder, CompositeKeyDecoder};
    ///
    /// let value1 = String::from("hello");
    /// let value2 = 42_u32;
    /// let mut encoder = CompositeKeyEncoder::default();
    /// encoder.encode(&value1).unwrap();
    /// encoder.encode(&value2).unwrap();
    /// let encoded = encoder.finish();
    ///
    /// let mut decoder = CompositeKeyDecoder::default_for(ByteSource::Borrowed(&encoded));
    /// let decoded_string = decoder.decode::<String>().unwrap();
    /// assert_eq!(decoded_string, value1);
    /// let decoded_u32 = decoder.decode::<u32>().unwrap();
    /// assert_eq!(decoded_u32, value2);
    /// decoder.finish().expect("trailing bytes");
    /// ```
    pub fn encode<'k, K: Key<'k>, T: KeyEncoding<'k, K> + ?Sized>(
        &mut self,
        value: &'k T,
    ) -> Result<(), CompositeKeyError> {
        let mut encoded = T::as_ord_bytes(value).map_err(CompositeKeyError::new)?;
        if T::LENGTH.is_none() {
            self.null_handling.handle_nulls(&mut encoded)?;
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

impl Default for CompositeKeyEncoder<EscapeNullBytes> {
    fn default() -> Self {
        Self {
            bytes: Vec::new(),
            encoded_lengths: Vec::new(),
            null_handling: EscapeNullBytes,
        }
    }
}

impl CompositeKeyEncoder<AllowNullBytes> {
    /// Returns an encoder that allows null bytes in variable length fields.
    ///
    /// See [`CompositeKeyFieldContainsNullByte`] for information about the edge
    /// cases this may introduce.
    #[must_use]
    pub const fn allowing_null_bytes() -> Self {
        Self {
            bytes: Vec::new(),
            encoded_lengths: Vec::new(),
            null_handling: AllowNullBytes,
        }
    }
}

impl CompositeKeyEncoder<DenyNullBytes> {
    /// Returns an encoder that denies null bytes in variable length fields by
    /// returning an error when any null bytes are detected.
    #[must_use]
    pub const fn denying_null_bytes() -> Self {
        Self {
            bytes: Vec::new(),
            encoded_lengths: Vec::new(),
            null_handling: DenyNullBytes,
        }
    }
}

/// Escapes null bytes in variable length fields in composite keys. This option
/// ensures proper sort order is maintained even when null bytes are used within
/// vairable fields.
///
/// To see more information about the edge case encoding prevents, see
/// [`CompositeKeyFieldContainsNullByte`].
#[derive(Default, Debug, Clone, Copy)]
pub struct EscapeNullBytes;
/// Prevents checking for null bytes in variable length fields in composite
/// keys. [`Key`] types that have a fixed width have no edge cases with null
/// bytes. See [`CompositeKeyFieldContainsNullByte`] for an explanation and
/// example of the edge case introduced when allowing null bytes to be used
/// without escaping.
#[derive(Default, Debug, Clone, Copy)]
pub struct AllowNullBytes;
#[derive(Default, Debug, Clone, Copy)]
/// Checks for null bytes when encoding variable length fields in composite keys
/// and returns an error if any are encountered. This prevents extra processing
/// when encoding fields, but may introduce incorrect sort ordering (see
/// [`CompositeKeyFieldContainsNullByte`] for more).
pub struct DenyNullBytes;

/// A null-byte handling approach for [`CompositeKeyEncoder`] and
/// [`CompositeKeyDecoder`]. This type is only used when the fields being
/// encoded are variable length.
///
/// Ensuring proper sort order with composite keys requires special handling of
/// null bytes. The safest option is to use [`EscapeNullBytes`], but this option
/// will cause extra allocations when keys contain null bytes.
/// [`AllowNullBytes`] allows null-bytes through without any extra checks, but
/// their usage can cause incorrect sorting behavior. See
/// [`CompositeKeyFieldContainsNullByte`] for more information on this edge
/// case. The last implementation is [`DenyNullBytes`] which checks for null
/// bytes when encoding and returns an error if any are encountered. When
/// decoding with this option, there is no extra processing performed before
/// decoding individual fields.
pub trait CompositeKeyNullHandler {
    /// Process the null bytes in `field_bytes`, if needed.
    fn handle_nulls(&self, field_bytes: &mut Cow<'_, [u8]>) -> Result<(), CompositeKeyError>;
    /// Decode the null bytes in `encoded`, if needed.
    fn decode_nulls_if_needed<'b, 'e>(
        &self,
        encoded: ByteSource<'b, 'e>,
    ) -> Result<ByteSource<'b, 'e>, CompositeKeyError>;
}

impl CompositeKeyNullHandler for DenyNullBytes {
    #[inline]
    fn handle_nulls(&self, encoded: &mut Cow<'_, [u8]>) -> Result<(), CompositeKeyError> {
        if encoded.iter().any(|b| *b == 0) {
            Err(CompositeKeyError::new(std::io::Error::new(
                ErrorKind::InvalidData,
                CompositeKeyFieldContainsNullByte,
            )))
        } else {
            Ok(())
        }
    }

    #[inline]
    fn decode_nulls_if_needed<'b, 'e>(
        &self,
        encoded: ByteSource<'b, 'e>,
    ) -> Result<ByteSource<'b, 'e>, CompositeKeyError> {
        Ok(encoded)
    }
}

impl CompositeKeyNullHandler for AllowNullBytes {
    #[inline]
    fn handle_nulls(&self, _encoded: &mut Cow<'_, [u8]>) -> Result<(), CompositeKeyError> {
        Ok(())
    }

    #[inline]
    fn decode_nulls_if_needed<'b, 'e>(
        &self,
        encoded: ByteSource<'b, 'e>,
    ) -> Result<ByteSource<'b, 'e>, CompositeKeyError> {
        Ok(encoded)
    }
}

impl CompositeKeyNullHandler for EscapeNullBytes {
    #[inline]
    fn handle_nulls(&self, unescaped: &mut Cow<'_, [u8]>) -> Result<(), CompositeKeyError> {
        let null_bytes = bytecount::count(unescaped, 0);
        if null_bytes > 0 {
            let mut null_encoded = Vec::with_capacity(unescaped.len() + null_bytes);
            for unescaped in unescaped.as_ref() {
                if *unescaped == 0 {
                    null_encoded.extend_from_slice(&[0, 0]);
                } else {
                    null_encoded.push(*unescaped);
                }
            }
            *unescaped = Cow::Owned(null_encoded);
        }
        Ok(())
    }

    #[inline]
    fn decode_nulls_if_needed<'b, 'e>(
        &self,
        mut encoded: ByteSource<'b, 'e>,
    ) -> Result<ByteSource<'b, 'e>, CompositeKeyError> {
        // Find the first null byte
        if let Some(mut index) = encoded
            .iter()
            .enumerate()
            .find_map(|(index, b)| (*b == 0).then_some(index))
        {
            // The field has at least one null byte, we need to mutate the
            // bytes.
            let mut bytes = encoded.into_owned();
            loop {
                // Check that the next byte is a null. If so, we remove the
                // current byte and look for the next null.
                let next_index = index + 1;
                if Some(0) == bytes.get(next_index).copied() {
                    bytes.remove(index);
                    index = next_index;
                } else {
                    todo!("error: unescaped null byte")
                    // return Err(CompositeKeyError::new("unescaped null byte encountered"));
                }

                // Find the next null byte, if one exists.
                let Some(next_index) = bytes[index..]
                    .iter()
                    .enumerate()
                    .find_map(|(index, b)| (*b == 0).then_some(index))
                    else { break };
                index += next_index;
            }
            encoded = ByteSource::Owned(bytes);
        }
        Ok(encoded)
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
/// [`CompositeKeyEncoder::allowing_null_bytes()`] if you wish
/// to allow null bytes despite this edge case.
#[derive(thiserror::Error, Debug)]
#[error("a variable length field contained a null byte.")]
pub struct CompositeKeyFieldContainsNullByte;

/// Decodes multiple [`Key`] values from a byte slice previously encoded with
/// [`CompositeKeyEncoder`].
pub struct CompositeKeyDecoder<'key, 'ephemeral, NullHandling = EscapeNullBytes> {
    bytes: ByteSource<'key, 'ephemeral>,
    offset: usize,
    end: usize,
    null_handling: NullHandling,
}

impl<'key, 'ephemeral> CompositeKeyDecoder<'key, 'ephemeral, EscapeNullBytes> {
    /// Returns a decoder for `bytes` that decodes escaped null bytes.
    ///
    /// This function is compatible with keys encoded with
    /// [`CompositeKeyEncoder::default()`].
    #[must_use]
    pub fn default_for(bytes: ByteSource<'key, 'ephemeral>) -> Self {
        Self {
            end: bytes.as_ref().len(),
            bytes,
            offset: 0,
            null_handling: EscapeNullBytes,
        }
    }
}

impl<'key, 'ephemeral> CompositeKeyDecoder<'key, 'ephemeral, AllowNullBytes> {
    /// Returns a decoder for `bytes` that ignores null bytes.
    ///
    /// This function is compatible with keys encoded with
    /// [`CompositeKeyEncoder::allowing_null_bytes()`].
    #[must_use]
    pub fn allowing_null_bytes(bytes: ByteSource<'key, 'ephemeral>) -> Self {
        Self {
            end: bytes.as_ref().len(),
            bytes,
            offset: 0,
            null_handling: AllowNullBytes,
        }
    }
}

impl<'key, 'ephemeral> CompositeKeyDecoder<'key, 'ephemeral, DenyNullBytes> {
    /// Returns a decoder for `bytes` that ignores null bytes.
    ///
    /// This function is compatible with keys encoded with
    /// [`CompositeKeyEncoder::denying_null_bytes()`].
    #[must_use]
    pub fn denying_null_bytes(bytes: ByteSource<'key, 'ephemeral>) -> Self {
        Self {
            end: bytes.as_ref().len(),
            bytes,
            offset: 0,
            null_handling: DenyNullBytes,
        }
    }
}

impl<'key, 'ephemeral, NullHandling> CompositeKeyDecoder<'key, 'ephemeral, NullHandling>
where
    NullHandling: CompositeKeyNullHandler,
{
    /// Decodes a value previously encoded using [`CompositeKeyEncoder`]. Calls
    /// to decode must be made in the same order as the values were encoded in.
    ///
    /// ```rust
    /// # use bonsaidb_core::key::{ByteSource, CompositeKeyEncoder, CompositeKeyDecoder};
    ///
    /// let value1 = String::from("hello");
    /// let value2 = 42_u32;
    /// let mut encoder = CompositeKeyEncoder::default();
    /// encoder.encode(&value1).unwrap();
    /// encoder.encode(&value2).unwrap();
    /// let encoded = encoder.finish();
    ///
    /// let mut decoder = CompositeKeyDecoder::default_for(ByteSource::Borrowed(&encoded));
    /// let decoded_string = decoder.decode::<String>().unwrap();
    /// assert_eq!(decoded_string, value1);
    /// let decoded_u32 = decoder.decode::<u32>().unwrap();
    /// assert_eq!(decoded_u32, value2);
    /// decoder.finish().expect("trailing bytes");
    /// ```
    pub fn decode<T: Key<'key>>(&mut self) -> Result<T, CompositeKeyError> {
        let length = if let Some(length) = T::LENGTH {
            length
        } else {
            // Read a variable-encoded length from the tail of the bytes.
            let mut length = 0;
            let mut found_end = false;

            let bytes = self.bytes.as_ref();

            let iterator = bytes[self.offset..self.end]
                .iter()
                .copied()
                .rev()
                .enumerate();

            for (index, byte) in iterator {
                length |= usize::from(byte & 0x7f) << (index * 7);

                if byte & 0x80 == 0 {
                    self.end = self.end - index - 1;
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
        if end <= self.end {
            let mut bytes = match &self.bytes {
                ByteSource::Borrowed(bytes) => ByteSource::Borrowed(&bytes[self.offset..end]),
                ByteSource::Ephemeral(bytes) => ByteSource::Ephemeral(&bytes[self.offset..end]),
                ByteSource::Owned(bytes) => ByteSource::Ephemeral(&bytes[self.offset..end]),
            };
            if T::LENGTH.is_none() {
                bytes = self.null_handling.decode_nulls_if_needed(bytes)?;
            }
            let decoded = T::from_ord_bytes(bytes).map_err(CompositeKeyError::new)?;
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
        if self.offset == self.end {
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
    let mut encoder = CompositeKeyEncoder::denying_null_bytes();
    encoder.encode(&vec![0_u8]).unwrap_err();

    let mut encoder = CompositeKeyEncoder::allowing_null_bytes();
    encoder.encode(&vec![0_u8]).unwrap();
    let bytes = encoder.finish();
    let mut decoder = CompositeKeyDecoder::allowing_null_bytes(ByteSource::Borrowed(&bytes));
    let decoded_value = decoder.decode::<Cow<'_, [u8]>>().unwrap();
    assert_eq!(decoded_value.as_ref(), &[0]);
    let mut decoder = CompositeKeyDecoder::denying_null_bytes(ByteSource::Borrowed(&bytes));
    let decoded_value = decoder.decode::<Cow<'_, [u8]>>().unwrap();
    assert_eq!(decoded_value.as_ref(), &[0]);

    let mut encoder = CompositeKeyEncoder::default();
    encoder.encode(&vec![1_u8, 0, 1]).unwrap();
    let bytes = encoder.finish();
    // One byte for the length, 3 original bytes, 1 escaped null, 1 field-delimiting null
    assert_eq!(bytes.len(), 6);
    let mut decoder = CompositeKeyDecoder::default_for(ByteSource::Borrowed(&bytes));
    let decoded_value = decoder.decode::<Cow<'_, [u8]>>().unwrap();
    assert_eq!(decoded_value.as_ref(), &[1, 0, 1]);
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
    fn verify_key_ordering<T: for<'k> Key<'k> + Ord + Eq + std::fmt::Debug>(
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
            .map(|encoded| T::from_ord_bytes(ByteSource::Borrowed(encoded)).unwrap())
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
    /// Returns a new instance wrapping `error`.
    pub fn new<E: AnyError>(error: E) -> Self {
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

impl<'k> Key<'k> for Signed {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes.as_ref())
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

impl<'k> KeyEncoding<'k, Self> for Signed {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Signed);
    }

    fn as_ord_bytes(&self) -> Result<Cow<'k, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

impl<'k> Key<'k> for Unsigned {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes.as_ref())
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

impl<'k> KeyEncoding<'k, Self> for Unsigned {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Unsigned);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

impl<'k> Key<'k> for isize {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes.as_ref())
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(0)
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.checked_add(1).ok_or(NextValueError::WouldWrap)
    }
}

impl<'k> KeyEncoding<'k, Self> for isize {
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Signed);
    }

    fn as_ord_bytes(&self) -> Result<Cow<'k, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

impl<'k> Key<'k> for usize {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Self::decode_variable(bytes.as_ref())
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(0)
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.checked_add(1).ok_or(NextValueError::WouldWrap)
    }
}

impl<'k> KeyEncoding<'k, Self> for usize {
    type Error = std::io::Error;

    const LENGTH: Option<Self> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Unsigned);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        self.to_variable_vec().map(Cow::Owned)
    }
}

#[cfg(feature = "uuid")]
impl<'k> Key<'k> for uuid::Uuid {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        Ok(Self::from_bytes(bytes.as_ref().try_into()?))
    }
}

#[cfg(feature = "uuid")]
impl<'k> KeyEncoding<'k, Self> for uuid::Uuid {
    type Error = std::array::TryFromSliceError;

    const LENGTH: Option<usize> = Some(16);

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_composite(CompositeKind::Struct(Cow::Borrowed("uuid::Uuid")), 1);
        visitor.visit_type(KeyKind::Bytes);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        Ok(Cow::Borrowed(self.as_bytes()))
    }
}

fn decode_skipping_first_byte<'k, 'e, T>(bytes: ByteSource<'k, 'e>) -> Result<T, T::Error>
where
    T: Key<'k>,
{
    match bytes {
        ByteSource::Borrowed(bytes) => T::from_ord_bytes(ByteSource::Borrowed(&bytes[1..])),
        ByteSource::Ephemeral(bytes) => T::from_ord_bytes(ByteSource::Ephemeral(&bytes[1..])),
        ByteSource::Owned(mut bytes) if T::CAN_OWN_BYTES => {
            bytes.remove(0);
            T::from_ord_bytes(ByteSource::Owned(bytes))
        }
        ByteSource::Owned(bytes) => T::from_ord_bytes(ByteSource::Ephemeral(&bytes[1..])),
    }
}

impl<'k, T> Key<'k> for Option<T>
where
    T: Key<'k>,
    Self: KeyEncoding<'k, Self, Error = <T as KeyEncoding<'k, T>>::Error>,
{
    const CAN_OWN_BYTES: bool = T::CAN_OWN_BYTES;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        if bytes.as_ref().is_empty() || bytes.as_ref()[0] == 0 {
            Ok(None)
        } else {
            decode_skipping_first_byte(bytes).map(Some)
        }
    }

    fn first_value() -> Result<Self, NextValueError> {
        Ok(Some(T::first_value()?))
    }

    fn next_value(&self) -> Result<Self, NextValueError> {
        self.as_ref().map(T::next_value).transpose()
    }
}

impl<'k, T, K> KeyEncoding<'k, Option<K>> for Option<T>
where
    T: KeyEncoding<'k, K>,
    K: Key<'k>,
{
    type Error = T::Error;

    const LENGTH: Option<usize> = match T::LENGTH {
        Some(length) => Some(1 + length),
        None => None,
    };

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_composite(CompositeKind::Option, 1);
        T::describe(visitor);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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

impl<'k, T, E> Key<'k> for Result<T, E>
where
    T: Key<'k>,
    E: Key<'k, Error = <T as KeyEncoding<'k, T>>::Error>,
    Self: KeyEncoding<'k, Self, Error = <T as KeyEncoding<'k, T>>::Error>,
{
    const CAN_OWN_BYTES: bool = T::CAN_OWN_BYTES || E::CAN_OWN_BYTES;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        match bytes.as_ref().first() {
            Some(&RESULT_OK) => decode_skipping_first_byte(bytes).map(Ok),
            Some(_) => decode_skipping_first_byte(bytes).map(Err),
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

impl<'k, T, E, TBorrowed, EBorrowed> KeyEncoding<'k, Result<T, E>> for Result<TBorrowed, EBorrowed>
where
    TBorrowed: KeyEncoding<'k, T>,
    T: Key<'k, Error = TBorrowed::Error>,
    EBorrowed: KeyEncoding<'k, E, Error = TBorrowed::Error>,
    E: Key<'k, Error = TBorrowed::Error>,
{
    type Error = <TBorrowed as KeyEncoding<'k, T>>::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_composite(CompositeKind::Result, 2);
        TBorrowed::describe(visitor);
        EBorrowed::describe(visitor);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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
    assert_eq!(
        Result::from_ord_bytes(ByteSource::Borrowed(&ok_1_encoded)).unwrap(),
        ok_1
    );
    assert_eq!(
        Result::from_ord_bytes(ByteSource::Borrowed(&ok_2_encoded)).unwrap(),
        ok_2
    );
    assert_eq!(
        Result::from_ord_bytes(ByteSource::Borrowed(&err_1_encoded)).unwrap(),
        err_1
    );
    assert_eq!(
        Result::from_ord_bytes(ByteSource::Borrowed(&err_2_encoded)).unwrap(),
        err_2
    );
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
impl<'k, T> Key<'k> for EnumKey<T>
where
    T: ToPrimitive + FromPrimitive + Clone + Eq + Ord + std::fmt::Debug + Send + Sync,
{
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'b>(bytes: ByteSource<'k, 'b>) -> Result<Self, Self::Error> {
        let primitive = u64::decode_variable(bytes.as_ref())?;
        T::from_u64(primitive)
            .map(Self)
            .ok_or_else(|| std::io::Error::new(ErrorKind::InvalidData, UnknownEnumVariant))
    }
}

impl<'k, T> KeyEncoding<'k, Self> for EnumKey<T>
where
    T: ToPrimitive + FromPrimitive + Clone + Eq + Ord + std::fmt::Debug + Send + Sync,
{
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: KeyVisitor,
    {
        visitor.visit_type(KeyKind::Unsigned);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
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
    ($type:ident, $keykind:expr) => {
        impl<'k> Key<'k> for $type {
            const CAN_OWN_BYTES: bool = false;

            fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
                Ok($type::from_be_bytes(bytes.as_ref().try_into()?))
            }

            fn first_value() -> Result<Self, NextValueError> {
                Ok(0)
            }

            fn next_value(&self) -> Result<Self, NextValueError> {
                self.checked_add(1).ok_or(NextValueError::WouldWrap)
            }
        }
        impl<'k> KeyEncoding<'k, Self> for $type {
            type Error = IncorrectByteLength;

            const LENGTH: Option<usize> = Some(std::mem::size_of::<$type>());

            fn describe<Visitor>(visitor: &mut Visitor)
            where
                Visitor: KeyVisitor,
            {
                visitor.visit_type($keykind);
            }

            fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
                Ok(Cow::from(self.to_be_bytes().to_vec()))
            }
        }
    };
}

impl_key_for_primitive!(i8, KeyKind::I8);
impl_key_for_primitive!(u8, KeyKind::U8);
impl_key_for_primitive!(i16, KeyKind::I16);
impl_key_for_primitive!(u16, KeyKind::U16);
impl_key_for_primitive!(i32, KeyKind::I32);
impl_key_for_primitive!(u32, KeyKind::U32);
impl_key_for_primitive!(i64, KeyKind::I64);
impl_key_for_primitive!(u64, KeyKind::U64);
impl_key_for_primitive!(i128, KeyKind::I128);
impl_key_for_primitive!(u128, KeyKind::U128);

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
                $type::from_ord_bytes(ByteSource::Borrowed(&$type::MAX.as_ord_bytes()?))?
            );
            assert_eq!(
                $type::MIN,
                $type::from_ord_bytes(ByteSource::Borrowed(&$type::MIN.as_ord_bytes()?))?
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
        usize::from_ord_bytes(ByteSource::Borrowed(&usize::MAX.as_ord_bytes().unwrap())).unwrap(),
        usize::MAX
    );
    assert_eq!(
        usize::from_ord_bytes(ByteSource::Borrowed(&usize::MIN.as_ord_bytes().unwrap())).unwrap(),
        usize::MIN
    );
    assert_eq!(
        isize::from_ord_bytes(ByteSource::Borrowed(&isize::MAX.as_ord_bytes().unwrap())).unwrap(),
        isize::MAX
    );
    assert_eq!(
        isize::from_ord_bytes(ByteSource::Borrowed(&isize::MIN.as_ord_bytes().unwrap())).unwrap(),
        isize::MIN
    );

    Ok(())
}

#[test]
fn optional_key_encoding_tests() -> anyhow::Result<()> {
    let some_string = Some("hello").as_ord_bytes()?;
    let empty_string = Some("").as_ord_bytes()?;
    let none_string = Option::<String>::None.as_ord_bytes()?;
    assert_eq!(
        Option::<String>::from_ord_bytes(ByteSource::Borrowed(&some_string))
            .unwrap()
            .as_deref(),
        Some("hello")
    );
    assert_eq!(
        Option::<String>::from_ord_bytes(ByteSource::Borrowed(&empty_string))
            .unwrap()
            .as_deref(),
        Some("")
    );
    assert_eq!(
        Option::<String>::from_ord_bytes(ByteSource::Borrowed(&none_string)).unwrap(),
        None
    );

    #[allow(deprecated)]
    {
        let some_string = OptionKeyV1(Some("hello")).as_ord_bytes()?.to_vec();
        let none_string = OptionKeyV1::<String>(None).as_ord_bytes()?;
        assert_eq!(
            OptionKeyV1::<String>::from_ord_bytes(ByteSource::Borrowed(&some_string))
                .unwrap()
                .0
                .as_deref(),
            Some("hello")
        );
        assert_eq!(
            OptionKeyV1::<String>::from_ord_bytes(ByteSource::Borrowed(&none_string))
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
    assert_eq!((), <() as Key>::from_ord_bytes(ByteSource::Borrowed(&[]))?);
    Ok(())
}

#[test]
#[allow(clippy::unit_cmp)] // this is more of a compilation test
fn bool_key_encoding_tests() -> anyhow::Result<()> {
    let true_as_bytes = true.as_ord_bytes()?;
    let false_as_bytes = false.as_ord_bytes()?;
    assert!(bool::from_ord_bytes(ByteSource::Borrowed(&true_as_bytes))?);
    assert!(!bool::from_ord_bytes(ByteSource::Borrowed(
        &false_as_bytes
    ))?);
    Ok(())
}

#[test]
fn vec_key_encoding_tests() -> anyhow::Result<()> {
    const ORIGINAL_VALUE: &[u8] = b"bonsaidb";
    let vec = Cow::<'_, [u8]>::from(ORIGINAL_VALUE);
    assert_eq!(
        vec.clone(),
        Cow::<'_, [u8]>::from_ord_bytes(ByteSource::Borrowed(&vec.as_ord_bytes()?))?
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
    let value = EnumKey::<SomeEnum>::from_ord_bytes(ByteSource::Borrowed(&encoded))?;
    assert!(matches!(value.0, SomeEnum::One));

    let encoded = EnumKey(SomeEnum::NineNineNine).as_ord_bytes()?;
    let value = EnumKey::<SomeEnum>::from_ord_bytes(ByteSource::Borrowed(&encoded))?;
    assert!(matches!(value.0, SomeEnum::NineNineNine));

    Ok(())
}

#[test]
fn key_descriptions() {
    use time::limited::TimeEpoch;
    assert_eq!(
        KeyDescription::for_key::<Vec<u8>>(),
        KeyDescription::Basic(KeyKind::Bytes)
    );
    assert_eq!(
        dbg!(KeyDescription::for_key::<time::TimestampAsNanoseconds>()),
        KeyDescription::Composite(CompositeKeyDescription {
            kind: CompositeKind::Struct(Cow::Borrowed(
                "bonsaidb::core::key::time::LimitedResolutionTimestamp"
            )),
            fields: vec![KeyDescription::Basic(KeyKind::I64),],
            attributes: [(
                Cow::Borrowed("epoch"),
                KeyAttibuteValue::U128(time::limited::BonsaiEpoch::epoch_offset().as_nanos())
            )]
            .into_iter()
            .collect()
        })
    );
    assert_eq!(
        KeyDescription::for_key::<(u64, String, Bytes)>(),
        KeyDescription::Composite(CompositeKeyDescription {
            kind: CompositeKind::Tuple,
            fields: vec![
                KeyDescription::Basic(KeyKind::U64),
                KeyDescription::Basic(KeyKind::String),
                KeyDescription::Basic(KeyKind::Bytes),
            ],
            attributes: HashMap::new(),
        })
    );
}
