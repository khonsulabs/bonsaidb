use std::borrow::Cow;
use std::fmt::Display;
use std::ops::{Deref, DerefMut};

use ordered_varint::Variable;
use serde::{Deserialize, Serialize};

use super::ByteSource;
use crate::key::{Key, KeyEncoding, KeyKind};

/// A wrapper type for Rust's built-in integer types that encodes with variable
/// length and implements the [`Key`] trait.
///
/// This type supports all of Rust's built-in integer types:
///
/// - `u8`
/// - `u16`
/// - `u32`
/// - `u64`
/// - `u128`
/// - `usize`
/// - `i8`
/// - `i16`
/// - `i32`
/// - `i64`
/// - `i128`
/// - `isize`
///
/// ```rust
/// use bonsaidb_core::key::{Key, KeyEncoding, VarInt};
///
/// #[derive(Key, Default, Clone)]
/// # #[key(core = bonsaidb_core)]
/// #[key(allow_null_bytes = true)]
/// struct CustomKeyVariable {
///     pub customer_id: VarInt<u64>,
///     pub order_id: VarInt<u64>,
/// }
///
/// #[derive(Key, Default, Clone)]
/// # #[key(core = bonsaidb_core)]
/// struct CustomKey {
///     pub customer_id: u64,
///     pub order_id: u64,
/// }
///
/// // `CustomKey` type will always encode to 16 bytes, since u64 will encode
/// // using `u64::to_be_bytes`.
/// let default_key_len = CustomKey::default().as_ord_bytes().unwrap().len();
/// assert_eq!(default_key_len, 16);
/// let another_key_len = CustomKey {
///     customer_id: u64::MAX,
///     order_id: u64::MAX,
/// }
/// .as_ord_bytes()
/// .unwrap()
/// .len();
/// assert_eq!(another_key_len, 16);
///
/// // However, `CustomKeyVariable` will be able to encode in as few as 6 bytes,
/// // but can take up to 22 bytes if the entire u64 range is utilized.
/// let default_key_len = CustomKeyVariable::default().as_ord_bytes().unwrap().len();
/// assert_eq!(default_key_len, 6);
/// let another_key_len = CustomKeyVariable {
///     customer_id: VarInt(u64::MAX),
///     order_id: VarInt(u64::MAX),
/// }
/// .as_ord_bytes()
/// .unwrap()
/// .len();
/// assert_eq!(another_key_len, 22);
/// ```
///
///
/// # Why does this type exist?
///
/// The [`Key`] trait is implemented for all of Rust's native integer types by
/// using `to_be_bytes()`/`from_be_bytes()`. This provides some benefits: very
/// fast encoding and decoding, and known-width encoding is faster to decode.
///
/// This type uses [`ordered_varint`] to encode the types using a variable
/// length encoding that is still compatible with the [`Key`] trait. This allows
/// a value of 0 to encode as a single byte while still preserving the correct
/// sort order required by `Key`.
///
/// Additionally, this encoding format allows for upgrading the in-memory size
/// transparently if the value range needs increases over time. This only works
/// between types that are signed the same.
///
/// # Behavior with Serde
///
/// This type implements [`serde::Serialize`] and [`serde::Deserialize`]
/// transparently, as many serialization formats implement native variable
/// integer encoding, and do not benefit from an ordered implementation.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VarInt<T>(pub T)
where
    T: VariableInteger;

impl<T> Serialize for VarInt<T>
where
    T: Serialize + VariableInteger,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for VarInt<T>
where
    T: Deserialize<'de> + VariableInteger,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self)
    }
}

impl<T> Display for VarInt<T>
where
    T: Display + VariableInteger,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> From<T> for VarInt<T>
where
    T: VariableInteger,
{
    fn from(value: T) -> Self {
        Self(value)
    }
}

macro_rules! impl_varint_op {
    ($trait:ident, $method:ident) => {
        impl<T> std::ops::$trait<T> for VarInt<T>
        where
            T: std::ops::$trait<T, Output = T> + VariableInteger,
        {
            type Output = Self;

            fn $method(self, rhs: T) -> Self::Output {
                Self(self.0.$method(rhs))
            }
        }
    };
}

impl_varint_op!(Add, add);
impl_varint_op!(Sub, sub);
impl_varint_op!(Mul, mul);
impl_varint_op!(Div, div);
impl_varint_op!(Rem, rem);
impl_varint_op!(BitAnd, bitand);
impl_varint_op!(BitOr, bitor);
impl_varint_op!(BitXor, bitxor);
impl_varint_op!(Shl, shl);
impl_varint_op!(Shr, shr);

impl<T> std::ops::Not for VarInt<T>
where
    T: std::ops::Not<Output = T> + VariableInteger,
{
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(self.0.not())
    }
}

impl<T> Deref for VarInt<T>
where
    T: VariableInteger,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for VarInt<T>
where
    T: VariableInteger,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'k, T> Key<'k> for VarInt<T>
where
    T: VariableInteger,
{
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        T::decode_variable(bytes.as_ref()).map(Self)
    }
}

impl<'k, T> KeyEncoding<'k> for VarInt<T>
where
    T: VariableInteger,
{
    type Error = std::io::Error;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: super::KeyVisitor,
    {
        visitor.visit_composite(
            super::CompositeKind::Struct(Cow::Borrowed("bonsaidb::core::key::VarInt")),
            1,
        );
        T::describe_contents(visitor);
    }

    fn as_ord_bytes(&'k self) -> Result<Cow<'k, [u8]>, Self::Error> {
        let mut output = Vec::with_capacity(16);
        self.0.encode_variable(&mut output)?;
        Ok(Cow::Owned(output))
    }
}

/// A type that is compatible with [`VarInt`].
///
/// This trait is implemented by all of Rust's built-in integer types.
pub trait VariableInteger: Variable + Send + Sync + Clone + sealed::Sealed {}

mod sealed {
    pub trait Sealed {
        fn describe_contents<Visitor>(visitor: &mut Visitor)
        where
            Visitor: crate::key::KeyVisitor;
    }
}

macro_rules! impl_variable_integer {
    ($type:ty, $kind:expr, $test:ident) => {
        impl VariableInteger for $type {}

        impl sealed::Sealed for $type {
            fn describe_contents<Visitor>(visitor: &mut Visitor)
            where
                Visitor: super::KeyVisitor,
            {
                visitor.visit_type($kind);
            }
        }

        impl From<VarInt<$type>> for $type {
            fn from(value: VarInt<$type>) -> Self {
                value.0
            }
        }

        #[test]
        fn $test() {
            let i = VarInt::<$type>::from(0);
            let r = 0;
            let i = i + 2;
            let r = r + 2;
            assert_eq!(i, VarInt(r));
            let i = i - 1;
            let r = r - 1;
            assert_eq!(i, VarInt(r));
            let i = i * 6;
            let r = r * 6;
            assert_eq!(i, VarInt(r));
            let i = i / 3;
            let r = r / 3;
            assert_eq!(i, VarInt(r));
            let i = i % 2;
            let r = r % 2;
            assert_eq!(i, VarInt(r));
            let i = !i;
            let r = !r;
            assert_eq!(i, VarInt(r));
            let i = i >> 1;
            let r = r >> 1;
            assert_eq!(i, VarInt(r));
            let i = i << 1;
            let r = r << 1;
            assert_eq!(i, VarInt(r));
            let i = i & 0xF;
            let r = r & 0xF;
            assert_eq!(i, VarInt(r));
            let i = i | 0x70;
            let r = r | 0x70;
            assert_eq!(i, VarInt(r));
            let i = i ^ 0x18;
            let r = r ^ 0x18;
            assert_eq!(i, VarInt(r));
            assert_eq!(Into::<$type>::into(i), r);

            let encoded = i.as_ord_bytes().unwrap();
            let decoded =
                VarInt::<$type>::from_ord_bytes(crate::key::ByteSource::Borrowed(&encoded))
                    .unwrap();
            assert_eq!(i, decoded);

            let pot = transmog_pot::Pot::default();
            let pot_encoded = transmog::Format::serialize(&pot, &i).unwrap();
            let decoded =
                transmog::OwnedDeserializer::deserialize_owned(&pot, &pot_encoded).unwrap();
            assert_eq!(i, decoded);
        }
    };
}

impl_variable_integer!(u8, KeyKind::Unsigned, varint_u8_tests);
impl_variable_integer!(u16, KeyKind::Unsigned, varint_u16_tests);
impl_variable_integer!(u32, KeyKind::Unsigned, varint_u32_tests);
impl_variable_integer!(u64, KeyKind::Unsigned, varint_u64_tests);
impl_variable_integer!(u128, KeyKind::Unsigned, varint_u126_tests);
impl_variable_integer!(usize, KeyKind::Unsigned, varint_usize_tests);
impl_variable_integer!(i8, KeyKind::Signed, varint_i8_tests);
impl_variable_integer!(i16, KeyKind::Signed, varint_i16_tests);
impl_variable_integer!(i32, KeyKind::Signed, varint_i32_tests);
impl_variable_integer!(i64, KeyKind::Signed, varint_i64_tests);
impl_variable_integer!(i128, KeyKind::Signed, varint_i128_tests);
impl_variable_integer!(isize, KeyKind::Signed, varint_isize_tests);
