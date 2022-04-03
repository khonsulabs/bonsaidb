use std::{
    borrow::Cow,
    io::{ErrorKind, Write},
};

use ordered_varint::Variable;

use crate::key::{CompositeKeyError, Key, KeyEncoding, NextValueError};

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
#[deprecated = "use `CompositeKeyEncoder` instead. This function does not properly sort variable length encoded fields. See #240."]
pub fn encode_composite_field<'a, K: Key<'a>, T: KeyEncoding<'a, K>, Bytes: Write>(
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
#[deprecated = "use `CompositeKeyDecoder` instead. This function does not properly sort variable length encoded fields. See #240."]
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

/// This type enables wrapping a tuple to preserve the behavior of the initial
/// implementation of tuple key encoding. This type should not be used in new
/// code and should only be used to preserve backwards compatibility. See
/// <https://github.com/khonsulabs/bonsaidb/issues/240> for more information
/// about why this implementation should be avoided.
#[derive(Debug, Clone)]
#[deprecated = "This type preserves a version of tuple encoding for backwards compatibility. It it is known to have improper key ordering. See https://github.com/khonsulabs/bonsaidb/issues/240."]
pub struct TupleEncodingV1<T>(pub T);

macro_rules! impl_key_for_tuple_v1 {
    ($(($index:tt, $varname:ident, $generic:ident)),+) => {
        #[allow(deprecated)]
        impl<'a, $($generic),+> Key<'a> for TupleEncodingV1<($($generic),+,)>
        where
            $($generic: Key<'a>),+
        {
            fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
                $(let ($varname, bytes) = decode_composite_field::<$generic>(bytes)?;)+

                if bytes.is_empty() {
                    Ok(Self(($($varname),+,)))
                } else {
                    Err(CompositeKeyError::new(std::io::Error::from(
                        ErrorKind::InvalidData,
                    )))
                }
            }
        }

        #[allow(deprecated)]
        impl<'a, $($generic),+> KeyEncoding<'a, Self> for TupleEncodingV1<($($generic),+,)>
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

                $(encode_composite_field(&self.0.$index, &mut bytes)?;)+

                Ok(Cow::Owned(bytes))
            }
        }
    };
}

impl_key_for_tuple_v1!((0, t1, T1));
impl_key_for_tuple_v1!((0, t1, T1), (1, t2, T2));
impl_key_for_tuple_v1!((0, t1, T1), (1, t2, T2), (2, t3, T3));
impl_key_for_tuple_v1!((0, t1, T1), (1, t2, T2), (2, t3, T3), (3, t4, T4));
impl_key_for_tuple_v1!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5)
);
impl_key_for_tuple_v1!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5),
    (5, t6, T6)
);
impl_key_for_tuple_v1!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5),
    (5, t6, T6),
    (6, t7, T7)
);
impl_key_for_tuple_v1!(
    (0, t1, T1),
    (1, t2, T2),
    (2, t3, T3),
    (3, t4, T4),
    (4, t5, T5),
    (5, t6, T6),
    (6, t7, T7),
    (7, t8, T8)
);

/// A type that preserves the original implementation of [`Key`] for
/// `Option<T>`. This should not be used in new code and will be removed in a
/// future version.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
#[deprecated = "this type should not be used in new code and should only be used in transitionary code."]
#[allow(deprecated)]
pub struct OptionKeyV1<T>(pub Option<T>);

#[allow(deprecated)]
impl<'a, T> Key<'a> for OptionKeyV1<T>
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
impl<'a, K, T> KeyEncoding<'a, OptionKeyV1<K>> for OptionKeyV1<T>
where
    T: KeyEncoding<'a, K>,
    K: for<'k> Key<'k>,
{
    type Error = T::Error;

    const LENGTH: Option<usize> = T::LENGTH;

    /// # Panics
    ///
    /// Panics if `T::into_big_endian_bytes` returns an empty `IVec`
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
