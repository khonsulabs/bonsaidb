
use arc_bytes::serde::Bytes;
use serde::{Deserialize, Serialize};

mod timestamp;

pub use self::timestamp::Timestamp;
use crate::Error;

mod implementation {
    use arc_bytes::serde::Bytes;
    use async_trait::async_trait;
    use futures::future::BoxFuture;
    use serde::Serialize;

    use crate::{
        keyvalue::{Command, KeyCheck, KeyOperation, KeyStatus, Output, Timestamp},
        Error,
    };

    /// Types for executing get operations.
    pub mod get;
    /// Types for executing increment/decrement operations.
    pub mod increment;
    /// Types for handling key namespaces.
    pub mod namespaced;
    /// Types for executing set operations.
    pub mod set;

    use namespaced::Namespaced;

    use super::{IncompatibleTypeError, Numeric, Value};
    /// Key-Value store methods. The Key-Value store is designed to be a
    /// high-performance, lightweight storage mechanism.
    ///
    /// When compared to Collections, the Key-Value store does not offer
    /// ACID-compliant transactions. Instead, the Key-Value store is made more
    /// efficient by periodically flushing the store to disk rather than during
    /// each operation. As such, the Key-Value store is intended to be used as a
    /// lightweight caching layer. However, because each of the operations it
    /// supports are executed atomically, the Key-Value store can also be
    /// utilized for synchronized locking.
    ///
    /// ## Floating Point Operations
    ///
    /// When using [`KeyValue::set_numeric_key()`] or any numeric operations, if
    /// a [Not a Number (NaN) value][nan] is encountered, [`Error::NotANumber`]
    /// will be returned without allowing the operation to succeed.
    ///
    /// Positive and negative infinity values are allowed, as they do not break
    /// comparison operations.
    ///
    /// [nan]: https://en.wikipedia.org/wiki/NaN
    pub trait KeyValue: Sized + Send + Sync {
        /// Executes a single [`KeyOperation`].
        fn execute_key_operation(&self, op: KeyOperation) -> Result<Output, Error>;

        /// Sets `key` to `value`. This function returns a builder that is also a
        /// Future. Awaiting the builder will execute [`Command::Set`] with the options
        /// given.
        fn set_key<'a, S: Into<String>, V: Serialize + Send + Sync>(
            &'a self,
            key: S,
            value: &'a V,
        ) -> set::Builder<'a, Self, V> {
            set::Builder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                PendingValue::Serializeable(value),
            )
        }

        /// Sets `key` to `bytes`. This function returns a builder that is also
        /// a Future. Awaiting the builder will execute [`Command::Set`] with
        /// the options given.
        fn set_binary_key<'a, S: Into<String>>(
            &'a self,
            key: S,
            bytes: &'a [u8],
        ) -> set::Builder<'a, Self, ()> {
            set::Builder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                PendingValue::Bytes(bytes),
            )
        }

        /// Sets `key` to `value`. This stores the value as a `Numeric`,
        /// enabling atomic math operations to be performed on this key. This
        /// function returns a builder that is also a Future. Awaiting the
        /// builder will execute [`Command::Set`] with the options given.
        fn set_numeric_key<S: Into<String>, V: Into<Numeric>>(
            &self,
            key: S,
            value: V,
        ) -> set::Builder<'_, Self, ()> {
            set::Builder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                PendingValue::Numeric(value.into()),
            )
        }

        /// Increments `key` by `value`. The value stored must be a `Numeric`,
        /// otherwise an error will be returned. The result of the increment
        /// will be the `value`'s type. For example, if the stored value is
        /// currently a `u64`, but `value` is a `f64`, the current value will be
        /// converted to an `f64`, and the stored value will be an `f64`.
        fn increment_key_by<
            S: Into<String> + Send + Sync,
            V: Into<Numeric> + TryFrom<Numeric, Error = IncompatibleTypeError> + Send + Sync,
        >(
            &self,
            key: S,
            value: V,
        ) -> increment::Builder<'_, Self, V> {
            increment::Builder::new(
                self,
                self.key_namespace().map(Into::into),
                true,
                key.into(),
                value.into(),
            )
        }

        /// Decrements `key` by `value`. The value stored must be a `Numeric`,
        /// otherwise an error will be returned. The result of the decrement
        /// will be the `value`'s type. For example, if the stored value is
        /// currently a `u64`, but `value` is a `f64`, the current value will be
        /// converted to an `f64`, and the stored value will be an `f64`.
        fn decrement_key_by<
            S: Into<String> + Send + Sync,
            V: Into<Numeric> + TryFrom<Numeric, Error = IncompatibleTypeError> + Send + Sync,
        >(
            &self,
            key: S,
            value: V,
        ) -> increment::Builder<'_, Self, V> {
            increment::Builder::new(
                self,
                self.key_namespace().map(Into::into),
                false,
                key.into(),
                value.into(),
            )
        }

        /// Gets the value stored at `key`. This function returns a builder that is also a
        /// Future. Awaiting the builder will execute [`Command::Get`] with the options
        /// given.
        fn get_key<S: Into<String>>(&'_ self, key: S) -> get::Builder<'_, Self> {
            get::Builder::new(self, self.key_namespace().map(Into::into), key.into())
        }

        /// Deletes the value stored at `key`.
        fn delete_key<S: Into<String> + Send>(&'_ self, key: S) -> Result<KeyStatus, Error> {
            match self.execute_key_operation(KeyOperation {
                namespace: self.key_namespace().map(ToOwned::to_owned),
                key: key.into(),
                command: Command::Delete,
            })? {
                Output::Status(status) => Ok(status),
                Output::Value(_) => unreachable!("invalid output from delete operation"),
            }
        }

        /// The current namespace.
        fn key_namespace(&self) -> Option<&'_ str> {
            None
        }

        /// Access this Key-Value store within a namespace. When using the returned
        /// [`Namespaced`] instance, all keys specified will be separated into their
        /// own storage designated by `namespace`.
        fn with_key_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self> {
            Namespaced::new(namespace.to_string(), self)
        }
    }

    /// Key-Value store methods. The Key-Value store is designed to be a
    /// high-performance, lightweight storage mechanism.
    ///
    /// When compared to Collections, the Key-Value store does not offer
    /// ACID-compliant transactions. Instead, the Key-Value store is made more
    /// efficient by periodically flushing the store to disk rather than during
    /// each operation. As such, the Key-Value store is intended to be used as a
    /// lightweight caching layer. However, because each of the operations it
    /// supports are executed atomically, the Key-Value store can also be
    /// utilized for synchronized locking.
    ///
    /// ## Floating Point Operations
    ///
    /// When using [`KeyValue::set_numeric_key()`] or any numeric operations, if
    /// a [Not a Number (NaN) value][nan] is encountered, [`Error::NotANumber`]
    /// will be returned without allowing the operation to succeed.
    ///
    /// Positive and negative infinity values are allowed, as they do not break
    /// comparison operations.
    ///
    /// [nan]: https://en.wikipedia.org/wiki/NaN
    #[async_trait]
    pub trait AsyncKeyValue: Sized + Send + Sync {
        /// Executes a single [`KeyOperation`].
        async fn execute_key_operation(&self, op: KeyOperation) -> Result<Output, Error>;

        /// Sets `key` to `value`. This function returns a builder that is also a
        /// Future. Awaiting the builder will execute [`Command::Set`] with the options
        /// given.
        fn set_key<'a, S: Into<String>, V: Serialize + Send + Sync>(
            &'a self,
            key: S,
            value: &'a V,
        ) -> set::AsyncBuilder<'a, Self, V> {
            set::AsyncBuilder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                PendingValue::Serializeable(value),
            )
        }

        /// Sets `key` to `bytes`. This function returns a builder that is also
        /// a Future. Awaiting the builder will execute [`Command::Set`] with
        /// the options given.
        fn set_binary_key<'a, S: Into<String>>(
            &'a self,
            key: S,
            bytes: &'a [u8],
        ) -> set::AsyncBuilder<'a, Self, ()> {
            set::AsyncBuilder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                PendingValue::Bytes(bytes),
            )
        }

        /// Sets `key` to `value`. This stores the value as a `Numeric`,
        /// enabling atomic math operations to be performed on this key. This
        /// function returns a builder that is also a Future. Awaiting the
        /// builder will execute [`Command::Set`] with the options given.
        fn set_numeric_key<S: Into<String>, V: Into<Numeric>>(
            &self,
            key: S,
            value: V,
        ) -> set::AsyncBuilder<'_, Self, ()> {
            set::AsyncBuilder::new(
                self,
                self.key_namespace().map(Into::into),
                key.into(),
                PendingValue::Numeric(value.into()),
            )
        }

        /// Increments `key` by `value`. The value stored must be a `Numeric`,
        /// otherwise an error will be returned. The result of the increment
        /// will be the `value`'s type. For example, if the stored value is
        /// currently a `u64`, but `value` is a `f64`, the current value will be
        /// converted to an `f64`, and the stored value will be an `f64`.
        fn increment_key_by<
            S: Into<String> + Send + Sync,
            V: Into<Numeric> + TryFrom<Numeric, Error = IncompatibleTypeError> + Send + Sync,
        >(
            &self,
            key: S,
            value: V,
        ) -> increment::AsyncBuilder<'_, Self, V> {
            increment::AsyncBuilder::new(
                self,
                self.key_namespace().map(Into::into),
                true,
                key.into(),
                value.into(),
            )
        }

        /// Decrements `key` by `value`. The value stored must be a `Numeric`,
        /// otherwise an error will be returned. The result of the decrement
        /// will be the `value`'s type. For example, if the stored value is
        /// currently a `u64`, but `value` is a `f64`, the current value will be
        /// converted to an `f64`, and the stored value will be an `f64`.
        fn decrement_key_by<
            S: Into<String> + Send + Sync,
            V: Into<Numeric> + TryFrom<Numeric, Error = IncompatibleTypeError> + Send + Sync,
        >(
            &self,
            key: S,
            value: V,
        ) -> increment::AsyncBuilder<'_, Self, V> {
            increment::AsyncBuilder::new(
                self,
                self.key_namespace().map(Into::into),
                false,
                key.into(),
                value.into(),
            )
        }

        /// Gets the value stored at `key`. This function returns a builder that is also a
        /// Future. Awaiting the builder will execute [`Command::Get`] with the options
        /// given.
        fn get_key<S: Into<String>>(&'_ self, key: S) -> get::AsyncBuilder<'_, Self> {
            get::AsyncBuilder::new(self, self.key_namespace().map(Into::into), key.into())
        }

        /// Deletes the value stored at `key`.
        async fn delete_key<S: Into<String> + Send>(&'_ self, key: S) -> Result<KeyStatus, Error> {
            match self
                .execute_key_operation(KeyOperation {
                    namespace: self.key_namespace().map(ToOwned::to_owned),
                    key: key.into(),
                    command: Command::Delete,
                })
                .await?
            {
                Output::Status(status) => Ok(status),
                Output::Value(_) => unreachable!("invalid output from delete operation"),
            }
        }

        /// The current namespace.
        fn key_namespace(&self) -> Option<&'_ str> {
            None
        }

        /// Access this Key-Value store within a namespace. When using the returned
        /// [`Namespaced`] instance, all keys specified will be separated into their
        /// own storage designated by `namespace`.
        fn with_key_namespace(&'_ self, namespace: &str) -> Namespaced<'_, Self> {
            Namespaced::new(namespace.to_string(), self)
        }
    }

    enum BuilderState<'a, T, V> {
        Pending(Option<T>),
        Executing(BoxFuture<'a, V>),
    }

    #[allow(clippy::redundant_pub_crate)]
    pub(crate) enum PendingValue<'a, V> {
        Bytes(&'a [u8]),
        Serializeable(&'a V),
        Numeric(Numeric),
    }

    impl<'a, V> PendingValue<'a, V>
    where
        V: Serialize,
    {
        fn prepare(self) -> Result<Value, Error> {
            match self {
                Self::Bytes(bytes) => Ok(Value::Bytes(Bytes::from(bytes))),
                Self::Serializeable(value) => Ok(Value::Bytes(Bytes::from(pot::to_vec(value)?))),
                Self::Numeric(numeric) => Ok(Value::Numeric(numeric)),
            }
        }
    }
}

pub use implementation::*;

/// Checks for existing keys.
#[derive(Serialize, Deserialize, Copy, Clone, Debug)]
pub enum KeyCheck {
    /// Only allow the operation if an existing key is present.
    OnlyIfPresent,
    /// Only allow the opeartion if the key isn't present.
    OnlyIfVacant,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
/// An operation performed on a key.
pub struct KeyOperation {
    /// The namespace for the key.
    pub namespace: Option<String>,
    /// The key to operate on.
    pub key: String,
    /// The command to execute.
    pub command: Command,
}

/// Commands for a key-value store.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Command {
    /// Set a key/value pair.
    Set(SetCommand),
    /// Get the value from a key.
    Get {
        /// Remove the key after retrieving the value.
        delete: bool,
    },
    /// Increment a numeric key. Returns an error if the key cannot be
    /// deserialized to the same numeric type as `amount`. If `saturating` is
    /// true, overflows will be prevented and the value will remain within the
    /// numeric bounds.
    Increment {
        /// The amount to increment by.
        amount: Numeric,
        /// If true, the result will be constrained to the numerical bounds of
        /// the type of `amount`.
        saturating: bool,
    },
    /// Decrement a numeric key. Returns an error if the key cannot be
    /// deserialized to the same numeric type as `amount`. If `saturating` is
    /// true, overflows will be prevented and the value will remain within the
    /// numeric bounds.
    Decrement {
        /// The amount to increment by.
        amount: Numeric,
        /// If true, the result will be constrained to the numerical bounds of
        /// the type of `amount`.
        saturating: bool,
    },
    /// Delete a key.
    Delete,
}

/// Set a key/value pair.
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SetCommand {
    /// The value.
    pub value: Value,
    /// If set, the key will be set to expire automatically.
    pub expiration: Option<Timestamp>,
    /// If true and the key already exists, the expiration will not be
    /// updated. If false and an expiration is provided, the expiration will
    /// be set.
    pub keep_existing_expiration: bool,
    /// Conditional checks for whether the key is already present or not.
    pub check: Option<KeyCheck>,
    /// If true and the key already exists, the existing key will be returned if overwritten.
    pub return_previous_value: bool,
}

/// A value stored in a key.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Value {
    /// A value stored as a byte array.
    Bytes(Bytes),
    /// A numeric value.
    Numeric(Numeric),
}

impl Value {
    /// Validates this value to ensure it is safe to store.
    pub fn validate(self) -> Result<Self, Error> {
        match self {
            Self::Numeric(numeric) => numeric.validate().map(Self::Numeric),
            Self::Bytes(vec) => Ok(Self::Bytes(vec)),
        }
    }

    /// Deserializes the bytes contained inside of this value. Returns an error
    /// if this value doesn't contain bytes.
    pub fn deserialize<V: for<'de> Deserialize<'de>>(&self) -> Result<V, Error> {
        match self {
            Self::Bytes(bytes) => Ok(pot::from_slice(bytes)?),
            Self::Numeric(_) => Err(Error::other(
                "key-value",
                "key contains numeric value, not serialized data",
            )),
        }
    }

    /// Returns this value as an `i64`, allowing for precision to be lost if the type was not an `i64` originally. If saturating is true, the conversion will not allow overflows. Returns None if the value is bytes.
    #[must_use]
    pub fn as_i64_lossy(&self, saturating: bool) -> Option<i64> {
        match self {
            Self::Bytes(_) => None,
            Self::Numeric(value) => Some(value.as_i64_lossy(saturating)),
        }
    }

    /// Returns this value as an `u64`, allowing for precision to be lost if the type was not an `u64` originally. If saturating is true, the conversion will not allow overflows. Returns None if the value is bytes.
    #[must_use]
    pub fn as_u64_lossy(&self, saturating: bool) -> Option<u64> {
        match self {
            Self::Bytes(_) => None,
            Self::Numeric(value) => Some(value.as_u64_lossy(saturating)),
        }
    }

    /// Returns this value as an `f64`, allowing for precision to be lost if the type was not an `f64` originally. Returns None if the value is bytes.
    #[must_use]
    pub const fn as_f64_lossy(&self) -> Option<f64> {
        match self {
            Self::Bytes(_) => None,
            Self::Numeric(value) => Some(value.as_f64_lossy()),
        }
    }

    /// Returns this numeric as an `i64`, allowing for precision to be lost if the type was not an `i64` originally. Returns None if the value is bytes.
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Bytes(_) => None,
            Self::Numeric(value) => value.as_i64(),
        }
    }

    /// Returns this numeric as an `u64`, allowing for precision to be lost if the type was not an `u64` originally. Returns None if the value is bytes.
    #[must_use]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Bytes(_) => None,
            Self::Numeric(value) => value.as_u64(),
        }
    }

    /// Returns this numeric as an `f64`, allowing for precision to be lost if the type was not an `f64` originally. Returns None if the value is bytes.
    #[must_use]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Bytes(_) => None,
            Self::Numeric(value) => value.as_f64(),
        }
    }
}

/// A numerical value.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Numeric {
    /// A 64-bit signed integer.
    Integer(i64),
    /// A 64-bit unsigned integer.
    UnsignedInteger(u64),
    /// A 64-bit floating point number.
    Float(f64),
}

impl Numeric {
    /// Ensures this value contains a valid value.
    ///
    /// # Errors
    ///
    /// [`Error::NotANumber`] is returned if this contains a NaN floating point
    /// value.
    pub fn validate(self) -> Result<Self, Error> {
        if let Self::Float(float) = self {
            if float.is_nan() {
                return Err(Error::NotANumber);
            }
        }

        Ok(self)
    }

    /// Returns this numeric as an `i64`. If this conversion cannot be done
    /// without losing precision or overflowing, None will be returned.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            Self::UnsignedInteger(value) => (*value).try_into().ok(),
            Self::Float(value) => {
                if value.fract().abs() > 0. {
                    None
                } else {
                    Some(*value as i64)
                }
            }
        }
    }

    /// Returns this numeric as an `i64`, allowing for precision to be lost if
    /// the type was not an `i64` originally. If saturating is true, the
    /// conversion will not allow overflows.
    #[must_use]
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    pub fn as_i64_lossy(&self, saturating: bool) -> i64 {
        match self {
            Self::Integer(value) => *value,
            Self::UnsignedInteger(value) => {
                if saturating {
                    (*value).try_into().unwrap_or(i64::MAX)
                } else {
                    *value as i64
                }
            }
            Self::Float(value) => *value as i64,
        }
    }

    /// Returns this numeric as an `u64`. If this conversion cannot be done
    /// without losing precision or overflowing, None will be returned.
    #[must_use]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::UnsignedInteger(value) => Some(*value),
            Self::Integer(value) => (*value).try_into().ok(),
            Self::Float(value) => {
                if value.fract() < f64::EPSILON && value.is_sign_positive() {
                    Some(*value as u64)
                } else {
                    None
                }
            }
        }
    }

    /// Returns this numeric as an `u64`, allowing for precision to be lost if
    /// the type was not an `i64` originally. If saturating is true, the
    /// conversion will not allow overflows.
    #[must_use]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    pub fn as_u64_lossy(&self, saturating: bool) -> u64 {
        match self {
            Self::UnsignedInteger(value) => *value,
            Self::Integer(value) => {
                if saturating {
                    (*value).try_into().unwrap_or(0)
                } else {
                    *value as u64
                }
            }
            Self::Float(value) => *value as u64,
        }
    }

    /// Returns this numeric as an `f64`. If this conversion cannot be done
    /// without losing precision, None will be returned.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub const fn as_f64(&self) -> Option<f64> {
        match self {
            Self::UnsignedInteger(value) => {
                if *value > 2_u64.pow(f64::MANTISSA_DIGITS) {
                    None
                } else {
                    Some(*value as f64)
                }
            }
            Self::Integer(value) => {
                if *value > 2_i64.pow(f64::MANTISSA_DIGITS)
                    || *value < -(2_i64.pow(f64::MANTISSA_DIGITS))
                {
                    None
                } else {
                    Some(*value as f64)
                }
            }
            Self::Float(value) => Some(*value),
        }
    }

    /// Returns this numeric as an `f64`, allowing for precision to be lost if
    /// the type was not an `f64` originally.
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub const fn as_f64_lossy(&self) -> f64 {
        match self {
            Self::UnsignedInteger(value) => *value as f64,
            Self::Integer(value) => *value as f64,
            Self::Float(value) => *value,
        }
    }
}

/// A conversion between numeric types wasn't supported.
#[derive(thiserror::Error, Debug)]
#[error("incompatible numeric type")]
pub struct IncompatibleTypeError;

impl From<f64> for Numeric {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<i64> for Numeric {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<u64> for Numeric {
    fn from(value: u64) -> Self {
        Self::UnsignedInteger(value)
    }
}

#[allow(clippy::fallible_impl_from)]
impl TryFrom<Numeric> for f64 {
    type Error = IncompatibleTypeError;
    fn try_from(value: Numeric) -> Result<Self, IncompatibleTypeError> {
        if let Numeric::Float(value) = value {
            Ok(value)
        } else {
            Err(IncompatibleTypeError)
        }
    }
}

#[allow(clippy::fallible_impl_from)]
impl TryFrom<Numeric> for u64 {
    type Error = IncompatibleTypeError;
    fn try_from(value: Numeric) -> Result<Self, IncompatibleTypeError> {
        if let Numeric::UnsignedInteger(value) = value {
            Ok(value)
        } else {
            Err(IncompatibleTypeError)
        }
    }
}

#[allow(clippy::fallible_impl_from)]
impl TryFrom<Numeric> for i64 {
    type Error = IncompatibleTypeError;
    fn try_from(value: Numeric) -> Result<Self, IncompatibleTypeError> {
        if let Numeric::Integer(value) = value {
            Ok(value)
        } else {
            Err(IncompatibleTypeError)
        }
    }
}

/// The result of a [`KeyOperation`].
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum Output {
    /// A status was returned.
    Status(KeyStatus),
    /// A value was returned.
    Value(Option<Value>),
}
/// The status of an operation on a Key.
#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum KeyStatus {
    /// A new key was inserted.
    Inserted,
    /// An existing key was updated with a new value.
    Updated,
    /// A key was deleted.
    Deleted,
    /// No changes were made.
    NotChanged,
}
