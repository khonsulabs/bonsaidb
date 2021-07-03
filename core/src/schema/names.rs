use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display, Write},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

/// A valid schema name. Must be alphanumeric (`a-zA-Z9-0`) or a hyphen (`-`).
/// Cloning this structure shares the underlying string data, regardless of
/// whether it's a static string literal or an owned String.
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct Name(Arc<Cow<'static, str>>);

/// An invalid name was used in a schema definition.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone)]
#[error("invalid name: {0}")]
pub struct InvalidNameError(pub String);

impl Name {
    /// Creates a new name after validating it.
    ///
    /// # Errors
    /// Returns [`InvalidNameError`] if the value passed contains any characters
    /// other than `a-zA-Z9-0` or a hyphen (`-`).
    pub fn new<T: TryInto<Self, Error = InvalidNameError>>(
        contents: T,
    ) -> Result<Self, InvalidNameError> {
        contents.try_into()
    }

    fn validate_name(name: &str) -> Result<(), InvalidNameError> {
        if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            Ok(())
        } else {
            Err(InvalidNameError(name.to_string()))
        }
    }
}

impl TryFrom<&'static str> for Name {
    type Error = InvalidNameError;

    fn try_from(value: &'static str) -> Result<Self, InvalidNameError> {
        Self::validate_name(value)?;
        Ok(Self(Arc::new(Cow::Borrowed(value))))
    }
}

impl TryFrom<String> for Name {
    type Error = InvalidNameError;

    fn try_from(value: String) -> Result<Self, InvalidNameError> {
        Self::validate_name(&value)?;
        Ok(Self(Arc::new(Cow::Owned(value))))
    }
}

#[allow(clippy::from_over_into)] // the auto into impl doesn't work with serde(into)
impl Into<String> for Name {
    fn into(self) -> String {
        self.0.to_string()
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

/// The owner of a schema item. This should represent the company, group, or
/// individual that created the item in question. This value is used for
/// namespacing. Changing this after values are in use is not supported without
/// manual migrations at this time.
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(transparent)]
pub struct Authority(Name);

impl TryFrom<&'static str> for Authority {
    type Error = InvalidNameError;

    fn try_from(value: &'static str) -> Result<Self, InvalidNameError> {
        Ok(Self(Name::new(value)?))
    }
}

impl TryFrom<String> for Authority {
    type Error = InvalidNameError;

    fn try_from(value: String) -> Result<Self, InvalidNameError> {
        Ok(Self(Name::new(value)?))
    }
}

impl Display for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// The name of a [`Schema`](super::Schema).
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
pub struct SchemaName {
    /// The authority of this schema.
    pub authority: Authority,

    /// The name of this schema.
    pub name: Name,
}

impl SchemaName {
    /// Creates a new schema name.
    pub fn new<
        A: TryInto<Authority, Error = InvalidNameError>,
        N: TryInto<Name, Error = InvalidNameError>,
    >(
        authority: A,
        name: N,
    ) -> Result<Self, InvalidNameError> {
        let authority = authority.try_into()?;
        let name = name.try_into()?;
        Ok(Self { authority, name })
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.authority, f)?;
        f.write_char('.')?;
        Display::fmt(&self.name, f)
    }
}

impl TryFrom<&str> for SchemaName {
    type Error = InvalidNameError;

    fn try_from(schema_name: &str) -> Result<Self, InvalidNameError> {
        let parts = schema_name.split('.').collect::<Vec<&str>>();
        if parts.len() == 2 {
            let mut parts = parts.into_iter();
            let authority = parts.next().unwrap();
            let name = parts.next().unwrap();

            Self::new(authority.to_string(), name.to_string())
        } else {
            Err(InvalidNameError(schema_name.to_string()))
        }
    }
}

/// The name of a [`Collection`](super::Collection).
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
pub struct CollectionName {
    /// The authority of this collection.
    pub authority: Authority,

    /// The name of this collection.
    pub name: Name,
}

impl CollectionName {
    /// Creates a new collection name.
    pub fn new<
        A: TryInto<Authority, Error = InvalidNameError>,
        N: TryInto<Name, Error = InvalidNameError>,
    >(
        authority: A,
        name: N,
    ) -> Result<Self, InvalidNameError> {
        let authority = authority.try_into()?;
        let name = name.try_into()?;
        Ok(Self { authority, name })
    }
}

impl Display for CollectionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.authority, f)?;
        f.write_char('.')?;
        Display::fmt(&self.name, f)
    }
}

impl TryFrom<&str> for CollectionName {
    type Error = InvalidNameError;

    fn try_from(collection_name: &str) -> Result<Self, InvalidNameError> {
        let parts = collection_name.split('.').collect::<Vec<&str>>();
        if parts.len() == 2 {
            let mut parts = parts.into_iter();
            let authority = parts.next().unwrap();
            let name = parts.next().unwrap();

            Self::new(authority.to_string(), name.to_string())
        } else {
            Err(InvalidNameError(collection_name.to_string()))
        }
    }
}

/// The name of a [`View`](super::View).
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone)]
pub struct ViewName {
    /// The name of the collection that contains this view.
    pub collection: CollectionName,
    /// The name of this view.
    pub name: Name,
}

impl ViewName {
    /// Creates a new view name.
    pub fn new<
        C: TryInto<CollectionName, Error = InvalidNameError>,
        N: TryInto<Name, Error = InvalidNameError>,
    >(
        collection: C,
        name: N,
    ) -> Result<Self, InvalidNameError> {
        let collection = collection.try_into()?;
        let name = name.try_into()?;
        Ok(Self { collection, name })
    }
}

impl Display for ViewName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.collection, f)?;
        f.write_char('.')?;
        Display::fmt(&self.name, f)
    }
}

#[test]
fn name_validation_tests() {
    assert!(Name::new("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-").is_ok());
    assert!(matches!(Name::new("."), Err(InvalidNameError(_))));
}
