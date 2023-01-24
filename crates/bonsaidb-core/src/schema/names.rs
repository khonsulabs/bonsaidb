use std::{
    borrow::Cow,
    fmt::{Debug, Display, Write},
    ops::Deref,
    sync::Arc,
};

use serde::{Deserialize, Serialize};

/// A schema name. Cloning is inexpensive.
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub struct Name {
    name: Arc<Cow<'static, str>>,
    needs_escaping: bool,
}

/// A name was unable to e parsed.
#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone)]
#[error("invalid name: {0}")]
pub struct InvalidNameError(pub String);

impl Name {
    /// Creates a new name.
    pub fn new<T: Into<Self>>(contents: T) -> Self {
        contents.into()
    }

    /// Parses a name that was previously encoded via [`Self::encoded()`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidNameError`] if the name contains invalid escape
    /// sequences.
    pub fn parse_encoded(encoded: &str) -> Result<Self, InvalidNameError> {
        let mut bytes = encoded.bytes();
        let mut decoded = Vec::with_capacity(encoded.len());
        while let Some(byte) = bytes.next() {
            if byte == b'_' {
                if let (Some(high), Some(low)) = (bytes.next(), bytes.next()) {
                    if let Some(byte) = hex_chars_to_byte(high, low) {
                        decoded.push(byte);
                        continue;
                    }
                }
                return Err(InvalidNameError(encoded.to_string()));
            }

            decoded.push(byte);
        }

        String::from_utf8(decoded)
            .map(Self::from)
            .map_err(|_| InvalidNameError(encoded.to_string()))
    }

    /// Returns an encoded version of this name that contains only alphanumeric
    /// ASCII, underscore, and hyphen.
    #[must_use]
    pub fn encoded(&self) -> String {
        format!("{self:#}")
    }
}

impl From<Cow<'static, str>> for Name {
    fn from(value: Cow<'static, str>) -> Self {
        let needs_escaping = !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-');
        Self {
            name: Arc::new(value),
            needs_escaping,
        }
    }
}

impl From<&'static str> for Name {
    fn from(value: &'static str) -> Self {
        Self::from(Cow::Borrowed(value))
    }
}

impl From<String> for Name {
    fn from(value: String) -> Self {
        Self::from(Cow::Owned(value))
    }
}

#[allow(clippy::from_over_into)] // the auto into impl doesn't work with serde(into)
impl Into<String> for Name {
    fn into(self) -> String {
        self.name.to_string()
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() && self.needs_escaping {
            for byte in self.name.bytes() {
                if byte.is_ascii_alphanumeric() || byte == b'-' {
                    f.write_char(byte as char)?;
                } else {
                    // Encode the byte as _FF
                    f.write_char('_')?;
                    f.write_char(nibble_to_hex_char(byte >> 4))?;
                    f.write_char(nibble_to_hex_char(byte & 0xF))?;
                }
            }
            Ok(())
        } else {
            Display::fmt(&self.name, f)
        }
    }
}

const fn nibble_to_hex_char(nibble: u8) -> char {
    let ch = match nibble {
        0..=9 => b'0' + nibble,
        _ => b'a' + nibble - 10,
    };
    ch as char
}

const fn hex_chars_to_byte(high_nibble: u8, low_nibble: u8) -> Option<u8> {
    match (
        hex_char_to_nibble(high_nibble),
        hex_char_to_nibble(low_nibble),
    ) {
        (Some(high_nibble), Some(low_nibble)) => Some(high_nibble << 4 | low_nibble),
        _ => None,
    }
}

const fn hex_char_to_nibble(nibble: u8) -> Option<u8> {
    let ch = match nibble {
        b'0'..=b'9' => nibble - b'0',
        b'a'..=b'f' => nibble - b'a' + 10,
        _ => return None,
    };
    Some(ch)
}

impl AsRef<str> for Name {
    fn as_ref(&self) -> &str {
        self.name.as_ref()
    }
}

/// The owner of a schema item. This should represent the company, group, or
/// individual that created the item in question. This value is used for
/// namespacing. Changing this after values are in use is not supported without
/// manual migrations at this time.
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(transparent)]
pub struct Authority(Name);

impl From<Cow<'static, str>> for Authority {
    fn from(value: Cow<'static, str>) -> Self {
        Self::from(Name::from(value))
    }
}

impl From<&'static str> for Authority {
    fn from(value: &'static str) -> Self {
        Self::from(Cow::Borrowed(value))
    }
}

impl From<String> for Authority {
    fn from(value: String) -> Self {
        Self::from(Cow::Owned(value))
    }
}

impl From<Name> for Authority {
    fn from(value: Name) -> Self {
        Self(value)
    }
}

impl Display for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl AsRef<str> for Authority {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

/// A namespaced name.
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
pub struct QualifiedName {
    /// The authority that defines this name.
    pub authority: Authority,

    /// The name, unique within `authority`.
    pub name: Name,
}

impl Display for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.authority, f)?;
        f.write_char('.')?;
        Display::fmt(&self.name, f)
    }
}

/// Functions for creating qualified names
pub trait Qualified: Display + Sized {
    /// Creates a name that is not meant to be shared with other developers or
    /// projects.
    #[must_use]
    fn private<N: Into<Name>>(name: N) -> Self {
        Self::new(Authority::from("private"), name)
    }

    /// Creates a new qualified name.
    #[must_use]
    fn new<A: Into<Authority>, N: Into<Name>>(authority: A, name: N) -> Self;

    /// Parses a schema name that was previously encoded via
    /// [`Self::encoded()`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidNameError`] if the name contains invalid escape
    /// sequences or contains more than two periods.
    fn parse_encoded(schema_name: &str) -> Result<Self, InvalidNameError> {
        let mut parts = schema_name.split('.');
        if let (Some(authority), Some(name), None) = (parts.next(), parts.next(), parts.next()) {
            let authority = Name::parse_encoded(authority)?;
            let name = Name::parse_encoded(name)?;

            Ok(Self::new(authority, name))
        } else {
            Err(InvalidNameError(schema_name.to_string()))
        }
    }

    /// Encodes this schema name such that the authority and name can be
    /// safely parsed using [`Self::parse_encoded`].
    #[must_use]
    fn encoded(&self) -> String {
        format!("{self:#}")
    }
}

impl Qualified for QualifiedName {
    fn new<A: Into<Authority>, N: Into<Name>>(authority: A, name: N) -> Self {
        Self {
            authority: authority.into(),
            name: name.into(),
        }
    }
}

/// The name of a [`Schema`](super::Schema).
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(transparent)]
pub struct SchemaName(pub(crate) QualifiedName);

impl Qualified for SchemaName {
    fn new<A: Into<Authority>, N: Into<Name>>(authority: A, name: N) -> Self {
        Self(QualifiedName::new(authority, name))
    }
}

impl Display for SchemaName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Deref for SchemaName {
    type Target = QualifiedName;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<CollectionName> for SchemaName {
    fn from(name: CollectionName) -> Self {
        Self(name.0)
    }
}

/// The namespaced name of a [`Collection`](super::Collection).
#[derive(Hash, PartialEq, Eq, Deserialize, Serialize, Debug, Clone, Ord, PartialOrd)]
#[serde(transparent)]
pub struct CollectionName(pub(crate) QualifiedName);

impl Qualified for CollectionName {
    fn new<A: Into<Authority>, N: Into<Name>>(authority: A, name: N) -> Self {
        Self(QualifiedName::new(authority, name))
    }
}

impl Display for CollectionName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl Deref for CollectionName {
    type Target = QualifiedName;

    fn deref(&self) -> &Self::Target {
        &self.0
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
fn name_escaping_tests() {
    const VALID_CHARS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-";
    const INVALID_CHARS: &str = "._hello\u{1F680}";
    const ESCAPED_INVALID: &str = "_2e_5fhello_f0_9f_9a_80";
    assert_eq!(Name::new(VALID_CHARS).to_string(), VALID_CHARS);
    assert_eq!(Name::new(INVALID_CHARS).to_string(), INVALID_CHARS);
    assert_eq!(Name::new(INVALID_CHARS).encoded(), ESCAPED_INVALID);
    assert_eq!(
        Name::parse_encoded(ESCAPED_INVALID).unwrap(),
        Name::new(INVALID_CHARS)
    );
    Name::parse_encoded("_").unwrap_err();
    Name::parse_encoded("_0").unwrap_err();
    Name::parse_encoded("_z").unwrap_err();
    Name::parse_encoded("_0z").unwrap_err();
}

#[test]
fn joined_names_tests() {
    const INVALID_CHARS: &str = "._hello\u{1F680}.._world\u{1F680}";
    const ESCAPED_INVALID: &str = "_2e_5fhello_f0_9f_9a_80._2e_5fworld_f0_9f_9a_80";
    let collection = CollectionName::parse_encoded(ESCAPED_INVALID).unwrap();
    assert_eq!(collection.to_string(), INVALID_CHARS);
    assert_eq!(collection.encoded(), ESCAPED_INVALID);

    let schema_name = SchemaName::parse_encoded(ESCAPED_INVALID).unwrap();
    assert_eq!(schema_name.to_string(), INVALID_CHARS);
    assert_eq!(schema_name.encoded(), ESCAPED_INVALID);
}
