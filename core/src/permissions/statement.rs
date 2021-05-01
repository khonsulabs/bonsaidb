use std::borrow::Cow;

use super::{Action, ActionName};

/// A statement of permissions. A statement describes whether one or more
/// `actions` should be `allowed` to be taken against `resources`.
#[derive(Debug)]
pub struct Statement {
    /// The list of resources this statement applies to.
    pub resources: Vec<ResourceName>,
    /// The list of actions this statement applies to.
    pub actions: ActionNameList,
    /// Whether the `actions` should be allowed or disallowed.
    pub allowed: bool,
}

/// A single element of a [`ResourceName`]
#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub enum Identifier<'a> {
    /// When checking for allowed permissions, allow any match where this identifier is used.
    Any,
    /// An integer identifier.
    Integer(u64),
    /// A string identifier.
    String(Cow<'a, str>),
}

impl<'a> From<u64> for Identifier<'a> {
    fn from(id: u64) -> Self {
        Self::Integer(id)
    }
}

impl<'a> From<&'a str> for Identifier<'a> {
    fn from(id: &'a str) -> Self {
        Self::String(Cow::Borrowed(id))
    }
}

impl<'a> From<String> for Identifier<'a> {
    fn from(id: String) -> Self {
        Self::String(Cow::Owned(id))
    }
}

/// A list of [`ActionName`]s.
#[derive(Debug)]
pub enum ActionNameList {
    /// A specific list of names.
    List(Vec<ActionName>),
    /// All actions.
    All,
}

impl<T> From<T> for ActionNameList
where
    T: Action,
{
    fn from(action: T) -> Self {
        Self::List(vec![action.name()])
    }
}

impl From<ActionName> for ActionNameList {
    fn from(name: ActionName) -> Self {
        Self::List(vec![name])
    }
}

impl From<Vec<ActionName>> for ActionNameList {
    fn from(names: Vec<ActionName>) -> Self {
        Self::List(names)
    }
}

/// A unique name/identifier of a resource.
#[derive(Default, Debug)]
pub struct ResourceName(Vec<Identifier<'static>>);

impl ResourceName {
    /// Creates a `ResourceName` that matches any identifier.
    #[must_use]
    pub fn any() -> Self {
        Self::named(Identifier::Any)
    }

    /// Creates a `ResourceName` with `name`.
    #[must_use]
    pub fn named<I: Into<Identifier<'static>>>(name: I) -> Self {
        Self(vec![name.into()])
    }

    /// Adds another name segment.
    #[must_use]
    pub fn and<I: Into<Identifier<'static>>>(mut self, name: I) -> Self {
        self.0.push(name.into());
        self
    }
}

impl AsRef<[Identifier<'static>]> for ResourceName {
    fn as_ref(&self) -> &[Identifier<'static>] {
        &self.0
    }
}

impl IntoIterator for ResourceName {
    type Item = Identifier<'static>;

    type IntoIter = std::vec::IntoIter<Identifier<'static>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
