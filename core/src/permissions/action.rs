/// An action that can be allowed or disalled.
pub trait Action {
    /// The full name of this action.
    fn name(&self) -> ActionName;
}

impl Action for () {
    fn name(&self) -> ActionName {
        ActionName::default()
    }
}

/// A unique name of an action.
#[derive(Default, Debug)]
#[allow(clippy::module_name_repetitions)] // exported without the module name
pub struct ActionName(pub Vec<&'static str>);

impl From<Vec<&'static str>> for ActionName {
    fn from(inner: Vec<&'static str>) -> Self {
        Self(inner)
    }
}

impl IntoIterator for ActionName {
    type Item = &'static str;

    type IntoIter = std::vec::IntoIter<&'static str>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub use pliantdb_macros::Action;
