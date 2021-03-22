use std::borrow::Cow;

use crate::schema::Schema;
use serde::{Deserialize, Serialize};

/// a unique collection id. Choose collection names that aren't likely to
/// conflict with others, so that if someone mixes collections from multiple
/// authors in a single database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Id(pub Cow<'static, str>);

impl From<&'static str> for Id {
    fn from(str: &'static str) -> Self {
        Self(Cow::from(str))
    }
}

impl From<String> for Id {
    fn from(str: String) -> Self {
        Self(Cow::from(str))
    }
}

/// a namespaced collection of `Document<Self>` items and views
pub trait Collection: Send + Sync {
    /// the `Id` of this collection
    fn id() -> Id;

    /// implementors define all of their `View`s in `schema`
    fn define_views(schema: &mut Schema);
}
