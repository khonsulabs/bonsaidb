use std::borrow::Cow;

use super::Schema;

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

pub trait Collection: Send + Sync {
    fn id() -> Id;
    fn define_views(schema: &mut Schema);
}
