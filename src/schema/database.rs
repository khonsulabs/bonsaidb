use std::{
    any::{Any, TypeId},
    borrow::Cow,
    collections::HashMap,
};

use crate::schema::collection::{self, Collection};

use super::View;

pub trait Database: Send + Sync {
    fn define_collections(collections: &mut Schema);
}

trait ThreadsafeAny: Any + Send + Sync {}

impl<T> ThreadsafeAny for T where T: Any + Send + Sync {}

#[derive(Default)]
pub struct Schema {
    collections: HashMap<TypeId, collection::Id>,
    views: HashMap<TypeId, Cow<'static, str>>,
}

impl Schema {
    pub fn define_collection<C: Collection + 'static>(&mut self) {
        self.collections.insert(TypeId::of::<C>(), C::id());
        C::define_views(self)
    }

    pub fn define_view<V: View<C> + 'static, C: Collection + 'static>(&mut self) {
        self.views.insert(TypeId::of::<V>(), V::name());
    }

    #[must_use]
    pub fn contains<C: Collection + 'static>(&self) -> bool {
        self.collections.contains_key(&TypeId::of::<C>())
    }
}
