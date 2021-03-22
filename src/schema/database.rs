use std::{
    any::{Any, TypeId},
    borrow::Cow,
    collections::HashMap,
};

use super::View;
use crate::schema::collection::{self, Collection};

/// a trait that defines a group of collections that are stored into a single database
pub trait Database: Send + Sync {
    /// implementors must define their `Collection`s in `schema`
    fn define_collections(schema: &mut Schema);
}

trait ThreadsafeAny: Any + Send + Sync {}

impl<T> ThreadsafeAny for T where T: Any + Send + Sync {}

/// a collection of defined collections and views
#[derive(Default)]
pub struct Schema {
    collections: HashMap<TypeId, collection::Id>,
    views: HashMap<TypeId, Cow<'static, str>>,
}

impl Schema {
    /// adds the collection `C` and its views
    pub fn define_collection<C: Collection + 'static>(&mut self) {
        self.collections.insert(TypeId::of::<C>(), C::id());
        C::define_views(self)
    }

    /// adds the view `V`
    pub fn define_view<'k, V: View<'k, C> + 'static, C: Collection>(&mut self) {
        self.views.insert(TypeId::of::<V>(), V::name());
    }

    /// returns true if this schema contains the collection `C`
    #[must_use]
    pub fn contains<C: Collection + 'static>(&self) -> bool {
        self.collections.contains_key(&TypeId::of::<C>())
    }
}

impl<T> Database for T
where
    T: Collection + 'static,
{
    fn define_collections(collections: &mut Schema) {
        collections.define_collection::<Self>();
    }
}
