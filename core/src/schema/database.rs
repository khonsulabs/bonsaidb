use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt::Debug,
};

use crate::schema::{
    collection::{self, Collection},
    View,
};

use super::view;

/// Defines a group of collections that are stored into a single database.
pub trait Database: Send + Sync + Debug + 'static {
    /// Defines the `Collection`s into `schema`
    fn define_collections(schema: &mut Schema);
}

trait ThreadsafeAny: Any + Send + Sync {}

impl<T> ThreadsafeAny for T where T: Any + Send + Sync {}

/// A collection of defined collections and views.
#[derive(Default, Debug)]
pub struct Schema {
    collections: HashMap<TypeId, collection::Id>,
    views: HashMap<TypeId, Box<dyn view::Serialized>>,
    views_by_name: HashMap<String, TypeId>,
}

impl Schema {
    /// Adds the collection `C` and its views.
    pub fn define_collection<C: Collection + 'static>(&mut self) {
        self.collections.insert(TypeId::of::<C>(), C::id());
        C::define_views(self)
    }

    /// Adds the view `V`.
    pub fn define_view<V: View + 'static>(&mut self, view: V) {
        let name = view.name();
        self.views.insert(TypeId::of::<V>(), Box::new(view));
        self.views_by_name
            .insert(name.to_string(), TypeId::of::<V>());
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn contains<C: Collection + 'static>(&self) -> bool {
        self.collections.contains_key(&TypeId::of::<C>())
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn view_by_name(&self, name: &str) -> Option<&'_ dyn view::Serialized> {
        self.views_by_name
            .get(name)
            .and_then(|type_id| self.views.get(type_id))
            .map(AsRef::as_ref)
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn view<V: View + 'static>(&self) -> Option<&'_ dyn view::Serialized> {
        self.views.get(&TypeId::of::<V>()).map(AsRef::as_ref)
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

#[test]
fn schema_tests() {
    use crate::test_util::{BasicCollection, BasicCount, BasicDatabase};
    let mut schema = Schema::default();
    BasicDatabase::define_collections(&mut schema);

    assert_eq!(schema.collections.len(), 1);
    assert_eq!(
        schema.collections[&TypeId::of::<BasicCollection>()],
        BasicCollection::id()
    );
    assert_eq!(schema.views.len(), 1);
    assert_eq!(
        schema.views[&TypeId::of::<BasicCount>()].name(),
        BasicCount.name()
    );
}
