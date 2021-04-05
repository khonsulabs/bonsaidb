use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use crate::schema::{
    collection::{self, Collection},
    view, Serialized, View,
};

/// A collection of defined collections and views.
#[derive(Default, Debug)]
pub struct Schematic {
    contained_collections: HashSet<collection::Id>,
    collections_by_type_id: HashMap<TypeId, collection::Id>,
    views: HashMap<TypeId, Box<dyn view::Serialized>>,
    views_by_name: HashMap<String, TypeId>,
    views_by_collection: HashMap<collection::Id, Vec<TypeId>>,
}

impl Schematic {
    /// Adds the collection `C` and its views.
    pub fn define_collection<C: Collection + 'static>(&mut self) {
        self.collections_by_type_id
            .insert(TypeId::of::<C>(), C::collection_id());
        self.contained_collections.insert(C::collection_id());
        C::define_views(self)
    }

    /// Adds the view `V`.
    pub fn define_view<V: View + 'static>(&mut self, view: V) {
        let name = view.name();
        let collection = view.collection();
        self.views.insert(TypeId::of::<V>(), Box::new(view));
        self.views_by_name
            .insert(name.to_string(), TypeId::of::<V>());
        let views = self
            .views_by_collection
            .entry(collection)
            .or_insert_with(Vec::new);
        views.push(TypeId::of::<V>());
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn contains<C: Collection + 'static>(&self) -> bool {
        self.collections_by_type_id.contains_key(&TypeId::of::<C>())
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn contains_collection_id(&self, collection: &collection::Id) -> bool {
        self.contained_collections.contains(collection)
    }

    /// Looks up a [`view::Serialized`] by name.
    #[must_use]
    pub fn view_by_name(&self, name: &str) -> Option<&'_ dyn view::Serialized> {
        self.views_by_name
            .get(name)
            .and_then(|type_id| self.views.get(type_id))
            .map(AsRef::as_ref)
    }

    /// Looks up a [`view::Serialized`] through the the type `V`.
    #[must_use]
    pub fn view<V: View + 'static>(&self) -> Option<&'_ dyn view::Serialized> {
        self.views.get(&TypeId::of::<V>()).map(AsRef::as_ref)
    }

    /// Iterates over all registered views.
    pub fn views(&self) -> impl Iterator<Item = &'_ dyn view::Serialized> {
        self.views.values().map(AsRef::as_ref)
    }

    /// Iterates over all views that belong to `collection`.
    #[must_use]
    pub fn views_in_collection(
        &self,
        collection: &collection::Id,
    ) -> Option<Vec<&'_ dyn view::Serialized>> {
        self.views_by_collection.get(collection).map(|view_ids| {
            view_ids
                .iter()
                .filter_map(|id| self.views.get(id).map(AsRef::as_ref))
                .collect()
        })
    }
}

#[test]
fn schema_tests() {
    use crate::{
        schema::Schema,
        test_util::{Basic, BasicCount, BasicSchema},
    };
    let mut schema = Schematic::default();
    BasicSchema::define_collections(&mut schema);

    assert_eq!(schema.collections_by_type_id.len(), 1);
    assert_eq!(
        schema.collections_by_type_id[&TypeId::of::<Basic>()],
        Basic::collection_id()
    );
    assert_eq!(schema.views.len(), 3);
    assert_eq!(
        schema.views[&TypeId::of::<BasicCount>()].name(),
        View::name(&BasicCount)
    );
}
