use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use crate::{
    schema::{
        collection::Collection,
        view::{self, Serialized},
        CollectionName, View, ViewName,
    },
    Error,
};

/// A collection of defined collections and views.
#[derive(Default, Debug)]
pub struct Schematic {
    contained_collections: HashSet<CollectionName>,
    collections_by_type_id: HashMap<TypeId, CollectionName>,
    views: HashMap<TypeId, Box<dyn view::Serialized>>,
    views_by_name: HashMap<ViewName, TypeId>,
    views_by_collection: HashMap<CollectionName, Vec<TypeId>>,
    unique_views_by_collection: HashMap<CollectionName, Vec<TypeId>>,
}

impl Schematic {
    /// Adds the collection `C` and its views.
    pub fn define_collection<C: Collection + 'static>(&mut self) -> Result<(), Error> {
        self.collections_by_type_id
            .insert(TypeId::of::<C>(), C::collection_name()?);
        self.contained_collections.insert(C::collection_name()?);
        C::define_views(self)
    }

    /// Adds the view `V`.
    pub fn define_view<V: View + 'static>(&mut self, view: V) -> Result<(), Error> {
        let name = view.view_name()?;
        let collection = view.collection()?;
        let unique = view.unique();
        self.views.insert(TypeId::of::<V>(), Box::new(view));
        // TODO check for name collision
        self.views_by_name.insert(name, TypeId::of::<V>());
        if unique {
            let unique_views = self
                .unique_views_by_collection
                .entry(collection.clone())
                .or_insert_with(Vec::new);
            unique_views.push(TypeId::of::<V>());
        }
        let views = self
            .views_by_collection
            .entry(collection)
            .or_insert_with(Vec::new);
        views.push(TypeId::of::<V>());

        Ok(())
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn contains<C: Collection + 'static>(&self) -> bool {
        self.collections_by_type_id.contains_key(&TypeId::of::<C>())
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn contains_collection_id(&self, collection: &CollectionName) -> bool {
        self.contained_collections.contains(collection)
    }

    /// Looks up a [`view::Serialized`] by name.
    #[must_use]
    pub fn view_by_name(&self, name: &ViewName) -> Option<&'_ dyn view::Serialized> {
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
        collection: &CollectionName,
    ) -> Option<Vec<&'_ dyn view::Serialized>> {
        self.views_by_collection.get(collection).map(|view_ids| {
            view_ids
                .iter()
                .filter_map(|id| self.views.get(id).map(AsRef::as_ref))
                .collect()
        })
    }

    /// Iterates over all views that are unique that belong to `collection`.
    #[must_use]
    pub fn unique_views_in_collection(
        &self,
        collection: &CollectionName,
    ) -> Option<Vec<&'_ dyn view::Serialized>> {
        self.unique_views_by_collection
            .get(collection)
            .map(|view_ids| {
                view_ids
                    .iter()
                    .filter_map(|id| self.views.get(id).map(AsRef::as_ref))
                    .collect()
            })
    }
}

#[test]
fn schema_tests() -> anyhow::Result<()> {
    use crate::{
        schema::Schema,
        test_util::{Basic, BasicCount, BasicSchema},
    };
    let mut schema = Schematic::default();
    BasicSchema::define_collections(&mut schema)?;

    assert_eq!(schema.collections_by_type_id.len(), 2);
    assert_eq!(
        schema.collections_by_type_id[&TypeId::of::<Basic>()],
        Basic::collection_name()?
    );
    assert_eq!(schema.views.len(), 4);
    assert_eq!(
        schema.views[&TypeId::of::<BasicCount>()].view_name()?,
        View::view_name(&BasicCount)?
    );

    Ok(())
}
