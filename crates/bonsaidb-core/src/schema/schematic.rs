use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use crate::{
    document::KeyId,
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
    collection_encryption_keys: HashMap<CollectionName, KeyId>,
    views: HashMap<TypeId, Box<dyn view::Serialized>>,
    views_by_name: HashMap<ViewName, TypeId>,
    views_by_collection: HashMap<CollectionName, Vec<TypeId>>,
    unique_views_by_collection: HashMap<CollectionName, Vec<TypeId>>,
}

impl Schematic {
    /// Adds the collection `C` and its views.
    pub fn define_collection<C: Collection + 'static>(&mut self) -> Result<(), Error> {
        let name = C::collection_name()?;
        if self.contained_collections.contains(&name) {
            Err(Error::CollectionAlreadyDefined)
        } else {
            self.collections_by_type_id
                .insert(TypeId::of::<C>(), name.clone());
            if let Some(key) = C::encryption_key() {
                self.collection_encryption_keys.insert(name.clone(), key);
            }
            self.contained_collections.insert(name);
            C::define_views(self)
        }
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

    /// Returns a collection's default encryption key, if one was defined.
    #[must_use]
    pub fn encryption_key_for_collection(&self, collection: &CollectionName) -> Option<&KeyId> {
        self.collection_encryption_keys.get(collection)
    }

    /// Returns a list of all collections contained in this schematic.
    #[must_use]
    pub fn collections(&self) -> Vec<CollectionName> {
        self.contained_collections.iter().cloned().collect()
    }
}

#[test]
fn schema_tests() -> anyhow::Result<()> {
    use crate::{
        schema::Schema,
        test_util::{Basic, BasicCount},
    };
    let mut schema = Schematic::default();
    Basic::define_collections(&mut schema)?;

    assert_eq!(schema.collections_by_type_id.len(), 1);
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