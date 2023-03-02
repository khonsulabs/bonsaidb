use std::any::TypeId;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::marker::PhantomData;

use derive_where::derive_where;

use crate::document::{BorrowedDocument, DocumentId, KeyId};
use crate::key::Key;
use crate::schema::collection::Collection;
use crate::schema::view::map::{self, MappedValue};
use crate::schema::view::{self, Serialized, SerializedView, ViewSchema};
use crate::schema::{CollectionName, Schema, SchemaName, View, ViewName};
use crate::Error;

/// A collection of defined collections and views.
#[derive(Debug)]
pub struct Schematic {
    /// The name of the schema this was built from.
    pub name: SchemaName,
    contained_collections: HashSet<CollectionName>,
    collections_by_type_id: HashMap<TypeId, CollectionName>,
    collection_encryption_keys: HashMap<CollectionName, KeyId>,
    collection_id_generators: HashMap<CollectionName, Box<dyn IdGenerator>>,
    views: HashMap<TypeId, Box<dyn view::Serialized>>,
    views_by_name: HashMap<ViewName, TypeId>,
    views_by_collection: HashMap<CollectionName, Vec<TypeId>>,
    eager_views_by_collection: HashMap<CollectionName, Vec<TypeId>>,
}

impl Schematic {
    /// Returns an initialized version from `S`.
    pub fn from_schema<S: Schema + ?Sized>() -> Result<Self, Error> {
        let mut schematic = Self {
            name: S::schema_name(),
            contained_collections: HashSet::new(),
            collections_by_type_id: HashMap::new(),
            collection_encryption_keys: HashMap::new(),
            collection_id_generators: HashMap::new(),
            views: HashMap::new(),
            views_by_name: HashMap::new(),
            views_by_collection: HashMap::new(),
            eager_views_by_collection: HashMap::new(),
        };
        S::define_collections(&mut schematic)?;
        Ok(schematic)
    }

    /// Adds the collection `C` and its views.
    pub fn define_collection<C: Collection + 'static>(&mut self) -> Result<(), Error> {
        let name = C::collection_name();
        if self.contained_collections.contains(&name) {
            Err(Error::CollectionAlreadyDefined)
        } else {
            self.collections_by_type_id
                .insert(TypeId::of::<C>(), name.clone());
            if let Some(key) = C::encryption_key() {
                self.collection_encryption_keys.insert(name.clone(), key);
            }
            self.collection_id_generators
                .insert(name.clone(), Box::<KeyIdGenerator<C>>::default());
            self.contained_collections.insert(name);
            C::define_views(self)
        }
    }

    /// Adds the view `V`.
    pub fn define_view<V: ViewSchema<View = V> + SerializedView + Clone + 'static>(
        &mut self,
        view: V,
    ) -> Result<(), Error> {
        self.define_view_with_schema(view.clone(), view)
    }

    /// Adds the view `V`.
    pub fn define_view_with_schema<
        V: SerializedView + 'static,
        S: ViewSchema<View = V> + 'static,
    >(
        &mut self,
        view: V,
        schema: S,
    ) -> Result<(), Error> {
        let instance = ViewInstance { view, schema };
        let name = instance.view_name();
        if self.views_by_name.contains_key(&name) {
            return Err(Error::ViewAlreadyRegistered(name));
        }

        let collection = instance.collection();
        let eager = instance.eager();
        self.views.insert(TypeId::of::<V>(), Box::new(instance));
        self.views_by_name.insert(name, TypeId::of::<V>());

        if eager {
            let unique_views = self
                .eager_views_by_collection
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
    pub fn contains_collection<C: Collection + 'static>(&self) -> bool {
        self.collections_by_type_id.contains_key(&TypeId::of::<C>())
    }

    /// Returns `true` if this schema contains the collection `C`.
    #[must_use]
    pub fn contains_collection_name(&self, collection: &CollectionName) -> bool {
        self.contained_collections.contains(collection)
    }

    /// Returns the next id in sequence for the collection, if the primary key
    /// type supports the operation and the next id would not overflow.
    pub fn next_id_for_collection(
        &self,
        collection: &CollectionName,
        id: Option<DocumentId>,
    ) -> Result<DocumentId, Error> {
        let generator = self
            .collection_id_generators
            .get(collection)
            .ok_or(Error::CollectionNotFound)?;
        generator.next_id(id)
    }

    /// Looks up a [`view::Serialized`] by name.
    pub fn view_by_name(&self, name: &ViewName) -> Result<&'_ dyn view::Serialized, Error> {
        self.views_by_name
            .get(name)
            .and_then(|type_id| self.views.get(type_id))
            .map(AsRef::as_ref)
            .ok_or(Error::ViewNotFound)
    }

    /// Looks up a [`view::Serialized`] through the the type `V`.
    pub fn view<V: View + 'static>(&self) -> Result<&'_ dyn view::Serialized, Error> {
        self.views
            .get(&TypeId::of::<V>())
            .map(AsRef::as_ref)
            .ok_or(Error::ViewNotFound)
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

    /// Iterates over all views that are eagerly updated that belong to
    /// `collection`.
    #[must_use]
    pub fn eager_views_in_collection(
        &self,
        collection: &CollectionName,
    ) -> Option<Vec<&'_ dyn view::Serialized>> {
        self.eager_views_by_collection
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

#[derive(Debug)]
struct ViewInstance<V, S> {
    view: V,
    schema: S,
}

impl<V, S> Serialized for ViewInstance<V, S>
where
    V: SerializedView,
    S: ViewSchema<View = V>,
    <V as View>::Key: 'static,
{
    fn collection(&self) -> CollectionName {
        <<V as View>::Collection as Collection>::collection_name()
    }

    fn unique(&self) -> bool {
        self.schema.unique()
    }

    fn lazy(&self) -> bool {
        self.schema.lazy()
    }

    fn version(&self) -> u64 {
        self.schema.version()
    }

    fn view_name(&self) -> ViewName {
        self.view.view_name()
    }

    fn map(&self, document: &BorrowedDocument<'_>) -> Result<Vec<map::Serialized>, view::Error> {
        let map = self.schema.map(document)?;

        map.into_iter()
            .map(|map| map.serialized::<V>())
            .collect::<Result<Vec<_>, view::Error>>()
    }

    fn reduce(&self, mappings: &[(&[u8], &[u8])], rereduce: bool) -> Result<Vec<u8>, view::Error> {
        let mappings = mappings
            .iter()
            .map(
                |(key, value)| match <V::Key as Key>::from_ord_bytes(Cow::Borrowed(key)) {
                    Ok(key) => {
                        let value = V::deserialize(value)?;
                        Ok(MappedValue::new(key, value))
                    }
                    Err(err) => Err(view::Error::key_serialization(err)),
                },
            )
            .collect::<Result<Vec<_>, view::Error>>()?;

        let reduced_value = self.schema.reduce(&mappings, rereduce)?;

        V::serialize(&reduced_value).map_err(view::Error::from)
    }
}

pub trait IdGenerator: Debug + Send + Sync {
    fn next_id(&self, id: Option<DocumentId>) -> Result<DocumentId, Error>;
}

#[derive(Debug)]
#[derive_where(Default)]
pub struct KeyIdGenerator<C: Collection>(PhantomData<C>);

impl<C> IdGenerator for KeyIdGenerator<C>
where
    C: Collection,
{
    fn next_id(&self, id: Option<DocumentId>) -> Result<DocumentId, Error> {
        let key = id.map(|id| id.deserialize::<C::PrimaryKey>()).transpose()?;
        let key = if let Some(key) = key {
            key
        } else {
            <C::PrimaryKey as Key<'_>>::first_value()
                .map_err(|err| Error::DocumentPush(C::collection_name(), err))?
        };
        let next_value = key
            .next_value()
            .map_err(|err| Error::DocumentPush(C::collection_name(), err))?;
        DocumentId::new(&next_value)
    }
}

#[test]
fn schema_tests() -> anyhow::Result<()> {
    use crate::test_util::{Basic, BasicCount};
    let schema = Schematic::from_schema::<Basic>()?;

    assert_eq!(schema.collections_by_type_id.len(), 1);
    assert_eq!(
        schema.collections_by_type_id[&TypeId::of::<Basic>()],
        Basic::collection_name()
    );
    assert_eq!(schema.views.len(), 5);
    assert_eq!(
        schema.views[&TypeId::of::<BasicCount>()].view_name(),
        View::view_name(&BasicCount)
    );

    Ok(())
}
