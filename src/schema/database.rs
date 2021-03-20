use std::{
    any::{Any, TypeId},
    borrow::Cow,
    collections::HashMap,
};

use crate::schema::Collection;

pub trait Database: Send + Sync {
    fn name(&self) -> Cow<'static, str>;
    fn define_collections(&self, collections: &mut Collections);
}

#[derive(Default)]
pub struct Collections {
    collections: HashMap<TypeId, Box<dyn Collection>>,
}

impl Collections {
    pub fn push<C: Collection + 'static>(&mut self, collection: C) {
        self.collections
            .insert(collection.type_id(), collection.boxed());
    }

    #[must_use]
    pub fn get<C: Collection + 'static>(&self) -> Option<&'_ dyn Collection> {
        self.collections.get(&TypeId::of::<C>()).map(AsRef::as_ref)
    }
}
