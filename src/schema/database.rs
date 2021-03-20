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

trait ThreadsafeAny: Any + Send + Sync {}

impl<T> ThreadsafeAny for T where T: Any + Send + Sync {}

#[derive(Default)]
pub struct Collections {
    collections: HashMap<TypeId, Box<dyn ThreadsafeAny>>,
}

impl Collections {
    pub fn push<C: Collection + 'static>(&mut self, collection: C) {
        self.collections
            .insert(collection.type_id(), Box::new(collection));
    }

    #[must_use]
    pub fn get<C: Collection + 'static>(&self) -> Option<&'_ C> {
        self.collections
            .get(&TypeId::of::<C>())
            .map(|collection| Any::downcast_ref(collection).unwrap())
    }
}
