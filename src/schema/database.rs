use crate::schema::Collection;

pub trait Database {
    fn add_collections(collections: &mut DatabaseCollections);
}

#[derive(Default)]
pub struct DatabaseCollections {
    collections: Vec<Box<dyn Collection>>,
}

impl DatabaseCollections {
    pub fn push<C: Collection + 'static>(&mut self, collection: C) {
        self.collections.push(collection.boxed());
    }
}
