use std::borrow::Cow;

use pliantdb_core::schema::{collection, map};

use self::integrity_scanner::IntegrityScan;
pub struct MapEntry<'a> {
    pub view_version: usize,
    pub maps: Vec<map::Serialized<'a>>,
}

pub mod integrity_scanner;

pub fn view_entries_tree_name(collection: &collection::Id, view_name: &str) -> String {
    format!("{}::{}", collection.0, view_name)
}

/// Used to store Document ID -> Key mappings, so that when a document is updated, we can remove the old entry.
pub fn view_document_map_tree_name(collection: &collection::Id, view_name: &str) -> String {
    format!("{}::{}::document-map", collection.0, view_name)
}

pub fn view_invalidated_docs_tree_name(collection: &collection::Id, view_name: &str) -> String {
    format!("{}::{}::invalidated", collection.0, view_name)
}

pub fn view_omitted_docs_tree_name(collection: &collection::Id, view_name: &str) -> String {
    format!("{}::{}::omitted", collection.0, view_name)
}

pub enum Task<'a> {
    IntegrityScan(Cow<'a, IntegrityScan>),
    // ViewMap(Cow<'a, IntegrityScan>),
}
