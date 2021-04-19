use std::fmt::Display;

use pliantdb_core::schema::CollectionName;
use serde::{Deserialize, Serialize};

use self::{integrity_scanner::IntegrityScan, mapper::Map};

#[derive(Serialize, Deserialize)]
pub struct ViewEntry {
    pub view_version: u64,
    pub mappings: Vec<EntryMapping>,
    pub reduced_value: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct EntryMapping {
    pub source: u64,
    pub value: Vec<u8>,
}

pub mod integrity_scanner;
pub mod mapper;

pub fn view_entries_tree_name(collection: &CollectionName, view_name: &impl Display) -> String {
    format!("{}::{}", collection, view_name)
}

/// Used to store Document ID -> Key mappings, so that when a document is updated, we can remove the old entry.
pub fn view_document_map_tree_name(
    collection: &CollectionName,
    view_name: &impl Display,
) -> String {
    format!("{}::{}::document-map", collection, view_name)
}

pub fn view_invalidated_docs_tree_name(
    collection: &CollectionName,
    view_name: &impl Display,
) -> String {
    format!("{}::{}::invalidated", collection, view_name)
}

pub fn view_omitted_docs_tree_name(
    collection: &CollectionName,
    view_name: &impl Display,
) -> String {
    format!("{}::{}::omitted", collection, view_name)
}

pub fn view_versions_tree_name(collection: &CollectionName) -> String {
    format!("{}::view-versions", collection)
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Task {
    IntegrityScan(IntegrityScan),
    ViewMap(Map),
}
