use std::fmt::Display;

use bonsaidb_core::{document::Header, schema::CollectionName};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewEntry {
    pub view_version: u64,
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    pub mappings: Vec<EntryMapping>,
    #[serde(with = "serde_bytes")]
    pub reduced_value: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntryMapping {
    pub source: Header,
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

pub mod integrity_scanner;
pub mod mapper;

pub fn view_entries_tree_name(view_name: &impl Display) -> String {
    format!("view.{}", view_name)
}

/// Used to store Document ID -> Key mappings, so that when a document is updated, we can remove the old entry.
pub fn view_document_map_tree_name(view_name: &impl Display) -> String {
    format!("view.{}.document-map", view_name)
}

pub fn view_invalidated_docs_tree_name(view_name: &impl Display) -> String {
    format!("view.{}.invalidated", view_name)
}

pub fn view_omitted_docs_tree_name(view_name: &impl Display) -> String {
    format!("view.{}.omitted", view_name)
}

pub fn view_versions_tree_name(collection: &CollectionName) -> String {
    format!("view-versions.{}", collection)
}
