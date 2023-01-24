use std::fmt::Display;

use bonsaidb_core::arc_bytes::serde::Bytes;
use bonsaidb_core::document::Header;
use bonsaidb_core::schema::CollectionName;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ViewEntry {
    pub view_version: u64,
    pub key: Bytes,
    pub mappings: Vec<EntryMapping>,

    pub reduced_value: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntryMapping {
    pub source: Header,
    pub value: Bytes,
}

pub mod integrity_scanner;
pub mod mapper;

pub fn view_entries_tree_name(view_name: &impl Display) -> String {
    format!("view.{view_name:#}")
}

/// Used to store Document ID -> Key mappings, so that when a document is updated, we can remove the old entry.
pub fn view_document_map_tree_name(view_name: &impl Display) -> String {
    format!("view.{view_name:#}.document-map")
}

pub fn view_invalidated_docs_tree_name(view_name: &impl Display) -> String {
    format!("view.{view_name:#}.invalidated")
}

pub fn view_versions_tree_name(collection: &CollectionName) -> String {
    format!("view-versions.{collection:#}")
}
