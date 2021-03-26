use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use view::Map;

use crate::{
    document::Document,
    schema::{collection, view, Collection, Database, MapResult, Schema, View},
};

impl Collection for Basic {
    fn id() -> collection::Id {
        collection::Id::from("tests.basic")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view(BasicCount);
        schema.define_view(BasicByParentId);
    }
}

#[derive(Debug)]
pub struct BasicCount;

impl View for BasicCount {
    type Collection = Basic;
    type MapKey = ();
    type MapValue = usize;
    type Reduce = usize;

    fn version(&self) -> usize {
        0
    }

    fn name(&self) -> Cow<'static, str> {
        Cow::from("count")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::MapKey, Self::MapValue> {
        Ok(Some(document.emit_key_and_value((), 1)))
    }

    fn reduce(
        &self,
        mappings: &[Map<Self::MapKey, Self::MapValue>],
        _rereduce: bool,
    ) -> Result<Self::Reduce, view::Error> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Debug)]
pub struct BasicByParentId;

impl View for BasicByParentId {
    type Collection = Basic;
    type MapKey = Option<u64>;
    type MapValue = usize;
    type Reduce = usize;

    fn version(&self) -> usize {
        0
    }

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-parent-id")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::MapKey, Self::MapValue> {
        let contents = document.contents::<Basic>()?;
        Ok(Some(document.emit_key_and_value(contents.parent_id, 1)))
    }

    fn reduce(
        &self,
        mappings: &[Map<Self::MapKey, Self::MapValue>],
        _rereduce: bool,
    ) -> Result<Self::Reduce, view::Error> {
        Ok(mappings.iter().map(|map| map.value).sum())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct Basic {
    pub value: String,
    pub parent_id: Option<u64>,
}

#[derive(Debug)]
pub struct BasicDatabase;

impl Database for BasicDatabase {
    fn define_collections(schema: &mut Schema) {
        schema.define_collection::<Basic>();
    }
}

pub struct TestDirectory(pub PathBuf);

impl TestDirectory {
    pub fn new<S: AsRef<Path>>(name: S) -> Self {
        let path = std::env::temp_dir().join(name);
        if path.exists() {
            std::fs::remove_dir_all(&path).expect("error clearing temporary directory");
        }
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_dir_all(&self.0) {
            eprintln!("Failed to clean up temporary folder: {:?}", err);
        }
    }
}

impl AsRef<Path> for TestDirectory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}
