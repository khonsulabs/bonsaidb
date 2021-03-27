use std::{
    borrow::Cow,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use view::Map;

use crate::{
    document::Document,
    schema::{collection, view, Collection, Database, MapResult, Schema, View},
};

#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct Basic {
    pub value: String,
    pub category: Option<String>,
    pub parent_id: Option<u64>,
}

impl Basic {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            category: None,
            parent_id: None,
        }
    }

    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    #[must_use]
    pub const fn with_parent_id(mut self, parent_id: u64) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

impl Collection for Basic {
    fn id() -> collection::Id {
        collection::Id::from("tests.basic")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view(BasicCount);
        schema.define_view(BasicByParentId);
        schema.define_view(BasicByCategory)
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

#[derive(Debug)]
pub struct BasicByCategory;

impl View for BasicByCategory {
    type Collection = Basic;
    type MapKey = String;
    type MapValue = usize;
    type Reduce = usize;

    fn version(&self) -> usize {
        0
    }

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-category")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::MapKey, Self::MapValue> {
        let contents = document.contents::<Basic>()?;
        if let Some(category) = &contents.category {
            Ok(Some(
                document.emit_key_and_value(category.to_lowercase(), 1),
            ))
        } else {
            Ok(None)
        }
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
            if err.kind() != ErrorKind::NotFound {
                eprintln!("Failed to clean up temporary folder: {:?}", err);
            }
        }
    }
}

impl AsRef<Path> for TestDirectory {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug)]
pub struct BasicCollectionWithNoViews;

impl Collection for BasicCollectionWithNoViews {
    fn id() -> collection::Id {
        Basic::id()
    }

    fn define_views(_schema: &mut Schema) {}
}

#[derive(Debug)]
pub struct UnassociatedCollection;

impl Collection for UnassociatedCollection {
    fn id() -> collection::Id {
        collection::Id::from("unassociated")
    }

    fn define_views(_schema: &mut Schema) {}
}
