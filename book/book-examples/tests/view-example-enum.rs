use bonsaidb::{
    core::{
        connection::Connection,
        document::Document,
        schema::{
            view::{self, EnumKey},
            Collection, CollectionName, DefaultSerialization, DefaultViewSerialization,
            InvalidNameError, MapResult, MappedValue, Name, View,
        },
        Error,
    },
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use serde::{Deserialize, Serialize};

// ANCHOR: enum
#[derive(
    Serialize, Deserialize, Debug, num_derive::FromPrimitive, num_derive::ToPrimitive, Clone,
)]
pub enum Category {
    Rust,
    Cooking,
}

impl EnumKey for Category {}
// ANCHOR_END: enum

// ANCHOR: struct
#[derive(Serialize, Deserialize, Debug)]
pub struct BlogPost {
    pub title: String,
    pub body: String,
    pub category: Option<Category>,
}
// ANCHOR_END: struct

impl Collection for BlogPost {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("view-example", "blog-post")
    }

    fn define_views(schema: &mut bonsaidb::core::schema::Schematic) -> Result<(), Error> {
        schema.define_view(BlogPostsByCategory)
    }
}

impl DefaultSerialization for BlogPost {}

#[derive(Debug)]
pub struct BlogPostsByCategory;

// ANCHOR: view
impl View for BlogPostsByCategory {
    type Collection = BlogPost;
    type Key = Option<Category>;
    type Value = u32;

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let post = document.contents::<BlogPost>()?;
        Ok(document.emit_key_and_value(post.category, 1))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|mapping| mapping.value).sum())
    }

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-category")
    }
}
// ANCHOR_END: view

impl DefaultViewSerialization for BlogPostsByCategory {}

#[allow(unused_variables)]
#[tokio::test]
async fn example() -> Result<(), Error> {
    let db = Database::open::<BlogPost>(StorageConfiguration::new("example.bonsaidb")).await?;
    // ANCHOR: query_with_docs
    let rust_posts = db
        .view::<BlogPostsByCategory>()
        .with_key(Some(Category::Rust))
        .query_with_docs()
        .await?;
    // ANCHOR_END: query_with_docs
    // ANCHOR: reduce_one_key
    let rust_post_count = db
        .view::<BlogPostsByCategory>()
        .with_key(Some(Category::Rust))
        .reduce()
        .await?;
    // ANCHOR_END: reduce_one_key
    // ANCHOR: reduce_multiple_keys
    let total_post_count = db.view::<BlogPostsByCategory>().reduce().await?;
    // ANCHOR_END: reduce_multiple_keys
    Ok(())
}
