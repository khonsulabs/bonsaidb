use bonsaidb::{
    core::{
        connection::Connection,
        document::{BorrowedDocument, Document},
        schema::{
            view::{map::ViewMappedValue, EnumKey},
            Collection, CollectionName, DefaultSerialization, DefaultViewSerialization, Name,
            ReduceResult, View, ViewMapResult, ViewSchema,
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
    fn collection_name() -> CollectionName {
        CollectionName::new("view-example", "blog-post")
    }

    fn define_views(schema: &mut bonsaidb::core::schema::Schematic) -> Result<(), Error> {
        schema.define_view(BlogPostsByCategory)
    }
}

impl DefaultSerialization for BlogPost {}

#[derive(Debug, Clone)]
pub struct BlogPostsByCategory;

// ANCHOR: view
impl View for BlogPostsByCategory {
    type Collection = BlogPost;
    type Key = Option<Category>;
    type Value = u32;

    fn name(&self) -> Name {
        Name::new("by-category")
    }
}

impl ViewSchema for BlogPostsByCategory {
    type View = Self;

    fn map(&self, document: &BorrowedDocument<'_>) -> ViewMapResult<Self::View> {
        let post = document.contents::<BlogPost>()?;
        Ok(document.emit_key_and_value(post.category, 1))
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|mapping| mapping.value).sum())
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
