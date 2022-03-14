use bonsaidb::{
    core::{
        connection::AsyncConnection,
        document::{BorrowedDocument, Emit},
        key::EnumKey,
        schema::{
            view::map::ViewMappedValue, Collection, ReduceResult, SerializedCollection, View,
            ViewMapResult, ViewSchema,
        },
        Error,
    },
    local::{
        config::{Builder, StorageConfiguration},
        AsyncDatabase,
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
#[derive(Serialize, Deserialize, Debug, Collection)]
#[collection(name = "blog-post", views = [BlogPostsByCategory])]
pub struct BlogPost {
    pub title: String,
    pub body: String,
    pub category: Option<Category>,
}
// ANCHOR_END: struct

// ANCHOR: view

#[derive(Debug, Clone, View)]
#[view(collection = BlogPost, key = Option<Category>, value = u32, name = "by-category")]
pub struct BlogPostsByCategory;

impl ViewSchema for BlogPostsByCategory {
    type View = Self;

    fn map(&self, document: &BorrowedDocument<'_>) -> ViewMapResult<Self::View> {
        let post = BlogPost::document_contents(document)?;
        document.header.emit_key_and_value(post.category, 1)
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

#[allow(unused_variables)]
#[tokio::test]
async fn example() -> Result<(), Error> {
    drop(tokio::fs::remove_dir_all("example.bonsaidb").await);
    let db = AsyncDatabase::open::<BlogPost>(StorageConfiguration::new("example.bonsaidb")).await?;
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
