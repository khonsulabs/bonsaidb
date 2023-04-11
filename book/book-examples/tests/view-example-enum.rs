use bonsaidb::core::connection::Connection;
use bonsaidb::core::document::{BorrowedDocument, Emit};
use bonsaidb::core::key::Key;
use bonsaidb::core::schema::view::map::ViewMappedValue;
use bonsaidb::core::schema::{
    Collection, MapReduce, ReduceResult, SerializedCollection, View, ViewMapResult, ViewSchema,
};
use bonsaidb::core::Error;
use bonsaidb::local::config::{Builder, StorageConfiguration};
use bonsaidb::local::Database;
use serde::{Deserialize, Serialize};

// ANCHOR: enum
#[derive(Serialize, Deserialize, Eq, PartialEq, Debug, Key, Clone)]
pub enum Category {
    Rust,
    Cooking,
}
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

#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = BlogPost, key = Option<Category>, value = u32, name = "by-category")]
pub struct BlogPostsByCategory;

impl MapReduce for BlogPostsByCategory {
    fn map<'doc>(&self, document: &'doc BorrowedDocument<'_>) -> ViewMapResult<'doc, Self> {
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
#[test]
fn example() -> Result<(), Error> {
    drop(std::fs::remove_dir_all("example.bonsaidb"));
    let db = Database::open::<BlogPost>(StorageConfiguration::new("example.bonsaidb"))?;
    // ANCHOR: query_with_docs
    let rust_posts = db
        .view::<BlogPostsByCategory>()
        .with_key(&Some(Category::Rust))
        .query_with_docs()?;
    // ANCHOR_END: query_with_docs
    // ANCHOR: reduce_one_key
    let rust_post_count = db
        .view::<BlogPostsByCategory>()
        .with_key(&Some(Category::Rust))
        .reduce()?;
    // ANCHOR_END: reduce_one_key
    // ANCHOR: reduce_multiple_keys
    let total_post_count = db.view::<BlogPostsByCategory>().reduce()?;
    // ANCHOR_END: reduce_multiple_keys
    Ok(())
}
