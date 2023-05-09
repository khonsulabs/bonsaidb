use bonsaidb::core::connection::Connection;
use bonsaidb::core::document::{BorrowedDocument, Emit};
use bonsaidb::core::schema::view::map::ViewMappedValue;
use bonsaidb::core::schema::{
    Collection, MapReduce, ReduceResult, SerializedCollection, View, ViewMapResult, ViewSchema,
};
use bonsaidb::core::Error;
use bonsaidb::local::config::{Builder, StorageConfiguration};
use bonsaidb::local::Database;
use serde::{Deserialize, Serialize};

// ANCHOR: struct
#[derive(Serialize, Deserialize, Debug, Collection)]
#[collection(name = "blog-post", views = [BlogPostsByCategory])]
pub struct BlogPost {
    pub title: String,
    pub body: String,
    pub category: Option<String>,
}
// ANCHOR_END: struct

// ANCHOR: view
#[derive(Debug, Clone, View, ViewSchema)]
#[view(collection = BlogPost, key = Option<String>, value = u32, name = "by-category")]
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
    // ANCHOR: insert_data
    BlogPost {
        title: String::from("New version of BonsaiDb released"),
        body: String::from("..."),
        category: Some(String::from("Rust")),
    }
    .push_into(&db)?;

    BlogPost {
        title: String::from("New Rust version released"),
        body: String::from("..."),
        category: Some(String::from("Rust")),
    }
    .push_into(&db)?;

    BlogPost {
        title: String::from("Check out this great cinnamon roll recipe"),
        body: String::from("..."),
        category: Some(String::from("Cooking")),
    }
    .push_into(&db)?;
    // ANCHOR_END: insert_data
    // ANCHOR: query_with_docs
    let rust_posts = db
        .view::<BlogPostsByCategory>()
        .with_key(&Some(String::from("Rust")))
        .query_with_docs()?;
    for mapping in &rust_posts {
        let post = BlogPost::document_contents(mapping.document)?;
        println!(
            "Retrieved post #{} \"{}\"",
            mapping.document.header.id, post.title
        );
    }
    // ANCHOR_END: query_with_docs
    assert_eq!(rust_posts.len(), 2);
    // ANCHOR: query_with_collection_docs
    let rust_posts = db
        .view::<BlogPostsByCategory>()
        .with_key(&Some(String::from("Rust")))
        .query_with_collection_docs()?;
    for mapping in &rust_posts {
        println!(
            "Retrieved post #{} \"{}\"",
            mapping.document.header.id, mapping.document.contents.title
        );
    }
    // ANCHOR_END: query_with_collection_docs
    assert_eq!(rust_posts.len(), 2);
    // ANCHOR: reduce_one_key
    let rust_post_count = db
        .view::<BlogPostsByCategory>()
        .with_key(&Some(String::from("Rust")))
        .reduce()?;
    assert_eq!(rust_post_count, 2);
    // ANCHOR_END: reduce_one_key
    // ANCHOR: reduce_multiple_keys
    let total_post_count = db.view::<BlogPostsByCategory>().reduce()?;
    assert_eq!(total_post_count, 3);
    // ANCHOR_END: reduce_multiple_keys
    Ok(())
}
