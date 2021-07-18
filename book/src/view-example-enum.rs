// ANCHOR: enum
#[derive(Serialize, Deserialize, Debug, num_derive::FromPrimitive, num_derive::ToPrimitive)]
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

// ANCHOR: view
pub trait BlogPostsByCategory {
    type Collection = BlogPost;
    type Key = Option<Category>;
    type Value = u32;

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let post = document.contents::<BlogPost>()?;
        Ok(Some(document.emit_key_and_value(post.category.clone(), 1)))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, Error> {
        Ok(mappings.iter().map(|mapping| mapping.value).sum())
    }
}
// ANCHOR_END: view

#[tokio::main]
async fn main() -> Result<(), Error> {
    let db =
        Database::<BlogPost>::open_local("example.bonsaidb", &Configuration::default()).await?;
    // ANCHOR: query_with_docs
    let rust_posts = db
        .view::<BlogPostsByCategory>()
        .with_key(Some(Category::Rust))
        .query_with_docs().await?;
    // ANCHOR_END: query_with_docs
    // ANCHOR: reduce_one_key
    let rust_post_count = db
        .view::<BlogPostsByCategory>()
        .with_key(Some(Category::Rust))
        .reduce().await?;
    // ANCHOR_END: reduce_one_key
    // ANCHOR: reduce_multiple_keys
    let total_post_count = db
        .view::<BlogPostsByCategory>()
        .reduce().await?;
    // ANCHOR_END: reduce_multiple_keys
    Ok(())
}