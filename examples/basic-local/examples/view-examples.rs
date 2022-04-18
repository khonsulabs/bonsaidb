use bonsaidb::{
    core::{
        connection::Connection,
        document::{CollectionDocument, Emit},
        schema::{
            view::CollectionViewSchema, Collection, ReduceResult, SerializedCollection, View,
            ViewMapResult, ViewMappedValue,
        },
    },
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use serde::{Deserialize, Serialize};

// begin rustme snippet: snippet-a
#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "shapes", views = [ShapesByNumberOfSides])]
struct Shape {
    pub sides: u32,
}

#[derive(Debug, Clone, View)]
#[view(collection = Shape, key = u32, value = usize, name = "by-number-of-sides")]
struct ShapesByNumberOfSides;

impl CollectionViewSchema for ShapesByNumberOfSides {
    type View = Self;

    fn map(&self, document: CollectionDocument<Shape>) -> ViewMapResult<Self::View> {
        document
            .header
            .emit_key_and_value(document.contents.sides, 1)
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
// end rustme snippet

fn main() -> Result<(), bonsaidb::core::Error> {
    // begin rustme snippet: snippet-b
    let db = Database::open::<Shape>(StorageConfiguration::new("view-examples.bonsaidb"))?;

    // Insert a new document into the Shape collection.
    Shape { sides: 3 }.push_into(&db)?;
    // end rustme snippet

    // Views in BonsaiDb are written using a Map/Reduce approach. In this
    // example, we take a look at how document mapping can be used to filter and
    // retrieve data
    //
    // Let's start by seeding the database with some shapes of various sizes:
    for sides in 3..=20 {
        Shape { sides }.push_into(&db)?;
    }

    // And, let's add a few shapes with the same number of sides
    Shape { sides: 3 }.push_into(&db)?;
    Shape { sides: 4 }.push_into(&db)?;

    // At this point, our database should have 3 triangles:
    // begin rustme snippet: snippet-c
    let triangles = db.view::<ShapesByNumberOfSides>().with_key(&3).query()?;
    println!("Number of triangles: {}", triangles.len());
    // end rustme snippet

    // What is returned is a list of entries containing the document id
    // (source), the key of the entry, and the value of the entry:
    println!("Triangles: {:#?}", triangles);

    // If you want the associated documents, use query_with_collection_docs:
    for entry in &db
        .view::<ShapesByNumberOfSides>()
        .with_key(&3)
        .query_with_collection_docs()?
    {
        println!(
            "Shape ID {} has {} sides",
            entry.document.header.id, entry.document.contents.sides
        );
    }

    // The reduce() function takes the "values" emitted during the map()
    // function, and reduces a list down to a single value. In this example, the
    // reduce function is acting as a count. So, if you want to query for the
    // number of shapes, we don't need to fetch all the records, we can just
    // retrieve the result of the calculation directly.
    //
    // So, here we're using reduce() to count the number of shapes with 4 sides.
    println!(
        "Number of quads: {} (expected 2)",
        db.view::<ShapesByNumberOfSides>().with_key(&4).reduce()?
    );

    // Or, 5 shapes that are triangles or quads
    println!(
        "Number of quads and triangles: {} (expected 5)",
        db.view::<ShapesByNumberOfSides>()
            .with_keys(&[3, 4])
            .reduce()?
    );

    // And, 10 shapes that have more than 10 sides
    println!(
        "Number of shapes with more than 10 sides: {} (expected 10)",
        db.view::<ShapesByNumberOfSides>()
            .with_key_range(11..)
            .reduce()?
    );

    Ok(())
}

#[test]
fn runs() {
    drop(std::fs::remove_dir_all("view-examples.bonsaidb"));
    main().unwrap()
}
