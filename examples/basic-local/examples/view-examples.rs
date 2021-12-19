use bonsaidb::{
    core::{
        connection::Connection,
        schema::{
            view, view::CollectionView, Collection, CollectionDocument, CollectionName,
            InvalidNameError, MapResult, MappedValue, Name, Schematic,
        },
        Error,
    },
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use serde::{Deserialize, Serialize};

// begin rustme snippet: snippet-a
#[derive(Debug, Serialize, Deserialize)]
struct Shape {
    pub sides: u32,
}

impl Collection for Shape {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("khonsulabs", "shapes")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), Error> {
        schema.define_view(ShapesByNumberOfSides)
    }
}

#[derive(Debug)]
struct ShapesByNumberOfSides;

impl CollectionView for ShapesByNumberOfSides {
    type Collection = Shape;

    type Key = u32;

    type Value = usize;

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-number-of-sides")
    }

    fn map(&self, document: CollectionDocument<Shape>) -> MapResult<Self::Key, Self::Value> {
        Ok(document.emit_key_and_value(document.contents.sides, 1))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
// end rustme snippet

impl Shape {
    fn new(sides: u32) -> Self {
        Self { sides }
    }
}

#[tokio::main]
async fn main() -> Result<(), bonsaidb::core::Error> {
    // begin rustme snippet: snippet-b
    let db = Database::open::<Shape>(StorageConfiguration::new("view-examples.bonsaidb")).await?;

    // Insert a new document into the Shape collection.
    Shape::new(3).insert_into(&db).await?;
    // end rustme snippet

    // Views in `BonsaiDb` are written using a Map/Reduce approach. In this
    // example, we take a look at how document mapping can be used to filter and
    // retrieve data
    //
    // Let's start by seeding the database with some shapes of various sizes:
    for sides in 3..=20 {
        Shape::new(sides).insert_into(&db).await?;
    }

    // And, let's add a few shapes with the same number of sides
    Shape::new(3).insert_into(&db).await?;
    Shape::new(4).insert_into(&db).await?;

    // At this point, our database should have 3 triangles:
    // begin rustme snippet: snippet-c
    let triangles = db
        .view::<ShapesByNumberOfSides>()
        .with_key(3)
        .query()
        .await?;
    println!("Number of triangles: {}", triangles.len());
    // end rustme snippet

    // What is returned is a list of entries containing the document id
    // (source), the key of the entry, and the value of the entry:
    println!("Triangles: {:#?}", triangles);

    // If you want the associated documents, use query_with_docs:
    for entry in db
        .view::<ShapesByNumberOfSides>()
        .with_key(3)
        .query_with_docs()
        .await?
    {
        let shape = entry.document.contents::<Shape>()?;
        println!(
            "Shape ID {} has {} sides",
            entry.document.header.id, shape.sides
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
        db.view::<ShapesByNumberOfSides>()
            .with_key(4)
            .reduce()
            .await?
    );

    // Or, 5 shapes that are triangles or quads
    println!(
        "Number of quads and triangles: {} (expected 5)",
        db.view::<ShapesByNumberOfSides>()
            .with_keys([3, 4])
            .reduce()
            .await?
    );

    // And, 10 shapes that have more than 10 sides
    println!(
        "Number of shapes with more than 10 sides: {} (expected 10)",
        db.view::<ShapesByNumberOfSides>()
            .with_key_range(11..u32::MAX)
            .reduce()
            .await?
    );

    Ok(())
}
