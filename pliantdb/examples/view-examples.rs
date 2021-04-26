use pliantdb::{
    core::{
        connection::Connection,
        document::Document,
        schema::{
            view, Collection, CollectionName, InvalidNameError, MapResult, MappedValue, Name,
            Schematic, View,
        },
        Error,
    },
    local::{config::Configuration, Database},
};
use serde::{Deserialize, Serialize};

// [md-bakery: begin @ snippet-a]
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

impl View for ShapesByNumberOfSides {
    type Collection = Shape;

    type Key = u32;

    type Value = usize;

    fn version(&self) -> u64 {
        1
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("by-number-of-sides")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::Key, Self::Value> {
        let shape = document.contents::<Shape>()?;
        Ok(Some(document.emit_key_and_value(shape.sides, 1)))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        Ok(mappings.iter().map(|m| m.value).sum())
    }
}
// [md-bakery: end]

impl Shape {
    fn new(sides: u32) -> Self {
        Self { sides }
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // [md-bakery: begin @ snippet-b]
    let db =
        Database::<Shape>::open_local("view-examples.pliantdb", &Configuration::default()).await?;

    // Insert a new document into the Shape collection.
    db.collection::<Shape>().push(&Shape::new(3)).await?;
    // [md-bakery: end]

    // Views in `PliantDB` are written using a Map/Reduce approach. In this
    // example, we take a look at how document mapping can be used to filter and
    // retrieve data
    //
    // Let's start by seeding the database with some shapes of various sizes:
    for sides in 3..=20 {
        db.collection::<Shape>().push(&Shape::new(sides)).await?;
    }

    // And, let's add a few shapes with the same number of sides
    db.collection::<Shape>().push(&Shape::new(3)).await?;
    db.collection::<Shape>().push(&Shape::new(4)).await?;

    // At this point, our database should have 3 triangles:
    // [md-bakery: begin @ snippet-c]
    let triangles = db
        .view::<ShapesByNumberOfSides>()
        .with_key(3)
        .query()
        .await?;
    println!("Number of triangles: {}", triangles.len());
    // [md-bakery: end]

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
            .with_keys(vec![3, 4])
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
