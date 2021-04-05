use std::borrow::Cow;

use pliantdb::local::Storage;
use pliantdb_core::{
    connection::Connection,
    document::Document,
    schema::{collection, map::MappedValue, view, Collection, MapResult, Schematic, View},
};
use pliantdb_local::Configuration;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Shape {
    pub sides: u32,
}

impl Shape {
    fn new(sides: u32) -> Self {
        Self { sides }
    }
}

impl Collection for Shape {
    fn collection_id() -> collection::Id {
        collection::Id::from("shapes")
    }

    fn define_views(schema: &mut Schematic) {
        schema.define_view(ShapesByNumberOfSides);
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

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-number-of-sides")
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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let db =
        Storage::<Shape>::open_local("view-examples.pliantdb", &Configuration::default()).await?;
    let shapes = db.collection::<Shape>();

    // Views in `PliantDB` are written using a Map/Reduce approach. In this
    // example, we take a look at how document mapping can be used to filter and
    // retrieve data
    //
    // Let's start by seeding the database with some shapes of various sizes:
    for sides in 3..=20 {
        shapes.push(&Shape::new(sides)).await?;
    }

    // And, let's add a few shapes with the same number of sides
    shapes.push(&Shape::new(3)).await?;
    shapes.push(&Shape::new(3)).await?;
    shapes.push(&Shape::new(4)).await?;

    // At this point, our database should have 3 triangles:
    let triangles = db
        .view::<ShapesByNumberOfSides>()
        .with_key(3)
        .query()
        .await?;
    println!("Number of triangles: {} (expected 3)", triangles.len());

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
