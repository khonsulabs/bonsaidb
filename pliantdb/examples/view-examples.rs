use std::{borrow::Cow, time::SystemTime};

use pliantdb::local::Storage;
use pliantdb_core::{
    connection::Connection,
    document::Document,
    schema::{collection, Collection, MapResult, Schema, View},
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
    fn id() -> collection::Id {
        collection::Id::from("shapes")
    }

    fn define_views(schema: &mut Schema) {
        schema.define_view(ShapesByNumberOfSides);
    }
}

#[derive(Debug)]
struct ShapesByNumberOfSides;

impl View for ShapesByNumberOfSides {
    type Collection = Shape;

    type MapKey = u32;

    type MapValue = ();

    type Reduce = ();

    fn version(&self) -> usize {
        1
    }

    fn name(&self) -> Cow<'static, str> {
        Cow::from("by-color")
    }

    fn map(&self, document: &Document<'_>) -> MapResult<Self::MapKey> {
        let shape = document.contents::<Shape>()?;
        Ok(Some(document.emit_key(shape.sides as u32)))
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let db =
        Storage::<Shape>::open_local("view-examples.pliantdb", &Configuration::default()).await?;
    let shapes = db.collection::<Shape>()?;

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
    println!(
        "Number of triangles: {} (expected 3)",
        db.view::<ShapesByNumberOfSides>()
            .with_key(3)
            .query()
            .await?
            .len()
    );

    // And, we should have 2 quads:
    println!(
        "Number of quads: {} (expected 2)",
        db.view::<ShapesByNumberOfSides>()
            .with_key(4)
            .query()
            .await?
            .len()
    );

    // Or, 5 shapes that are triangles or quads
    println!(
        "Number of quads and triangles: {} (expected 5)",
        db.view::<ShapesByNumberOfSides>()
            .with_keys(vec![3, 4])
            .query()
            .await?
            .len()
    );

    // And, 10 shapes that have more than 10 sides
    println!(
        "Number of shapes with more than 10 sides: {} (expected 10)",
        db.view::<ShapesByNumberOfSides>()
            .with_key_range(11..u32::MAX)
            .query()
            .await?
            .len()
    );

    Ok(())
}
