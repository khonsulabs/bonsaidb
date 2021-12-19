//! This example shows a creative use case for map/reduce views: generating
//! histograms.
//!
//! This example uses the [`hdrhistogram`] crate to create a histogram of
//! "samples" stored in the [`Samples`] type. The raw sample data is stored in a
//! collection with a timestamp (u64) and a `Vec<u64>` of samples.
//!
//! The [`AsHistogram`] view maps the sample data into a [`StoredHistogram`],
//! which knows how to serialize and deserialize a [`SyncHistogram`]. The view
//! also reduces multiple instaces of `StoredHistogram` into a single
//! `StoredHistogram`.
//!
//! This enables the ability to use the `reduce()` API to retrieve
//! server-reduced values in an efficient manner.

use std::ops::Deref;

use bonsaidb::{
    core::{
        connection::{AccessPolicy, Connection},
        schema::{
            view, view::CollectionView, Collection, CollectionDocument, CollectionName,
            InvalidNameError, MappedValue, Name, Schematic,
        },
    },
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use hdrhistogram::{
    serialization::{Serializer, V2Serializer},
    Histogram, SyncHistogram,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::{de::Visitor, Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), bonsaidb::local::Error> {
    let db =
        Database::open::<Samples>(StorageConfiguration::new("view-histogram.bonsaidb")).await?;

    println!("inserting 100 new sets of samples");
    let mut rng = StdRng::from_entropy();
    for timestamp in 1..100 {
        // This inserts a new record, generating a random range that will trend
        // upwards as `timestamp` increases.
        db.collection::<Samples>()
            .push(&Samples {
                timestamp,
                entries: (0..100)
                    .map(|_| rng.gen_range(50 + timestamp / 2..115 + timestamp))
                    .collect(),
            })
            .await?;
    }
    println!("done inserting new samples");

    // We can ask for a histogram of all the data:
    let total_histogram = db
        .reduce::<AsHistogram>(None, AccessPolicy::UpdateBefore)
        .await?;
    println!(
        "99th Percentile overall: {} ({} samples)",
        total_histogram.value_at_quantile(0.99),
        total_histogram.len()
    );

    // Or we can request just a specific range:
    let range_histogram = db
        .view::<AsHistogram>()
        .with_key_range(10..20)
        .reduce()
        .await?;
    println!(
        "99th Percentile from 10..20: {} ({} samples)",
        range_histogram.value_at_quantile(0.99),
        range_histogram.len()
    );
    let range_histogram = db
        .view::<AsHistogram>()
        .with_key_range(80..100)
        .reduce()
        .await?;
    println!(
        "99th Percentile from 80..100: {} ({} samples)",
        range_histogram.value_at_quantile(0.99),
        range_histogram.len()
    );

    Ok(())
}

/// A set of samples that were taken at a specific time.
#[derive(Debug, Serialize, Deserialize)]
pub struct Samples {
    /// The timestamp of the samples.
    pub timestamp: u64,
    /// The raw samples.
    pub entries: Vec<u64>,
}

impl Collection for Samples {
    fn collection_name() -> Result<CollectionName, InvalidNameError> {
        CollectionName::new("histogram-example", "samples")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), bonsaidb::core::Error> {
        schema.define_view(AsHistogram)
    }
}

/// A view for [`Samples`] which produces a histogram.
#[derive(Debug)]
pub struct AsHistogram;

impl CollectionView for AsHistogram {
    type Collection = Samples;

    type Key = u64;

    type Value = StoredHistogram;

    fn version(&self) -> u64 {
        0
    }

    fn name(&self) -> Result<Name, InvalidNameError> {
        Name::new("as-histogram")
    }

    fn map(
        &self,
        document: CollectionDocument<Self::Collection>,
    ) -> bonsaidb::core::schema::MapResult<Self::Key, Self::Value> {
        let mut histogram = Histogram::new(4).unwrap();
        for sample in &document.contents.entries {
            histogram.record(*sample).unwrap();
        }

        Ok(document.emit_key_and_value(
            document.contents.timestamp,
            StoredHistogram(histogram.into_sync()),
        ))
    }

    fn reduce(
        &self,
        mappings: &[MappedValue<Self::Key, Self::Value>],
        _rereduce: bool,
    ) -> Result<Self::Value, view::Error> {
        let mut mappings = mappings.iter();
        let mut combined = SyncHistogram::from(
            mappings
                .next()
                .map(|h| h.value.0.deref().clone())
                .unwrap_or_else(|| Histogram::new(4).unwrap()),
        );
        for map in mappings {
            combined.add(map.value.0.deref()).unwrap();
        }
        Ok(StoredHistogram(combined))
    }
}

/// A serializable wrapper for [`SyncHistogram`].
pub struct StoredHistogram(SyncHistogram<u64>);

impl Deref for StoredHistogram {
    type Target = SyncHistogram<u64>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl serde::Serialize for StoredHistogram {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut vec = Vec::new();
        V2Serializer::new()
            .serialize(&self.0, &mut vec)
            .map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&vec)
    }
}

impl<'de> serde::Deserialize<'de> for StoredHistogram {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(StoredHistogramVisitor)
    }
}

struct StoredHistogramVisitor;

impl<'de> Visitor<'de> for StoredHistogramVisitor {
    type Value = StoredHistogram;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("expected borrowed bytes")
    }

    fn visit_borrowed_bytes<E>(self, mut v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let histogram = hdrhistogram::serialization::Deserializer::new()
            .deserialize(&mut v)
            .map_err(serde::de::Error::custom)?;
        Ok(StoredHistogram(SyncHistogram::from(histogram)))
    }
}
