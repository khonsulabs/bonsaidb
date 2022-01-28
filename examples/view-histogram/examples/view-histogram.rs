//! This example shows a creative use case for map/reduce views: generating
//! histograms.
//!
//! This example uses the [`hdrhistogram`] crate to create a histogram of
//! "samples" stored in the [`Samples`] type. The raw sample data is stored in a
//! collection with a timestamp (u64) and a `Vec<u64>` of samples.
//!
//! The [`AsHistogram`] view maps the sample data into a [`SyncHistogram`], and
//! this code provides an example on how to ipmlement a custom serializer using
//! the [`transmog::Format`] trait. This allows using `SyncHistogram`'s native
//! serialization to store the histogram into the view.
//!
//! All of this combined enables the ability to use the `reduce()` API to
//! retrieve server-reduced values in an efficient manner.

use std::ops::Deref;

use bonsaidb::{
    core::{
        connection::Connection,
        schema::{
            view::CollectionViewSchema, Collection, CollectionDocument, CollectionName,
            DefaultSerialization, Name, ReduceResult, Schematic, SerializedView, View,
            ViewMappedValue,
        },
        transmog::{Format, OwnedDeserializer},
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
use serde::{Deserialize, Serialize};

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
    let total_histogram = db.view::<AsHistogram>().reduce().await?;
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
    fn collection_name() -> CollectionName {
        CollectionName::new("histogram-example", "samples")
    }

    fn define_views(schema: &mut Schematic) -> Result<(), bonsaidb::core::Error> {
        schema.define_view(AsHistogram)
    }
}

impl DefaultSerialization for Samples {}

/// A view for [`Samples`] which produces a histogram.
#[derive(Debug, Clone)]
pub struct AsHistogram;

impl View for AsHistogram {
    type Collection = Samples;
    type Key = u64;
    type Value = SyncHistogram<u64>;

    fn name(&self) -> Name {
        Name::new("as-histogram")
    }
}

impl CollectionViewSchema for AsHistogram {
    type View = Self;

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> bonsaidb::core::schema::ViewMapResult<Self::View> {
        let mut histogram = Histogram::new(4).unwrap();
        for sample in &document.contents.entries {
            histogram.record(*sample).unwrap();
        }

        Ok(document.emit_key_and_value(document.contents.timestamp, histogram.into_sync()))
    }

    fn reduce(
        &self,
        mappings: &[ViewMappedValue<Self::View>],
        _rereduce: bool,
    ) -> ReduceResult<Self::View> {
        let mut mappings = mappings.iter();
        let mut combined = SyncHistogram::from(
            mappings
                .next()
                .map(|h| h.value.deref().clone())
                .unwrap_or_else(|| Histogram::new(4).unwrap()),
        );
        for map in mappings {
            combined.add(map.value.deref()).unwrap();
        }
        Ok(combined)
    }
}

impl SerializedView for AsHistogram {
    type Format = Self;

    fn format() -> Self::Format {
        Self
    }
}

impl Format<'static, SyncHistogram<u64>> for AsHistogram {
    type Error = HistogramError;

    fn serialize_into<W: std::io::Write>(
        &self,
        value: &SyncHistogram<u64>,
        mut writer: W,
    ) -> Result<(), Self::Error> {
        V2Serializer::new()
            .serialize(value, &mut writer)
            .map_err(HistogramError::Serialization)?;
        Ok(())
    }
}

impl OwnedDeserializer<SyncHistogram<u64>> for AsHistogram {
    fn deserialize_from<R: std::io::Read>(
        &self,
        mut reader: R,
    ) -> Result<SyncHistogram<u64>, Self::Error> {
        hdrhistogram::serialization::Deserializer::new()
            .deserialize(&mut reader)
            .map(SyncHistogram::from)
            .map_err(HistogramError::Deserialization)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum HistogramError {
    #[error("serialization error: {0}")]
    Serialization(#[from] hdrhistogram::serialization::V2SerializeError),
    #[error("deserialization error: {0}")]
    Deserialization(#[from] hdrhistogram::serialization::DeserializeError),
}

impl From<std::io::Error> for HistogramError {
    fn from(err: std::io::Error) -> Self {
        Self::Deserialization(hdrhistogram::serialization::DeserializeError::from(err))
    }
}

#[test]
fn runs() {
    main().unwrap()
}
