use std::{
    convert::TryFrom,
    fmt::Display,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

mod logs;

fn main() {
    println!("Executing logs benchmark");
    logs::inserts();
    logs::single_reads();
}

use tabled::{Alignment, Column, Modify, Table, Tabled};

pub trait SimpleBench: Sized {
    type Config: BenchConfig;

    fn name(config: &Self::Config) -> String;

    fn can_execute() -> bool {
        true
    }

    fn run(
        target: impl Into<String>,
        batches: &[<Self::Config as BenchConfig>::Batch],
        config: &Self::Config,
    ) -> Result<BenchReport, anyhow::Error> {
        let mut bench = Self::initialize(config)?;

        // When tracing is enabled, we output flamegraphs of the benchmarks.
        #[cfg(feature = "tracing")]
        {
            use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
            let fmt_layer = tracing_subscriber::fmt::Layer::default();

            let (flame_layer, _guard) =
                tracing_flame::FlameLayer::with_file(format!("{}.folded", Self::name(config)))
                    .unwrap();
            let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
                .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
                .unwrap();

            let subscriber = tracing_subscriber::Registry::default()
                .with(flame_layer)
                .with(filter_layer)
                .with(fmt_layer);

            tracing::subscriber::with_default(subscriber, || {
                bench.execute_iterations(batches, target, config)
            })
        }

        #[cfg(not(feature = "tracing"))]
        {
            bench.execute_iterations(batches, target, config)
        }
    }

    fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error>;

    fn execute_measured(
        &mut self,
        batch: &<Self::Config as BenchConfig>::Batch,
        config: &Self::Config,
    ) -> Result<(), anyhow::Error>;

    fn execute_iterations(
        &mut self,
        batches: &[<Self::Config as BenchConfig>::Batch],
        target: impl Into<String>,
        config: &Self::Config,
    ) -> Result<BenchReport, anyhow::Error> {
        let start = Instant::now();
        let mut histogram = Histogram::<u64>::new(4)?;

        for batch in batches {
            let iter_start = Instant::now();
            self.execute_measured(batch, config)?;
            let iter_end = Instant::now();
            if let Some(elapsed) = iter_end.checked_duration_since(iter_start) {
                histogram.record(u64::try_from(elapsed.as_micros())?)?;
            }
        }

        let total_duration = Instant::now()
            .checked_duration_since(start)
            .ok_or_else(|| anyhow::anyhow!("time went backwards. please restart benchmarks"))?;

        Ok(BenchReport {
            target: target.into(),
            transactions: config.iterations(),
            total_duration,
            histogram,
        })
    }
}

pub trait BenchConfig {
    type Batch;

    fn generate(&self) -> Vec<Self::Batch>;
    fn iterations(&self) -> usize;
}

pub struct BenchReport {
    pub target: String,
    pub transactions: usize,
    pub total_duration: Duration,
    pub histogram: Histogram<u64>,
}

impl Tabled for BenchReport {
    fn fields(&self) -> Vec<String> {
        vec![
            self.target.clone(),
            format!("{:0.03}", self.total_duration.as_secs_f64()),
            format!(
                "{:0.03}",
                self.transactions as f64 / self.total_duration.as_secs_f64()
            ),
            format!("{:0.03}", self.histogram.min() as f64 / 1_000.0),
            format!("{:0.03}", self.histogram.max() as f64 / 1_000.0),
            format!("{:0.03}", self.histogram.stdev() / 1_000.0),
            format!(
                "{:0.03}",
                self.histogram.value_at_quantile(0.99) as f64 / 1_000.0
            ),
        ]
    }

    fn headers() -> Vec<String> {
        vec![
            String::from("target"),
            String::from("total (s)"),
            String::from("batch/s"),
            String::from("min (ms)"),
            String::from("max (ms)"),
            String::from("stdev (ms)"),
            String::from("99th pctl (ms)"),
        ]
    }
}

#[derive(Default)]
pub struct SuiteReport {
    pub reports: Vec<BenchReport>,
}

impl Display for SuiteReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Table::new(&self.reports)
            .with(Modify::new(Column(1..)).with(Alignment::right()))
            .fmt(f)
    }
}
