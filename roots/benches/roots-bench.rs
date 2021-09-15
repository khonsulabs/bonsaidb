use std::{
    convert::TryFrom,
    fmt::Display,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

mod logs;

fn main() {
    println!("Executing logs benchmark");
    logs::run();
}

use tabled::{Alignment, Column, Modify, Table, Tabled};

pub trait AsyncBench: Sized {
    type Config: BenchConfig;

    fn can_execute() -> bool {
        true
    }

    fn run(target: impl Into<String>, config: &Self::Config) -> Result<BenchReport, anyhow::Error> {
        Self::initialize(config)?.execute_iterations(target, config)
    }

    fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error>;

    fn execute_measured(&mut self, config: &Self::Config) -> Result<(), anyhow::Error>;

    fn execute_iterations(
        &mut self,
        target: impl Into<String>,
        config: &Self::Config,
    ) -> Result<BenchReport, anyhow::Error> {
        let start = Instant::now();
        let mut histogram = Histogram::<u64>::new(4)?;

        for _ in 0..config.iterations() {
            let iter_start = Instant::now();
            self.execute_measured(config)?;
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
            String::from("tx/s"),
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
