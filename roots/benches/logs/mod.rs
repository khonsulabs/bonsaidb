use bonsaidb_roots::StdFile;
use nanorand::{Pcg64, Rng};
use serde::{Deserialize, Serialize};

use crate::{BenchConfig, SimpleBench, SuiteReport};

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub message: String,
}

const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

impl LogEntry {
    pub fn generate_batch(id_base: usize, count: usize, rng: &mut Pcg64) -> Vec<LogEntry> {
        let mut batch = Vec::new();
        for id in 0..count {
            batch.push(LogEntry {
                id: (id_base + id) as u64,
                timestamp: rng.generate(),
                message: (0..rng.generate_range(50..1024))
                    .map(|_| {
                        let idx = rng.generate_range(0..ALPHABET.len());
                        &ALPHABET[idx..idx + 1]
                    })
                    .collect(),
            })
        }
        // roots requires that keys be sorted on insert.
        batch.sort_by(|a, b| a.id.cmp(&b.id));
        batch
    }

    pub fn generate(count: usize) -> Vec<LogEntry> {
        let mut rng = Pcg64::new_seed(1);
        Self::generate_batch(0, count, &mut rng)
    }

    pub fn generate_batches(config: &InsertConfig) -> Vec<Vec<LogEntry>> {
        let mut rng = Pcg64::new_seed(1);
        let mut transactions = Vec::new();
        for tx in 0..config.transactions {
            transactions.push(Self::generate_batch(
                tx * config.entries_per_transaction,
                config.entries_per_transaction,
                &mut rng,
            ));
        }
        transactions
    }
}

mod couchdb;
mod roots;
mod sqlite;

pub struct InsertConfig {
    pub sequential_ids: bool,
    pub entries_per_transaction: usize,
    pub transactions: usize,
}

impl BenchConfig for InsertConfig {
    fn iterations(&self) -> usize {
        self.transactions
    }

    type Batch = Vec<LogEntry>;

    fn generate(&self) -> Vec<Self::Batch> {
        LogEntry::generate_batches(self)
    }
}

pub fn inserts() {
    for (transactions, entries_per_transaction) in
        [(1_000, 1), (1_000, 100), (100, 1_000), (10, 10_000)]
    {
        println!(
            "{} transactions, {} entries per transaction",
            transactions, entries_per_transaction
        );
        let mut suite = SuiteReport::default();
        let config = InsertConfig {
            sequential_ids: true,
            transactions,
            entries_per_transaction,
        };
        let batches = config.generate();

        suite
            .reports
            .push(roots::InsertLogs::<StdFile>::run("roots", &batches, &config).unwrap());
        suite
            .reports
            .push(sqlite::InsertLogs::run("sqlite", &batches, &config).unwrap());
        if couchdb::InsertLogs::can_execute() {
            suite
                .reports
                .push(couchdb::InsertLogs::run("couchdb", &batches, &config).unwrap());
        }
        println!("{}", suite);
    }
}

pub struct ReadConfig {
    pub database: Vec<LogEntry>,
    database_size: usize,
}

pub fn single_reads() {
    for database_size in [
        1_000, 100_000, 1_000_000, 10_000_000, 20_000_000, 30_000_000, 40_000_000,
    ] {
        println!(
            "searching for 100 ids in a data set of {} entries",
            database_size
        );
        let mut suite = SuiteReport::default();
        let config = ReadConfig::new(database_size);
        let batches = config.generate();

        suite
            .reports
            .push(roots::ReadLogs::<StdFile>::run("roots", &batches, &config).unwrap());
        suite
            .reports
            .push(sqlite::ReadLogs::run("sqlite", &batches, &config).unwrap());
        if couchdb::InsertLogs::can_execute() {
            suite
                .reports
                .push(couchdb::ReadLogs::run("couchdb", &batches, &config).unwrap());
        }
        println!("{}", suite);
    }
}

impl ReadConfig {
    fn new(database_size: usize) -> Self {
        Self {
            database: LogEntry::generate(database_size),
            database_size,
        }
    }
}

const READ_ITERATIONS: usize = 1_000;

impl BenchConfig for ReadConfig {
    type Batch = Vec<LogEntry>;

    fn generate(&self) -> Vec<Self::Batch> {
        let mut rng = Pcg64::new_seed(1);
        let mut batches = Vec::new();
        for _ in 0..READ_ITERATIONS {
            let mut batch = Vec::new();
            for _ in 0..100 {
                batch.push(self.database[rng.generate_range(0..self.database_size)].clone());
            }
            batches.push(batch);
        }
        batches
    }

    fn iterations(&self) -> usize {
        READ_ITERATIONS
    }
}
