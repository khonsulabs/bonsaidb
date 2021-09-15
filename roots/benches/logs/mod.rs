use bonsaidb_roots::StdFile;
use nanorand::{Pcg64, Rng};
use serde::{Deserialize, Serialize};

use crate::{AsyncBench, BenchConfig, SuiteReport};

#[derive(Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    pub timestamp: u64,
    pub message: String,
}

const ALPHABET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

impl LogEntry {
    pub fn generate(config: &LogConfig) -> Vec<Vec<LogEntry>> {
        let mut rng = Pcg64::new_seed(1);
        let mut transactions = Vec::new();
        for tx in 0..config.transactions {
            let mut batch = Vec::new();
            for id in 0..config.entries_per_transaction {
                batch.push(LogEntry {
                    id: (tx * config.entries_per_transaction + id) as u64,
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
            transactions.push(batch);
        }
        transactions
    }
}

// mod couchdb;
mod roots;
mod sqlite;

pub struct LogConfig {
    pub sequential_ids: bool,
    pub entries_per_transaction: usize,
    pub transactions: usize,
}

impl BenchConfig for LogConfig {
    fn iterations(&self) -> usize {
        self.transactions
    }
}

pub fn run() {
    for (transactions, entries_per_transaction) in
        [(10_000, 1), (1_000, 100), (100, 1_000), (10, 10_000)]
    {
        println!(
            "{} transactions, {} entries per transaction",
            transactions, entries_per_transaction
        );
        let mut suite = SuiteReport::default();
        let config = LogConfig {
            sequential_ids: true,
            transactions,
            entries_per_transaction,
        };
        suite
            .reports
            .push(roots::RootsLogs::<StdFile>::run("roots", &config).unwrap());
        suite
            .reports
            .push(sqlite::SqliteLogs::run("sqlite", &config).unwrap());
        println!("{}", suite);
    }
}
