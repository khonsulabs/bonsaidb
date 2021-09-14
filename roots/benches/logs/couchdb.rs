// This file isn't included because after running it, it became evident that
// CouchDb's performance isn't remotely on par with sqlite or roots. Couchbase's
// C version might be if used directly, but it's not worth bringing in. The
// order of magnitude of early benchmarks showed:
//
// 1000 transactions, 10 entries per transaction
// +---------+-----------+-----------+----------+----------+------------+----------------+
// | target  | total (s) |      tx/s | min (ms) | max (ms) | stdev (ms) | 99th pctl (ms) |
// +---------+-----------+-----------+----------+----------+------------+----------------+
// |  roots  |     0.083 | 12080.220 |    0.068 |    0.182 |      0.010 |          0.112 |
// +---------+-----------+-----------+----------+----------+------------+----------------+
// | couchdb |     9.433 |   106.006 |    7.090 |   33.361 |      1.884 |         14.133 |
// +---------+-----------+-----------+----------+----------+------------+----------------+
// | sqlite  |     0.062 | 16011.448 |    0.051 |    0.313 |      0.016 |          0.125 |
// +---------+-----------+-----------+----------+----------+------------+----------------+
//
// 100 transactions, 100 entries per transaction
// +---------+-----------+----------+----------+----------+------------+----------------+
// | target  | total (s) |     tx/s | min (ms) | max (ms) | stdev (ms) | 99th pctl (ms) |
// +---------+-----------+----------+----------+----------+------------+----------------+
// |  roots  |     0.055 | 1828.976 |    0.481 |    0.800 |      0.052 |          0.776 |
// +---------+-----------+----------+----------+----------+------------+----------------+
// | couchdb |     6.646 |   15.047 |   58.028 |  120.095 |      6.538 |         86.219 |
// +---------+-----------+----------+----------+----------+------------+----------------+
// | sqlite  |     0.021 | 4662.210 |    0.186 |    0.263 |      0.016 |          0.249 |
// +---------+-----------+----------+----------+----------+------------+----------------+

use super::{LogConfig, LogEntry};
use crate::AsyncBench;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub struct CouchDbLogs {
    logs: Vec<Vec<LogEntry>>,
}

#[async_trait(?Send)]
impl AsyncBench for CouchDbLogs {
    type Config = LogConfig;

    async fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error> {
        // TODO have function to delete the database.
        Ok(Self {
            logs: LogEntry::generate(config),
        })
    }

    async fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        let mut handles = Vec::new();
        for log in self.logs.pop().unwrap() {
            handles.push(tokio::spawn(async {
                let client = reqwest::Client::new();
                client
                    .post("http://localhost:5984/roots-log-benchmark?batch=ok")
                    .json(&Document { doc: log })
                    .send()
                    .await
                    .unwrap();
            }));
        }
        futures::future::join_all(handles).await;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Document<T> {
    // id: String,
    doc: T,
}
