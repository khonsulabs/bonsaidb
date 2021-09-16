use http_auth_basic::Credentials;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{InsertConfig, ReadConfig};
use crate::{BenchConfig, SimpleBench};

const USERNAME: &str = "COUCHDB_USER";
const PASSWORD: &str = "COUCHDB_PASSWORD";

pub struct InsertLogs {}

impl SimpleBench for InsertLogs {
    type Config = InsertConfig;

    fn name(config: &Self::Config) -> String {
        format!(
            "logs-insert-{}x{}-couchdb",
            config.transactions, config.entries_per_transaction
        )
    }

    fn can_execute() -> bool {
        if std::env::var(USERNAME).is_err() {
            println!("Skipping couchdb benchmark. To run, provide environment variables COUCHDB_USER and COUCHDB_PASSWORD.");
            false
        } else {
            true
        }
    }

    fn initialize(_config: &Self::Config) -> Result<Self, anyhow::Error> {
        let username = std::env::var(USERNAME).expect("missing username");
        let password = std::env::var(PASSWORD)
            .map_err(|_| anyhow::anyhow!("missing {} environment variable", PASSWORD))?;
        let authorization_header = Credentials::new(&username, &password).as_http_header();

        // Delete the database
        ureq::delete("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()?;

        // Create the database
        ureq::put("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()?;

        // Set the security model to none, allowing the benchmark to execute without security.
        ureq::put("http://localhost:5984/roots-log-benchmark/_security")
            .set("Authorization", &authorization_header)
            .send_json(Value::Object(Map::default()))?;

        Ok(Self {})
    }

    fn execute_measured(
        &mut self,
        batch: &<Self::Config as BenchConfig>::Batch,
        _config: &Self::Config,
    ) -> Result<(), anyhow::Error> {
        ureq::post("http://localhost:5984/roots-log-benchmark/_bulk_docs").send_json(
            serde_json::to_value(&Documents {
                docs: batch.to_vec(),
            })?,
        )?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Documents<T> {
    docs: Vec<T>,
}

#[derive(Serialize, Deserialize)]
struct LogEntryDoc {
    #[serde(rename = "_id")]
    id: String,
    timestamp: u64,
    message: String,
}

pub struct ReadLogs {}

impl SimpleBench for ReadLogs {
    type Config = ReadConfig;

    fn name(config: &Self::Config) -> String {
        format!("logs-read-{}-couchdb", config.database_size)
    }

    fn can_execute() -> bool {
        if std::env::var(USERNAME).is_err() {
            println!("Skipping couchdb benchmark. To run, provide environment variables COUCHDB_USER and COUCHDB_PASSWORD.");
            false
        } else {
            true
        }
    }

    fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error> {
        let username = std::env::var(USERNAME).expect("missing username");
        let password = std::env::var(PASSWORD)
            .map_err(|_| anyhow::anyhow!("missing {} environment variable", PASSWORD))?;
        let authorization_header = Credentials::new(&username, &password).as_http_header();

        // Delete the database
        ureq::delete("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()?;

        // Create the database
        ureq::put("http://localhost:5984/roots-log-benchmark")
            .set("Authorization", &authorization_header)
            .call()?;

        // Set the security model to none, allowing the benchmark to execute without security.
        ureq::put("http://localhost:5984/roots-log-benchmark/_security")
            .set("Authorization", &authorization_header)
            .send_json(Value::Object(Map::default()))?;

        for chunk in config.database.chunks(10_000) {
            ureq::post("http://localhost:5984/roots-log-benchmark/_bulk_docs").send_json(
                serde_json::to_value(&Documents {
                    docs: chunk
                        .iter()
                        .map(|entry| LogEntryDoc {
                            id: entry.id.to_string(),
                            timestamp: entry.timestamp,
                            message: entry.message.clone(),
                        })
                        .collect(),
                })?,
            )?;
        }

        Ok(Self {})
    }

    fn execute_measured(
        &mut self,
        batch: &<Self::Config as BenchConfig>::Batch,
        _config: &Self::Config,
    ) -> Result<(), anyhow::Error> {
        for entry in batch {
            let result = ureq::get(&format!(
                "http://localhost:5984/roots-log-benchmark/{}",
                entry.id
            ))
            .call()?
            .into_json::<LogEntryDoc>()?;
            assert_eq!(&result.timestamp, &entry.timestamp);
            assert_eq!(&result.message, &entry.message);
        }
        Ok(())
    }
}
