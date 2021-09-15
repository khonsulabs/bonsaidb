use http_auth_basic::Credentials;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::{LogConfig, LogEntry};
use crate::AsyncBench;

const USERNAME: &str = "COUCHDB_USER";
const PASSWORD: &str = "COUCHDB_PASSWORD";

pub struct CouchDbLogs {
    logs: Vec<Vec<LogEntry>>,
}

impl AsyncBench for CouchDbLogs {
    type Config = LogConfig;

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

        Ok(Self {
            logs: LogEntry::generate(config),
        })
    }

    fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        ureq::post("http://localhost:5984/roots-log-benchmark/_bulk_docs").send_json(
            serde_json::to_value(&Documents {
                docs: self.logs.pop().unwrap(),
            })?,
        )?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Documents<T> {
    docs: Vec<T>,
}
