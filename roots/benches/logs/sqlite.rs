use async_trait::async_trait;
use rusqlite::{params, Connection};
use tempfile::NamedTempFile;

use crate::AsyncBench;

use super::{LogConfig, LogEntry};

pub struct SqliteLogs {
    sqlite: Connection,
    _tempfile: NamedTempFile,
    logs: Vec<Vec<LogEntry>>,
}

#[async_trait(?Send)]
impl AsyncBench for SqliteLogs {
    type Config = LogConfig;

    async fn initialize(config: &Self::Config) -> Result<Self, anyhow::Error> {
        // For fair testing, this needs to use ACID-compliant settings that a
        // user would use in production. While a WAL might be used in
        // production, it alters more than just insert performance. A more
        // complete benchmark which includes both inserts and queries would be
        // better to compare roots against sqlite's WAL performance.
        let tempfile = NamedTempFile::new()?;
        let sqlite = Connection::open(tempfile.path())?;
        // Sets the journal to what seems to be the most optimal, safe setting
        // for @ecton. See:
        // https://www.sqlite.org/pragma.html#pragma_journal_mode
        sqlite.pragma_update(None, "journal_mode", &"TRUNCATE")?;
        // Sets synchronous to NORMAL, which "should" be safe and provides
        // better performance. See:
        // https://www.sqlite.org/pragma.html#pragma_synchronous
        sqlite.pragma_update(None, "synchronous", &"NORMAL")?;
        sqlite.execute(
            "create table logs (id integer primary key, timestamp integer, message text)",
            [],
        )?;
        let logs = LogEntry::generate(config);
        Ok(Self {
            sqlite,
            _tempfile: tempfile,
            logs,
        })
    }

    async fn execute_measured(&mut self, _config: &Self::Config) -> Result<(), anyhow::Error> {
        self.sqlite.execute("begin transaction;", [])?;

        let mut prepared = self
            .sqlite
            .prepare("insert into logs (id, timestamp, message) values (?, ?, ?)")?;
        for log in self.logs.pop().unwrap() {
            // sqlite doesn't support u64, so we're going to cast to i64
            prepared.execute(params![log.id as i64, log.timestamp as i64, log.message])?;
        }

        self.sqlite.execute("commit transaction;", [])?;

        Ok(())
    }
}
